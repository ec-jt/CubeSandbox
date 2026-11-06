/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */

/*
 * Deterministic asynchronous harness for s3_recovery_blank_scan().
 *
 * There is no bdev and no hardware here. Every SPDK bdev, I/O-channel and DMA
 * symbol the scanner uses is redirected with `-Wl,--wrap` to a mock that:
 *
 *   - records every read range, allocation and cleanup call;
 *   - defers completions instead of calling back inline, so the test drives the
 *     scanner's state machine one event at a time;
 *   - can deliver SPDK_BDEV_EVENT_REMOVE / RESIZE while a read is outstanding.
 *
 * The important property this proves is that a device event cannot turn into a
 * success. The old scanner only logged the event, so a pending zero-filled read
 * that completed after a REMOVE still produced "blank". These tests complete
 * exactly that read with success and require a failure, one callback, no further
 * reads, and cleanup only after the outstanding I/O finished.
 *
 * Compile with -ffunction-sections -fdata-sections and link with
 * -Wl,--gc-sections so the unrelated recovery functions in s3_recovery.c (which
 * need the S3 client and the checkpoint loader) are discarded instead of
 * dragging in the full SPDK archive set.
 */

#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/thread.h"

#include "s3lvol/s3_recovery.h"

#define SCAN_CHUNK      (1024 * 1024)

static int g_failures;

#define CHECK(cond) do {                                                \
	if (!(cond)) {                                                  \
		fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, \
			#cond);                                         \
		g_failures++;                                           \
	}                                                               \
} while (0)

/* ------------------------------------------------------------------ mock */

struct mock_dev {
	const char     *name;
	uint64_t        num_blocks;
	uint32_t        block_size;
	bool            present;

	/* 0 = all zero, 1 = valid-looking super block, 2 = corrupt-looking. */
	int             pattern;
	uint64_t        nonzero_at;
};

struct mock_desc {
	struct mock_dev              *dev;
	spdk_bdev_event_cb_t          event_cb;
	void                         *event_ctx;
};

struct mock_io {
	void                         *buf;
	uint64_t                      offset;
	uint64_t                      nbytes;
	spdk_bdev_io_completion_cb    cb;
	void                         *arg;
};

struct mock_chan {
	int unused;
};

struct mock_read {
	uint64_t offset;
	uint64_t len;
};

struct mock_counters {
	int open;
	int close;
	int chan_get;
	int chan_put;
	int dma_alloc;
	int dma_free;
	int free_io;
	int reads;
};

static struct mock_dev      g_dev;
static struct mock_desc    *g_desc;
static struct mock_io      *g_pending;
static struct mock_io      *g_completing;
static struct mock_chan     g_chan;
static struct mock_counters g_counters;
static struct mock_read     g_reads[16];

static bool     g_chan_fail;
static bool     g_dma_fail;
static int      g_submit_fail_index;
static bool     g_write_open;
static size_t   g_alloc_size;
static bool     g_free_io_seen;
static bool     g_close_saw_pending;
static bool     g_put_saw_pending;
static bool     g_free_saw_pending;

static void
reset_mock(uint64_t num_blocks, uint32_t block_size)
{
	/* A prior case may have failed its expectations (the baseline build
	 * does). Drop anything it left behind so the next case starts clean. */
	if (g_pending) {
		free(g_pending);
		g_pending = NULL;
	}
	if (g_desc) {
		free(g_desc);
		g_desc = NULL;
	}

	memset(&g_dev, 0, sizeof(g_dev));
	g_dev.name = "mock0";
	g_dev.present = true;
	g_dev.num_blocks = num_blocks;
	g_dev.block_size = block_size;
	g_dev.nonzero_at = UINT64_MAX;

	memset(&g_counters, 0, sizeof(g_counters));
	memset(g_reads, 0, sizeof(g_reads));

	g_chan_fail = false;
	g_dma_fail = false;
	g_submit_fail_index = -1;
	g_write_open = false;
	g_alloc_size = 0;
	g_completing = NULL;
	g_free_io_seen = false;
	g_close_saw_pending = false;
	g_put_saw_pending = false;
	g_free_saw_pending = false;
}

static uint64_t
mock_total(void)
{
	return g_dev.num_blocks * g_dev.block_size;
}

/* Fill the buffer the way the mock device would answer the read. */
static void
mock_fill(struct mock_io *io)
{
	uint8_t *p = io->buf;
	static const uint8_t magic[8] = {
		'S', '3', 'L', 'V', 'S', 'B', 0x01, 0x00
	};

	memset(p, 0, (size_t)io->nbytes);

	if (g_dev.pattern != 0 && io->offset == 0) {
		memcpy(p, magic, sizeof(magic));
		if (g_dev.pattern == 2) {
			/* Wrong version/CRC byte: a parser would reject this,
			 * but the scanner must reject it as non-zero, not
			 * mistake the parse error for blankness. */
			p[8] = 0xff;
		}
	}

	if (g_dev.nonzero_at != UINT64_MAX &&
	    g_dev.nonzero_at >= io->offset &&
	    g_dev.nonzero_at < io->offset + io->nbytes) {
		p[g_dev.nonzero_at - io->offset] = 0xa5;
	}
}

static void
mock_complete(bool success)
{
	struct mock_io *io = g_pending;

	CHECK(io != NULL);
	if (!io) {
		return;
	}
	g_pending = NULL;

	if (success) {
		mock_fill(io);
	}

	g_completing = io;
	g_free_io_seen = false;
	io->cb((struct spdk_bdev_io *)io, success, io->arg);
	CHECK(g_free_io_seen);
	g_completing = NULL;
	free(io);
}

static void
mock_notify(enum spdk_bdev_event_type type)
{
	CHECK(g_desc != NULL);
	if (!g_desc) {
		return;
	}
	g_desc->event_cb(type, (struct spdk_bdev *)g_desc->dev,
			 g_desc->event_ctx);
}

/* ------------------------------------------------------------------ wraps */

int
__wrap_spdk_bdev_open_ext(const char *bdev_name, bool write,
			  spdk_bdev_event_cb_t event_cb, void *event_ctx,
			  struct spdk_bdev_desc **desc)
{
	struct mock_desc *md;

	g_counters.open++;
	if (!g_dev.present || !bdev_name ||
	    strcmp(bdev_name, g_dev.name) != 0) {
		return -ENOENT;
	}
	if (write) {
		g_write_open = true;
	}

	md = calloc(1, sizeof(*md));
	if (!md) {
		return -ENOMEM;
	}
	md->dev = &g_dev;
	md->event_cb = event_cb;
	md->event_ctx = event_ctx;
	g_desc = md;
	*desc = (struct spdk_bdev_desc *)md;
	return 0;
}

void
__wrap_spdk_bdev_close(struct spdk_bdev_desc *desc)
{
	struct mock_desc *md = (struct mock_desc *)desc;

	g_counters.close++;
	if (g_pending) {
		g_close_saw_pending = true;
	}
	if (md == g_desc) {
		g_desc = NULL;
	}
	free(md);
}

struct spdk_bdev *
__wrap_spdk_bdev_desc_get_bdev(struct spdk_bdev_desc *desc)
{
	return (struct spdk_bdev *)((struct mock_desc *)desc)->dev;
}

uint32_t
__wrap_spdk_bdev_get_block_size(const struct spdk_bdev *bdev)
{
	return ((const struct mock_dev *)bdev)->block_size;
}

uint64_t
__wrap_spdk_bdev_get_num_blocks(const struct spdk_bdev *bdev)
{
	return ((const struct mock_dev *)bdev)->num_blocks;
}

const char *
__wrap_spdk_bdev_get_name(const struct spdk_bdev *bdev)
{
	return ((const struct mock_dev *)bdev)->name;
}

struct spdk_io_channel *
__wrap_spdk_bdev_get_io_channel(struct spdk_bdev_desc *desc)
{
	(void)desc;
	g_counters.chan_get++;
	if (g_chan_fail) {
		return NULL;
	}
	return (struct spdk_io_channel *)&g_chan;
}

void
__wrap_spdk_put_io_channel(struct spdk_io_channel *ch)
{
	CHECK((void *)ch == (void *)&g_chan);
	g_counters.chan_put++;
	if (g_pending) {
		g_put_saw_pending = true;
	}
}

void *
__wrap_spdk_dma_zmalloc(size_t size, size_t align, uint64_t *unused)
{
	void *p;

	(void)unused;
	g_counters.dma_alloc++;
	g_alloc_size = size;
	CHECK(align == 0);
	CHECK(size == (size_t)SCAN_CHUNK);
	if (g_dma_fail) {
		return NULL;
	}
	p = malloc(size);
	if (p) {
		memset(p, 0, size);
	}
	return p;
}

void
__wrap_spdk_dma_free(void *buf)
{
	if (!buf) {
		return;
	}
	g_counters.dma_free++;
	if (g_pending) {
		g_free_saw_pending = true;
	}
	free(buf);
}

int
__wrap_spdk_bdev_read(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		      void *buf, uint64_t offset, uint64_t nbytes,
		      spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct mock_io *io;
	uint64_t total = mock_total();
	int index = g_counters.reads;

	g_counters.reads++;
	if (index < (int)(sizeof(g_reads) / sizeof(g_reads[0]))) {
		g_reads[index].offset = offset;
		g_reads[index].len = nbytes;
	}

	CHECK(desc != NULL);
	CHECK(ch == (struct spdk_io_channel *)&g_chan);
	CHECK(buf != NULL);
	CHECK(cb != NULL);

	if (g_dev.block_size != 0) {
		CHECK(nbytes > 0);
		CHECK(nbytes % g_dev.block_size == 0);
		CHECK(offset % g_dev.block_size == 0);
	}
	CHECK(offset <= total);
	CHECK(nbytes <= total - offset);

	if (g_submit_fail_index == index) {
		return -ENOMEM;
	}

	CHECK(g_pending == NULL);
	io = calloc(1, sizeof(*io));
	if (!io) {
		return -ENOMEM;
	}
	io->buf = buf;
	io->offset = offset;
	io->nbytes = nbytes;
	io->cb = cb;
	io->arg = cb_arg;
	g_pending = io;
	return 0;
}

void
__wrap_spdk_bdev_free_io(struct spdk_bdev_io *bdev_io)
{
	CHECK(g_completing != NULL);
	CHECK((void *)bdev_io == (void *)g_completing);
	g_free_io_seen = true;
	g_counters.free_io++;
}

void
__wrap_spdk_log(enum spdk_log_level level, const char *file, const int line,
		const char *func, const char *format, ...)
{
	(void)level;
	(void)file;
	(void)line;
	(void)func;
	(void)format;
}

/* ----------------------------------------------------------------- runner */

struct scan_result {
	int calls;
	int status;
};

static void
scan_cb(void *arg, int status)
{
	struct scan_result *r = arg;

	r->calls++;
	r->status = status;
}

static void
drive_success(struct scan_result *r)
{
	int guard = 0;

	while (r->calls == 0) {
		CHECK(g_pending != NULL);
		if (!g_pending || ++guard > 16) {
			break;
		}
		mock_complete(true);
	}
}

static void
check_aligned_reads(uint32_t block_size, uint64_t total)
{
	int i;

	for (i = 0; i < g_counters.reads; i++) {
		CHECK(g_reads[i].len > 0);
		CHECK(g_reads[i].len % block_size == 0);
		CHECK(g_reads[i].offset % block_size == 0);
		CHECK(g_reads[i].offset + g_reads[i].len <= total);
	}
}

/* Resources released exactly once, and only after any pending I/O finished. */
static void
check_cleanup(bool channel_expected, bool buffer_expected, int completed_ios)
{
	CHECK(g_counters.open == 1);
	CHECK(g_counters.close == 1);
	CHECK(g_counters.chan_get == (channel_expected ? 1 : 0));
	CHECK(g_counters.chan_put == (channel_expected ? 1 : 0));
	CHECK(g_counters.dma_alloc == (buffer_expected ? 1 : 0));
	CHECK(g_counters.dma_free == (buffer_expected ? 1 : 0));
	CHECK(g_counters.free_io == completed_ios);
	CHECK(g_pending == NULL);
	CHECK(!g_close_saw_pending);
	CHECK(!g_put_saw_pending);
	CHECK(!g_free_saw_pending);
}

static void
begin_case(const char *name, int *before)
{
	printf("CASE %s\n", name);
	*before = g_failures;
}

static void
end_case(const char *name, int before)
{
	if (g_failures == before) {
		printf("PASS %s\n", name);
	} else {
		printf("FAILED %s (%d check(s))\n", name, g_failures - before);
	}
}

/* -------------------------------------------------------------- test cases */

static void
case_zero_multi_chunk_partial_last(void)
{
	const uint32_t bs = 4096;
	const uint64_t blocks = 700;    /* 2,867,200 bytes = 2 full chunks + 188 blocks */
	const uint64_t total = blocks * bs;
	struct scan_result r = {0};
	int before;

	begin_case("zero_multi_chunk_partial_last", &before);
	reset_mock(blocks, bs);
	s3_recovery_blank_scan("mock0", scan_cb, &r);

	CHECK(g_pending != NULL);
	CHECK(r.calls == 0);
	drive_success(&r);

	CHECK(r.calls == 1);
	CHECK(r.status == 0);
	CHECK(!g_write_open);
	CHECK(g_counters.reads == 3);
	CHECK(g_reads[0].offset == 0);
	CHECK(g_reads[0].len == (uint64_t)SCAN_CHUNK);
	CHECK(g_reads[1].offset == (uint64_t)SCAN_CHUNK);
	CHECK(g_reads[1].len == (uint64_t)SCAN_CHUNK);
	CHECK(g_reads[2].offset == 2 * (uint64_t)SCAN_CHUNK);
	CHECK(g_reads[2].len == total - 2 * (uint64_t)SCAN_CHUNK);
	check_aligned_reads(bs, total);
	check_cleanup(true, true, 3);
	CHECK(g_alloc_size == (size_t)SCAN_CHUNK);
	end_case("zero_multi_chunk_partial_last", before);
}

static void
case_zero_single_block(void)
{
	struct scan_result r = {0};
	int before;

	begin_case("zero_single_block", &before);
	reset_mock(1, 4096);
	s3_recovery_blank_scan("mock0", scan_cb, &r);
	drive_success(&r);

	CHECK(r.calls == 1);
	CHECK(r.status == 0);
	CHECK(g_counters.reads == 1);
	CHECK(g_reads[0].offset == 0);
	CHECK(g_reads[0].len == 4096);
	check_cleanup(true, true, 1);
	end_case("zero_single_block", before);
}

static void
case_nonzero_last_byte(void)
{
	const uint32_t bs = 4096;
	const uint64_t blocks = 700;
	struct scan_result r = {0};
	int before;

	begin_case("nonzero_last_byte", &before);
	reset_mock(blocks, bs);
	g_dev.nonzero_at = blocks * bs - 1;
	s3_recovery_blank_scan("mock0", scan_cb, &r);
	drive_success(&r);

	CHECK(r.calls == 1);
	CHECK(r.status == -EILSEQ);
	CHECK(g_counters.reads == 3);
	check_cleanup(true, true, 3);
	end_case("nonzero_last_byte", before);
}

static void
case_nonzero_second_chunk(void)
{
	const uint32_t bs = 4096;
	const uint64_t blocks = 700;
	struct scan_result r = {0};
	int before;

	begin_case("nonzero_second_chunk", &before);
	reset_mock(blocks, bs);
	g_dev.nonzero_at = (uint64_t)SCAN_CHUNK + 123;
	s3_recovery_blank_scan("mock0", scan_cb, &r);
	drive_success(&r);

	CHECK(r.calls == 1);
	CHECK(r.status == -EILSEQ);
	/* The second read covers the byte, and no third read is submitted. */
	CHECK(g_counters.reads == 2);
	CHECK(g_pending == NULL);
	check_cleanup(true, true, 2);
	end_case("nonzero_second_chunk", before);
}

static void
case_nonzero_beyond_header(void)
{
	struct scan_result r = {0};
	int before;

	begin_case("nonzero_beyond_header", &before);
	reset_mock(64, 4096);
	/* Beyond any plausible super block header, still inside chunk one. */
	g_dev.nonzero_at = 4096 + 17;
	s3_recovery_blank_scan("mock0", scan_cb, &r);
	drive_success(&r);

	CHECK(r.calls == 1);
	CHECK(r.status == -EILSEQ);
	CHECK(g_counters.reads == 1);
	check_cleanup(true, true, 1);
	end_case("nonzero_beyond_header", before);
}

static void
superblock_case(const char *name, int pattern)
{
	struct scan_result r = {0};
	int before;

	begin_case(name, &before);
	reset_mock(64, 4096);
	g_dev.pattern = pattern;
	s3_recovery_blank_scan("mock0", scan_cb, &r);
	drive_success(&r);

	CHECK(r.calls == 1);
	CHECK(r.status == -EILSEQ);
	CHECK(g_counters.reads == 1);
	check_cleanup(true, true, 1);
	end_case(name, before);
}

static void
case_open_missing(void)
{
	struct scan_result r = {0};
	int before;

	begin_case("open_missing", &before);
	reset_mock(64, 4096);
	g_dev.present = false;
	s3_recovery_blank_scan("mock0", scan_cb, &r);

	CHECK(r.calls == 1);
	CHECK(r.status == -ENOENT);
	CHECK(g_counters.open == 1);
	CHECK(g_counters.close == 0);
	CHECK(g_counters.chan_get == 0);
	CHECK(g_counters.dma_alloc == 0);
	CHECK(g_counters.reads == 0);
	CHECK(g_pending == NULL);
	end_case("open_missing", before);
}

static void
case_read_submit_error(void)
{
	const uint32_t bs = 4096;
	const uint64_t blocks = 700;
	struct scan_result r = {0};
	int before;

	begin_case("read_submit_error", &before);
	reset_mock(blocks, bs);
	g_submit_fail_index = 1;
	s3_recovery_blank_scan("mock0", scan_cb, &r);
	drive_success(&r);

	CHECK(r.calls == 1);
	CHECK(r.status == -ENOMEM);
	CHECK(g_counters.reads == 2);
	CHECK(g_counters.free_io == 1);
	CHECK(g_pending == NULL);
	check_cleanup(true, true, 1);
	end_case("read_submit_error", before);
}

static void
case_completion_failure(void)
{
	const uint32_t bs = 4096;
	const uint64_t blocks = 700;
	struct scan_result r = {0};
	int before;

	begin_case("completion_failure", &before);
	reset_mock(blocks, bs);
	s3_recovery_blank_scan("mock0", scan_cb, &r);

	CHECK(g_pending != NULL);
	mock_complete(false);

	CHECK(r.calls == 1);
	CHECK(r.status == -EIO);
	CHECK(g_counters.reads == 1);
	CHECK(g_pending == NULL);
	check_cleanup(true, true, 1);
	end_case("completion_failure", before);
}

static void
event_while_pending_case(const char *name, enum spdk_bdev_event_type type,
			 int expected)
{
	const uint32_t bs = 4096;
	const uint64_t blocks = 700;
	struct scan_result r = {0};
	int before;

	begin_case(name, &before);
	reset_mock(blocks, bs);
	s3_recovery_blank_scan("mock0", scan_cb, &r);

	CHECK(g_pending != NULL);
	CHECK(r.calls == 0);

	/* The device disappears while the first (zero-filled) read is in
	 * flight. The event must latch a failure, not act as a completion. */
	mock_notify(type);
	CHECK(r.calls == 0);
	CHECK(g_pending != NULL);
	CHECK(g_counters.close == 0);
	CHECK(g_counters.chan_put == 0);
	CHECK(g_counters.dma_free == 0);

	/* The read still succeeds; the scan must not. */
	mock_complete(true);

	CHECK(r.calls == 1);
	CHECK(r.status == expected);
	CHECK(g_counters.reads == 1);
	CHECK(g_pending == NULL);
	check_cleanup(true, true, 1);
	end_case(name, before);
}

static void
case_duplicate_event_while_pending(void)
{
	const uint32_t bs = 4096;
	const uint64_t blocks = 700;
	struct scan_result r = {0};
	int before;

	begin_case("duplicate_remove_while_pending", &before);
	reset_mock(blocks, bs);
	s3_recovery_blank_scan("mock0", scan_cb, &r);

	mock_notify(SPDK_BDEV_EVENT_REMOVE);
	mock_notify(SPDK_BDEV_EVENT_REMOVE);
	CHECK(r.calls == 0);
	mock_complete(true);

	CHECK(r.calls == 1);
	CHECK(r.status == -ENODEV);
	CHECK(g_counters.reads == 1);
	check_cleanup(true, true, 1);
	end_case("duplicate_remove_while_pending", before);
}

static void
case_remove_after_first_chunk(void)
{
	const uint32_t bs = 4096;
	const uint64_t blocks = 700;
	struct scan_result r = {0};
	int before;

	begin_case("remove_after_first_chunk", &before);
	reset_mock(blocks, bs);
	s3_recovery_blank_scan("mock0", scan_cb, &r);

	/* Finish chunk one, then remove the device during chunk two. */
	mock_complete(true);
	CHECK(g_pending != NULL);
	CHECK(g_counters.reads == 2);
	mock_notify(SPDK_BDEV_EVENT_REMOVE);
	CHECK(r.calls == 0);
	mock_complete(true);

	CHECK(r.calls == 1);
	CHECK(r.status == -ENODEV);
	CHECK(g_counters.reads == 2);
	CHECK(g_pending == NULL);
	check_cleanup(true, true, 2);
	end_case("remove_after_first_chunk", before);
}

static void
case_invalid_zero_blocks(void)
{
	struct scan_result r = {0};
	int before;

	begin_case("invalid_zero_blocks", &before);
	reset_mock(0, 4096);
	s3_recovery_blank_scan("mock0", scan_cb, &r);

	CHECK(r.calls == 1);
	CHECK(r.status == -EINVAL);
	CHECK(g_counters.reads == 0);
	CHECK(g_counters.open == 1);
	CHECK(g_counters.close == 1);
	CHECK(g_counters.chan_get == 0);
	CHECK(g_counters.chan_put == 0);
	CHECK(g_counters.dma_alloc == 0);
	CHECK(g_counters.dma_free == 0);
	end_case("invalid_zero_blocks", before);
}

static void
case_invalid_zero_block_size(void)
{
	struct scan_result r = {0};
	int before;

	begin_case("invalid_zero_block_size", &before);
	reset_mock(64, 0);
	s3_recovery_blank_scan("mock0", scan_cb, &r);

	CHECK(r.calls == 1);
	CHECK(r.status == -EINVAL);
	CHECK(g_counters.reads == 0);
	CHECK(g_counters.chan_get == 0);
	CHECK(g_counters.dma_alloc == 0);
	CHECK(g_counters.open == 1);
	CHECK(g_counters.close == 1);
	end_case("invalid_zero_block_size", before);
}

static void
case_invalid_block_larger_than_chunk(void)
{
	struct scan_result r = {0};
	int before;

	begin_case("invalid_block_larger_than_chunk", &before);
	reset_mock(4, 2 * SCAN_CHUNK);
	s3_recovery_blank_scan("mock0", scan_cb, &r);

	CHECK(r.calls == 1);
	CHECK(r.status == -EINVAL);
	CHECK(g_counters.reads == 0);
	CHECK(g_counters.open == 1);
	CHECK(g_counters.close == 1);
	end_case("invalid_block_larger_than_chunk", before);
}

static void
case_overflow_block_count(void)
{
	struct scan_result r = {0};
	int before;

	begin_case("overflow_block_count", &before);
	/* (2^55 + 1) * 512 = 2^64 + 512: the unchecked product used to wrap
	 * to 512 and "prove" a 512-byte device blank. */
	reset_mock((UINT64_C(1) << 55) + 1, 512);
	s3_recovery_blank_scan("mock0", scan_cb, &r);

	CHECK(r.calls == 1);
	CHECK(r.status == -EINVAL);
	CHECK(g_counters.reads == 0);
	CHECK(g_counters.open == 1);
	CHECK(g_counters.close == 1);
	CHECK(g_counters.chan_get == 0);
	CHECK(g_counters.dma_alloc == 0);
	end_case("overflow_block_count", before);
}

static void
case_channel_alloc_failure(void)
{
	struct scan_result r = {0};
	int before;

	begin_case("channel_alloc_failure", &before);
	reset_mock(64, 4096);
	g_chan_fail = true;
	s3_recovery_blank_scan("mock0", scan_cb, &r);

	CHECK(r.calls == 1);
	CHECK(r.status == -ENOMEM);
	CHECK(g_counters.reads == 0);
	CHECK(g_counters.open == 1);
	CHECK(g_counters.close == 1);
	CHECK(g_counters.chan_get == 1);
	CHECK(g_counters.chan_put == 0);
	CHECK(g_counters.dma_alloc == 0);
	end_case("channel_alloc_failure", before);
}

static void
case_buffer_alloc_failure(void)
{
	struct scan_result r = {0};
	int before;

	begin_case("buffer_alloc_failure", &before);
	reset_mock(64, 4096);
	g_dma_fail = true;
	s3_recovery_blank_scan("mock0", scan_cb, &r);

	CHECK(r.calls == 1);
	CHECK(r.status == -ENOMEM);
	CHECK(g_counters.reads == 0);
	CHECK(g_counters.open == 1);
	CHECK(g_counters.close == 1);
	CHECK(g_counters.chan_get == 1);
	CHECK(g_counters.chan_put == 1);
	CHECK(g_counters.dma_alloc == 1);
	CHECK(g_counters.dma_free == 0);
	end_case("buffer_alloc_failure", before);
}

int
main(void)
{
	printf("s3_recovery_blank_scan deterministic async harness\n");

	case_zero_multi_chunk_partial_last();
	case_zero_single_block();
	case_nonzero_last_byte();
	case_nonzero_second_chunk();
	case_nonzero_beyond_header();
	superblock_case("superblock_like_valid_rejected", 1);
	superblock_case("superblock_like_corrupt_rejected", 2);
	case_open_missing();
	case_read_submit_error();
	case_completion_failure();
	event_while_pending_case("remove_while_pending",
				 SPDK_BDEV_EVENT_REMOVE, -ENODEV);
	event_while_pending_case("resize_while_pending",
				 SPDK_BDEV_EVENT_RESIZE, -ERANGE);
	case_duplicate_event_while_pending();
	case_remove_after_first_chunk();
	case_invalid_zero_blocks();
	case_invalid_zero_block_size();
	case_invalid_block_larger_than_chunk();
	case_overflow_block_count();
	case_channel_alloc_failure();
	case_buffer_alloc_failure();

	if (g_failures != 0) {
		printf("RESULT: FAIL (%d check(s))\n", g_failures);
		return 1;
	}
	printf("RESULT: PASS\n");
	return 0;
}
