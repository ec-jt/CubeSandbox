/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */

/*
 * Deterministic harness for the retained local claim:
 *
 *   s3_recovery_claim_open()
 *   s3_recovery_blank_scan_claimed()
 *   s3_recovery_claim_release()
 *
 * === What this proves ===
 *
 * The recovery chain used to scan the device, close the descriptor, and let the
 * formatter open it again. Between those two steps the device was unclaimed, so
 * any other writer in the process could take it. The claim API exists to remove
 * that window: one descriptor, one exclusive claim, held from before the blank
 * proof until after the format.
 *
 * The properties asserted here are the ones the chain depends on:
 *
 *   - a device another descriptor already has open for writing is refused
 *     *before* anything is scanned or formatted;
 *   - a second claim on a claimed device is refused;
 *   - the scan through a claim does not close the descriptor, so the caller can
 *     hand it to the formatter;
 *   - a failure on the second (cache) claim leaves the first claim held, and
 *     releasing it is what frees the device;
 *   - REMOVE / RESIZE during a claimed scan still fail closed, and the claim
 *     survives the failure so the caller can release it exactly once;
 *   - release is idempotent and NULL-safe.
 *
 * === Why the mock reproduces SPDK's rules rather than approximating them ===
 *
 * A mock that simply "returns -EPERM if claimed" would pass even if the real
 * API granted a claim over a pre-existing writer. The rules encoded below are
 * transcribed from the pinned tree, with the source line named at each one:
 *
 *   bdev.c bdev_open():        `if (write && claim_type != NONE) return -EPERM`
 *   bdev.c claim_verify_rwo(): refuses when `claim_type != NONE`, when the
 *                              descriptor already has a claim, and when any
 *                              *other* open descriptor is writable
 *   bdev.c claim_bdev():       records the claim on the descriptor and promotes
 *                              a read-only descriptor to writable
 *   bdev.c bdev_close():       drops the descriptor's claim
 *
 * The one thing the mock cannot prove is that the real SPDK behaves this way.
 * That is a property of the pinned source, cited above, not of this test.
 *
 * === What this test does NOT prove ===
 *
 * Process-external exclusion. The claim is process-local; another process can
 * still write the backing file of an aio bdev. Nothing here exercises that, and
 * nothing in the implementation claims to prevent it.
 *
 * It also never calls s3_local_dev_format() and never exercises the cleanup
 * order, so it cannot see a descriptor released under a live local device or a
 * formatter that reopens a claimed cache bdev. Those are covered by
 * s3_recovery_claim_lifetime_test.c, which links the real formatter.
 *
 * The v1 release mock now enforces bdev.c:9455 (claim_type must be
 * EXCL_WRITE). A v1 release on the v2 claim taken here is a failed check, so
 * this harness can no longer hide that defect.
 *
 * Usage:
 *   ./s3_recovery_claim_test
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
#include "spdk/bdev_module.h"
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

/* One mock bdev. The claim state lives here, exactly where SPDK keeps it
 * (bdev->internal.claim_type), so the rules below are checked against the same
 * shape of state the real code uses. */
struct mock_dev {
	const char     *name;
	uint64_t        num_blocks;
	uint32_t        block_size;
	bool            present;

	/* 0 = all zero, 1 = non-zero byte at nonzero_at. */
	int             pattern;
	uint64_t        nonzero_at;

	/* SPDK's bdev->internal.claim_type. The type matters: v1 (EXCL_WRITE)
	 * and v2 (READ_MANY_WRITE_ONE) have different release paths, and the
	 * mock must not let a v1 release clear a v2 claim. */
	enum spdk_bdev_claim_type claim_type;
	bool            claimed;
	struct spdk_bdev_module *claim_module;
};

struct mock_desc {
	struct mock_dev              *dev;
	spdk_bdev_event_cb_t          event_cb;
	void                         *event_ctx;
	bool                          write;
	bool                          has_claim;
	bool                          closed;
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

struct mock_counters {
	int open;
	int close;
	int chan_get;
	int chan_put;
	int dma_alloc;
	int dma_free;
	int free_io;
	int reads;
	int claim_attempts;
	int claim_granted;
	int claim_refused;
	int release;
};

#define MAX_DEVS 2

static struct mock_dev      g_devs[MAX_DEVS];
static int                  g_ndevs;
static struct mock_desc    *g_descs[8];
static int                  g_ndescs;
static struct mock_io      *g_pending;
static struct mock_io      *g_completing;
static struct mock_chan     g_chan;
static struct mock_counters g_counters;

static bool     g_chan_fail;
static bool     g_dma_fail;
static bool     g_close_saw_pending;

static struct spdk_bdev_module g_test_module = {
	.name = "s3lvol_test",
};

static struct mock_dev *
mock_dev_by_name(const char *name)
{
	int i;

	for (i = 0; i < g_ndevs; i++) {
		if (strcmp(g_devs[i].name, name) == 0) {
			return &g_devs[i];
		}
	}
	return NULL;
}

static void
reset_mock(void)
{
	int i;

	if (g_pending) {
		free(g_pending);
		g_pending = NULL;
	}
	for (i = 0; i < g_ndescs; i++) {
		free(g_descs[i]);
		g_descs[i] = NULL;
	}
	g_ndescs = 0;

	memset(g_devs, 0, sizeof(g_devs));
	g_ndevs = 0;

	memset(&g_counters, 0, sizeof(g_counters));
	g_chan_fail = false;
	g_dma_fail = false;
	g_completing = NULL;
	g_close_saw_pending = false;
}

/* Add a device. Returns its index. */
static int
add_dev(const char *name, uint64_t blocks, uint32_t bs)
{
	struct mock_dev *d = &g_devs[g_ndevs];

	d->name = name;
	d->num_blocks = blocks;
	d->block_size = bs;
	d->present = true;
	d->nonzero_at = UINT64_MAX;
	return g_ndevs++;
}

static uint64_t
mock_total(struct mock_dev *d)
{
	return d->num_blocks * d->block_size;
}

static void
mock_fill(struct mock_io *io)
{
	uint8_t *p = io->buf;

	memset(p, 0, (size_t)io->nbytes);
	if (g_devs[0].nonzero_at != UINT64_MAX &&
	    g_devs[0].nonzero_at >= io->offset &&
	    g_devs[0].nonzero_at < io->offset + io->nbytes) {
		p[g_devs[0].nonzero_at - io->offset] = 0xa5;
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
	io->cb((struct spdk_bdev_io *)io, success, io->arg);
	g_completing = NULL;
	free(io);
}

static void
mock_notify(struct mock_desc *md, enum spdk_bdev_event_type type)
{
	CHECK(md != NULL);
	if (!md) {
		return;
	}
	md->event_cb(type, (struct spdk_bdev *)md->dev, md->event_ctx);
}

/* ------------------------------------------------------------------ wraps */

int
__wrap_spdk_bdev_open_ext(const char *bdev_name, bool write,
			  spdk_bdev_event_cb_t event_cb, void *event_ctx,
			  struct spdk_bdev_desc **desc)
{
	struct mock_dev *d;
	struct mock_desc *md;

	g_counters.open++;
	d = mock_dev_by_name(bdev_name);
	if (!d || !d->present) {
		return -ENODEV;
	}

	/* bdev.c bdev_open(): a writable open is refused while the bdev is
	 * claimed. This is the rule that stops a new writer appearing after the
	 * claim is taken. */
	if (write && d->claimed) {
		return -EPERM;
	}

	md = calloc(1, sizeof(*md));
	if (!md) {
		return -ENOMEM;
	}
	md->dev = d;
	md->event_cb = event_cb;
	md->event_ctx = event_ctx;
	md->write = write;

	if (g_ndescs < (int)(sizeof(g_descs) / sizeof(g_descs[0]))) {
		g_descs[g_ndescs++] = md;
	}
	*desc = (struct spdk_bdev_desc *)md;
	return 0;
}

void
__wrap_spdk_bdev_close(struct spdk_bdev_desc *desc)
{
	struct mock_desc *md = (struct mock_desc *)desc;
	int i;

	g_counters.close++;
	if (g_pending) {
		g_close_saw_pending = true;
	}
	if (!md) {
		return;
	}
	md->closed = true;

	/* bdev.c bdev_close() -> bdev_desc_release_claims(): closing a
	 * descriptor drops its v2 claim. */
	if (md->has_claim) {
		md->dev->claim_type = SPDK_BDEV_CLAIM_NONE;
		md->dev->claimed = false;
		md->dev->claim_module = NULL;
		md->has_claim = false;
	}

	for (i = 0; i < g_ndescs; i++) {
		if (g_descs[i] == md) {
			g_descs[i] = NULL;
			break;
		}
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
}

void *
__wrap_spdk_dma_zmalloc(size_t size, size_t align, uint64_t *unused)
{
	void *p;

	(void)unused;
	g_counters.dma_alloc++;
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
	free(buf);
}

int
__wrap_spdk_bdev_read(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		      void *buf, uint64_t offset, uint64_t nbytes,
		      spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct mock_io *io;
	struct mock_dev *d = ((struct mock_desc *)desc)->dev;
	uint64_t total = mock_total(d);

	g_counters.reads++;
	CHECK(ch == (struct spdk_io_channel *)&g_chan);
	CHECK(buf != NULL);
	CHECK(cb != NULL);
	CHECK(nbytes > 0);
	CHECK(nbytes % d->block_size == 0);
	CHECK(offset % d->block_size == 0);
	CHECK(offset <= total);
	CHECK(nbytes <= total - offset);

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
	g_counters.free_io++;
}

const char *
__wrap_spdk_strerror(int errnum)
{
	/* The claim path only formats this into a log line. */
	return strerror(errnum < 0 ? -errnum : errnum);
}

void
__wrap_spdk_bdev_claim_opts_init(struct spdk_bdev_claim_opts *opts, size_t size)
{
	memset(opts, 0, size);
	opts->opts_size = size;
}

/* Transcribed from bdev.c claim_verify_rwo() + claim_bdev(). */
int
__wrap_spdk_bdev_module_claim_bdev_desc(struct spdk_bdev_desc *desc,
					enum spdk_bdev_claim_type type,
					struct spdk_bdev_claim_opts *opts,
					struct spdk_bdev_module *module)
{
	struct mock_dev *d;
	int i;

	g_counters.claim_attempts++;

	if (!desc || !module) {
		return -EINVAL;
	}
	d = ((struct mock_desc *)desc)->dev;

	/* claim_verify_rwo(): only READ_MANY_WRITE_ONE is exercised here. */
	if (type != SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE) {
		return -ENOTSUP;
	}
	if (opts && opts->shared_claim_key != 0) {
		return -EINVAL;
	}
	if (d->claimed) {
		g_counters.claim_refused++;
		return -EPERM;
	}
	if (((struct mock_desc *)desc)->has_claim) {
		g_counters.claim_refused++;
		return -EPERM;
	}
	/* The rule that matters: any *other* descriptor open for writing
	 * refuses the claim. */
	for (i = 0; i < g_ndescs; i++) {
		struct mock_desc *other = g_descs[i];

		if (!other || other == (struct mock_desc *)desc) {
			continue;
		}
		if (other->write) {
			g_counters.claim_refused++;
			return -EPERM;
		}
	}

	/* claim_bdev(): record it and promote the descriptor to writable. */
	d->claimed = true;
	d->claim_type = type;
	d->claim_module = module;
	((struct mock_desc *)desc)->has_claim = true;
	((struct mock_desc *)desc)->write = true;
	g_counters.claim_granted++;
	return 0;
}

/*
 * bdev.c:9450 spdk_bdev_module_release_bdev():
 *     assert(bdev->internal.claim.v1.module != NULL);
 *     assert(bdev->internal.claim_type == SPDK_BDEV_CLAIM_EXCL_WRITE);
 *
 * This is the v1 release path and must not be used for the v2 claim taken
 * here. The real assertion aborts; this mock records a failed check and leaves
 * the claim state untouched, so the bug cannot be hidden by a mock that is more
 * permissive than SPDK.
 */
void
__wrap_spdk_bdev_module_release_bdev(struct spdk_bdev *bdev)
{
	struct mock_dev *d = (struct mock_dev *)bdev;
	int i;

	g_counters.release++;
	CHECK(d != NULL);
	if (!d) {
		return;
	}
	if (d->claim_type != SPDK_BDEV_CLAIM_EXCL_WRITE) {
		CHECK(d->claim_type == SPDK_BDEV_CLAIM_EXCL_WRITE);
		return;
	}
	d->claimed = false;
	d->claim_type = SPDK_BDEV_CLAIM_NONE;
	d->claim_module = NULL;

	for (i = 0; i < g_ndescs; i++) {
		if (g_descs[i] && g_descs[i]->dev == d) {
			g_descs[i]->has_claim = false;
		}
	}
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

/* A claim is granted on a free device, and the descriptor it hands out is the
 * one the scan reads through. */
static void
case_claim_granted(void)
{
	struct s3_recovery_claim *claim = NULL;
	struct scan_result r = {0};
	int before;
	int rc;

	begin_case("claim_granted", &before);
	reset_mock();
	add_dev("mock0", 64, 4096);

	rc = s3_recovery_claim_open("mock0", &g_test_module, &claim);
	CHECK(rc == 0);
	CHECK(claim != NULL);
	CHECK(g_counters.open == 1);
	CHECK(g_counters.claim_attempts == 1);
	CHECK(g_counters.claim_granted == 1);
	CHECK(g_devs[0].claimed);
	CHECK(s3_recovery_claim_desc(claim) != NULL);
	CHECK(s3_recovery_claim_bdev(claim) == (struct spdk_bdev *)&g_devs[0]);

	s3_recovery_blank_scan_claimed(claim, scan_cb, &r);
	drive_success(&r);

	CHECK(r.calls == 1);
	CHECK(r.status == 0);
	/* The scan took and released its own channel; the claim is untouched. */
	CHECK(g_counters.chan_get == 1);
	CHECK(g_counters.chan_put == 1);
	CHECK(g_counters.close == 0);
	CHECK(g_devs[0].claimed);

	s3_recovery_claim_release(claim);
	/* v2 claims are released by closing the descriptor, not by the v1
	 * spdk_bdev_module_release_bdev() path. */
	CHECK(g_counters.release == 0);
	CHECK(g_counters.close == 1);
	CHECK(!g_devs[0].claimed);
	end_case("claim_granted", before);
}

/* A descriptor already open for writing refuses the claim. This is the
 * pre-existing-writer case: without claim_verify_rwo the claim would be
 * granted and the format would run over a device somebody else is writing. */
static void
case_prior_writer_refused(void)
{
	struct s3_recovery_claim *claim = NULL;
	struct spdk_bdev_desc *writer = NULL;
	int before;
	int rc;

	begin_case("prior_writer_refused", &before);
	reset_mock();
	add_dev("mock0", 64, 4096);

	rc = spdk_bdev_open_ext("mock0", true, NULL, NULL, &writer);
	CHECK(rc == 0);
	CHECK(writer != NULL);

	rc = s3_recovery_claim_open("mock0", &g_test_module, &claim);
	CHECK(rc == -EPERM);
	CHECK(claim == NULL);
	CHECK(g_counters.claim_refused == 1);
	CHECK(g_counters.claim_granted == 0);
	/* The failed claim closed its own descriptor and left the writer's
	 * alone. */
	CHECK(g_counters.close == 1);
	CHECK(!g_devs[0].claimed);

	spdk_bdev_close(writer);
	end_case("prior_writer_refused", before);
}

/* A second claim on a claimed device is refused, and the first one survives. */
static void
case_second_claim_refused(void)
{
	struct s3_recovery_claim *first = NULL;
	struct s3_recovery_claim *second = NULL;
	int before;
	int rc;

	begin_case("second_claim_refused", &before);
	reset_mock();
	add_dev("mock0", 64, 4096);

	rc = s3_recovery_claim_open("mock0", &g_test_module, &first);
	CHECK(rc == 0);
	CHECK(first != NULL);

	rc = s3_recovery_claim_open("mock0", &g_test_module, &second);
	CHECK(rc == -EPERM);
	CHECK(second == NULL);
	CHECK(g_counters.claim_granted == 1);
	CHECK(g_counters.claim_refused == 1);
	CHECK(g_devs[0].claimed);

	s3_recovery_claim_release(first);
	CHECK(!g_devs[0].claimed);
	end_case("second_claim_refused", before);
}

/* A writable open after the claim is refused. This is the rule that keeps the
 * window closed for the whole scan+format span. */
static void
case_writer_after_claim_refused(void)
{
	struct s3_recovery_claim *claim = NULL;
	struct spdk_bdev_desc *writer = NULL;
	int before;
	int rc;

	begin_case("writer_after_claim_refused", &before);
	reset_mock();
	add_dev("mock0", 64, 4096);

	rc = s3_recovery_claim_open("mock0", &g_test_module, &claim);
	CHECK(rc == 0);

	rc = spdk_bdev_open_ext("mock0", true, NULL, NULL, &writer);
	CHECK(rc == -EPERM);
	CHECK(writer == NULL);

	s3_recovery_claim_release(claim);
	/* Once released, a writer is allowed again. */
	rc = spdk_bdev_open_ext("mock0", true, NULL, NULL, &writer);
	CHECK(rc == 0);
	spdk_bdev_close(writer);
	end_case("writer_after_claim_refused", before);
}

/* The scan through a claim does not close the descriptor, so the formatter can
 * adopt it. This is the retained-handle property. */
static void
case_scan_retains_descriptor(void)
{
	struct s3_recovery_claim *claim = NULL;
	struct spdk_bdev_desc *desc;
	struct scan_result r = {0};
	int before;
	int rc;

	begin_case("scan_retains_descriptor", &before);
	reset_mock();
	add_dev("mock0", 700, 4096);

	rc = s3_recovery_claim_open("mock0", &g_test_module, &claim);
	CHECK(rc == 0);
	desc = s3_recovery_claim_desc(claim);

	s3_recovery_blank_scan_claimed(claim, scan_cb, &r);
	drive_success(&r);

	CHECK(r.calls == 1);
	CHECK(r.status == 0);
	/* No close at all: the descriptor the claim holds is still open. */
	CHECK(g_counters.close == 0);
	CHECK(s3_recovery_claim_desc(claim) == desc);
	CHECK(g_devs[0].claimed);

	s3_recovery_claim_release(claim);
	CHECK(g_counters.close == 1);
	end_case("scan_retains_descriptor", before);
}

/* A non-blank device fails the claimed scan, and the claim is still held so the
 * caller can release it. Nothing is formatted. */
static void
case_claimed_scan_nonzero(void)
{
	struct s3_recovery_claim *claim = NULL;
	struct scan_result r = {0};
	int before;
	int rc;

	begin_case("claimed_scan_nonzero", &before);
	reset_mock();
	add_dev("mock0", 700, 4096);
	g_devs[0].nonzero_at = 5000;

	rc = s3_recovery_claim_open("mock0", &g_test_module, &claim);
	CHECK(rc == 0);

	s3_recovery_blank_scan_claimed(claim, scan_cb, &r);
	drive_success(&r);

	CHECK(r.calls == 1);
	CHECK(r.status == -EILSEQ);
	CHECK(g_counters.close == 0);
	CHECK(g_devs[0].claimed);

	s3_recovery_claim_release(claim);
	CHECK(g_counters.release == 0);
	CHECK(g_counters.close == 1);
	end_case("claimed_scan_nonzero", before);
}

/* REMOVE during a claimed scan fails closed, and the claim survives so the
 * caller releases it exactly once. */
static void
case_claimed_scan_remove(void)
{
	struct s3_recovery_claim *claim = NULL;
	struct scan_result r = {0};
	int before;
	int rc;

	begin_case("claimed_scan_remove", &before);
	reset_mock();
	add_dev("mock0", 700, 4096);

	rc = s3_recovery_claim_open("mock0", &g_test_module, &claim);
	CHECK(rc == 0);

	s3_recovery_blank_scan_claimed(claim, scan_cb, &r);
	CHECK(g_pending != NULL);
	CHECK(r.calls == 0);

	/* The claim owns the descriptor, so it owns the event callback: the
	 * event must reach the scan through the claim, not be dropped. */
	mock_notify((struct mock_desc *)s3_recovery_claim_desc(claim),
		    SPDK_BDEV_EVENT_REMOVE);
	CHECK(r.calls == 0);
	CHECK(g_counters.close == 0);

	mock_complete(true);

	CHECK(r.calls == 1);
	CHECK(r.status == -ENODEV);
	CHECK(g_counters.close == 0);
	CHECK(g_devs[0].claimed);

	s3_recovery_claim_release(claim);
	CHECK(g_counters.release == 0);
	CHECK(g_counters.close == 1);
	end_case("claimed_scan_remove", before);
}

/* RESIZE during a claimed scan fails closed with -ERANGE. */
static void
case_claimed_scan_resize(void)
{
	struct s3_recovery_claim *claim = NULL;
	struct scan_result r = {0};
	int before;
	int rc;

	begin_case("claimed_scan_resize", &before);
	reset_mock();
	add_dev("mock0", 700, 4096);

	rc = s3_recovery_claim_open("mock0", &g_test_module, &claim);
	CHECK(rc == 0);

	s3_recovery_blank_scan_claimed(claim, scan_cb, &r);
	mock_notify((struct mock_desc *)s3_recovery_claim_desc(claim),
		    SPDK_BDEV_EVENT_RESIZE);
	mock_complete(true);

	CHECK(r.calls == 1);
	CHECK(r.status == -ERANGE);
	CHECK(g_devs[0].claimed);

	s3_recovery_claim_release(claim);
	end_case("claimed_scan_resize", before);
}

/* Two devices, as in a dual-bdev layout: the second claim failing must leave
 * the first held, and releasing the first is what frees it. This is the
 * "failure on the second cache claim releases the first" requirement. */
static void
case_second_device_claim_fails(void)
{
	struct s3_recovery_claim *wal = NULL;
	struct s3_recovery_claim *cache = NULL;
	struct spdk_bdev_desc *cache_writer = NULL;
	int before;
	int rc;

	begin_case("second_device_claim_fails", &before);
	reset_mock();
	add_dev("wal0", 64, 4096);
	add_dev("cache0", 64, 4096);

	rc = s3_recovery_claim_open("wal0", &g_test_module, &wal);
	CHECK(rc == 0);
	CHECK(g_devs[0].claimed);

	/* Somebody already writes the cache device. */
	rc = spdk_bdev_open_ext("cache0", true, NULL, NULL, &cache_writer);
	CHECK(rc == 0);

	rc = s3_recovery_claim_open("cache0", &g_test_module, &cache);
	CHECK(rc == -EPERM);
	CHECK(cache == NULL);

	/* The WAL claim is untouched by the cache failure. */
	CHECK(g_devs[0].claimed);
	CHECK(!g_devs[1].claimed);

	/* Releasing it is what frees the device -- the unwind does exactly
	 * this through lvs_setup_ctx_free(). */
	s3_recovery_claim_release(wal);
	CHECK(!g_devs[0].claimed);
	CHECK(g_counters.release == 0);

	spdk_bdev_close(cache_writer);
	end_case("second_device_claim_fails", before);
}

/* Release is idempotent for NULL and releases the claim exactly once. */
static void
case_release_once(void)
{
	struct s3_recovery_claim *claim = NULL;
	int before;
	int rc;

	begin_case("release_once", &before);
	reset_mock();
	add_dev("mock0", 64, 4096);

	s3_recovery_claim_release(NULL);
	CHECK(g_counters.release == 0);
	CHECK(g_counters.close == 0);

	rc = s3_recovery_claim_open("mock0", &g_test_module, &claim);
	CHECK(rc == 0);
	s3_recovery_claim_release(claim);

	CHECK(g_counters.release == 0);
	CHECK(g_counters.close == 1);
	CHECK(!g_devs[0].claimed);
	end_case("release_once", before);
}

/* A missing device is -ENODEV and nothing is claimed. */
static void
case_claim_missing_device(void)
{
	struct s3_recovery_claim *claim = NULL;
	int before;
	int rc;

	begin_case("claim_missing_device", &before);
	reset_mock();

	rc = s3_recovery_claim_open("nope", &g_test_module, &claim);
	CHECK(rc == -ENODEV);
	CHECK(claim == NULL);
	CHECK(g_counters.claim_attempts == 0);
	CHECK(g_counters.close == 0);
	end_case("claim_missing_device", before);
}

/* A NULL claim is refused by the scan entry point rather than dereferenced. */
static void
case_scan_null_claim(void)
{
	struct scan_result r = {0};
	int before;

	begin_case("scan_null_claim", &before);
	reset_mock();

	s3_recovery_blank_scan_claimed(NULL, scan_cb, &r);
	CHECK(r.calls == 1);
	CHECK(r.status == -EINVAL);
	CHECK(g_counters.reads == 0);
	end_case("scan_null_claim", before);
}

/* The unclaimed entry point still opens and closes its own descriptor: the
 * historical behaviour is preserved for callers that do not need the claim. */
static void
case_unclaimed_scan_unchanged(void)
{
	struct scan_result r = {0};
	int before;

	begin_case("unclaimed_scan_unchanged", &before);
	reset_mock();
	add_dev("mock0", 64, 4096);

	s3_recovery_blank_scan("mock0", scan_cb, &r);
	drive_success(&r);

	CHECK(r.calls == 1);
	CHECK(r.status == 0);
	CHECK(g_counters.open == 1);
	CHECK(g_counters.close == 1);
	CHECK(g_counters.claim_attempts == 0);
	CHECK(!g_devs[0].claimed);
	end_case("unclaimed_scan_unchanged", before);
}

int
main(void)
{
	printf("s3_recovery_claim deterministic harness\n");

	case_claim_granted();
	case_prior_writer_refused();
	case_second_claim_refused();
	case_writer_after_claim_refused();
	case_scan_retains_descriptor();
	case_claimed_scan_nonzero();
	case_claimed_scan_remove();
	case_claimed_scan_resize();
	case_second_device_claim_fails();
	case_release_once();
	case_claim_missing_device();
	case_scan_null_claim();
	case_unclaimed_scan_unchanged();

	if (g_failures != 0) {
		printf("RESULT: FAIL (%d check(s))\n", g_failures);
		return 1;
	}
	printf("RESULT: PASS\n");
	return 0;
}
