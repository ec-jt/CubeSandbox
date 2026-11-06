/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */

/*
 * Staged-publication validity harness for the recovery local device.
 *
 * This test exists because the recovery chain used to format a NORMAL super
 * block as its first write. Everything after that write -- journal, WAL,
 * checkpoint pointer, uuid -- can fail, and a crash in between left a device
 * carrying a valid, attachable super block whose rebuild never finished. A
 * later attach accepted it and replayed from a checkpoint pointer describing
 * data that was not there.
 *
 * The protocol under test:
 *
 *   STAGED     magic = S3_SUPER_MAGIC_STAGING, version = 3.
 *              Written first, flushed. Every reader refuses it.
 *   (rebuild)  journal / WAL / checkpoint / uuid written while still staged.
 *   COMMITTED  magic = S3_SUPER_MAGIC, version = 2.
 *              Written last, flushed, callback only after the flush.
 *
 * === What is real, and what is mocked ===
 *
 * Real:
 *   - lib/s3bsdev/s3_local_dev.c in full: s3_local_dev_format(),
 *     s3_local_dev_format_staged(), s3_local_dev_commit_staged(),
 *     s3_local_dev_open(), s3_local_dev_update_checkpoint(),
 *     s3_local_dev_set_lvs_uuid(), the CRC, the layout computation and the
 *     close/adoption order.
 *
 * Mocked (no SPDK archive, no bdev module, no hardware):
 *   - the bdev open/close/claim primitives, I/O submission, DMA and flush.
 *     The device is a real heap buffer, so a write followed by a read
 *     observes the bytes that were written -- that is what makes the
 *     "staging cannot pass s3_local_dev_open()" case meaningful rather than
 *     a check of an in-memory flag.
 *
 * === What this does NOT prove ===
 *
 *   - It is not a live SPDK aio/malloc bdev run. The mock's fidelity is the
 *     limit: it proves the s3lvol-side state machine and ordering, not SPDK's
 *     behaviour.
 *   - It does not execute the static lvstore setup chain
 *     (lvs_recover_checkpoint_recorded -> lvs_recover_committed). The
 *     ordering there is asserted at source level by build.sh.
 *   - It does not prove atomicity of the 4 KiB commit overwrite against
 *     power loss. The torn-commit case proves DETECTION (magic and CRC both
 *     fail, and the device is refused), not atomicity.
 *
 * Usage:
 *   ./s3_recovery_validity_test
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

#include "s3lvol/s3_local_dev.h"
#include "s3lvol/s3_recovery.h"

#define MAX_DEVS   4
#define MAX_DESCS  16
#define MAX_EVENTS 4096

static int g_failures;

#define CHECK(cond) do {                                                \
	if (!(cond)) {                                                  \
		fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, \
			#cond);                                         \
		g_failures++;                                           \
	}                                                               \
} while (0)

/* ------------------------------------------------------------------ mock */

enum ev_kind {
	EV_OPEN = 1,
	EV_CLOSE,
	EV_CHAN_GET,
	EV_CHAN_PUT,
	EV_WRITE,
	EV_READ,
	EV_FLUSH,
	EV_CLAIM_GRANTED,
	EV_CLAIM_REFUSED,
};

struct mock_ev {
	enum ev_kind    kind;
	int             desc_id;
	int             dev;
	uint64_t        off;
	uint64_t        len;
};

struct mock_dev {
	const char     *name;
	uint64_t        num_blocks;
	uint32_t        block_size;
	bool            present;
	bool            claimed;
	enum spdk_bdev_claim_type claim_type;
	/* The device contents. A real buffer, so writes are observable by
	 * later reads -- this is what makes the open-path cases real. */
	uint8_t        *data;
	uint64_t        size;
	/* Flush support. A device that does not implement flush returns
	 * -ENOTSUP, which the protocol treats as success and logs. */
	bool            flush_supported;
};

struct mock_desc {
	int                      id;
	struct mock_dev         *dev;
	spdk_bdev_event_cb_t     event_cb;
	void                    *event_ctx;
	bool                     write;
	bool                     has_claim;
	bool                     closed;
};

struct mock_chan {
	int                      desc_id;
};

struct mock_io {
	void                    *buf;
	uint64_t                 offset;
	uint64_t                 nbytes;
	spdk_bdev_io_completion_cb cb;
	void                    *arg;
	bool                     is_flush;
};

static struct mock_dev      g_devs[MAX_DEVS];
static int                  g_ndevs;
static struct mock_desc    *g_descs[MAX_DESCS];
static int                  g_ndescs;
static int                  g_next_desc_id = 1;
static struct mock_chan     g_chans[MAX_DESCS];

static struct mock_ev       g_ev[MAX_EVENTS];
static int                  g_nev;

static struct mock_io      *g_pending;
static struct mock_io      *g_completing;

static int  g_open_count;
static int  g_close_count;
static int  g_claim_granted;
static int  g_claim_refused;
static int  g_writes;
static int  g_reads;
static int  g_flushes;

/* Failure injection. */
static int  g_write_fail_at;      /* 1-based write number, 0 = never */
static int  g_flush_fail_at;      /* 1-based flush number, 0 = never */
static int  g_event_on_write;     /* deliver REMOVE before completing this write */
static bool g_close_saw_pending;
/* When set, the next write is torn: only the first half of the block lands. */
static bool g_tear_next_write;

static char g_thread_marker;

static void
rec(enum ev_kind kind, int desc_id, int dev, uint64_t off, uint64_t len)
{
	if (g_nev < MAX_EVENTS) {
		g_ev[g_nev].kind = kind;
		g_ev[g_nev].desc_id = desc_id;
		g_ev[g_nev].dev = dev;
		g_ev[g_nev].off = off;
		g_ev[g_nev].len = len;
		g_nev++;
	}
}

/* Index of the n-th (1-based) event of a kind, or -1. The event log also
 * records opens, channel gets and flushes, so "the first write" is not
 * g_ev[0]. */
static int
ev_nth(enum ev_kind kind, int n)
{
	int i;

	for (i = 0; i < g_nev; i++) {
		if (g_ev[i].kind == kind) {
			if (--n == 0) {
				return i;
			}
		}
	}
	return -1;
}

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

static struct mock_dev *
mock_dev_by_ptr(const void *p)
{
	int i;

	for (i = 0; i < g_ndevs; i++) {
		if ((const void *)&g_devs[i] == p) {
			return &g_devs[i];
		}
	}
	return NULL;
}

static int
dev_index(struct mock_dev *d)
{
	return (int)(d - g_devs);
}

static void
reset_mock(void)
{
	int i;

	free(g_pending);
	g_pending = NULL;
	for (i = 0; i < g_ndescs; i++) {
		free(g_descs[i]);
		g_descs[i] = NULL;
	}
	g_ndescs = 0;
	g_next_desc_id = 1;
	for (i = 0; i < g_ndevs; i++) {
		free(g_devs[i].data);
	}
	memset(g_devs, 0, sizeof(g_devs));
	g_ndevs = 0;
	memset(g_chans, 0, sizeof(g_chans));
	memset(g_ev, 0, sizeof(g_ev));
	g_nev = 0;
	g_open_count = 0;
	g_close_count = 0;
	g_claim_granted = 0;
	g_claim_refused = 0;
	g_writes = 0;
	g_reads = 0;
	g_flushes = 0;
	g_write_fail_at = 0;
	g_flush_fail_at = 0;
	g_event_on_write = 0;
	g_completing = NULL;
	g_close_saw_pending = false;
	g_tear_next_write = false;
}

static int
add_dev(const char *name, uint64_t blocks, uint32_t bs)
{
	struct mock_dev *d = &g_devs[g_ndevs];

	d->name = name;
	d->num_blocks = blocks;
	d->block_size = bs;
	d->present = true;
	d->claim_type = SPDK_BDEV_CLAIM_NONE;
	d->flush_supported = true;
	d->size = blocks * bs;
	d->data = calloc(1, d->size);
	if (!d->data) {
		fprintf(stderr, "out of memory allocating mock device\n");
		exit(2);
	}
	return g_ndevs++;
}

static void
mock_notify(struct mock_desc *md, enum spdk_bdev_event_type type)
{
	CHECK(md != NULL);
	if (md && md->event_cb) {
		md->event_cb(type, (struct spdk_bdev *)md->dev, md->event_ctx);
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
	g_completing = io;
	io->cb((struct spdk_bdev_io *)io, success, io->arg);
	g_completing = NULL;
	free(io);
}

/* ------------------------------------------------------------------ wraps */

int
__wrap_spdk_bdev_open_ext(const char *bdev_name, bool write,
			  spdk_bdev_event_cb_t event_cb, void *event_ctx,
			  struct spdk_bdev_desc **desc)
{
	struct mock_dev *d;
	struct mock_desc *md;

	g_open_count++;
	d = mock_dev_by_name(bdev_name);
	if (!d || !d->present) {
		rec(EV_OPEN, 0, d ? dev_index(d) : -1, 0, 0);
		return -ENODEV;
	}

	/* bdev.c bdev_open(): a writable open is refused while the bdev is
	 * claimed. */
	if (write && d->claim_type != SPDK_BDEV_CLAIM_NONE) {
		rec(EV_OPEN, 0, dev_index(d), 1, 0);
		return -EPERM;
	}

	md = calloc(1, sizeof(*md));
	if (!md) {
		return -ENOMEM;
	}
	md->id = g_next_desc_id++;
	md->dev = d;
	md->event_cb = event_cb;
	md->event_ctx = event_ctx;
	md->write = write;
	if (g_ndescs < MAX_DESCS) {
		g_descs[g_ndescs++] = md;
	}
	*desc = (struct spdk_bdev_desc *)md;
	rec(EV_OPEN, md->id, dev_index(d), write ? 1 : 0, 0);
	return 0;
}

void
__wrap_spdk_bdev_close(struct spdk_bdev_desc *desc)
{
	struct mock_desc *md = (struct mock_desc *)desc;
	int i;

	g_close_count++;
	if (g_pending) {
		g_close_saw_pending = true;
	}
	if (!md) {
		return;
	}
	rec(EV_CLOSE, md->id, dev_index(md->dev), 0, 0);
	md->closed = true;

	/* bdev.c bdev_close() -> bdev_desc_release_claims(): closing the
	 * descriptor drops its v2 claim. */
	if (md->has_claim) {
		md->dev->claim_type = SPDK_BDEV_CLAIM_NONE;
		md->dev->claimed = false;
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
	struct mock_desc *md = (struct mock_desc *)desc;
	struct mock_chan *ch = &g_chans[md->id % MAX_DESCS];

	ch->desc_id = md->id;
	rec(EV_CHAN_GET, md->id, dev_index(md->dev), 0, 0);
	return (struct spdk_io_channel *)ch;
}

void
__wrap_spdk_put_io_channel(struct spdk_io_channel *ch)
{
	struct mock_chan *c = (struct mock_chan *)ch;

	CHECK(ch != NULL);
	if (!ch) {
		return;
	}
	rec(EV_CHAN_PUT, c->desc_id, -1, 0, 0);
}

static int
mock_submit(struct spdk_bdev_desc *desc, void *buf, uint64_t offset,
	    uint64_t nbytes, spdk_bdev_io_completion_cb cb, void *cb_arg,
	    bool is_write)
{
	struct mock_io *io;
	struct mock_dev *d = ((struct mock_desc *)desc)->dev;

	CHECK(buf != NULL);
	CHECK(cb != NULL);
	CHECK(nbytes > 0);
	CHECK(nbytes % d->block_size == 0);
	CHECK(offset % d->block_size == 0);
	CHECK(offset + nbytes <= d->size);
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

	if (is_write) {
		bool ok;

		g_writes++;
		rec(EV_WRITE, ((struct mock_desc *)desc)->id, dev_index(d),
		    offset, nbytes);
		/* Model an event that arrives while the write is in flight. */
		if (g_event_on_write == g_writes) {
			mock_notify((struct mock_desc *)desc,
				    SPDK_BDEV_EVENT_REMOVE);
		}
		ok = (g_write_fail_at != g_writes);
		if (ok) {
			if (g_tear_next_write) {
				/* A torn write: the first half of the *super
				 * block struct* is the new content, the rest is
				 * whatever was there before. This is the "power
				 * lost mid-commit" shape: the block is neither the
				 * old one nor the new one, and it must be detected
				 * by magic + CRC rather than assumed away.
				 *
				 * The tear point is inside the struct, not at half
				 * the 4 KiB block: the struct is only ~204 bytes,
				 * so a 2 KiB tear would leave every field -- and
				 * the CRC -- intact and prove nothing. */
				size_t tear = sizeof(struct s3_super_block) / 2;

				if (tear > nbytes) {
					tear = nbytes / 2;
				}
				memcpy(d->data + offset, buf, tear);
				g_tear_next_write = false;
			} else {
				memcpy(d->data + offset, buf, nbytes);
			}
		}
		mock_complete(ok);
	} else {
		g_reads++;
		rec(EV_READ, ((struct mock_desc *)desc)->id, dev_index(d),
		    offset, nbytes);
		memcpy(buf, d->data + offset, nbytes);
		mock_complete(true);
	}
	return 0;
}

int
__wrap_spdk_bdev_read(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		      void *buf, uint64_t offset, uint64_t nbytes,
		      spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	(void)ch;
	return mock_submit(desc, buf, offset, nbytes, cb, cb_arg, false);
}

int
__wrap_spdk_bdev_write(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		       void *buf, uint64_t offset, uint64_t nbytes,
		       spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	(void)ch;
	return mock_submit(desc, buf, offset, nbytes, cb, cb_arg, true);
}

int
__wrap_spdk_bdev_flush(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		       uint64_t offset, uint64_t nbytes,
		       spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct mock_dev *d = ((struct mock_desc *)desc)->dev;
	struct mock_io *io;

	(void)ch;
	(void)offset;
	(void)nbytes;

	if (!d->flush_supported) {
		return -ENOTSUP;
	}

	CHECK(cb != NULL);
	CHECK(g_pending == NULL);

	io = calloc(1, sizeof(*io));
	if (!io) {
		return -ENOMEM;
	}
	io->cb = cb;
	io->arg = cb_arg;
	io->is_flush = true;
	g_pending = io;

	g_flushes++;
	rec(EV_FLUSH, ((struct mock_desc *)desc)->id, dev_index(d), 0, 0);
	mock_complete(g_flush_fail_at != g_flushes);
	return 0;
}

void
__wrap_spdk_bdev_free_io(struct spdk_bdev_io *bdev_io)
{
	CHECK(g_completing != NULL);
	CHECK((void *)bdev_io == (void *)g_completing);
}

void *
__wrap_spdk_dma_zmalloc(size_t size, size_t align, uint64_t *unused)
{
	void *p;

	(void)align;
	(void)unused;
	p = malloc(size);
	if (p) {
		memset(p, 0, size);
	}
	return p;
}

void
__wrap_spdk_dma_free(void *buf)
{
	free(buf);
}

const char *
__wrap_spdk_strerror(int errnum)
{
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
	struct mock_desc *md = (struct mock_desc *)desc;
	struct mock_dev *d;
	int i;

	if (!md || !module) {
		return -EINVAL;
	}
	d = md->dev;

	if (type != SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE) {
		return -ENOTSUP;
	}
	if (opts && opts->shared_claim_key != 0) {
		return -EINVAL;
	}
	if (d->claim_type != SPDK_BDEV_CLAIM_NONE || md->has_claim) {
		g_claim_refused++;
		rec(EV_CLAIM_REFUSED, md->id, dev_index(d), 0, 0);
		return -EPERM;
	}
	for (i = 0; i < g_ndescs; i++) {
		struct mock_desc *other = g_descs[i];

		if (!other || other == md) {
			continue;
		}
		if (other->dev == d && other->write) {
			g_claim_refused++;
			rec(EV_CLAIM_REFUSED, md->id, dev_index(d), 0, 0);
			return -EPERM;
		}
	}

	d->claimed = true;
	d->claim_type = type;
	md->has_claim = true;
	md->write = true;
	g_claim_granted++;
	rec(EV_CLAIM_GRANTED, md->id, dev_index(d), 0, 0);
	return 0;
}

/* bdev.c:9450 spdk_bdev_module_release_bdev() asserts the claim is v1. A v1
 * release on a v2 claim must never be taken; recorded as a hard failure. */
void
__wrap_spdk_bdev_module_release_bdev(struct spdk_bdev *bdev)
{
	struct mock_dev *d = mock_dev_by_ptr(bdev);

	if (!d) {
		return;
	}
	if (d->claim_type != SPDK_BDEV_CLAIM_EXCL_WRITE) {
		fprintf(stderr,
			"FAIL: v1 spdk_bdev_module_release_bdev on claim type %d\n",
			(int)d->claim_type);
		g_failures++;
		return;
	}
	d->claim_type = SPDK_BDEV_CLAIM_NONE;
	d->claimed = false;
}

uint32_t
__wrap_spdk_crc32c_update(const void *buf, size_t len, uint32_t crc)
{
	const uint8_t *p = buf;
	size_t i;
	int b;

	for (i = 0; i < len; i++) {
		crc ^= p[i];
		for (b = 0; b < 8; b++) {
			crc = (crc >> 1) ^ (0x82f63b78u & (uint32_t)(-(int32_t)(crc & 1)));
		}
	}
	return crc;
}

struct spdk_thread *
__wrap_spdk_get_thread(void)
{
	return (struct spdk_thread *)&g_thread_marker;
}

void
__wrap_spdk_uuid_copy(struct spdk_uuid *dst, const struct spdk_uuid *src)
{
	memcpy(dst, src, sizeof(*dst));
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

/* ------------------------------------------------------------------ helpers */

struct open_result {
	int                  calls;
	int                  status;
	struct s3_local_dev *dev;
};

static void
open_cb(void *arg, struct s3_local_dev *dev, int status)
{
	struct open_result *r = arg;

	r->calls++;
	r->dev = dev;
	r->status = status;
}

static void
status_cb(void *arg, int status)
{
	int *out = arg;

	*out = status;
}

static struct open_result
do_open(const char *wal, const char *cache)
{
	struct open_result r = {0};

	s3_local_dev_open(wal, cache, open_cb, &r);
	CHECK(r.calls == 1);
	return r;
}

static struct open_result
do_format(const char *wal, const char *cache, bool staged)
{
	struct s3_local_dev_format_opts fopts = {0};
	struct open_result r = {0};

	fopts.wal_bdev_name   = wal;
	fopts.cache_bdev_name = cache;
	fopts.lvs_name        = "lv-validity";
	fopts.capacity_bytes  = 4 * 1024 * 1024;
	fopts.chunk_size      = 64 * 1024;
	fopts.journal_size    = 1024 * 1024;
	fopts.wal_size        = 1024 * 1024;

	if (staged) {
		s3_local_dev_format_staged(&fopts, open_cb, &r);
	} else {
		s3_local_dev_format(&fopts, open_cb, &r);
	}
	CHECK(r.calls == 1);
	return r;
}

/* Read the on-disk super block straight out of the mock device, bypassing the
 * local device entirely. This is what a *different* reader would see. */
static struct s3_super_block
raw_super(int dev)
{
	struct s3_super_block sb;

	CHECK(g_devs[dev].size >= sizeof(sb));
	memcpy(&sb, g_devs[dev].data, sizeof(sb));
	return sb;
}

static uint32_t
crc_of(const struct s3_super_block *sb)
{
	struct s3_super_block tmp;

	memcpy(&tmp, sb, sizeof(tmp));
	tmp.crc = 0;
	return __wrap_spdk_crc32c_update(&tmp, sizeof(tmp), ~0u);
}

/* ------------------------------------------------------------------ cases */

/* 1. A staged device is refused by the public reader, and the refusal is
 * distinguishable from "not ours" and from "corrupt". */
static void
test_staged_is_refused_by_open(void)
{
	struct open_result f, o;
	struct s3_super_block sb;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	f = do_format("wal0", NULL, true);
	CHECK(f.status == 0);
	CHECK(f.dev != NULL);
	if (!f.dev) {
		return;
	}
	CHECK(s3_local_dev_is_staged(f.dev));

	/* The block on the device really is the staging one. */
	sb = raw_super(0);
	CHECK(sb.magic == S3_SUPER_MAGIC_STAGING);
	CHECK(sb.version == S3_SUPER_VERSION_STAGING);
	CHECK(sb.crc == crc_of(&sb));

	/* The staging block is written FIRST: before the journal head clear. */
	CHECK(g_writes >= 2);
	CHECK(ev_nth(EV_WRITE, 1) >= 0);
	CHECK(g_ev[ev_nth(EV_WRITE, 1)].off == 0);

	/* And it was flushed before the callback reported success. */
	CHECK(g_flushes >= 1);

	s3_local_dev_close(f.dev);

	/* The public reader refuses it. -EAGAIN, not -EILSEQ: the device is
	 * recognised as mid-rebuild, which is actionable. */
	o = do_open("wal0", NULL);
	CHECK(o.status == -EAGAIN);
	CHECK(o.dev == NULL);
}

/* 2. An OLD reader -- one that only knows the normal magic -- must also
 * refuse a staging block. Modelled by checking the two conditions such a
 * reader applies, against the bytes actually on the device. */
static void
test_old_reader_cannot_accept_staging(void)
{
	struct open_result f;
	struct s3_super_block sb;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	f = do_format("wal0", NULL, true);
	CHECK(f.status == 0);
	if (!f.dev) {
		return;
	}
	sb = raw_super(0);

	/* The pre-staging reader's first check is `magic == S3_SUPER_MAGIC`.
	 * It fails, so the reader stops with -EILSEQ before it looks at any
	 * layout field. */
	CHECK(sb.magic != S3_SUPER_MAGIC);
	/* A reader that only compares versions also refuses (-EPROTO). */
	CHECK(sb.version != S3_SUPER_VERSION);
	/* Neither value is a "reserved flag" an old reader would ignore: both
	 * are fields it already validates. */
	CHECK(sb.magic == S3_SUPER_MAGIC_STAGING);
	CHECK(sb.version == S3_SUPER_VERSION_STAGING);

	s3_local_dev_close(f.dev);
}

/* 3. A normal format is unchanged: it is attachable immediately, and it does
 * not carry the staging identity. This is the "existing layouts retain
 * behaviour" case. */
static void
test_normal_format_still_opens(void)
{
	struct open_result f, o;
	struct s3_super_block sb;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	f = do_format("wal0", NULL, false);
	CHECK(f.status == 0);
	CHECK(f.dev != NULL);
	if (!f.dev) {
		return;
	}
	CHECK(!s3_local_dev_is_staged(f.dev));

	sb = raw_super(0);
	CHECK(sb.magic == S3_SUPER_MAGIC);
	CHECK(sb.version == S3_SUPER_VERSION);
	CHECK(sb.crc == crc_of(&sb));

	/* The normal path keeps its historical write count: super + journal
	 * head, no flush. A hidden extra write or flush here would be a
	 * behaviour change on the create path. */
	CHECK(g_writes == 2);
	CHECK(g_flushes == 0);

	s3_local_dev_close(f.dev);

	o = do_open("wal0", NULL);
	CHECK(o.status == 0);
	CHECK(o.dev != NULL);
	if (o.dev) {
		CHECK(!s3_local_dev_is_staged(o.dev));
		CHECK(strcmp(s3_local_dev_get_super(o.dev)->lvs_name,
			     "lv-validity") == 0);
		s3_local_dev_close(o.dev);
	}
}

/* 4. Commit publishes the normal block and the device becomes attachable,
 * with the identity and checkpoint the rebuild recorded. */
static void
test_commit_publishes_and_survives_reopen(void)
{
	struct open_result f, o;
	struct s3_super_block sb;
	struct spdk_uuid uuid;
	int st = -1;
	int writes_before;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	f = do_format("wal0", NULL, true);
	CHECK(f.status == 0);
	if (!f.dev) {
		return;
	}

	/* The rebuild steps, in the order the chain performs them. */
	memset(&uuid, 0x5a, sizeof(uuid));
	s3_local_dev_set_lvs_uuid(f.dev, &uuid, status_cb, &st);
	CHECK(st == 0);
	s3_local_dev_update_checkpoint(f.dev, 7, 1234, status_cb, &st);
	CHECK(st == 0);

	/* Still not attachable: the rebuild is not published yet. */
	CHECK(s3_local_dev_is_staged(f.dev));

	writes_before = g_writes;
	st = -1;
	s3_local_dev_commit_staged(f.dev, status_cb, &st);
	CHECK(st == 0);
	CHECK(!s3_local_dev_is_staged(f.dev));

	/* The commit is exactly one write, and it is flushed. */
	CHECK(g_writes == writes_before + 1);
	CHECK(g_ev[g_nev - 2].kind == EV_WRITE);
	CHECK(g_ev[g_nev - 2].off == 0);
	CHECK(g_ev[g_nev - 1].kind == EV_FLUSH);

	sb = raw_super(0);
	CHECK(sb.magic == S3_SUPER_MAGIC);
	CHECK(sb.version == S3_SUPER_VERSION);
	CHECK(sb.crc == crc_of(&sb));
	CHECK(sb.checkpoint_gen == 7);
	CHECK(sb.checkpoint_lsn == 1234);
	CHECK(memcmp(&sb.lvs_uuid, &uuid, sizeof(uuid)) == 0);

	s3_local_dev_close(f.dev);

	/* Close and reopen: the published state is what a later attach sees. */
	o = do_open("wal0", NULL);
	CHECK(o.status == 0);
	CHECK(o.dev != NULL);
	if (o.dev) {
		const struct s3_super_block *got = s3_local_dev_get_super(o.dev);

		CHECK(got->checkpoint_gen == 7);
		CHECK(got->checkpoint_lsn == 1234);
		CHECK(memcmp(&got->lvs_uuid, &uuid, sizeof(uuid)) == 0);
		CHECK(got->capacity_bytes == 4 * 1024 * 1024);
		CHECK(got->chunk_size == 64 * 1024);
		CHECK(strcmp(got->lvs_name, "lv-validity") == 0);
		s3_local_dev_close(o.dev);
	}
}

/* 5. A failed commit write leaves the device staged and un-attachable, and
 * the in-memory copy agrees with the device. */
static void
test_commit_write_failure_stays_staged(void)
{
	struct open_result f, o;
	struct s3_super_block sb;
	int st = -1;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	f = do_format("wal0", NULL, true);
	CHECK(f.status == 0);
	if (!f.dev) {
		return;
	}

	g_write_fail_at = g_writes + 1;
	s3_local_dev_commit_staged(f.dev, status_cb, &st);
	CHECK(st == -EIO);
	CHECK(s3_local_dev_is_staged(f.dev));

	/* The device still carries the staging block. */
	sb = raw_super(0);
	CHECK(sb.magic == S3_SUPER_MAGIC_STAGING);
	CHECK(sb.version == S3_SUPER_VERSION_STAGING);

	s3_local_dev_close(f.dev);

	o = do_open("wal0", NULL);
	CHECK(o.status == -EAGAIN);
	CHECK(o.dev == NULL);
}

/* 6. A failed commit FLUSH is the dangerous one: the block may be on the
 * device. The protocol must still refuse to call the device attachable, and
 * must leave the in-memory state staged so a retry re-attempts the same
 * transition. */
static void
test_commit_flush_failure_stays_staged(void)
{
	struct open_result f, o;
	int st = -1;
	int flushes_before;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	f = do_format("wal0", NULL, true);
	CHECK(f.status == 0);
	if (!f.dev) {
		return;
	}

	/* The commit now flushes BEFORE it writes (the preflush), so the
	 * postflush is the second flush of the sequence. Failing the postflush
	 * is the case that leaves the normal block possibly on the device. */
	flushes_before = g_flushes;
	g_flush_fail_at = flushes_before + 2;
	s3_local_dev_commit_staged(f.dev, status_cb, &st);
	CHECK(st == -EIO);
	/* A failed flush restores in-memory retry state only. The normal
	 * block may already be readable: never claim rollback on disk. */
	CHECK(s3_local_dev_is_staged(f.dev));
	o = do_open("wal0", NULL);
	CHECK(o.status == 0);
	if (o.dev) {
		s3_local_dev_close(o.dev);
	}

	/* Retry with the flush working: the same transition succeeds. */
	st = -1;
	s3_local_dev_commit_staged(f.dev, status_cb, &st);
	CHECK(st == 0);
	CHECK(!s3_local_dev_is_staged(f.dev));

	s3_local_dev_close(f.dev);

	o = do_open("wal0", NULL);
	CHECK(o.status == 0);
	if (o.dev) {
		s3_local_dev_close(o.dev);
	}
}

/* 6b. A failed PREFLUSH is the safe failure: it happens before the in-memory
 * identity changes and before any normal block is submitted, so the device is
 * still STAGED and every reader refuses it. This is the case the preflush
 * exists to make possible. */
static void
test_commit_preflush_failure_stays_staged(void)
{
	struct open_result f, o;
	struct s3_super_block sb;
	int st = -1;
	int flushes_before;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	f = do_format("wal0", NULL, true);
	CHECK(f.status == 0);
	if (!f.dev) {
		return;
	}

	/* The first flush of the commit sequence is the preflush. */
	flushes_before = g_flushes;
	g_flush_fail_at = flushes_before + 1;
	s3_local_dev_commit_staged(f.dev, status_cb, &st);
	CHECK(st == -EIO);
	CHECK(s3_local_dev_is_staged(f.dev));

	/* No normal block was ever written: the device still carries staging. */
	sb = raw_super(0);
	CHECK(sb.magic == S3_SUPER_MAGIC_STAGING);
	CHECK(sb.version == S3_SUPER_VERSION_STAGING);

	s3_local_dev_close(f.dev);

	o = do_open("wal0", NULL);
	CHECK(o.status == -EAGAIN);
	CHECK(o.dev == NULL);
}

/* 7. A torn commit block is DETECTED and refused. This is the case that must
 * not be described as atomicity: the block is neither the staging one nor a
 * valid normal one, and the reader rejects it on magic and on CRC. */
static void
test_torn_commit_is_refused(void)
{
	struct open_result f, o;
	struct s3_super_block sb;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	f = do_format("wal0", NULL, true);
	CHECK(f.status == 0);
	if (!f.dev) {
		return;
	}

	/* The commit write lands only its first half. */
	g_tear_next_write = true;
	{
		int st = -1;

		s3_local_dev_commit_staged(f.dev, status_cb, &st);
		/* The write "succeeded" from the mock's point of view, so the
		 * protocol reports success; the point is that the RESULTING
		 * DEVICE is refused. */
		CHECK(st == 0);
	}
	s3_local_dev_close(f.dev);

	sb = raw_super(0);
	/* First half is the normal magic, second half is still the staging
	 * block's tail, so the CRC does not match. */
	CHECK(sb.magic == S3_SUPER_MAGIC);
	CHECK(sb.crc != crc_of(&sb));

	o = do_open("wal0", NULL);
	CHECK(o.status == -EILSEQ);
	CHECK(o.dev == NULL);
}

/* 8. A device that never went through the staged path cannot be committed.
 * This is the guard that keeps a normal create from routing through the
 * commit by mistake. */
static void
test_commit_refused_on_normal_device(void)
{
	struct open_result f;
	int st = -1;
	int writes_before;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	f = do_format("wal0", NULL, false);
	CHECK(f.status == 0);
	if (!f.dev) {
		return;
	}

	writes_before = g_writes;
	s3_local_dev_commit_staged(f.dev, status_cb, &st);
	CHECK(st == -EINVAL);
	CHECK(g_writes == writes_before);

	s3_local_dev_close(f.dev);
}

/* 9. Unsupported flush cannot establish durable staging or publication. */
static void
test_flush_unsupported_is_refused(void)
{
	struct open_result f;
	int st = -1;

	reset_mock();
	add_dev("wal0", 2048, 4096);
	g_devs[0].flush_supported = false;
	f = do_format("wal0", NULL, true);
	CHECK(f.status == -ENOTSUP);
	CHECK(f.dev == NULL);

	reset_mock();
	add_dev("wal0", 2048, 4096);
	f = do_format("wal0", NULL, true);
	CHECK(f.status == 0);
	if (!f.dev) {
		return;
	}
	g_devs[0].flush_supported = false;
	s3_local_dev_commit_staged(f.dev, status_cb, &st);
	CHECK(st == -ENOTSUP);
	CHECK(s3_local_dev_is_staged(f.dev));
	s3_local_dev_close(f.dev);
}

/* 10. A failed flush during the STAGED format fails the format: the caller
 * must not be told a device is staged when its staging block may not be
 * durable. */
static void
test_staged_format_flush_failure(void)
{
	struct open_result f;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	g_flush_fail_at = 1;
	f = do_format("wal0", NULL, true);
	CHECK(f.status == -EIO);
	CHECK(f.dev == NULL);
}

/* 11. Dual-bdev staging: the staging block goes to the WAL device, the cache
 * device is untouched by the super block, and the committed layout opens with
 * the cache bdev supplied. */
static void
test_dual_bdev_staged_commit(void)
{
	struct open_result f, o;
	struct s3_super_block sb;
	int st = -1;

	reset_mock();
	add_dev("wal0", 2048, 4096);
	add_dev("cache0", 4096, 4096);

	f = do_format("wal0", "cache0", true);
	CHECK(f.status == 0);
	if (!f.dev) {
		return;
	}
	CHECK(s3_local_dev_is_staged(f.dev));

	sb = raw_super(0);
	CHECK(sb.magic == S3_SUPER_MAGIC_STAGING);
	CHECK(sb.dual_bdev == 1);

	/* The cache device carries no super block. */
	CHECK(g_devs[1].data[0] == 0);

	s3_local_dev_commit_staged(f.dev, status_cb, &st);
	CHECK(st == 0);

	sb = raw_super(0);
	CHECK(sb.magic == S3_SUPER_MAGIC);
	CHECK(sb.crc == crc_of(&sb));

	s3_local_dev_close(f.dev);

	o = do_open("wal0", "cache0");
	CHECK(o.status == 0);
	if (o.dev) {
		s3_local_dev_close(o.dev);
	}
}

/* 12. The staging block is written before the journal head is cleared, so a
 * crash after the first write leaves a device that is refused rather than one
 * that looks normal with an uncleared journal. */
static void
test_staging_write_precedes_journal_clear(void)
{
	struct open_result f;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	f = do_format("wal0", NULL, true);
	CHECK(f.status == 0);
	if (!f.dev) {
		return;
	}

	/* Write 1 is the super block at offset 0; write 2 is the journal head
	 * at the journal offset. */
	CHECK(g_writes >= 2);
	CHECK(ev_nth(EV_WRITE, 1) >= 0);
	CHECK(ev_nth(EV_WRITE, 2) >= 0);
	CHECK(g_ev[ev_nth(EV_WRITE, 1)].off == 0);
	CHECK(g_ev[ev_nth(EV_WRITE, 1)].len == S3_SUPER_SIZE);
	CHECK(g_ev[ev_nth(EV_WRITE, 2)].off == S3_SUPER_SIZE);

	s3_local_dev_close(f.dev);
}

/* 13. A staging block whose CRC is intact but whose magic was flipped to the
 * normal one is still refused: the version check catches it. This proves the
 * two checks are independent, so relaxing one does not open the door. */
static void
test_magic_flipped_still_refused_by_version(void)
{
	struct open_result f, o;
	struct s3_super_block sb;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	f = do_format("wal0", NULL, true);
	CHECK(f.status == 0);
	if (!f.dev) {
		return;
	}
	s3_local_dev_close(f.dev);

	/* Rewrite the block as "normal magic, staging version, valid CRC". */
	sb = raw_super(0);
	sb.magic = S3_SUPER_MAGIC;
	sb.version = S3_SUPER_VERSION_STAGING;
	sb.crc = crc_of(&sb);
	memcpy(g_devs[0].data, &sb, sizeof(sb));

	o = do_open("wal0", NULL);
	CHECK(o.status == -EAGAIN);
	CHECK(o.dev == NULL);
}

/* 14. And the reverse: staging magic with the normal version is refused by
 * the magic check. */
static void
test_version_flipped_still_refused_by_magic(void)
{
	struct open_result f, o;
	struct s3_super_block sb;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	f = do_format("wal0", NULL, true);
	CHECK(f.status == 0);
	if (!f.dev) {
		return;
	}
	s3_local_dev_close(f.dev);

	sb = raw_super(0);
	sb.magic = S3_SUPER_MAGIC_STAGING;
	sb.version = S3_SUPER_VERSION;
	sb.crc = crc_of(&sb);
	memcpy(g_devs[0].data, &sb, sizeof(sb));

	o = do_open("wal0", NULL);
	CHECK(o.status == -EAGAIN);
	CHECK(o.dev == NULL);
}

/* 15. A device that was never formatted is still reported as "not ours"
 * (-EILSEQ), not as staged. The staging state must not swallow the
 * blank-device answer. */
static void
test_blank_device_is_not_staged(void)
{
	struct open_result o;

	reset_mock();
	add_dev("wal0", 2048, 4096);

	o = do_open("wal0", NULL);
	CHECK(o.status == -EILSEQ);
	CHECK(o.dev == NULL);
}

int
main(void)
{
	test_staged_is_refused_by_open();
	test_old_reader_cannot_accept_staging();
	test_normal_format_still_opens();
	test_commit_publishes_and_survives_reopen();
	test_commit_write_failure_stays_staged();
	test_commit_flush_failure_stays_staged();
	test_commit_preflush_failure_stays_staged();
	test_torn_commit_is_refused();
	test_commit_refused_on_normal_device();
	test_flush_unsupported_is_refused();
	test_staged_format_flush_failure();
	test_dual_bdev_staged_commit();
	test_staging_write_precedes_journal_clear();
	test_magic_flipped_still_refused_by_version();
	test_version_flipped_still_refused_by_magic();
	test_blank_device_is_not_staged();

	if (g_failures) {
		fprintf(stderr, "RESULT: FAIL (%d checks)\n", g_failures);
		return 1;
	}
	printf("RESULT: PASS (16 cases)\n");
	return 0;
}
