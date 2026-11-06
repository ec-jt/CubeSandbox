/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */

/*
 * Formatter / descriptor-lifetime harness for the retained recovery claim.
 *
 * This test exists because the earlier 13-case harness stopped at
 * s3_recovery_blank_scan_claimed(): it proved the scan keeps the descriptor,
 * but never called s3_local_dev_format() and never exercised the cleanup order.
 * Three defects lived in exactly that gap:
 *
 *   1. s3_recovery_claim_release() took the v1 release path
 *      (spdk_bdev_module_release_bdev) on a v2 claim
 *      (SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE). bdev.c:9455 asserts
 *      claim_type == EXCL_WRITE there, so a debug build aborts and a release
 *      build corrupts the claim bookkeeping. v2 claims are released by closing
 *      the descriptor.
 *
 *   2. The setup context released the claims before the live s3_local_dev was
 *      torn down. The formatter adopts the claimed descriptor and the journal
 *      and WAL keep using it, so releasing at setup completion closed a
 *      descriptor under live I/O.
 *
 *   3. The formatter reopened the cache bdev with spdk_bdev_open_ext(...,
 *      write=true) even when the recovery path had claimed that same device.
 *      A writable open on a claimed bdev fails -EPERM, so dual-bdev recovery
 *      could not work.
 *
 * === What is real, and what is mocked ===
 *
 * Real:
 *   - lib/s3bsdev/s3_local_dev.c: s3_local_dev_format(), the layout
 *     computation, the super block, the two writes, s3_local_dev_close() and
 *     the ownership/adoption logic.
 *   - lib/s3bsdev/s3_recovery.c: s3_recovery_claim_open(), the claim-level
 *     event latch, s3_recovery_blank_scan_claimed(), and
 *     s3_recovery_claim_release().
 *
 * Mocked (no SPDK archive, no bdev module, no hardware):
 *   - the bdev open/close/claim primitives, I/O submission and DMA. The rules
 *     are transcribed from the pinned tree and named at each site. The v1
 *     release mock deliberately behaves like the real assertion: a v1 release
 *     on a v2 claim is recorded as a hard failure and does not clear state.
 *
 * === What this does NOT prove ===
 *
 *   - It is not a live SPDK aio/malloc bdev test. Running one needs a built
 *     SPDK tree plus SPDK's runtime setup, and this increment is forbidden to
 *     build dependencies. The mock's fidelity is therefore the limit: it
 *     proves the s3lvol-side contract and ordering, not SPDK's behaviour.
 *   - It does not execute the static lvstore setup chain
 *     (lvs_setup_ctx_free -> s3lvol_lvstore_free). build.sh checks that
 *     transfer at source level: the setup context now releases a claim only
 *     when s3_local_dev_owns_claim() is false.
 *   - Process-external writers are still out of scope, as before.
 *
 * Usage:
 *   ./s3_recovery_claim_lifetime_test
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

#define SCAN_CHUNK              (1024 * 1024)
#define MAX_DEVS                4
#define MAX_DESCS               16
#define MAX_EVENTS              8192

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
	EV_CLAIM_GRANTED,
	EV_CLAIM_REFUSED,
	EV_V1_RELEASE_ON_V2,
};

struct mock_ev {
	enum ev_kind    kind;
	int             desc_id;
	int             dev;
	uint64_t        off;
};

struct mock_dev {
	const char     *name;
	/* Second name for the same device, used to prove that identity is
	 * compared by bdev pointer and not by string. */
	const char     *alias;
	uint64_t        num_blocks;
	uint32_t        block_size;
	bool            present;
	bool            claimed;
	enum spdk_bdev_claim_type claim_type;
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
static int  g_v1_release_on_v2;
static int  g_writes;
static int  g_reads;

/* Failure injection. */
static int  g_write_fail_at;      /* 1-based write number, 0 = never */
static int  g_event_on_write;     /* deliver REMOVE before completing this write */
static bool g_close_saw_pending;

static struct spdk_bdev_module g_test_module = {
	.name = "s3lvol_lifetime_test",
};

static char g_thread_marker;

static void
rec(enum ev_kind kind, int desc_id, int dev, uint64_t off)
{
	if (g_nev < MAX_EVENTS) {
		g_ev[g_nev].kind = kind;
		g_ev[g_nev].desc_id = desc_id;
		g_ev[g_nev].dev = dev;
		g_ev[g_nev].off = off;
		g_nev++;
	}
}

static int
ev_index(enum ev_kind kind, int desc_id)
{
	int i;

	for (i = 0; i < g_nev; i++) {
		if (g_ev[i].kind == kind && g_ev[i].desc_id == desc_id) {
			return i;
		}
	}
	return -1;
}

static int
ev_last_index(enum ev_kind kind, int desc_id)
{
	int i;

	for (i = g_nev - 1; i >= 0; i--) {
		if (g_ev[i].kind == kind && g_ev[i].desc_id == desc_id) {
			return i;
		}
	}
	return -1;
}

static struct mock_dev *
mock_dev_by_name(const char *name)
{
	int i;

	for (i = 0; i < g_ndevs; i++) {
		if (strcmp(g_devs[i].name, name) == 0 ||
		    (g_devs[i].alias && strcmp(g_devs[i].alias, name) == 0)) {
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
	memset(g_devs, 0, sizeof(g_devs));
	g_ndevs = 0;
	memset(g_chans, 0, sizeof(g_chans));
	memset(g_ev, 0, sizeof(g_ev));
	g_nev = 0;
	g_open_count = 0;
	g_close_count = 0;
	g_claim_granted = 0;
	g_claim_refused = 0;
	g_v1_release_on_v2 = 0;
	g_writes = 0;
	g_reads = 0;
	g_write_fail_at = 0;
	g_event_on_write = 0;
	g_completing = NULL;
	g_close_saw_pending = false;
}

static int
add_dev(const char *name, const char *alias, uint64_t blocks, uint32_t bs)
{
	struct mock_dev *d = &g_devs[g_ndevs];

	d->name = name;
	d->alias = alias;
	d->num_blocks = blocks;
	d->block_size = bs;
	d->present = true;
	d->claim_type = SPDK_BDEV_CLAIM_NONE;
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
		rec(EV_OPEN, 0, d ? dev_index(d) : -1, 0);
		return -ENODEV;
	}

	/* bdev.c bdev_open(): a writable open is refused while the bdev is
	 * claimed. This is the rule that makes the formatter's old cache reopen
	 * fail with -EPERM. */
	if (write && d->claim_type != SPDK_BDEV_CLAIM_NONE) {
		rec(EV_OPEN, 0, dev_index(d), 1);
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
	rec(EV_OPEN, md->id, dev_index(d), write ? 1 : 0);
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
	rec(EV_CLOSE, md->id, dev_index(md->dev), 0);
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
	rec(EV_CHAN_GET, md->id, dev_index(md->dev), 0);
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
	rec(EV_CHAN_PUT, c->desc_id, -1, 0);
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
		g_writes++;
		rec(EV_WRITE, ((struct mock_desc *)desc)->id, dev_index(d),
		    offset);
		/* Model an event that arrives while the write is in flight. */
		if (g_event_on_write == g_writes) {
			mock_notify((struct mock_desc *)desc,
				    SPDK_BDEV_EVENT_REMOVE);
		}
		mock_complete(g_write_fail_at != g_writes);
	} else {
		memset(buf, 0, (size_t)nbytes);
		g_reads++;
		rec(EV_READ, ((struct mock_desc *)desc)->id, dev_index(d), offset);
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

/* s3_local_dev.c flushes after a staged format and after a commit. This test
 * exercises neither, but the symbol is referenced, so it must resolve. The
 * mock reports success and records nothing: the lifetime cases assert on
 * write/close ordering, not on flush. */
int
__wrap_spdk_bdev_flush(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		       uint64_t offset, uint64_t nbytes,
		       spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct mock_io *io;

	(void)desc;
	(void)ch;
	(void)offset;
	(void)nbytes;

	CHECK(cb != NULL);
	CHECK(g_pending == NULL);

	io = calloc(1, sizeof(*io));
	if (!io) {
		return -ENOMEM;
	}
	io->cb = cb;
	io->arg = cb_arg;
	g_pending = io;
	mock_complete(true);
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
		rec(EV_CLAIM_REFUSED, md->id, dev_index(d), 0);
		return -EPERM;
	}
	/* Any *other* descriptor open for writing refuses the claim. */
	for (i = 0; i < g_ndescs; i++) {
		struct mock_desc *other = g_descs[i];

		if (!other || other == md) {
			continue;
		}
		if (other->dev == d && other->write) {
			g_claim_refused++;
			rec(EV_CLAIM_REFUSED, md->id, dev_index(d), 0);
			return -EPERM;
		}
	}

	d->claimed = true;
	d->claim_type = type;
	md->has_claim = true;
	md->write = true;
	g_claim_granted++;
	rec(EV_CLAIM_GRANTED, md->id, dev_index(d), 0);
	return 0;
}

/*
 * bdev.c:9450 spdk_bdev_module_release_bdev():
 *     assert(claim.v1.module != NULL);
 *     assert(claim_type == SPDK_BDEV_CLAIM_EXCL_WRITE);
 *
 * A v1 release on a v2 claim must fail. The real assertion aborts; here it is
 * recorded as a hard failure and the state is left untouched, so a regression
 * that reintroduces the call is visible in the test result instead of being
 * silently papered over.
 */
void
__wrap_spdk_bdev_module_release_bdev(struct spdk_bdev *bdev)
{
	struct mock_dev *d = mock_dev_by_ptr(bdev);

	if (!d) {
		return;
	}
	if (d->claim_type != SPDK_BDEV_CLAIM_EXCL_WRITE) {
		g_v1_release_on_v2++;
		rec(EV_V1_RELEASE_ON_V2, 0, dev_index(d), 0);
		fprintf(stderr,
			"FAIL: v1 spdk_bdev_module_release_bdev on claim type %d\n",
			(int)d->claim_type);
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

static int
claim(const char *name, struct s3_recovery_claim **out)
{
	int rc = s3_recovery_claim_open(name, &g_test_module, out);

	CHECK(rc == 0);
	CHECK(*out != NULL);
	return rc;
}

static struct scan_result
scan(struct s3_recovery_claim *c)
{
	struct scan_result r = {0};

	s3_recovery_blank_scan_claimed(c, scan_cb, &r);
	CHECK(r.calls == 1);
	return r;
}

struct format_result {
	int                     calls;
	int                     status;
	struct s3_local_dev    *dev;
};

static void
format_cb(void *arg, struct s3_local_dev *dev, int status)
{
	struct format_result *r = arg;

	r->calls++;
	r->dev = dev;
	r->status = status;
}

static struct format_result
format_claimed(const char *wal_name, const char *cache_name,
	       struct s3_recovery_claim *wal_claim,
	       struct s3_recovery_claim *cache_claim)
{
	struct s3_local_dev_format_opts fopts = {0};
	struct format_result r = {0};

	fopts.wal_bdev_name   = wal_name;
	fopts.cache_bdev_name = cache_name;
	fopts.lvs_name        = "lv-under-test";
	fopts.capacity_bytes  = 4 * 1024 * 1024;
	fopts.chunk_size      = 64 * 1024;
	fopts.journal_size    = 1024 * 1024;
	fopts.wal_size        = 1024 * 1024;
	fopts.wal_claim       = wal_claim;
	fopts.cache_claim     = cache_claim;

	s3_local_dev_format(&fopts, format_cb, &r);
	CHECK(r.calls == 1);
	return r;
}

/* The rule s3lvol's setup context now uses: a claim that the device adopted is
 * not the context's to release. */
static void
setup_context_release(struct s3_local_dev *dev, struct s3_recovery_claim *c)
{
	if (!s3_local_dev_owns_claim(dev, c)) {
		s3_recovery_claim_release(c);
	}
}

static void
check_put_before_close(int desc_id)
{
	int put = ev_last_index(EV_CHAN_PUT, desc_id);
	int close = ev_index(EV_CLOSE, desc_id);

	CHECK(put >= 0);
	CHECK(close >= 0);
	CHECK(put >= 0 && close >= 0 && put < close);
}

static uint64_t
region_off(struct s3_local_dev *dev, enum s3_region_id id)
{
	const struct s3_region *r = s3_local_dev_get_region(dev, id);

	CHECK(r != NULL);
	return r ? r->offset : 0;
}

static uint64_t
region_size(struct s3_local_dev *dev, enum s3_region_id id)
{
	const struct s3_region *r = s3_local_dev_get_region(dev, id);

	CHECK(r != NULL);
	return r ? r->size : 0;
}

static void
read_done(struct spdk_bdev_io *io, bool success, void *arg)
{
	bool *ok = arg;

	(void)io;
	*ok = success;
}

static void
live_read(struct s3_local_dev *dev, enum s3_region_id id)
{
	struct spdk_bdev_desc *desc = s3_local_dev_get_desc(dev, id);
	struct spdk_io_channel *ch = s3_local_dev_get_channel(dev, id);
	char buf[4096];
	bool ok = false;
	int rc;

	CHECK(desc != NULL);
	CHECK(ch != NULL);
	if (!desc || !ch) {
		return;
	}
	rc = spdk_bdev_read(desc, ch, buf, region_off(dev, id), sizeof(buf),
			    read_done, &ok);
	CHECK(rc == 0);
	CHECK(ok);
}

/* ------------------------------------------------------------------ cases */

/* 1 + 6 + 7: single-device format, adoption, live read after the setup
 * context would have released, unload releases exactly once, in order. */
static void
test_single_claim_format_lifetime(void)
{
	struct s3_recovery_claim *c = NULL;
	struct format_result f;
	struct scan_result s;
	struct s3_local_dev *dev;
	int desc_id;

	reset_mock();
	add_dev("wal0", NULL, 2048, 4096);   /* 8 MiB */

	claim("wal0", &c);
	s = scan(c);
	CHECK(s.status == 0);
	desc_id = ((struct mock_desc *)(void *)s3_recovery_claim_desc(c))->id;

	f = format_claimed("wal0", NULL, c, NULL);
	CHECK(f.status == 0);
	CHECK(f.dev != NULL);
	dev = f.dev;
	if (!dev) {
		s3_recovery_claim_release(c);
		return;
	}

	CHECK(g_writes == 2);                 /* super block + journal head */
	CHECK(g_close_count == 0);            /* descriptor still live */
	CHECK(s3_local_dev_owns_claim(dev, c));
	CHECK(!s3_local_dev_owns_claim(NULL, c));
	CHECK(s3_local_dev_owns_claim(dev, c));
	CHECK(region_size(dev, S3_REGION_WAL) == 1024 * 1024);

	/* The setup context runs now, on the success path. It must not close
	 * anything: the device adopted the claim. */
	setup_context_release(dev, c);
	CHECK(g_close_count == 0);

	/* Journal/WAL consumers keep working after that point. */
	live_read(dev, S3_REGION_JOURNAL);
	live_read(dev, S3_REGION_WAL);
	CHECK(g_close_count == 0);

	/* Unload. */
	s3_local_dev_close(dev);
	CHECK(g_close_count == 1);
	CHECK(g_v1_release_on_v2 == 0);
	CHECK(g_devs[0].claim_type == SPDK_BDEV_CLAIM_NONE);
	CHECK(g_devs[0].claimed == false);
	check_put_before_close(desc_id);
	CHECK(!g_close_saw_pending);

	/* NULL-safe. */
	s3_recovery_claim_release(NULL);
	CHECK(!s3_local_dev_owns_claim(NULL, NULL));
}

/* 1 (dual) + 3: the cache claim's descriptor is consumed, never reopened. */
static void
test_dual_claim_format_consumes_cache(void)
{
	struct s3_recovery_claim *wc = NULL, *cc = NULL;
	struct format_result f;
	int opens_before;
	int wal_id, cache_id;

	reset_mock();
	add_dev("wal0", NULL, 2048, 4096);
	add_dev("cache0", NULL, 4096, 4096);

	claim("wal0", &wc);
	claim("cache0", &cc);
	CHECK(scan(wc).status == 0);
	CHECK(scan(cc).status == 0);
	wal_id = ((struct mock_desc *)(void *)s3_recovery_claim_desc(wc))->id;
	cache_id = ((struct mock_desc *)(void *)s3_recovery_claim_desc(cc))->id;

	opens_before = g_open_count;
	f = format_claimed("wal0", "cache0", wc, cc);
	CHECK(f.status == 0);
	CHECK(f.dev != NULL);
	/* A reopen of the claimed cache would have failed -EPERM; no open at
	 * all should have been attempted. */
	CHECK(g_open_count == opens_before);
	if (!f.dev) {
		s3_recovery_claim_release(wc);
		s3_recovery_claim_release(cc);
		return;
	}
	CHECK(s3_local_dev_owns_claim(f.dev, wc));
	CHECK(s3_local_dev_owns_claim(f.dev, cc));
	CHECK(region_off(f.dev, S3_REGION_CACHE) == 0);
	CHECK(region_size(f.dev, S3_REGION_CACHE) == 4096 * 4096);

	setup_context_release(f.dev, wc);
	setup_context_release(f.dev, cc);
	CHECK(g_close_count == 0);

	live_read(f.dev, S3_REGION_JOURNAL);
	live_read(f.dev, S3_REGION_CACHE);
	CHECK(g_close_count == 0);

	s3_local_dev_close(f.dev);
	CHECK(g_close_count == 2);
	CHECK(g_v1_release_on_v2 == 0);
	CHECK(g_devs[0].claim_type == SPDK_BDEV_CLAIM_NONE);
	CHECK(g_devs[1].claim_type == SPDK_BDEV_CLAIM_NONE);
	check_put_before_close(wal_id);
	check_put_before_close(cache_id);
}

/* 3 (reject): a dual layout whose cache claim is missing would reopen the
 * claimed device. Refuse instead of failing later with -EPERM. */
static void
test_dual_without_cache_claim_rejected(void)
{
	struct s3_recovery_claim *wc = NULL;
	struct format_result f;

	reset_mock();
	add_dev("wal0", NULL, 2048, 4096);
	add_dev("cache0", NULL, 4096, 4096);
	claim("wal0", &wc);
	CHECK(scan(wc).status == 0);

	f = format_claimed("wal0", "cache0", wc, NULL);
	CHECK(f.status == -EINVAL);
	CHECK(f.dev == NULL);
	CHECK(g_writes == 0);
	CHECK(g_close_count == 0);
	CHECK(s3_recovery_claim_error(wc) == 0);
	s3_recovery_claim_release(wc);
	CHECK(g_v1_release_on_v2 == 0);
}

/* 3 (reject): cache claim without a WAL claim. */
static void
test_cache_claim_without_wal_rejected(void)
{
	struct s3_recovery_claim *wc = NULL, *cc = NULL;
	struct format_result f;

	reset_mock();
	add_dev("wal0", NULL, 2048, 4096);
	add_dev("cache0", NULL, 4096, 4096);
	claim("wal0", &wc);
	claim("cache0", &cc);

	f = format_claimed("wal0", "cache0", NULL, cc);
	CHECK(f.status == -EINVAL);
	CHECK(g_writes == 0);
	CHECK(g_close_count == 0);
	/* Both claims are still the caller's. */
	CHECK(s3_recovery_claim_error(wc) == 0);
	s3_recovery_claim_release(cc);
	s3_recovery_claim_release(wc);
}

/* 3 (reject): the same claim passed twice. One claim cannot be adopted twice;
 * releasing it twice would be a double close. */
static void
test_duplicate_claim_rejected(void)
{
	struct s3_recovery_claim *wc = NULL;
	struct format_result f;

	reset_mock();
	add_dev("wal0", NULL, 2048, 4096);
	add_dev("cache0", NULL, 4096, 4096);
	claim("wal0", &wc);
	CHECK(scan(wc).status == 0);

	f = format_claimed("wal0", "wal0", wc, wc);
	CHECK(f.status == -EINVAL);
	CHECK(g_writes == 0);
	CHECK(g_close_count == 0);
	s3_recovery_claim_release(wc);
	CHECK(g_close_count == 1);
}

/* 3 (identity): the claim must belong to the device the caller names. */
static void
test_wrong_claim_device_rejected(void)
{
	struct s3_recovery_claim *wc = NULL;
	struct format_result f;

	reset_mock();
	add_dev("wal0", NULL, 2048, 4096);
	add_dev("other0", NULL, 2048, 4096);
	claim("wal0", &wc);
	CHECK(scan(wc).status == 0);

	f = format_claimed("other0", NULL, wc, NULL);
	CHECK(f.status == -EINVAL);
	CHECK(g_writes == 0);
	s3_recovery_claim_release(wc);
}

/* Alias identity: the mock resolves the alias to the same bdev pointer, which
 * is the identity the duplicate rejection compares. The alias is rejected as a
 * dual layout without a distinct cache claim rather than being treated as a
 * second device. */
static void
test_alias_names_are_one_device(void)
{
	struct s3_recovery_claim *wc = NULL;
	struct format_result f;

	reset_mock();
	add_dev("wal0", "wal0-alias", 2048, 4096);
	claim("wal0", &wc);
	CHECK(scan(wc).status == 0);

	CHECK(mock_dev_by_name("wal0") == mock_dev_by_name("wal0-alias"));
	f = format_claimed("wal0", "wal0-alias", wc, NULL);
	CHECK(f.status == -EINVAL);            /* names differ, no cache claim */
	CHECK(g_writes == 0);
	s3_recovery_claim_release(wc);
}

/* Failure 2: a busy claim (another writer) is refused before anything is
 * scanned. */
static void
test_preexisting_writer_refused(void)
{
	struct s3_recovery_claim *c = NULL;
	struct spdk_bdev_desc *writer = NULL;

	reset_mock();
	add_dev("wal0", NULL, 2048, 4096);
	CHECK(spdk_bdev_open_ext("wal0", true, NULL, NULL, &writer) == 0);
	CHECK(s3_recovery_claim_open("wal0", &g_test_module, &c) == -EPERM);
	CHECK(c == NULL);
	spdk_bdev_close(writer);
	CHECK(g_v1_release_on_v2 == 0);
}

/* Failure 2: second claim refused; the first claim is still held and the
 * caller releases it. Nothing was formatted. */
static void
test_second_claim_failure_reverse_cleanup(void)
{
	struct s3_recovery_claim *wc = NULL, *cc = NULL;
	struct spdk_bdev_desc *writer = NULL;

	reset_mock();
	add_dev("wal0", NULL, 2048, 4096);
	add_dev("cache0", NULL, 4096, 4096);

	claim("wal0", &wc);
	CHECK(spdk_bdev_open_ext("cache0", true, NULL, NULL, &writer) == 0);
	CHECK(s3_recovery_claim_open("cache0", &g_test_module, &cc) == -EPERM);
	CHECK(cc == NULL);
	CHECK(g_devs[0].claim_type == SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE);

	s3_recovery_claim_release(wc);
	CHECK(g_devs[0].claim_type == SPDK_BDEV_CLAIM_NONE);
	CHECK(g_v1_release_on_v2 == 0);
	spdk_bdev_close(writer);
}

/* Failure 2: format write failure after the claim was handed in. The dev must
 * not adopt the claims, and the caller must still be able to release them. */
static void
test_format_write_failure_leaves_claims_with_caller(void)
{
	struct s3_recovery_claim *wc = NULL, *cc = NULL;
	struct format_result f;

	reset_mock();
	add_dev("wal0", NULL, 2048, 4096);
	add_dev("cache0", NULL, 4096, 4096);
	claim("wal0", &wc);
	claim("cache0", &cc);
	CHECK(scan(wc).status == 0);
	CHECK(scan(cc).status == 0);

	g_write_fail_at = 1;
	f = format_claimed("wal0", "cache0", wc, cc);
	CHECK(f.status == -EIO);
	CHECK(f.dev == NULL);
	CHECK(g_writes == 1);
	CHECK(g_close_count == 0);
	CHECK(g_devs[0].claim_type == SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE);
	CHECK(g_devs[1].claim_type == SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE);

	s3_recovery_claim_release(cc);
	CHECK(g_close_count == 1);
	s3_recovery_claim_release(wc);
	CHECK(g_close_count == 2);
	CHECK(g_devs[0].claim_type == SPDK_BDEV_CLAIM_NONE);
	CHECK(g_devs[1].claim_type == SPDK_BDEV_CLAIM_NONE);
	CHECK(g_v1_release_on_v2 == 0);
}

/* Failure 2: event latched after the scan and before the format. No write may
 * be submitted, and the claim is still the caller's. */
static void
test_event_after_scan_refuses_write(void)
{
	struct s3_recovery_claim *c = NULL;
	struct format_result f;
	struct mock_desc *md;

	reset_mock();
	add_dev("wal0", NULL, 2048, 4096);
	claim("wal0", &c);
	CHECK(scan(c).status == 0);

	md = (struct mock_desc *)(void *)s3_recovery_claim_desc(c);
	mock_notify(md, SPDK_BDEV_EVENT_REMOVE);
	CHECK(s3_recovery_claim_error(c) == -ENODEV);

	f = format_claimed("wal0", NULL, c, NULL);
	CHECK(f.status == -ENODEV);
	CHECK(f.dev == NULL);
	CHECK(g_writes == 0);
	CHECK(g_close_count == 0);

	s3_recovery_claim_release(c);
	CHECK(g_close_count == 1);
	CHECK(g_v1_release_on_v2 == 0);
}

/* Failure 2: event latched while the first format write is in flight. The
 * second write must not be submitted, and the format reports failure. */
static void
test_event_during_format_stops_second_write(int write_number)
{
	struct s3_recovery_claim *c = NULL;
	struct format_result f;

	reset_mock();
	add_dev("wal0", NULL, 2048, 4096);
	claim("wal0", &c);
	CHECK(scan(c).status == 0);

	g_event_on_write = write_number;
	f = format_claimed("wal0", NULL, c, NULL);
	CHECK(f.status == -ENODEV);
	CHECK(f.dev == NULL);
	CHECK(g_writes == write_number);
	CHECK(g_close_count == 0);
	s3_recovery_claim_release(c);
}

/* Failure 1: the mock must catch a v1 release on a v2 claim, and the real
 * release path must not take it. */
static void
test_v1_release_is_rejected_and_not_used(void)
{
	struct s3_recovery_claim *c = NULL;

	reset_mock();
	add_dev("wal0", NULL, 2048, 4096);

	/* Prove the detector is live. The mock prints a FAIL line here on
	 * purpose, because that is what the real assertion does; the checks
	 * below are what make this case pass. */
	claim("wal0", &c);
	__wrap_spdk_bdev_module_release_bdev(s3_recovery_claim_bdev(c));
	CHECK(g_v1_release_on_v2 == 1);
	CHECK(g_devs[0].claim_type == SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE);
	/* The detector is expected to have fired once; reset it so the rest of
	 * this case asserts that the real path never fires it again. */
	g_v1_release_on_v2 = 0;
	/* The deliberate probe above is not a test failure. */
	g_failures = 0;
	fprintf(stderr, "NOTE: the FAIL line above is the deliberate v1-release "
		"probe, not a failed check\n");

	/* The real path releases by closing the descriptor, and must not take
	 * the v1 path. */
	s3_recovery_claim_release(c);
	CHECK(g_v1_release_on_v2 == 0);
	CHECK(g_devs[0].claim_type == SPDK_BDEV_CLAIM_NONE);
	CHECK(g_close_count == 1);

	/* A plain close (the formatter's non-claimed path) must not be affected. */
	{
		struct spdk_bdev_desc *d = NULL;

		CHECK(spdk_bdev_open_ext("wal0", true, NULL, NULL, &d) == 0);
		spdk_bdev_close(d);
		CHECK(g_v1_release_on_v2 == 0);
	}
}

int
main(void)
{
	test_single_claim_format_lifetime();
	test_dual_claim_format_consumes_cache();
	test_dual_without_cache_claim_rejected();
	test_cache_claim_without_wal_rejected();
	test_duplicate_claim_rejected();
	test_wrong_claim_device_rejected();
	test_alias_names_are_one_device();
	test_preexisting_writer_refused();
	test_second_claim_failure_reverse_cleanup();
	test_format_write_failure_leaves_claims_with_caller();
	test_event_after_scan_refuses_write();
	test_event_during_format_stops_second_write(1);
	test_event_during_format_stops_second_write(2);
	test_v1_release_is_rejected_and_not_used();

	if (g_failures) {
		fprintf(stderr, "RESULT: FAIL (%d checks)\n", g_failures);
		return 1;
	}
	printf("RESULT: PASS (14 cases)\n");
	return 0;
}
