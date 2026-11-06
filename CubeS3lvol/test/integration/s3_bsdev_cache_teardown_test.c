/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_bs_dev teardown vs. delayed cache I/O -- component-to-bsdev integration
 *   (ASan/UBSan, deterministic, single process, MOCKED external boundaries)
 *
 *   === What is REAL (production source compiled into this binary) ===
 *     lib/s3bsdev/s3_bs_dev.c    s3_bs_dev_create / attach_journal / attach_wal /
 *                                attach_cache / journal_apply / read / write /
 *                                checkpoint / destroy / teardown / quiesce
 *                                callback / flusher drained+retired / WAL closed
 *                                / free_cb -- the whole destroy chain
 *     lib/s3bsdev/s3_cache.c     populate, fill completion, quiesce
 *     lib/s3bsdev/s3_flusher.c   kick / drain / retire / destroy / poller
 *     lib/s3bsdev/s3_overlay.c   WAL-path overlay
 *     lib/s3bsdev/s3_chunk_map.c chunk map (memory-only or fake journal)
 *   No production function is copied into this file.
 *
 *   === What is FAKE (defined below) ===
 *     SPDK thread / message queue / pollers / io_device / ticks / uuid / log /
 *       dma alloc -- a deterministic single-thread reactor driven by pump().
 *     spdk_bdev_read / spdk_bdev_write / spdk_bdev_free_io -- a fake local
 *       device whose completions the test parks and delivers on demand. It has
 *       an explicit descriptor: closing it with I/O outstanding, completing an
 *       I/O after close, or submitting after close is counted as a violation.
 *     s3_client (get_range / put / delete / copy_object), s3_wal (append /
 *       close / truncate / sync_super), s3_journal, s3_local_dev accessors,
 *       s3_checkpoint_save, s3_recovery_publish -- parked or inline, test
 *       controlled. No network, no S3, no real WAL, no real bdev, no PCI.
 *
 *   === Ownership scope (stated exactly) ===
 *     The owner marker is NOT exercised. "Release ownership / close the local
 *     descriptor" is represented by the user destroy callback registered with
 *     s3_bs_dev_set_destroy_cb(), which closes the fake descriptor. This test
 *     proves ordering up to and including that callback; it does not prove
 *     anything about a real S3 owner marker or cross-process exclusion.
 *
 *   === Cases ===
 *     T1  no WAL: fill pending at destroy; nothing downstream until the fill
 *         lands (success); destroy() + quiesce park allocate nothing.
 *     T2  no WAL: fill fails (completion error) -> same chain, status 0.
 *     T3  no WAL: three fills, mixed success/failure, completed out of order;
 *         SYNCHRONOUS io_device unregister so free_cb (cache destroy, ctx free)
 *         runs inside the last fill's completion frame (ASan oracle for the
 *         "callback may destroy the cache" contract).
 *     T4  WAL+flusher: fill issued by the flusher's own populate; teardown
 *         parks; then drain -> retire (next poller tick) -> WAL close (parked)
 *         -> io_device unregister -> free_cb -> destroy_cb, in that order;
 *         whole teardown with allocation failure injected.
 *     T5  WAL+flusher: fill failure + WAL close error -> destroy_cb gets the
 *         WAL close error exactly once.
 *     T6  WAL+flusher: fill never completes for 10000 reactor iterations with
 *         the fake clock advanced one hour (past the 30 s flusher drain
 *         deadline): teardown stays parked, no drain is started, nothing is
 *         closed or freed. The fill is then delivered late and teardown
 *         completes normally. (No universal timeout exists; a truly lost
 *         completion leaves the bs_dev parked -- by design.)
 *     T7  WAL+flusher: upload in flight + fill pending at destroy. A failed
 *         upload during the cache wait is re-queued and re-submitted (flusher
 *         is NOT retiring while the cache is busy). After the fill lands the
 *         drain waits for the re-submitted upload; its success does NOT start
 *         a new cache fill (destroying); destroy_cb status 0.
 *     T8  WAL+flusher: as T7, but the drain times out after the fill lands and
 *         the in-flight upload then fails -> retire reports dirty ->
 *         destroy_cb(-ETIMEDOUT) exactly once, after the upload completion.
 *     T9  re-entry composition: checkpoint in flight + deferred delete +
 *         pending fill. teardown is re-entered from ckpt_finish() and from
 *         s3_deferred_deleted(); each re-entry parks (on deletes, then on the
 *         cache) and nothing downstream runs until the fill lands.
 *     T10 control: cache attached but idle at destroy -> quiesce returns 0 and
 *         teardown proceeds (no-WAL and WAL variants).
 */

#include "spdk/stdinc.h"
#include "spdk/bdev.h"
#include "spdk/blob.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"
#include "spdk/uuid.h"

#include "s3lvol/s3_bs_dev.h"
#include "s3lvol/s3_cache.h"
#include "s3lvol/s3_checkpoint.h"
#include "s3lvol/s3_client.h"
#include "s3lvol/s3_journal.h"
#include "s3lvol/s3_local_dev.h"
#include "s3lvol/s3_recovery.h"
#include "s3lvol/s3_types.h"
#include "s3lvol/s3_wal.h"

static int g_pass, g_fail;

#define REQUIRE(cond) do { if (!(cond)) { \
	fprintf(stderr, "REQUIRE %s:%d: %s\n", __FILE__, __LINE__, #cond); \
	abort(); } } while (0)

#define CHECK(cond) do { \
	if (cond) { g_pass++; } else { g_fail++; \
		fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); } \
} while (0)

#define CHUNK      (64u * 1024u)
#define NCHUNKS    16u
#define CAPACITY   ((uint64_t)NCHUNKS * CHUNK)
#define BLK_PER_CHUNK (CHUNK / 4096u)

/* ------------------------------------------------------- allocator fail */

static bool g_fail_alloc;
static uint64_t g_alloc_attempts_while_failing;

void *__real_malloc(size_t n);
void *__real_calloc(size_t a, size_t b);

void *
__wrap_malloc(size_t n)
{
	if (g_fail_alloc) {
		g_alloc_attempts_while_failing++;
		return NULL;
	}
	return __real_malloc(n);
}

void *
__wrap_calloc(size_t a, size_t b)
{
	if (g_fail_alloc) {
		g_alloc_attempts_while_failing++;
		return NULL;
	}
	return __real_calloc(a, b);
}

/* ------------------------------------------------------ event ordering */

enum ev {
	EV_FILL_DONE = 0,       /* test delivered the last pending cache fill */
	EV_FLUSHER_POLLER_UNREG,
	EV_WAL_CLOSE_CALL,
	EV_WAL_CLOSE_DONE,
	EV_IODEV_UNREG,
	EV_REAP,
	EV_DESTROY_CB,
	EV_DESC_CLOSE,
	EV_CKPT_CB,
	EV_DELETE_DONE,
	EV_UPLOAD_DONE,
	EV_MAX
};

static uint64_t g_seq;
static uint64_t g_ev_first[EV_MAX];
static uint64_t g_ev_last[EV_MAX];
static uint32_t g_ev_count[EV_MAX];

/* Set by do_destroy(): EV_FILL_DONE is only recorded for fills that were
 * still outstanding when destroy() was called. */
static bool g_after_destroy;
static uint32_t g_outstanding_at_destroy;

static void
ev(enum ev e)
{
	g_seq++;
	if (g_ev_count[e]++ == 0) {
		g_ev_first[e] = g_seq;
	}
	g_ev_last[e] = g_seq;
}

#define BEFORE(a, b) (g_ev_count[a] && g_ev_count[b] && g_ev_first[a] < g_ev_first[b])

/* ------------------------------------------------------------ fake clock */

static uint64_t g_ticks;
uint64_t spdk_get_ticks(void) { return g_ticks; }
uint64_t spdk_get_ticks_hz(void) { return 1000000ULL; }   /* 1 tick = 1 us */

/* ------------------------------------------------- fake thread / msgs */

static struct spdk_thread *g_fake_thread = (struct spdk_thread *)0x1;
struct spdk_thread *spdk_get_thread(void) { return g_fake_thread; }

#define MAXMSG 256
static struct { spdk_msg_fn fn; void *ctx; } g_msgq[MAXMSG];
static uint32_t g_msg_head, g_msg_tail;

int
spdk_thread_send_msg(const struct spdk_thread *thread, spdk_msg_fn fn, void *ctx)
{
	REQUIRE(thread == g_fake_thread);
	REQUIRE(g_msg_tail - g_msg_head < MAXMSG);
	g_msgq[g_msg_tail % MAXMSG].fn = fn;
	g_msgq[g_msg_tail % MAXMSG].ctx = ctx;
	g_msg_tail++;
	return 0;
}

static void
run_msgs(void)
{
	while (g_msg_head != g_msg_tail) {
		spdk_msg_fn fn = g_msgq[g_msg_head % MAXMSG].fn;
		void *ctx = g_msgq[g_msg_head % MAXMSG].ctx;

		g_msg_head++;
		fn(ctx);
	}
}

/* --------------------------------------------------------- fake pollers */

struct spdk_poller {
	spdk_poller_fn fn;
	void *arg;
	char name[64];
	bool active;
};

#define MAXPOLL 64
static struct spdk_poller g_pollers[MAXPOLL];   /* static: never freed under a tick */
static uint32_t g_npollers;

struct spdk_poller *
spdk_poller_register_named(spdk_poller_fn fn, void *arg, uint64_t period_us,
			   const char *name)
{
	struct spdk_poller *p;

	(void)period_us;
	if (g_fail_alloc) {
		g_alloc_attempts_while_failing++;
		return NULL;
	}
	REQUIRE(g_npollers < MAXPOLL);
	p = &g_pollers[g_npollers++];
	p->fn = fn;
	p->arg = arg;
	snprintf(p->name, sizeof(p->name), "%s", name ? name : "?");
	p->active = true;
	return p;
}

void
spdk_poller_unregister(struct spdk_poller **pp)
{
	if (!pp || !*pp) {
		return;
	}
	if ((*pp)->active && strcmp((*pp)->name, "flusher_poll") == 0) {
		ev(EV_FLUSHER_POLLER_UNREG);
	}
	(*pp)->active = false;
	*pp = NULL;
}

static uint32_t
pollers_active(const char *name)
{
	uint32_t n = 0;

	for (uint32_t i = 0; i < g_npollers; i++) {
		if (g_pollers[i].active && strcmp(g_pollers[i].name, name) == 0) {
			n++;
		}
	}
	return n;
}

static void
run_pollers(void)
{
	for (uint32_t i = 0; i < g_npollers; i++) {
		if (g_pollers[i].active) {
			g_pollers[i].fn(g_pollers[i].arg);
		}
	}
}

static void
pump(uint32_t n)
{
	for (uint32_t i = 0; i < n; i++) {
		run_msgs();
		run_pollers();
		run_msgs();
	}
}

/* ------------------------------------------------------ fake io_device */

static void *g_iodev;
static spdk_io_device_unregister_cb g_iodev_unreg_cb;
static uint32_t g_iodev_reg, g_iodev_unreg;
static bool g_sync_unregister;

void
spdk_io_device_register(void *io_device, spdk_io_channel_create_cb create_cb,
			spdk_io_channel_destroy_cb destroy_cb, uint32_t ctx_size,
			const char *name)
{
	(void)create_cb; (void)destroy_cb; (void)ctx_size; (void)name;
	g_iodev = io_device;
	g_iodev_reg++;
}

static void
iodev_unreg_msg(void *ctx)
{
	spdk_io_device_unregister_cb cb = g_iodev_unreg_cb;

	REQUIRE(ctx == g_iodev);
	g_iodev_unreg_cb = NULL;
	g_iodev = NULL;
	cb(ctx);
}

/* No channels are ever taken in this harness (blobstore is not present), so
 * the unregister completes on the next message turn, as SPDK does once the
 * channel count reaches zero. g_sync_unregister makes it complete inline --
 * stricter than SPDK, used to prove free_cb may run inside the cache's own
 * completion frame. */
void
spdk_io_device_unregister(void *io_device, spdk_io_device_unregister_cb cb)
{
	REQUIRE(io_device == g_iodev);
	REQUIRE(g_iodev_unreg_cb == NULL);
	g_iodev_unreg++;
	ev(EV_IODEV_UNREG);
	g_iodev_unreg_cb = cb;
	if (g_sync_unregister) {
		iodev_unreg_msg(io_device);
	} else {
		spdk_thread_send_msg(g_fake_thread, iodev_unreg_msg, io_device);
	}
}

struct spdk_io_channel *spdk_get_io_channel(void *io_device) { (void)io_device; return NULL; }
void spdk_put_io_channel(struct spdk_io_channel *ch) { (void)ch; }

/* ----------------------------------------------------- misc SPDK fakes */

void *
spdk_dma_malloc(size_t size, size_t align, uint64_t *unused)
{
	void *p = NULL;

	(void)unused;
	if (g_fail_alloc) {
		g_alloc_attempts_while_failing++;
		return NULL;
	}
	if (posix_memalign(&p, align < sizeof(void *) ? sizeof(void *) : align,
			   size) != 0) {
		return NULL;
	}
	return p;
}

void spdk_dma_free(void *buf) { free(buf); }

uint32_t spdk_u32log2(uint32_t x) { return x ? 31u - (uint32_t)__builtin_clz(x) : 0; }
const char *spdk_strerror(int errnum) { return strerror(errnum); }

int
spdk_uuid_compare(const struct spdk_uuid *a, const struct spdk_uuid *b)
{
	return memcmp(a, b, sizeof(*a));
}

void spdk_uuid_copy(struct spdk_uuid *d, const struct spdk_uuid *s) { memcpy(d, s, sizeof(*d)); }
void spdk_uuid_set_null(struct spdk_uuid *u) { memset(u, 0, sizeof(*u)); }

bool
spdk_uuid_is_null(const struct spdk_uuid *u)
{
	static const struct spdk_uuid z;

	return memcmp(u, &z, sizeof(z)) == 0;
}

static uint32_t g_uuid_ctr;

void
spdk_uuid_generate(struct spdk_uuid *u)
{
	uint8_t *b = (uint8_t *)u;

	memset(u, 0, sizeof(*u));
	g_uuid_ctr++;
	b[0] = 0xA5;
	memcpy(b + 12, &g_uuid_ctr, sizeof(g_uuid_ctr));
}

int
spdk_uuid_fmt_lower(char *s, size_t n, const struct spdk_uuid *u)
{
	const uint8_t *b = (const uint8_t *)u;

	if (n < SPDK_UUID_STRING_LEN) {
		return -EINVAL;
	}
	snprintf(s, n, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-"
		 "%02x%02x%02x%02x%02x%02x", b[0], b[1], b[2], b[3], b[4], b[5],
		 b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
	return 0;
}

static int g_errlogs;
static bool g_quiet;

void
spdk_log(enum spdk_log_level level, const char *file, const int line,
	 const char *func, const char *format, ...)
{
	va_list ap;

	(void)file; (void)line;
	if (level == SPDK_LOG_ERROR) {
		g_errlogs++;
	}
	if (g_quiet && level != SPDK_LOG_ERROR) {
		return;
	}
	fprintf(stderr, "  [log %s] ", func ? func : "-");
	va_start(ap, format);
	vfprintf(stderr, format, ap);
	va_end(ap);
}

void spdk_log_register_flag(const char *name, struct spdk_log_flag *flag) { (void)name; (void)flag; }

/* ------------------------------------------------------ fake local bdev */

struct spdk_bdev_io { int idx; };

#define MAXIO 64
static struct {
	bool open;
	spdk_bdev_io_completion_cb cb[MAXIO];
	void *arg[MAXIO];
	bool is_write[MAXIO];
	struct spdk_bdev_io io[MAXIO];
	bool used[MAXIO];
	uint32_t outstanding;
	uint32_t submitted_reads, submitted_writes;
	uint32_t violations;
	uint32_t freed;
} g_dev;

#define FAKE_DESC ((struct spdk_bdev_desc *)&g_dev)
#define FAKE_CH   ((struct spdk_io_channel *)&g_dev.outstanding)

static void
fake_desc_close(void)
{
	if (!g_dev.open) {
		fprintf(stderr, "!! descriptor closed twice\n");
		g_dev.violations++;
	}
	if (g_dev.outstanding != 0) {
		fprintf(stderr, "!! descriptor closed with %u I/O outstanding\n",
			g_dev.outstanding);
		g_dev.violations++;
	}
	g_dev.open = false;
	ev(EV_DESC_CLOSE);
}

static int
fake_submit(bool is_write, spdk_bdev_io_completion_cb cb, void *arg)
{
	int i;

	if (!g_dev.open) {
		fprintf(stderr, "!! I/O submitted on a closed descriptor\n");
		g_dev.violations++;
		return -EBADF;
	}
	for (i = 0; i < MAXIO && g_dev.used[i]; i++) {
	}
	REQUIRE(i < MAXIO);
	g_dev.used[i] = true;
	g_dev.cb[i] = cb;
	g_dev.arg[i] = arg;
	g_dev.is_write[i] = is_write;
	g_dev.io[i].idx = i;
	g_dev.outstanding++;
	if (is_write) {
		g_dev.submitted_writes++;
	} else {
		g_dev.submitted_reads++;
	}
	return 0;
}

int
spdk_bdev_read(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
	       void *buf, uint64_t offset, uint64_t nbytes,
	       spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	(void)buf; (void)offset; (void)nbytes;
	REQUIRE(desc == FAKE_DESC && ch == FAKE_CH);
	return fake_submit(false, cb, cb_arg);
}

int
spdk_bdev_write(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		void *buf, uint64_t offset, uint64_t nbytes,
		spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	(void)buf; (void)offset; (void)nbytes;
	REQUIRE(desc == FAKE_DESC && ch == FAKE_CH);
	return fake_submit(true, cb, cb_arg);
}

void
spdk_bdev_free_io(struct spdk_bdev_io *io)
{
	REQUIRE(io >= &g_dev.io[0] && io < &g_dev.io[MAXIO]);
	g_dev.freed++;
}

/* The idx-th outstanding I/O in slot order, or -1. */
static int
dev_nth_outstanding(uint32_t nth)
{
	for (int i = 0; i < MAXIO; i++) {
		if (g_dev.used[i] && nth-- == 0) {
			return i;
		}
	}
	return -1;
}

static void
dev_complete_slot(int i, bool success)
{
	spdk_bdev_io_completion_cb cb;
	void *arg;

	REQUIRE(i >= 0 && i < MAXIO && g_dev.used[i]);
	if (!g_dev.open) {
		fprintf(stderr, "!! completion after the descriptor closed\n");
		g_dev.violations++;
	}
	cb = g_dev.cb[i];
	arg = g_dev.arg[i];
	g_dev.used[i] = false;
	g_dev.outstanding--;
	if (g_dev.outstanding == 0 && g_after_destroy) {
		ev(EV_FILL_DONE);
	}
	cb(&g_dev.io[i], success, arg);
}

/* ------------------------------------------------------------ fake S3 */

#define FAKE_CLIENT ((struct s3_client *)0x5c)
#define MAXOP 32

static struct {
	s3_get_cb cb[MAXOP];
	void *arg[MAXOP];
	void *buf[MAXOP];
	uint64_t len[MAXOP];
	bool pending[MAXOP];
	uint32_t n;
} g_get;

static struct {
	s3_op_cb cb[MAXOP];
	void *arg[MAXOP];
	bool pending[MAXOP];
	uint32_t n;
} g_put, g_del;

static uint32_t g_del_nocb;

int
s3_get_range(struct s3_client *client, const char *key, uint64_t offset,
	     uint64_t len, void *buf, s3_get_cb cb, void *cb_arg)
{
	(void)key; (void)offset;
	REQUIRE(client == FAKE_CLIENT && g_get.n < MAXOP);
	g_get.cb[g_get.n] = cb;
	g_get.arg[g_get.n] = cb_arg;
	g_get.buf[g_get.n] = buf;
	g_get.len[g_get.n] = len;
	g_get.pending[g_get.n] = true;
	g_get.n++;
	return 0;
}

static void
get_complete(uint32_t i, int status)
{
	REQUIRE(i < g_get.n && g_get.pending[i]);
	g_get.pending[i] = false;
	if (status == 0) {
		memset(g_get.buf[i], 0x5A, g_get.len[i]);
	}
	g_get.cb[i](g_get.arg[i], status == 0 ? g_get.len[i] : 0, status);
}

int
s3_put(struct s3_client *client, const char *key, struct iovec *iov,
       int iovcnt, bool if_none_match, s3_op_cb cb, void *cb_arg)
{
	(void)key; (void)iov; (void)iovcnt; (void)if_none_match;
	REQUIRE(client == FAKE_CLIENT && g_put.n < MAXOP);
	g_put.cb[g_put.n] = cb;
	g_put.arg[g_put.n] = cb_arg;
	g_put.pending[g_put.n] = true;
	g_put.n++;
	return 0;
}

static void
put_complete(uint32_t i, int status)
{
	REQUIRE(i < g_put.n && g_put.pending[i]);
	g_put.pending[i] = false;
	g_put.cb[i](g_put.arg[i], status);
	ev(EV_UPLOAD_DONE);
}

int
s3_delete(struct s3_client *client, const char *key, s3_op_cb cb, void *cb_arg)
{
	(void)key;
	REQUIRE(client == FAKE_CLIENT);
	if (!cb) {
		g_del_nocb++;
		return 0;
	}
	REQUIRE(g_del.n < MAXOP);
	g_del.cb[g_del.n] = cb;
	g_del.arg[g_del.n] = cb_arg;
	g_del.pending[g_del.n] = true;
	g_del.n++;
	return 0;
}

static void
del_complete(uint32_t i, int status)
{
	REQUIRE(i < g_del.n && g_del.pending[i]);
	g_del.pending[i] = false;
	g_del.cb[i](g_del.arg[i], status);
	ev(EV_DELETE_DONE);
}

int
s3_copy_object(struct s3_client *client, const char *src_bucket,
	       const char *src_key, const char *dst_key, s3_op_cb cb, void *cb_arg)
{
	(void)client; (void)src_bucket; (void)src_key; (void)dst_key;
	(void)cb; (void)cb_arg;
	REQUIRE(!"s3_copy_object is not used by this harness");
	return -ENOTSUP;
}

/* ------------------------------------------------------------ fake WAL */

#define FAKE_WAL ((struct s3_wal *)0x3a1)

static struct {
	s3_wal_cb cb[MAXOP];
	void *arg[MAXOP];
	bool pending[MAXOP];
	uint32_t n;
	uint64_t next_seq;
	uint64_t truncated;
	uint32_t sync_calls;
	s3_wal_cb close_cb;
	void *close_arg;
	uint32_t close_calls;
} g_wal;

static void
wal_park(s3_wal_cb cb, void *arg, uint64_t *out_seq)
{
	REQUIRE(g_wal.n < MAXOP);
	if (out_seq) {
		*out_seq = ++g_wal.next_seq;
	}
	g_wal.cb[g_wal.n] = cb;
	g_wal.arg[g_wal.n] = arg;
	g_wal.pending[g_wal.n] = true;
	g_wal.n++;
}

void
s3_wal_append_write(struct s3_wal *wal, uint64_t lba, uint32_t nblocks,
		    const void *payload, uint64_t chunk_index, uint64_t *out_seq,
		    s3_wal_cb cb_fn, void *cb_arg)
{
	(void)lba; (void)nblocks; (void)payload; (void)chunk_index;
	REQUIRE(wal == FAKE_WAL);
	wal_park(cb_fn, cb_arg, out_seq);
}

void
s3_wal_append_zeroes(struct s3_wal *wal, uint64_t lba, uint32_t nblocks,
		     uint64_t chunk_index, uint64_t *out_seq, s3_wal_cb cb_fn,
		     void *cb_arg)
{
	(void)lba; (void)nblocks; (void)chunk_index;
	REQUIRE(wal == FAKE_WAL);
	wal_park(cb_fn, cb_arg, out_seq);
}

void
s3_wal_append_unmap(struct s3_wal *wal, uint64_t lba, uint32_t nblocks,
		    uint64_t chunk_index, uint64_t *out_seq, s3_wal_cb cb_fn,
		    void *cb_arg)
{
	(void)lba; (void)nblocks; (void)chunk_index;
	REQUIRE(wal == FAKE_WAL);
	wal_park(cb_fn, cb_arg, out_seq);
}

static void
wal_complete_appends(void)
{
	for (uint32_t i = 0; i < g_wal.n; i++) {
		if (g_wal.pending[i]) {
			g_wal.pending[i] = false;
			g_wal.cb[i](g_wal.arg[i], 0);
		}
	}
}

uint64_t s3_wal_get_next_seq(const struct s3_wal *wal) { REQUIRE(wal == FAKE_WAL); return g_wal.next_seq + 1; }
bool s3_wal_is_backpressured(const struct s3_wal *wal) { REQUIRE(wal == FAKE_WAL); return false; }

void
s3_wal_truncate_to_seq(struct s3_wal *wal, uint64_t safe_seq)
{
	REQUIRE(wal == FAKE_WAL);
	g_wal.truncated = safe_seq;
}

/* Completes inline: the flusher clears super_sync_active from the callback. */
void
s3_wal_sync_super(struct s3_wal *wal, uint64_t ckpt_seq, s3_wal_cb cb_fn,
		  void *cb_arg)
{
	(void)ckpt_seq;
	REQUIRE(wal == FAKE_WAL);
	if (g_wal.close_calls) {
		fprintf(stderr, "!! WAL super sync after close\n");
		g_dev.violations++;
	}
	g_wal.sync_calls++;
	cb_fn(cb_arg, 0);
}

void
s3_wal_close(struct s3_wal *wal, s3_wal_cb cb_fn, void *cb_arg)
{
	REQUIRE(wal == FAKE_WAL);
	REQUIRE(g_wal.close_cb == NULL);
	g_wal.close_calls++;
	g_wal.close_cb = cb_fn;
	g_wal.close_arg = cb_arg;
	ev(EV_WAL_CLOSE_CALL);
}

static void
wal_close_complete(int status)
{
	s3_wal_cb cb = g_wal.close_cb;

	REQUIRE(cb != NULL);
	g_wal.close_cb = NULL;
	ev(EV_WAL_CLOSE_DONE);
	cb(g_wal.close_arg, status);
}

/* -------------------------------------------------------- fake journal */

#define FAKE_JOURNAL ((struct s3_journal *)0x10a)
static uint64_t g_jlsn;
static uint64_t g_jtrunc;

void
s3_journal_append_update(struct s3_journal *journal, uint64_t chunk_index,
			 const struct spdk_uuid *uuid, uint32_t valid_bytes,
			 uint64_t gen, uint32_t flags, uint64_t *out_lsn,
			 s3_journal_cb cb_fn, void *cb_arg)
{
	(void)chunk_index; (void)uuid; (void)valid_bytes; (void)gen; (void)flags;
	REQUIRE(journal == FAKE_JOURNAL);
	if (out_lsn) {
		*out_lsn = ++g_jlsn;
	}
	cb_fn(cb_arg, 0);
}

void
s3_journal_append_remove(struct s3_journal *journal, uint64_t chunk_index,
			 uint64_t *out_lsn, s3_journal_cb cb_fn, void *cb_arg)
{
	(void)chunk_index;
	REQUIRE(journal == FAKE_JOURNAL);
	if (out_lsn) {
		*out_lsn = ++g_jlsn;
	}
	cb_fn(cb_arg, 0);
}

uint64_t s3_journal_get_capacity_bytes(struct s3_journal *j) { REQUIRE(j == FAKE_JOURNAL); return 1024 * 1024; }
uint64_t s3_journal_get_used_bytes(struct s3_journal *j) { REQUIRE(j == FAKE_JOURNAL); return 0; }
void s3_journal_truncate(struct s3_journal *j, uint64_t lsn) { REQUIRE(j == FAKE_JOURNAL); g_jtrunc = lsn; }

/* ------------------------------------------------------ fake local dev */

#define FAKE_LOCAL ((struct s3_local_dev *)0x10cd)
static struct s3_super_block g_sb;
static struct s3_region g_regions[S3_REGION_COUNT];

const struct s3_super_block *
s3_local_dev_get_super(struct s3_local_dev *dev)
{
	REQUIRE(dev == FAKE_LOCAL);
	return &g_sb;
}

const struct s3_region *
s3_local_dev_get_region(struct s3_local_dev *dev, enum s3_region_id id)
{
	REQUIRE(dev == FAKE_LOCAL && id < S3_REGION_COUNT);
	return &g_regions[id];
}

struct spdk_bdev_desc *
s3_local_dev_get_desc(struct s3_local_dev *dev, enum s3_region_id id)
{
	REQUIRE(dev == FAKE_LOCAL);
	(void)id;
	return FAKE_DESC;
}

struct spdk_io_channel *
s3_local_dev_get_channel(struct s3_local_dev *dev, enum s3_region_id id)
{
	REQUIRE(dev == FAKE_LOCAL);
	(void)id;
	return FAKE_CH;
}

void
s3_local_dev_update_checkpoint(struct s3_local_dev *dev, uint64_t gen,
			       uint64_t lsn, s3_local_dev_cb cb_fn, void *cb_arg)
{
	REQUIRE(dev == FAKE_LOCAL);
	g_sb.checkpoint_gen = gen;
	g_sb.checkpoint_lsn = lsn;
	cb_fn(cb_arg, 0);
}

/* ---------------------------------------------- fake checkpoint/recovery */

static s3_checkpoint_cb g_ckpt_save_cb;
static void *g_ckpt_save_arg;
static uint32_t g_ckpt_saves, g_recovery_publishes;

void
s3_checkpoint_save(struct s3_client *client, const char *lvs_name,
		   const struct spdk_uuid *lvs_uuid, struct s3_chunk_map *map,
		   uint64_t lsn, uint64_t gen, s3_checkpoint_cb cb_fn, void *cb_arg)
{
	(void)lvs_name; (void)lvs_uuid; (void)map; (void)lsn; (void)gen;
	REQUIRE(client == FAKE_CLIENT && g_ckpt_save_cb == NULL);
	g_ckpt_saves++;
	g_ckpt_save_cb = cb_fn;
	g_ckpt_save_arg = cb_arg;
}

static void
ckpt_save_complete(int status)
{
	s3_checkpoint_cb cb = g_ckpt_save_cb;

	REQUIRE(cb != NULL);
	g_ckpt_save_cb = NULL;
	cb(g_ckpt_save_arg, status, 4096, 0x1234);
}

void
s3_recovery_publish(struct s3_client *client, const char *lvs_name,
		    const struct spdk_uuid *lvs_uuid, uint64_t capacity_bytes,
		    uint32_t chunk_size, uint64_t journal_size, uint64_t wal_size,
		    uint64_t checkpoint_gen, uint64_t checkpoint_lsn,
		    uint64_t checkpoint_bytes, uint32_t checkpoint_entries_crc,
		    s3_op_cb cb_fn, void *cb_arg)
{
	(void)lvs_name; (void)lvs_uuid; (void)capacity_bytes; (void)chunk_size;
	(void)journal_size; (void)wal_size; (void)checkpoint_gen;
	(void)checkpoint_lsn; (void)checkpoint_bytes; (void)checkpoint_entries_crc;
	REQUIRE(client == FAKE_CLIENT);
	g_recovery_publishes++;
	cb_fn(cb_arg, 0);
}

/* ------------------------------------------------------- test harness */

struct destroy_state {
	uint32_t calls;
	int status;
	uint32_t outstanding_at_cb;
	bool close_desc;
};

static void
user_destroy_cb(void *arg, int status)
{
	struct destroy_state *ds = arg;

	ds->calls++;
	ds->status = status;
	ds->outstanding_at_cb = g_dev.outstanding;
	ev(EV_DESTROY_CB);
	/* The representative of "caller releases the local device / ownership":
	 * close the fake descriptor here, exactly where a real caller would. */
	if (ds->close_desc) {
		fake_desc_close();
	}
}

static uint32_t g_reap_calls;

static void
user_reap_cb(void *arg, struct s3_chunk_map *map)
{
	(void)arg;
	REQUIRE(map != NULL);
	g_reap_calls++;
	ev(EV_REAP);
}

struct io_state {
	uint32_t calls;
	int status;
};

static void
bs_cpl(struct spdk_io_channel *ch, void *arg, int status)
{
	struct io_state *st = arg;

	(void)ch;
	st->calls++;
	st->status = status;
}

struct env {
	struct spdk_bs_dev *dev;
	struct destroy_state ds;
	bool wal;
	uint8_t *buf;           /* NCHUNKS * CHUNK */
};

static void
reset_fakes(void)
{
	memset(&g_dev, 0, sizeof(g_dev));
	memset(&g_get, 0, sizeof(g_get));
	memset(&g_put, 0, sizeof(g_put));
	memset(&g_del, 0, sizeof(g_del));
	memset(&g_wal, 0, sizeof(g_wal));
	memset(g_ev_first, 0, sizeof(g_ev_first));
	memset(g_ev_last, 0, sizeof(g_ev_last));
	memset(g_ev_count, 0, sizeof(g_ev_count));
	g_after_destroy = false;
	g_outstanding_at_destroy = 0;
	memset(g_pollers, 0, sizeof(g_pollers));
	g_npollers = 0;
	g_msg_head = g_msg_tail = 0;
	g_seq = 0;
	g_ticks = 1000;
	g_iodev = NULL;
	g_iodev_unreg_cb = NULL;
	g_iodev_reg = g_iodev_unreg = 0;
	g_sync_unregister = false;
	g_del_nocb = 0;
	g_jlsn = g_jtrunc = 0;
	g_ckpt_save_cb = NULL;
	g_ckpt_saves = g_recovery_publishes = 0;
	g_reap_calls = 0;
	g_fail_alloc = false;
	g_alloc_attempts_while_failing = 0;

	memset(&g_sb, 0, sizeof(g_sb));
	g_sb.lvs_uuid.u.raw[0] = 0x77;
	g_sb.chunk_size = CHUNK;
	g_sb.capacity_bytes = CAPACITY;
	memset(g_regions, 0, sizeof(g_regions));
	g_regions[S3_REGION_JOURNAL] = (struct s3_region){ 4096, 1 << 20, true };
	g_regions[S3_REGION_WAL] = (struct s3_region){ 2 << 20, 8 << 20, true };
	/* Four slots: enough for every case, small enough to evict. */
	g_regions[S3_REGION_CACHE] = (struct s3_region){ 16 << 20, 4 * CHUNK, true };

	g_dev.open = true;
}

static void
setup(struct env *e, bool wal, bool journal)
{
	struct s3_lvs_opts o;
	int rc;

	reset_fakes();
	memset(e, 0, sizeof(*e));
	e->wal = wal;
	e->ds.close_desc = true;
	e->buf = calloc(NCHUNKS, CHUNK);
	REQUIRE(e->buf);

	memset(&o, 0, sizeof(o));
	o.lvs_name = "it-bsdev-cache";
	o.chunk_size = CHUNK;

	rc = s3_bs_dev_create(&o, NULL, NULL, FAKE_CLIENT, CAPACITY, &e->dev);
	REQUIRE(rc == 0 && e->dev);
	REQUIRE(g_iodev_reg == 1);

	rc = s3_bs_dev_attach_journal(e->dev, journal ? FAKE_JOURNAL : NULL,
				      FAKE_LOCAL);
	REQUIRE(rc == 0);
	if (wal) {
		rc = s3_bs_dev_attach_wal(e->dev, FAKE_WAL, NULL);
		REQUIRE(rc == 0);
		REQUIRE(pollers_active("flusher_poll") == 1);
	}
	rc = s3_bs_dev_attach_cache(e->dev);
	REQUIRE(rc == 0);

	s3_bs_dev_set_destroy_cb(e->dev, user_destroy_cb, &e->ds);
	s3_bs_dev_set_reap_cb(e->dev, user_reap_cb, NULL);
}

static void
teardown_env(struct env *e)
{
	free(e->buf);
	e->buf = NULL;
}

/* The actual production entry point: spdk_bs_dev->destroy == s3_bs_dev_destroy. */
static void
do_destroy(struct env *e)
{
	g_after_destroy = true;
	g_outstanding_at_destroy = g_dev.outstanding;
	e->dev->destroy(e->dev);
}

static void
seed_chunk(struct env *e, uint64_t chunk, uint64_t lsn)
{
	struct s3_journal_record rec;

	memset(&rec, 0, sizeof(rec));
	rec.op = S3_JOURNAL_OP_CHUNK_UPDATE;
	rec.chunk_index = chunk;
	rec.valid_bytes = CHUNK;
	rec.lsn = lsn;
	rec.flags = S3_CHUNK_IN_S3;
	rec.uuid.u.raw[0] = 0x42;
	rec.uuid.u.raw[15] = (uint8_t)(chunk + 1);
	REQUIRE(s3_bs_dev_journal_apply(e->dev, &rec) == 0);
}

/* Read a whole chunk through the real bs_dev read path. The read misses the
 * cache, goes to the (fake) S3 GET, and its completion populates the cache:
 * that populate is a real s3_cache fill whose bdev write is parked. */
static void
read_chunk_leaving_fill(struct env *e, uint64_t chunk)
{
	struct spdk_bs_dev_cb_args cba = { .cb_fn = bs_cpl, .channel = NULL };
	struct io_state st = {0};
	uint32_t gets = g_get.n, writes = g_dev.submitted_writes;

	cba.cb_arg = &st;
	e->dev->read(e->dev, NULL, e->buf + chunk * CHUNK,
		     chunk * BLK_PER_CHUNK, BLK_PER_CHUNK, &cba);
	REQUIRE(g_get.n == gets + 1);
	get_complete(gets, 0);
	run_msgs();
	REQUIRE(st.calls == 1 && st.status == 0);
	REQUIRE(g_dev.submitted_writes == writes + 1);
}

/* Write a whole chunk through the real WAL path; leaves one S3 PUT parked. */
static uint32_t
write_chunk_leaving_put(struct env *e, uint64_t chunk)
{
	struct spdk_bs_dev_cb_args cba = { .cb_fn = bs_cpl, .channel = NULL };
	struct io_state st = {0};
	uint32_t puts = g_put.n;

	cba.cb_arg = &st;
	memset(e->buf + chunk * CHUNK, (int)(0x10 + chunk), CHUNK);
	e->dev->write(e->dev, NULL, e->buf + chunk * CHUNK,
		      chunk * BLK_PER_CHUNK, BLK_PER_CHUNK, &cba);
	run_msgs();
	wal_complete_appends();
	run_msgs();
	REQUIRE(st.calls == 1 && st.status == 0);
	REQUIRE(g_put.n == puts + 1);
	return puts;
}

/* Complete a parked PUT successfully. The real s3_flush_map_updated() then
 * populates the cache from the uploaded buffer (a parked fill). */
static void
put_ok_leaving_fill(uint32_t put_idx)
{
	uint32_t writes = g_dev.submitted_writes;

	put_complete(put_idx, 0);
	run_msgs();
	REQUIRE(g_dev.submitted_writes == writes + 1);
}

static void
check_nothing_downstream(struct env *e, const char *where)
{
	bool ok = g_wal.close_calls == 0 && g_iodev_unreg == 0 &&
		  g_reap_calls == 0 && e->ds.calls == 0 && g_dev.open &&
		  (!e->wal || pollers_active("flusher_poll") == 1);

	if (!ok) {
		fprintf(stderr, "  downstream ran early at %s: wal_close=%u "
			"unreg=%u reap=%u destroy_cb=%u desc_open=%d flusher=%u\n",
			where, g_wal.close_calls, g_iodev_unreg, g_reap_calls,
			e->ds.calls, (int)g_dev.open,
			pollers_active("flusher_poll"));
	}
	CHECK(ok);
}

static void
check_finished(struct env *e, int want_status)
{
	CHECK(e->ds.calls == 1);
	CHECK(e->ds.status == want_status);
	CHECK(e->ds.outstanding_at_cb == 0);
	CHECK(g_iodev_unreg == 1);
	CHECK(g_reap_calls == 1);
	CHECK(!g_dev.open);
	CHECK(g_dev.outstanding == 0);
	CHECK(g_dev.violations == 0);
	CHECK(g_ev_count[EV_DESTROY_CB] == 1 && g_ev_count[EV_DESC_CLOSE] == 1);
	CHECK(g_ev_count[EV_IODEV_UNREG] == 1 && g_ev_count[EV_REAP] == 1);
	if (g_outstanding_at_destroy > 0) {
		CHECK(g_ev_count[EV_FILL_DONE] == 1);
		CHECK(BEFORE(EV_FILL_DONE, EV_IODEV_UNREG));
	}
	CHECK(BEFORE(EV_IODEV_UNREG, EV_REAP));
	CHECK(BEFORE(EV_REAP, EV_DESTROY_CB));
	CHECK(BEFORE(EV_DESTROY_CB, EV_DESC_CLOSE));
	if (e->wal) {
		CHECK(g_wal.close_calls == 1);
		CHECK(g_ev_count[EV_WAL_CLOSE_DONE] == 1);
		CHECK(pollers_active("flusher_poll") == 0);
		CHECK(g_ev_count[EV_FLUSHER_POLLER_UNREG] == 1);
		if (g_outstanding_at_destroy > 0) {
			CHECK(BEFORE(EV_FILL_DONE, EV_FLUSHER_POLLER_UNREG));
		}
		CHECK(BEFORE(EV_FLUSHER_POLLER_UNREG, EV_WAL_CLOSE_CALL));
		CHECK(BEFORE(EV_WAL_CLOSE_DONE, EV_IODEV_UNREG));
	} else {
		CHECK(g_wal.close_calls == 0);
	}
	/* The pump afterwards must not produce a second callback or touch the
	 * freed context (ASan would flag the latter). */
	pump(5);
	CHECK(e->ds.calls == 1);
	CHECK(g_iodev_unreg == 1);
}

/* ------------------------------------------------------------- cases */

static void
t1_nowal_fill_success(void)
{
	struct env e;

	fprintf(stderr, "T1 no-WAL: pending fill, destroy, success\n");
	setup(&e, false, false);
	seed_chunk(&e, 2, 1);
	read_chunk_leaving_fill(&e, 2);
	CHECK(g_dev.outstanding == 1);

	/* From destroy() until the fill lands, nothing may allocate: the park
	 * itself is allocation-free. (The fakes on this path use static pools.) */
	g_fail_alloc = true;
	do_destroy(&e);
	check_nothing_downstream(&e, "T1 after destroy");
	pump(50);
	check_nothing_downstream(&e, "T1 after pump");

	dev_complete_slot(dev_nth_outstanding(0), true);
	/* Unregister requested from the fill's completion; free_cb (and so the
	 * descriptor close) only on the next message turn. */
	CHECK(g_iodev_unreg == 1);
	CHECK(g_reap_calls == 0 && e.ds.calls == 0 && g_dev.open);
	run_msgs();
	g_fail_alloc = false;
	CHECK(g_alloc_attempts_while_failing == 0);
	check_finished(&e, 0);
	teardown_env(&e);
}

static void
t2_nowal_fill_failure(void)
{
	struct env e;

	fprintf(stderr, "T2 no-WAL: pending fill fails\n");
	setup(&e, false, false);
	seed_chunk(&e, 5, 1);
	read_chunk_leaving_fill(&e, 5);
	do_destroy(&e);
	pump(20);
	check_nothing_downstream(&e, "T2 parked");
	dev_complete_slot(dev_nth_outstanding(0), false);
	run_msgs();
	check_finished(&e, 0);   /* a cache failure is not a teardown failure */
	teardown_env(&e);
}

static void
t3_nowal_multi_fill_sync_unregister(void)
{
	struct env e;
	int s0, s1, s2;

	fprintf(stderr, "T3 no-WAL: 3 fills mixed, out of order, sync unregister\n");
	setup(&e, false, false);
	seed_chunk(&e, 0, 1);
	seed_chunk(&e, 7, 2);
	seed_chunk(&e, 9, 3);
	read_chunk_leaving_fill(&e, 0);
	read_chunk_leaving_fill(&e, 7);
	read_chunk_leaving_fill(&e, 9);
	CHECK(g_dev.outstanding == 3);
	s0 = dev_nth_outstanding(0);
	s1 = dev_nth_outstanding(1);
	s2 = dev_nth_outstanding(2);

	g_sync_unregister = true;
	do_destroy(&e);
	check_nothing_downstream(&e, "T3 parked 3");
	dev_complete_slot(s1, true);
	pump(10);
	check_nothing_downstream(&e, "T3 parked 2");
	dev_complete_slot(s2, false);
	pump(10);
	check_nothing_downstream(&e, "T3 parked 1");
	/* free_cb, s3_cache_destroy, ctx free and the user callback all run
	 * inside this completion frame; ASan checks nothing touches them after. */
	dev_complete_slot(s0, true);
	CHECK(e.ds.calls == 1);
	check_finished(&e, 0);
	teardown_env(&e);
}

static void
t4_wal_flusher_fill_success_alloc_fail(void)
{
	struct env e;
	uint32_t p;

	fprintf(stderr, "T4 WAL: flusher-populated fill pending, full chain\n");
	setup(&e, true, false);
	p = write_chunk_leaving_put(&e, 3);
	put_ok_leaving_fill(p);
	CHECK(g_wal.sync_calls >= 1);
	CHECK(g_dev.outstanding == 1);

	g_fail_alloc = true;
	do_destroy(&e);
	pump(50);
	check_nothing_downstream(&e, "T4 parked");

	dev_complete_slot(dev_nth_outstanding(0), true);
	/* Drain answers inline (nothing dirty) and asks for retirement, which
	 * completes only on a later poller tick, never inside this frame. */
	CHECK(pollers_active("flusher_poll") == 1);
	CHECK(g_wal.close_calls == 0);
	pump(1);
	CHECK(pollers_active("flusher_poll") == 0);
	CHECK(g_wal.close_calls == 1);
	CHECK(g_iodev_unreg == 0 && e.ds.calls == 0 && g_dev.open);
	pump(20);
	CHECK(g_iodev_unreg == 0 && e.ds.calls == 0 && g_dev.open);
	wal_close_complete(0);
	run_msgs();
	g_fail_alloc = false;
	CHECK(g_alloc_attempts_while_failing == 0);
	check_finished(&e, 0);
	teardown_env(&e);
}

static void
t5_wal_fill_failure_close_error(void)
{
	struct env e;
	uint32_t p;

	fprintf(stderr, "T5 WAL: fill fails, WAL close fails\n");
	setup(&e, true, false);
	p = write_chunk_leaving_put(&e, 1);
	put_ok_leaving_fill(p);
	do_destroy(&e);
	pump(10);
	check_nothing_downstream(&e, "T5 parked");
	dev_complete_slot(dev_nth_outstanding(0), false);
	pump(2);
	CHECK(g_wal.close_calls == 1 && e.ds.calls == 0);
	wal_close_complete(-EIO);
	run_msgs();
	check_finished(&e, -EIO);
	teardown_env(&e);
}

static void
t6_wal_fill_never_then_late(void)
{
	struct env e;
	uint32_t p;

	fprintf(stderr, "T6 WAL: fill stalls past drain deadline, then late\n");
	setup(&e, true, false);
	p = write_chunk_leaving_put(&e, 4);
	put_ok_leaving_fill(p);
	do_destroy(&e);

	g_quiet = true;
	for (int i = 0; i < 10000; i++) {
		g_ticks += 360000;   /* 10000 * 0.36 s = one hour of fake time */
		pump(1);
	}
	g_quiet = false;
	/* If a drain had been started, its 30 s deadline would have fired by
	 * now, retirement would have followed and the WAL would be closed. */
	check_nothing_downstream(&e, "T6 after one fake hour");
	CHECK(g_dev.outstanding == 1);
	CHECK(g_put.n == 1);

	dev_complete_slot(dev_nth_outstanding(0), true);
	pump(2);
	CHECK(g_wal.close_calls == 1);
	wal_close_complete(0);
	run_msgs();
	check_finished(&e, 0);
	teardown_env(&e);
}

static void
t7_wal_upload_inflight_requeue(void)
{
	struct env e;
	uint32_t pa, pb, writes_after_destroy;

	fprintf(stderr, "T7 WAL: upload in flight + fill pending; requeue\n");
	setup(&e, true, false);
	pa = write_chunk_leaving_put(&e, 6);
	put_ok_leaving_fill(pa);
	pb = write_chunk_leaving_put(&e, 8);
	CHECK(g_dev.outstanding == 1);

	do_destroy(&e);
	writes_after_destroy = g_dev.submitted_writes;
	check_nothing_downstream(&e, "T7 parked");

	/* Behavioural proof that the flusher is not draining/retiring while the
	 * cache is busy: a failed upload is re-queued and re-submitted, which a
	 * retiring (stopping) flusher would refuse to do. */
	put_complete(pb, -EIO);
	run_msgs();
	pump(2);
	CHECK(g_put.n == pb + 2);
	check_nothing_downstream(&e, "T7 after upload failure");

	dev_complete_slot(dev_nth_outstanding(0), true);
	pump(5);
	/* Drain is now waiting for the re-submitted upload. */
	CHECK(g_wal.close_calls == 0 && pollers_active("flusher_poll") == 1);

	put_complete(pb + 1, 0);
	run_msgs();
	/* Published while destroying: no new cache fill may start. */
	CHECK(g_dev.submitted_writes == writes_after_destroy);
	pump(2);
	CHECK(g_wal.close_calls == 1);
	CHECK(BEFORE(EV_FILL_DONE, EV_FLUSHER_POLLER_UNREG));
	wal_close_complete(0);
	run_msgs();
	check_finished(&e, 0);
	teardown_env(&e);
}

static void
t8_wal_drain_timeout_then_upload_fails(void)
{
	struct env e;
	uint32_t pa, pb, nput;

	fprintf(stderr, "T8 WAL: drain times out after fill; upload fails\n");
	setup(&e, true, false);
	pa = write_chunk_leaving_put(&e, 10);
	put_ok_leaving_fill(pa);
	pb = write_chunk_leaving_put(&e, 11);

	do_destroy(&e);
	check_nothing_downstream(&e, "T8 parked");
	dev_complete_slot(dev_nth_outstanding(0), true);
	pump(2);
	CHECK(g_wal.close_calls == 0);

	g_ticks += 31ULL * 1000 * 1000;   /* past S3_FLUSHER_DRAIN_TIMEOUT_US */
	pump(3);
	/* Timed out -> retiring, but the upload is still in flight: nothing may be
	 * freed or closed yet. */
	CHECK(g_wal.close_calls == 0 && e.ds.calls == 0 && g_dev.open);
	CHECK(pollers_active("flusher_poll") == 1);

	nput = g_put.n;
	put_complete(pb, -EIO);
	run_msgs();
	CHECK(g_put.n == nput);         /* stopping: not re-submitted */
	CHECK(g_wal.close_calls == 0);  /* retire completes on a tick, not inline */
	pump(2);
	CHECK(g_wal.close_calls == 1);
	CHECK(g_ev_last[EV_UPLOAD_DONE] < g_ev_first[EV_WAL_CLOSE_CALL]);
	wal_close_complete(0);
	run_msgs();
	check_finished(&e, -ETIMEDOUT);
	teardown_env(&e);
}

static void
ckpt_user_cb(void *arg, int status)
{
	struct io_state *st = arg;

	st->calls++;
	st->status = status;
	ev(EV_CKPT_CB);
}

static void
t9_reentry_ckpt_deletes_cache(void)
{
	struct env e;
	struct io_state ck = {0};
	uint32_t p;

	fprintf(stderr, "T9 WAL+journal: ckpt -> deferred delete -> cache\n");
	setup(&e, true, true);

	/* First version of chunk 0, flushed and cached. */
	p = write_chunk_leaving_put(&e, 0);
	put_ok_leaving_fill(p);
	dev_complete_slot(dev_nth_outstanding(0), true);
	pump(2);
	/* Second version supersedes it: the old object is retained for a
	 * checkpoint (local_dev attached) and the new one is cached (fill left
	 * pending). */
	p = write_chunk_leaving_put(&e, 0);
	put_ok_leaving_fill(p);
	CHECK(g_dev.outstanding == 1);
	CHECK(g_del.n == 0 && g_del_nocb == 0);

	s3_bs_dev_checkpoint(e.dev, ckpt_user_cb, &ck);
	CHECK(g_ckpt_saves == 1);

	do_destroy(&e);
	pump(10);
	check_nothing_downstream(&e, "T9 waiting for checkpoint");

	/* Checkpoint lands -> publication releases the retained object (a delete
	 * with a completion) -> ckpt_finish() re-enters teardown, which must park
	 * on the delete. */
	ckpt_save_complete(0);
	run_msgs();
	CHECK(ck.calls == 1 && ck.status == 0);
	CHECK(g_recovery_publishes == 1);
	CHECK(g_del.n == 1);
	pump(10);
	check_nothing_downstream(&e, "T9 waiting for deferred delete");

	/* Delete lands -> s3_deferred_deleted() re-enters teardown, which must
	 * now park on the cache. */
	del_complete(0, 0);
	run_msgs();
	pump(10);
	check_nothing_downstream(&e, "T9 waiting for cache");

	dev_complete_slot(dev_nth_outstanding(0), true);
	pump(2);
	CHECK(g_wal.close_calls == 1);
	wal_close_complete(0);
	run_msgs();
	check_finished(&e, 0);
	CHECK(BEFORE(EV_CKPT_CB, EV_DELETE_DONE));
	CHECK(BEFORE(EV_DELETE_DONE, EV_FILL_DONE));
	teardown_env(&e);
}

static void
t10_idle_cache_controls(void)
{
	struct env e;

	fprintf(stderr, "T10a no-WAL: idle cache at destroy\n");
	setup(&e, false, false);
	seed_chunk(&e, 1, 1);
	read_chunk_leaving_fill(&e, 1);
	dev_complete_slot(dev_nth_outstanding(0), true);
	do_destroy(&e);
	CHECK(g_iodev_unreg == 1);   /* proceeded inline, no wait */
	run_msgs();
	check_finished(&e, 0);
	teardown_env(&e);

	fprintf(stderr, "T10b WAL: idle cache at destroy\n");
	setup(&e, true, false);
	do_destroy(&e);
	pump(1);
	CHECK(g_wal.close_calls == 1);
	wal_close_complete(0);
	run_msgs();
	CHECK(e.ds.calls == 1);
	CHECK(e.ds.status == 0);
	CHECK(!g_dev.open && g_dev.violations == 0);
	CHECK(g_reap_calls == 1);
	teardown_env(&e);
}

int
main(void)
{
	t1_nowal_fill_success();
	t2_nowal_fill_failure();
	t3_nowal_multi_fill_sync_unregister();
	t4_wal_flusher_fill_success_alloc_fail();
	t5_wal_fill_failure_close_error();
	t6_wal_fill_never_then_late();
	t7_wal_upload_inflight_requeue();
	t8_wal_drain_timeout_then_upload_fails();
	t9_reentry_ckpt_deletes_cache();
	t10_idle_cache_controls();

	printf("s3_bsdev_cache_teardown_test: %d passed, %d failed "
	       "(error logs seen: %d)\n", g_pass, g_fail, g_errlogs);
	return g_fail == 0 ? 0 : 1;
}
