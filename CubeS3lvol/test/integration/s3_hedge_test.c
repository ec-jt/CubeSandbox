/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   Hedged requests, per-chunk retry and the shared prefetch budget (B20)
 *
 *   Against an in-memory S3 whose requests complete only when the test says so,
 *   so which attempt finishes first, which fail, and how many are in flight are
 *   all chosen here. The fake honours the gate exactly as s3_client_aws.c does:
 *   it writes into the caller's buffer only between s3_gate_write_begin() and
 *   s3_gate_write_end(), and a request whose gate was abandoned completes with
 *   -ECANCELED and writes nothing.
 *
 *     [2] hedge: the duplicate wins; the caller's callback runs exactly once,
 *         with the duplicate's bytes; the primary is abandoned and cancelled,
 *         and its late completion neither calls back nor touches the buffer
 *     [3] hedge: the primary wins; the duplicate is abandoned
 *     [4] hedge: both fail -> one failure callback; a primary that fails
 *         before the hedge is due is delivered at once
 *     [5] hedge: the in-flight cap stops a second duplicate
 *     [6] hedged PUT: first success wins, exactly once
 *     [7] per-chunk retry: a chunk that fails N < limit times is fetched;
 *         nothing else in flight is disturbed
 *     [8] per-chunk retry: a chunk that keeps failing fails the prefetch only
 *         after the limit, and the other chunks still complete
 *     [9] two volumes prefetch concurrently under one shared cap
 *
 *   No network, no credentials, no s3_client_aws.o.
 */

#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/thread.h"
#include "spdk/blob.h"
#include "spdk/util.h"

#include "s3lvol/s3_export.h"
#include "s3lvol/s3_chunk_map.h"
#include "s3lvol/s3_client.h"
#include "s3lvol/s3_hedge.h"

#define CHUNK_SIZE  (64 * 1024)
#define NUM_CHUNKS  12
#define TEST_UUID   "3f2504e0-4f89-11d3-9a0c-0305e82c3302"
#define SRC_PREFIX  "srclvs"

static int g_pass, g_fail;

static void
check_true(const char *what, bool ok)
{
	if (ok) {
		g_pass++;
		printf("\t[PASS] %s\n", what);
	} else {
		g_fail++;
		printf("\t[FAIL] %s\n", what);
	}
}

static void
check_u64(const char *what, uint64_t got, uint64_t want)
{
	if (got == want) {
		g_pass++;
		printf("\t[PASS] %s (got %" PRIu64 ")\n", what, got);
	} else {
		g_fail++;
		printf("\t[FAIL] %s (got %" PRIu64 ", want %" PRIu64 ")\n", what, got, want);
	}
}

/* ==========================================================================
 * In-memory S3 that honours the gate
 * ========================================================================== */

struct fake_req {
	bool          put;
	char          key[S3_EXPORT_KEY_MAX];
	uint64_t      offset;
	uint64_t      len;
	void         *buf;
	struct s3_gate *gate;
	s3_get_cb     get_cb;
	s3_op_cb      op_cb;
	void         *cb_arg;
	uint32_t      seq;
	TAILQ_ENTRY(fake_req) link;
};

static TAILQ_HEAD(, fake_req) g_pending = TAILQ_HEAD_INITIALIZER(g_pending);
static uint32_t g_reqs, g_inflight, g_peak_inflight, g_seq;
static uint32_t g_cancels;
static uint32_t g_late_writes;   /* writes attempted after abandon: must stay 0 */

/* Per-key failure budget for [7]/[8]: fail this many times, then succeed. */
static char     g_flaky_key[S3_EXPORT_KEY_MAX];
static uint32_t g_flaky_left;
/* When a flaky GET fails, how many bytes of its range it delivers first,
 * like a CRT stream that aborts partway (crt=2073). */
static uint64_t g_flaky_partial;
static uint64_t g_last_get_offset, g_last_get_len;

static void
fake_acquire(void *t)
{
	(void)t;
}

static void
fake_release(void *t)
{
	(void)t;
}

static void
fake_cancel(void *t)
{
	(void)t;
	g_cancels++;
}

static const struct s3_deadline_ops g_fake_ops = {
	.acquire = fake_acquire,
	.release = fake_release,
	.cancel  = fake_cancel,
};

static uint8_t
obj_byte(const char *key, uint64_t off)
{
	uint32_t h = 2166136261u;
	const char *c;

	for (c = key; *c; c++) {
		h = (h ^ (uint8_t)*c) * 16777619u;
	}
	return (uint8_t)((h + off * 7 + off / 4096) % 255 + 1);
}

static struct fake_req *
fake_new(const char *key, struct s3_gate *gate, void *cb_arg)
{
	struct fake_req *r = calloc(1, sizeof(*r));

	snprintf(r->key, sizeof(r->key), "%s", key);
	r->gate = gate;
	r->cb_arg = cb_arg;
	r->seq = ++g_seq;
	g_reqs++;
	g_inflight++;
	if (g_inflight > g_peak_inflight) {
		g_peak_inflight = g_inflight;
	}
	TAILQ_INSERT_TAIL(&g_pending, r, link);
	if (gate) {
		/* Exactly where the real client attaches: after the request exists. */
		s3_gate_attach(gate, (void *)r, &g_fake_ops);
	}
	return r;
}

int
s3_get_range_gated(struct s3_client *client, const char *key, uint64_t offset,
		   uint64_t len, void *buf, struct s3_gate *gate, s3_get_cb cb,
		   void *cb_arg)
{
	struct fake_req *r;

	(void)client;
	r = fake_new(key, gate, cb_arg);
	r->offset = offset;
	r->len = len;
	g_last_get_offset = offset;
	g_last_get_len = len;
	r->buf = buf;
	r->get_cb = cb;
	return 0;
}

int
s3_get_range(struct s3_client *client, const char *key, uint64_t offset,
	     uint64_t len, void *buf, s3_get_cb cb, void *cb_arg)
{
	return s3_get_range_gated(client, key, offset, len, buf, NULL, cb, cb_arg);
}

int
s3_put_gated(struct s3_client *client, const char *key, struct iovec *iov,
	     int iovcnt, struct s3_gate *gate, s3_op_cb cb, void *cb_arg)
{
	struct fake_req *r;

	(void)client;
	(void)iov;
	(void)iovcnt;
	r = fake_new(key, gate, cb_arg);
	r->put = true;
	r->op_cb = cb;
	return 0;
}

int
s3_put(struct s3_client *client, const char *key, struct iovec *iov, int iovcnt,
       bool if_none_match, s3_op_cb cb, void *cb_arg)
{
	(void)if_none_match;
	return s3_put_gated(client, key, iov, iovcnt, NULL, cb, cb_arg);
}

int
s3_head(struct s3_client *client, const char *key, uint64_t *out_size,
	s3_op_cb cb, void *cb_arg)
{
	(void)client;
	(void)key;
	(void)out_size;
	cb(cb_arg, -ENOENT);
	return 0;
}

int
s3_delete(struct s3_client *client, const char *key, s3_op_cb cb, void *cb_arg)
{
	(void)client;
	(void)key;
	if (cb) {
		cb(cb_arg, 0);
	}
	return 0;
}

void
s3_client_put(struct s3_client *client)
{
	(void)client;
}

/* Complete one request: \p status 0 delivers the object's bytes. Exactly what
 * the real client does with the gate: the copy is fenced, and an abandoned
 * request writes nothing and reports -ECANCELED. */
static void
fake_finish(struct fake_req *r, int status)
{
	uint64_t i, got = 0;

	TAILQ_REMOVE(&g_pending, r, link);
	g_inflight--;
	if (status == 0 && g_flaky_key[0] && strcmp(r->key, g_flaky_key) == 0 &&
	    g_flaky_left > 0) {
		g_flaky_left--;
		status = -EIO;
		if (!r->put && g_flaky_partial > 0 && s3_gate_write_begin(r->gate)) {
			uint64_t n = g_flaky_partial < r->len ? g_flaky_partial : r->len;

			/* Exactly what the real client does on an aborted body: the
			 * prefix is in the buffer, its length is on the gate, and the
			 * callback still reports 0. */
			for (i = 0; i < n; i++) {
				((uint8_t *)r->buf)[i] = obj_byte(r->key, r->offset + i);
			}
			s3_gate_write_end(r->gate);
			s3_gate_set_partial(r->gate, n);
		}
	}
	if (status == 0 && !r->put) {
		if (s3_gate_write_begin(r->gate)) {
			for (i = 0; i < r->len; i++) {
				((uint8_t *)r->buf)[i] = obj_byte(r->key, r->offset + i);
			}
			s3_gate_write_end(r->gate);
			got = r->len;
		} else {
			status = -ECANCELED;
		}
	} else if (status == 0 && s3_gate_is_abandoned(r->gate)) {
		status = -ECANCELED;
	}
	s3_gate_finish(r->gate);
	if (r->put) {
		r->op_cb(r->cb_arg, status);
	} else {
		r->get_cb(r->cb_arg, got, status);
	}
	free(r);
}

static struct fake_req *
fake_nth(uint32_t n)
{
	struct fake_req *r;

	TAILQ_FOREACH(r, &g_pending, link) {
		if (n-- == 0) {
			return r;
		}
	}
	return NULL;
}

static uint32_t
fake_complete_all(void)
{
	uint32_t n = 0;

	while (!TAILQ_EMPTY(&g_pending)) {
		fake_finish(TAILQ_FIRST(&g_pending), 0);
		n++;
	}
	return n;
}

static void
reset_fake(void)
{
	g_reqs = 0;
	g_peak_inflight = 0;
	g_cancels = 0;
	g_late_writes = 0;
	g_flaky_key[0] = '\0';
	g_flaky_left = 0;
	g_flaky_partial = 0;
}

/* ==========================================================================
 * Threads and time
 * ========================================================================== */

static struct spdk_thread *g_thread;

static uint64_t
now_ms(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void
poll_some(int n)
{
	while (n-- > 0) {
		spdk_thread_poll(g_thread, 0, 0);
	}
}

/* Poll until \p pred or \p ms of wall time have gone by. */
static bool
poll_until(bool (*pred)(void *), void *arg, uint64_t ms)
{
	uint64_t end = now_ms() + ms;

	while (now_ms() < end) {
		spdk_thread_poll(g_thread, 0, 0);
		if (pred(arg)) {
			return true;
		}
		usleep(200);
	}
	return pred(arg);
}

static bool
pending_at_least(void *arg)
{
	uint32_t want = (uint32_t)(uintptr_t)arg, n = 0;
	struct fake_req *r;

	TAILQ_FOREACH(r, &g_pending, link) {
		n++;
	}
	return n >= want;
}

static void
poll_for_ms(uint64_t ms)
{
	uint64_t end = now_ms() + ms;

	while (now_ms() < end) {
		spdk_thread_poll(g_thread, 0, 0);
		usleep(200);
	}
}

/* ==========================================================================
 * Hedge tests
 * ========================================================================== */

struct get_seen {
	uint32_t calls;
	int      status;
	uint64_t bytes;
};

static void
get_cb(void *arg, uint64_t bytes, int status)
{
	struct get_seen *s = arg;

	s->calls++;
	s->status = status;
	s->bytes = bytes;
}

static void
op_cb(void *arg, int status)
{
	struct get_seen *s = arg;

	s->calls++;
	s->status = status;
}

static bool
buf_is_object(const uint8_t *b, const char *key, uint64_t len)
{
	uint64_t i;

	for (i = 0; i < len; i++) {
		if (b[i] != obj_byte(key, i)) {
			return false;
		}
	}
	return true;
}

static void
hedge_env(const char *on, const char *min_ms, const char *cap)
{
	setenv("S3LVOL_S3_HEDGE", on, 1);
	setenv("S3LVOL_S3_HEDGE_MIN_MS", min_ms, 1);
	setenv("S3LVOL_S3_HEDGE_MAX_INFLIGHT", cap, 1);
	s3_hedge_config_load();
}

static void
test_hedge(void)
{
	struct s3_client *cl = (struct s3_client *)0x1;
	const char *key = "srclvs/data/hedge-object";
	struct s3_hedge_counters ctr;
	struct get_seen seen;
	struct fake_req *primary, *dup;
	uint8_t *buf = malloc(CHUNK_SIZE);
	struct s3_hedge_stats st;

	printf("\n[2] hedge: the duplicate wins, exactly once\n");
	hedge_env("1", "30", "4");
	reset_fake();
	memset(&ctr, 0, sizeof(ctr));
	memset(&seen, 0, sizeof(seen));
	memset(buf, 0xEE, CHUNK_SIZE);
	check_true("submitted", s3_get_range_hedged(cl, key, 0, CHUNK_SIZE, buf, &ctr,
			get_cb, &seen) == 0);
	check_u64("one request before the hedge delay", g_reqs, 1);
	check_true("a duplicate is sent once the delay has passed",
		   poll_until(pending_at_least, (void *)2, 2000));
	check_u64("counters: one hedge issued", ctr.issued, 1);
	primary = fake_nth(0);
	dup = fake_nth(1);
	check_true("the duplicate reads into its own buffer", dup && dup->buf != buf);
	fake_finish(dup, 0);
	check_u64("the callback ran once", seen.calls, 1);
	check_true("with success and the full length",
		   seen.status == 0 && seen.bytes == CHUNK_SIZE);
	check_true("and the caller's buffer holds the object",
		   buf_is_object(buf, key, CHUNK_SIZE));
	check_u64("the primary was cancelled", g_cancels, 1);
	check_u64("counters: the hedge won", ctr.won, 1);
	/* The caller reuses its buffer at once, as a slot does. */
	memset(buf, 0x5A, CHUNK_SIZE);
	fake_finish(primary, 0);
	check_u64("the primary's late completion does not call back", seen.calls, 1);
	check_true("and does not write into the reused buffer",
		   buf[0] == 0x5A && buf[CHUNK_SIZE - 1] == 0x5A);
	s3_hedge_get_stats(&st);
	check_u64("no hedge is left in flight", st.inflight, 0);

	printf("\n[3] hedge: the primary wins\n");
	reset_fake();
	memset(&ctr, 0, sizeof(ctr));
	memset(&seen, 0, sizeof(seen));
	memset(buf, 0xEE, CHUNK_SIZE);
	s3_get_range_hedged(cl, key, 0, CHUNK_SIZE, buf, &ctr, get_cb, &seen);
	poll_until(pending_at_least, (void *)2, 2000);
	primary = fake_nth(0);
	dup = fake_nth(1);
	fake_finish(primary, 0);
	check_u64("one callback", seen.calls, 1);
	check_true("with the object", seen.status == 0 && buf_is_object(buf, key, CHUNK_SIZE));
	check_u64("the duplicate was cancelled", g_cancels, 1);
	fake_finish(dup, 0);
	check_u64("its completion is swallowed", seen.calls, 1);
	check_u64("counters: issued but not won", ctr.won, 0);

	printf("\n[4] hedge: failures\n");
	reset_fake();
	memset(&seen, 0, sizeof(seen));
	s3_get_range_hedged(cl, key, 0, CHUNK_SIZE, buf, NULL, get_cb, &seen);
	poll_until(pending_at_least, (void *)2, 2000);
	fake_finish(fake_nth(0), -EIO);
	check_u64("one attempt failing while the other runs is not delivered", seen.calls, 0);
	fake_finish(fake_nth(0), -ETIMEDOUT);
	check_u64("both failing: one callback", seen.calls, 1);
	check_true("carrying the first error", seen.status == -EIO);
	reset_fake();
	memset(&seen, 0, sizeof(seen));
	s3_get_range_hedged(cl, key, 0, CHUNK_SIZE, buf, NULL, get_cb, &seen);
	fake_finish(fake_nth(0), -EIO);
	check_u64("a primary failing before the hedge is due is delivered", seen.calls, 1);
	poll_for_ms(80);
	check_u64("and no duplicate is sent after it", g_reqs, 1);

	printf("\n[5] hedge: the in-flight cap\n");
	hedge_env("1", "30", "1");
	reset_fake();
	{
		struct get_seen s1 = {0}, s2 = {0};
		struct s3_hedge_counters c1 = {0}, c2 = {0};
		uint8_t *b2 = malloc(CHUNK_SIZE);

		s3_get_range_hedged(cl, key, 0, CHUNK_SIZE, buf, &c1, get_cb, &s1);
		s3_get_range_hedged(cl, "srclvs/data/other", 0, CHUNK_SIZE, b2, &c2,
				    get_cb, &s2);
		poll_until(pending_at_least, (void *)3, 2000);
		poll_for_ms(80);
		check_u64("two primaries and one duplicate", g_reqs, 3);
		check_u64("one hedge was skipped at the cap", c1.skipped + c2.skipped, 1);
		fake_complete_all();
		poll_some(10);
		check_true("both still complete once",
			   s1.calls == 1 && s2.calls == 1 && s1.status == 0 && s2.status == 0);
		free(b2);
	}

	printf("\n[6] hedged PUT\n");
	hedge_env("1", "30", "4");
	reset_fake();
	memset(&ctr, 0, sizeof(ctr));
	memset(&seen, 0, sizeof(seen));
	{
		struct iovec iov = { .iov_base = buf, .iov_len = CHUNK_SIZE };

		check_true("submitted", s3_put_hedged(cl, "srclvs/data/put-object", &iov, 1,
				&ctr, op_cb, &seen) == 0);
		poll_until(pending_at_least, (void *)2, 2000);
		check_u64("a duplicate PUT was sent", ctr.issued, 1);
		fake_finish(fake_nth(1), 0);
		check_true("one success", seen.calls == 1 && seen.status == 0);
		fake_finish(fake_nth(0), 0);
		check_u64("still exactly one", seen.calls, 1);
		check_u64("counters: the duplicate won", ctr.won, 1);
	}

	printf("\n[6b] a failed GET reports the prefix it delivered\n");
	hedge_env("0", "30", "4");
	reset_fake();
	memset(&seen, 0, sizeof(seen));
	memset(buf, 0xEE, CHUNK_SIZE);
	snprintf(g_flaky_key, sizeof(g_flaky_key), "%s", key);
	g_flaky_left = 1;
	g_flaky_partial = CHUNK_SIZE / 4;
	s3_get_range_hedged(cl, key, 0, CHUNK_SIZE, buf, NULL, get_cb, &seen);
	fake_complete_all();
	check_true("the failure is delivered", seen.calls == 1 && seen.status == -EIO);
	check_u64("with the prefix length", seen.bytes, CHUNK_SIZE / 4);
	check_true("and the prefix is the object's bytes",
		   buf_is_object(buf, key, CHUNK_SIZE / 4));
	reset_fake();

	printf("\n[6c] a hedge failing too: the longer prefix wins\n");
	hedge_env("1", "30", "4");
	reset_fake();
	memset(&seen, 0, sizeof(seen));
	memset(buf, 0xEE, CHUNK_SIZE);
	s3_get_range_hedged(cl, key, 0, CHUNK_SIZE, buf, NULL, get_cb, &seen);
	poll_until(pending_at_least, (void *)2, 2000);
	snprintf(g_flaky_key, sizeof(g_flaky_key), "%s", key);
	g_flaky_left = 2;
	g_flaky_partial = CHUNK_SIZE / 8;
	fake_finish(fake_nth(0), 0);   /* primary: 1/8 then fails */
	g_flaky_partial = CHUNK_SIZE / 2;
	fake_finish(fake_nth(0), 0);   /* duplicate: 1/2 then fails */
	check_true("one failure delivered", seen.calls == 1 && seen.status == -EIO);
	check_u64("carrying the duplicate's longer prefix", seen.bytes, CHUNK_SIZE / 2);
	check_true("copied into the caller's buffer",
		   buf_is_object(buf, key, CHUNK_SIZE / 2));
	reset_fake();

	printf("\n    hedging disabled: a plain request\n");
	hedge_env("0", "30", "4");
	reset_fake();
	memset(&seen, 0, sizeof(seen));
	s3_get_range_hedged(cl, key, 0, CHUNK_SIZE, buf, NULL, get_cb, &seen);
	poll_for_ms(80);
	check_u64("no duplicate with S3LVOL_S3_HEDGE=0", g_reqs, 1);
	fake_complete_all();
	check_u64("and it completes", seen.calls, 1);
	free(buf);
}

/* ==========================================================================
 * Export prefetch: retry and shared budget
 * ========================================================================== */

static struct spdk_uuid g_uuids[NUM_CHUNKS];

static struct s3_export_manifest *
make_manifest(void)
{
	struct s3_export_manifest *m = NULL;
	int i;

	if (s3_export_manifest_create(TEST_UUID, (uint64_t)NUM_CHUNKS * CHUNK_SIZE,
				      CHUNK_SIZE, S3_EXPORT_LAYOUT_REF, &m) != 0) {
		return NULL;
	}
	m->generation = 1;
	snprintf(m->src.prefix, sizeof(m->src.prefix), "%s", SRC_PREFIX);
	snprintf(m->srcs[0].prefix, sizeof(m->srcs[0].prefix), "%s", SRC_PREFIX);
	for (i = 0; i < NUM_CHUNKS; i++) {
		uint8_t raw[16];

		memset(raw, 0x40 + i, sizeof(raw));
		memcpy(&g_uuids[i], raw, sizeof(g_uuids[i]));
		s3_export_manifest_set_ref(m, i, &g_uuids[i], CHUNK_SIZE);
	}
	s3_export_manifest_seal(m);
	return m;
}

struct pf_seen {
	uint32_t ok;
	uint32_t failed;
};

static void
pf_cb(void *arg, uint64_t chunk_index, int status)
{
	struct pf_seen *s = arg;

	(void)chunk_index;
	if (status == 0) {
		s->ok++;
	} else {
		s->failed++;
	}
}

struct pf_done_arg {
	struct s3_export_prefetcher *p;
	uint64_t                     n;
};

static bool
pf_settled(void *arg)
{
	struct pf_done_arg *a = arg;
	struct s3_export_prefetch_progress pg;

	/* Complete whatever is pending; retries come back by themselves. */
	while (!TAILQ_EMPTY(&g_pending)) {
		fake_finish(TAILQ_FIRST(&g_pending), 0);
	}
	s3_export_prefetcher_release_below(a->p, UINT64_MAX);
	s3_export_prefetcher_get_progress(a->p, &pg);
	return s3_export_prefetcher_idle(a->p) &&
	       (pg.fetched == a->n || pg.status != 0);
}

/* Read one whole chunk through the device (it is served from the slot the
 * prefetch filled). */
struct read_arg {
	bool done;
	int  status;
	struct spdk_bs_dev_cb_args args;
};

static void
read_arg_done(struct spdk_io_channel *ch, void *arg, int status)
{
	struct read_arg *r = arg;

	(void)ch;
	r->done = true;
	r->status = status;
}

static bool
read_arg_settled(void *arg)
{
	return ((struct read_arg *)arg)->done;
}

static int
read_chunk(struct spdk_bs_dev *dev, uint64_t chunk, uint8_t *out, struct read_arg *r)
{
	struct spdk_io_channel *ch = dev->create_channel(dev);
	uint32_t bl = dev->blocklen;

	memset(r, 0, sizeof(*r));
	if (!ch) {
		return -ENOMEM;
	}
	r->args.cb_fn = read_arg_done;
	r->args.cb_arg = r;
	r->args.channel = ch;
	dev->read(dev, ch, out, chunk * CHUNK_SIZE / bl, CHUNK_SIZE / bl, &r->args);
	poll_until(read_arg_settled, r, 5000);
	dev->destroy_channel(dev, ch);
	poll_some(10);
	return r->done ? r->status : -ETIMEDOUT;
}

static void
test_retry(struct s3_export_manifest *m)
{
	struct spdk_bs_dev *dev = NULL;
	struct s3_export_prefetcher *pf = NULL;
	struct s3_export_prefetch_progress pg;
	struct s3_export_bs_dev_stats st;
	struct pf_seen seen = {0};
	struct pf_done_arg done;
	uint64_t chunks[NUM_CHUNKS];
	uint32_t i;
	int rc;

	for (i = 0; i < NUM_CHUNKS; i++) {
		chunks[i] = i;
	}
	hedge_env("0", "30", "4");
	setenv("S3LVOL_IMPORT_CHUNK_RETRIES", "3", 1);
	setenv("S3LVOL_IMPORT_RETRY_BASE_MS", "1", 1);
	setenv("S3LVOL_IMPORT_RETRY_CAP_MS", "4", 1);

	printf("\n[7] per-chunk retry: transient failures are retried\n");
	rc = s3_export_bs_dev_create((struct s3_client *)0x1, m, &dev);
	check_true("device created", rc == 0);
	reset_fake();
	s3_chunk_data_key(SRC_PREFIX, &g_uuids[4], g_flaky_key, sizeof(g_flaky_key));
	g_flaky_left = 2;
	rc = s3_export_prefetcher_create(dev, chunks, NUM_CHUNKS, 4, pf_cb, &seen, &pf);
	check_true("prefetcher created", rc == 0);
	s3_export_prefetcher_pump(pf);
	done.p = pf;
	done.n = NUM_CHUNKS;
	check_true("settles", poll_until(pf_settled, &done, 5000));
	s3_export_prefetcher_get_progress(pf, &pg);
	check_u64("every chunk fetched", pg.fetched, NUM_CHUNKS);
	check_true("status is clean", pg.status == 0);
	check_u64("two retries counted for the flaky chunk", pg.retries, 2);
	check_u64("one GET per chunk plus the retries", g_reqs, NUM_CHUNKS + 2);
	check_u64("no failure reached the callback", seen.failed, 0);
	s3_export_bs_dev_get_stats(dev, &st);
	check_u64("device stats count the retries", st.fetch_retries, 2);
	check_u64("and no final fetch error", st.fetch_errors, 0);
	s3_export_prefetcher_destroy(pf);
	pf = NULL;
	dev->destroy(dev);
	poll_some(50);

	printf("\n[8] per-chunk retry: failing past the limit\n");
	dev = NULL;
	rc = s3_export_bs_dev_create((struct s3_client *)0x1, m, &dev);
	reset_fake();
	memset(&seen, 0, sizeof(seen));
	s3_chunk_data_key(SRC_PREFIX, &g_uuids[1], g_flaky_key, sizeof(g_flaky_key));
	g_flaky_left = 1000;
	rc = s3_export_prefetcher_create(dev, chunks, NUM_CHUNKS, 4, pf_cb, &seen, &pf);
	s3_export_prefetcher_pump(pf);
	done.p = pf;
	check_true("settles", poll_until(pf_settled, &done, 5000));
	s3_export_prefetcher_get_progress(pf, &pg);
	check_true("status carries the error", pg.status == -EIO);
	check_u64("the limit's worth of retries", pg.retries, 3);
	check_u64("the failure reached the callback once", seen.failed, 1);
	check_true("the chunks in flight alongside it were not aborted",
		   seen.ok >= 3 && pg.fetched == seen.ok);
	s3_export_prefetcher_destroy(pf);
	dev->destroy(dev);
	poll_some(50);

	printf("\n[8b] ranged retry: a GET that aborts partway keeps its bytes\n");
	dev = NULL;
	rc = s3_export_bs_dev_create((struct s3_client *)0x1, m, &dev);
	reset_fake();
	memset(&seen, 0, sizeof(seen));
	s3_chunk_data_key(SRC_PREFIX, &g_uuids[2], g_flaky_key, sizeof(g_flaky_key));
	g_flaky_left = 2;
	g_flaky_partial = CHUNK_SIZE / 4;
	rc = s3_export_prefetcher_create(dev, &chunks[2], 1, 1, pf_cb, &seen, &pf);
	s3_export_prefetcher_pump(pf);
	done.p = pf;
	done.n = 1;
	check_true("settles", poll_until(pf_settled, &done, 5000));
	s3_export_prefetcher_get_progress(pf, &pg);
	check_true("the chunk is fetched", pg.fetched == 1 && pg.status == 0);
	check_u64("three GETs: two partial, one for the rest", g_reqs, 3);
	check_u64("the last asks only for the missing tail", g_last_get_len,
		  CHUNK_SIZE - 2 * (CHUNK_SIZE / 4));
	check_u64("starting where the second one stopped", g_last_get_offset,
		  2 * (CHUNK_SIZE / 4));
	check_u64("progressing attempts do not use the retry budget", pg.retries, 0);
	s3_export_bs_dev_get_stats(dev, &st);
	check_u64("two ranged resumes counted", st.ranged_resumes, 2);
	check_u64("keeping half the chunk", st.resumed_bytes, CHUNK_SIZE / 2);
	{
		/* Read the whole chunk back through the device: every byte must
		 * be the object's, the kept prefix as much as the fetched tail. */
		struct read_arg ra;
		uint8_t *out = calloc(1, CHUNK_SIZE);

		check_true("read the chunk back", read_chunk(dev, 2, out, &ra) == 0);
		check_true("every byte is the object's",
			   buf_is_object(out, g_flaky_key, CHUNK_SIZE));
		free(out);
	}
	s3_export_prefetcher_destroy(pf);
	dev->destroy(dev);
	poll_some(50);

	/* A 64 KiB chunk object is one range at any S3LVOL_PACK_RANGE_MIB: the
	 * tests above see exactly one GET per chunk. */
	check_u64("64 KiB chunk with 1 MiB ranges is one range",
		  s3_export_range_count(CHUNK_SIZE, 1u << 20), 1);
	check_u64("16 MiB pack with 4 MiB ranges is four",
		  s3_export_range_count(16u << 20, 4u << 20), 4);
	setenv("S3LVOL_IMPORT_CHUNK_RETRIES", "5", 1);
}

/* Per-range hedge and per-range resume, on 4 MiB objects split into 1 MiB
	* ranges. Uses the real s3_get_range_hedged against the gate-aware fake. */
#define BIG_SIZE   (4u << 20)
#define BIG_CHUNKS 2

static void
test_ranges(void)
{
	struct s3_export_manifest *m = NULL;
	struct spdk_uuid u[BIG_CHUNKS];
	struct spdk_bs_dev *dev = NULL;
	struct s3_export_prefetcher *pf = NULL;
	struct s3_export_prefetch_progress pg;
	struct s3_export_bs_dev_stats st;
	struct pf_seen seen = {0};
	struct pf_done_arg done;
	uint64_t chunks[1] = {0};
	struct fake_req *r;
	uint32_t i, n;
	uint64_t t0;

	if (s3_export_manifest_create(TEST_UUID, (uint64_t)BIG_CHUNKS * BIG_SIZE, BIG_SIZE,
				      S3_EXPORT_LAYOUT_REF, &m) != 0) {
		check_true("big manifest", false);
		return;
	}
	m->generation = 1;
	snprintf(m->src.prefix, sizeof(m->src.prefix), "%s", SRC_PREFIX);
	snprintf(m->srcs[0].prefix, sizeof(m->srcs[0].prefix), "%s", SRC_PREFIX);
	for (i = 0; i < BIG_CHUNKS; i++) {
		uint8_t raw[16];

		memset(raw, 0x70 + i, sizeof(raw));
		memcpy(&u[i], raw, sizeof(u[i]));
		s3_export_manifest_set_ref(m, i, &u[i], BIG_SIZE);
	}
	s3_export_manifest_seal(m);
	setenv("S3LVOL_PACK_RANGE_MIB", "1", 1);
	setenv("S3LVOL_IMPORT_CHUNK_RETRIES", "3", 1);
	setenv("S3LVOL_IMPORT_RETRY_BASE_MS", "1", 1);
	setenv("S3LVOL_IMPORT_RETRY_CAP_MS", "2", 1);

	printf("\n[8c] ranged fetch: a range that aborts partway resumes alone\n");
	hedge_env("0", "30", "4");
	reset_fake();
	s3_export_bs_dev_create((struct s3_client *)0x1, m, &dev);
	s3_chunk_data_key(SRC_PREFIX, &u[0], g_flaky_key, sizeof(g_flaky_key));
	g_flaky_left = 1;
	g_flaky_partial = 300 * 1024;
	s3_export_prefetcher_create(dev, chunks, 1, 1, pf_cb, &seen, &pf);
	s3_export_prefetcher_pump(pf);
	check_u64("four range GETs in flight at once", g_inflight, 4);
	done.p = pf;
	done.n = 1;
	check_true("settles", poll_until(pf_settled, &done, 5000));
	s3_export_prefetcher_get_progress(pf, &pg);
	check_true("object fetched", pg.fetched == 1 && pg.status == 0);
	check_u64("4 ranges + 1 resume", g_reqs, 5);
	check_u64("the resume asks only for that range's tail",
		  g_last_get_len, (1u << 20) - 300 * 1024);
	check_u64("starting where it stopped", g_last_get_offset, 300 * 1024);
	s3_export_bs_dev_get_stats(dev, &st);
	check_u64("one ranged fetch", st.ranged_fetches, 1);
	check_u64("one range retry", st.range_retries, 1);
	check_u64("prefix kept", st.resumed_bytes, 300 * 1024);
	{
		struct read_arg ra;
		uint8_t *out = calloc(1, CHUNK_SIZE);
		struct spdk_io_channel *ch = dev->create_channel(dev);
		uint32_t bl = dev->blocklen;

		/* The first 64 KiB of the object straddle nothing, so read the
		 * 64 KiB that crosses the resumed boundary at 300 KiB. */
		memset(&ra, 0, sizeof(ra));
		ra.args.cb_fn = read_arg_done;
		ra.args.cb_arg = &ra;
		ra.args.channel = ch;
		dev->read(dev, ch, out, (280 * 1024) / bl, CHUNK_SIZE / bl, &ra.args);
		poll_until(read_arg_settled, &ra, 5000);
		check_true("bytes across the resume boundary are the object's",
			   ra.done && ra.status == 0 && ({
				   bool ok = true;
				   uint64_t k;

				   for (k = 0; k < CHUNK_SIZE && ok; k++) {
					   ok = out[k] == obj_byte(g_flaky_key, 280 * 1024 + k);
				   }
				   ok;
			   }));
		dev->destroy_channel(dev, ch);
		poll_some(10);
		free(out);
	}
	s3_export_prefetcher_destroy(pf);
	pf = NULL;
	dev->destroy(dev);
	poll_some(50);

	printf("\n[8d] per-range hedge: one slow range is hedged, the others are not\n");
	hedge_env("1", "20", "8");
	reset_fake();
	s3_export_bs_dev_create((struct s3_client *)0x1, m, &dev);
	memset(&seen, 0, sizeof(seen));
	s3_export_prefetcher_create(dev, chunks, 1, 1, pf_cb, &seen, &pf);
	s3_export_prefetcher_pump(pf);
	check_u64("four range GETs", g_reqs, 4);
	/* Complete three ranges now; leave the third one (offset 2 MiB) slow. */
	n = 0;
	for (i = 0; i < 4; i++) {
		r = fake_nth(n);
		if (r && r->offset == 2u * (1u << 20)) {
			n++;
			continue;
		}
		if (r) {
			fake_finish(r, 0);
		}
	}
	poll_some(10);
	t0 = now_ms();
	while (g_reqs < 5 && now_ms() - t0 < 2000) {
		poll_some(5);
	}
	check_u64("exactly one hedge, for the slow range", g_reqs, 5);
	check_u64("of the same range", g_last_get_offset, 2u * (1u << 20));
	check_u64("same length", g_last_get_len, 1u << 20);
	/* The duplicate wins. */
	r = fake_nth(1);
	if (r) {
		fake_finish(r, 0);
	}
	done.p = pf;
	done.n = 1;
	check_true("settles", poll_until(pf_settled, &done, 5000));
	s3_export_prefetcher_get_progress(pf, &pg);
	check_true("object fetched", pg.fetched == 1 && pg.status == 0);
	s3_export_bs_dev_get_stats(dev, &st);
	check_u64("one hedge issued", st.hedges_issued, 1);
	check_u64("and it won", st.hedges_won, 1);
	s3_export_prefetcher_destroy(pf);
	dev->destroy(dev);
	poll_some(50);
	s3_export_manifest_unref(m);
	unsetenv("S3LVOL_PACK_RANGE_MIB");
	unsetenv("S3LVOL_IMPORT_RETRY_BASE_MS");
	unsetenv("S3LVOL_IMPORT_RETRY_CAP_MS");
	setenv("S3LVOL_IMPORT_CHUNK_RETRIES", "5", 1);
	hedge_env("0", "30", "4");
}

struct two_arg {
	struct s3_export_prefetcher *p[2];
	bool                         overlapped;
};

static bool
two_settled(void *arg)
{
	struct two_arg *a = arg;
	struct s3_export_prefetch_progress g0, g1;

	s3_export_prefetcher_get_progress(a->p[0], &g0);
	s3_export_prefetcher_get_progress(a->p[1], &g1);
	if (g0.inflight > 0 && g1.inflight > 0) {
		a->overlapped = true;
	}
	/* One completion at a time, so the interleaving is observable. */
	if (!TAILQ_EMPTY(&g_pending)) {
		fake_finish(TAILQ_FIRST(&g_pending), 0);
	}
	s3_export_prefetcher_release_below(a->p[0], UINT64_MAX);
	s3_export_prefetcher_release_below(a->p[1], UINT64_MAX);
	return g0.fetched == NUM_CHUNKS && g1.fetched == NUM_CHUNKS &&
	       s3_export_prefetcher_idle(a->p[0]) && s3_export_prefetcher_idle(a->p[1]);
}

static void
test_two_volumes(struct s3_export_manifest *m)
{
	struct spdk_bs_dev *dev[2] = {NULL, NULL};
	struct s3_export_prefetch_budget *b;
	struct s3_export_prefetch_progress g0, g1;
	struct pf_seen seen[2] = {{0}, {0}};
	struct two_arg a = {{NULL, NULL}, false};
	uint64_t chunks[NUM_CHUNKS];
	uint32_t i, cap = 4;

	printf("\n[9] two volumes prefetch concurrently under one shared cap\n");
	for (i = 0; i < NUM_CHUNKS; i++) {
		chunks[i] = i;
	}
	hedge_env("0", "30", "4");
	reset_fake();
	b = s3_export_prefetch_budget_create(cap);
	check_true("budget created", b != NULL);
	for (i = 0; i < 2; i++) {
		s3_export_bs_dev_create((struct s3_client *)0x1, m, &dev[i]);
		/* Each volume's own window is bigger than the shared cap, so only
		 * the budget can be what bounds them. */
		s3_export_prefetcher_create(dev[i], chunks, NUM_CHUNKS, 8, pf_cb, &seen[i],
					    &a.p[i]);
		s3_export_prefetcher_set_budget(a.p[i], b);
	}
	check_u64("both attached", s3_export_prefetch_budget_attached(b), 2);
	s3_export_prefetcher_pump(a.p[0]);
	s3_export_prefetcher_pump(a.p[1]);
	check_u64("the first pumps take the whole cap", g_inflight, cap);
	check_true("settles", poll_until(two_settled, &a, 5000));
	s3_export_prefetcher_get_progress(a.p[0], &g0);
	s3_export_prefetcher_get_progress(a.p[1], &g1);
	check_true("both volumes were in flight at the same time", a.overlapped);
	check_true("never more than the cap in flight across both",
		   g_peak_inflight <= cap && s3_export_prefetch_budget_peak(b) <= cap);
	check_true("the cap was actually used", s3_export_prefetch_budget_peak(b) == cap);
	check_true("both fetched every chunk",
		   g0.fetched == NUM_CHUNKS && g1.fetched == NUM_CHUNKS);
	check_u64("budget drained", s3_export_prefetch_budget_inflight(b), 0);
	check_u64("bytes counted for volume 0", g0.bytes, (uint64_t)NUM_CHUNKS * CHUNK_SIZE);
	for (i = 0; i < 2; i++) {
		s3_export_prefetcher_destroy(a.p[i]);
	}
	check_u64("destroy detaches", s3_export_prefetch_budget_attached(b), 0);
	s3_export_prefetch_budget_destroy(b);
	dev[0]->destroy(dev[0]);
	dev[1]->destroy(dev[1]);
	poll_some(50);
}

/* ==========================================================================
 * main
 * ========================================================================== */

int
main(void)
{
	struct spdk_env_opts opts;
	struct s3_export_manifest *m = NULL;
	int rc;

	spdk_log_set_print_level(SPDK_LOG_ERROR);
	spdk_log_open(NULL);
	printf("=== s3lvol hedge + retry + shared prefetch budget test (B20) ===\n");

	printf("\n[1] SPDK env and a thread\n");
	opts.opts_size = sizeof(opts);
	spdk_env_opts_init(&opts);
	opts.name = "s3_hedge_test";
	opts.no_huge = true;
	opts.mem_size = 64;
	if (spdk_env_init(&opts) < 0) {
		fprintf(stderr, "spdk_env_init failed; skipping\n");
		spdk_log_close();
		return 77;
	}
	rc = spdk_thread_lib_init(NULL, 0);
	check_true("spdk_thread_lib_init", rc == 0);
	g_thread = spdk_thread_create("hedge_test", NULL);
	check_true("spdk_thread_create", g_thread != NULL);
	if (!g_thread) {
		goto out;
	}
	spdk_set_thread(g_thread);

	test_hedge();

	m = make_manifest();
	check_true("manifest built", m != NULL);
	if (m) {
		test_retry(m);
		test_two_volumes(m);
		test_ranges();
	}

out:
	if (m) {
		s3_export_manifest_unref(m);
	}
	if (g_thread) {
		spdk_thread_exit(g_thread);
		while (!spdk_thread_is_exited(g_thread)) {
			spdk_thread_poll(g_thread, 0, 0);
		}
		spdk_thread_destroy(g_thread);
		spdk_set_thread(NULL);
	}
	spdk_thread_lib_fini();
	printf("\n=== %d passed, %d failed ===\n", g_pass, g_fail);
	spdk_log_close();
	return g_fail == 0 ? 0 : 1;
}
