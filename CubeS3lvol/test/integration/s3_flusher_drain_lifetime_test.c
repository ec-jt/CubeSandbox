/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_flusher drain deadline vs. retirement lifetime (ASan/UBSan unit test)
 *
 *   Links the REAL lib/s3bsdev/s3_flusher.c and s3_overlay.c. Everything SPDK
 *   or WAL is replaced by controllable fakes defined below (no SPDK library):
 *
 *     - a manual poller (the test decides when the flusher's tick runs),
 *     - a tick clock the test advances,
 *     - an uploader whose completions are parked and delivered on demand,
 *       with a chosen status (backend outage = -EIO, backend back = 0),
 *     - a WAL whose super sync completion can be held open.
 *
 *   What it pins:
 *     A. the drain deadline fires while uploads are IN FLIGHT (the bug: it
 *        could not, and outage unloads hung), exactly once;
 *     B. a timed-out drain stops nothing: the flusher keeps retrying, a late
 *        completion lands on a live flusher, and after the backend returns a
 *        second drain reports 0;
 *     C. retirement never completes while an upload or super sync is in
 *        flight, never inline, stops re-queueing, and reports 0 / -EAGAIN;
 *     D. an owner that frees itself AND the flusher from the retire callback
 *        sees no late access (ASan is the oracle);
 *     E. API edges: second drain -EBUSY, drain while retiring -ESHUTDOWN,
 *        second retire -EBUSY, pending drain answered by retire.
 */

#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/thread.h"

#include "s3lvol/s3_flusher.h"
#include "s3lvol/s3_overlay.h"
#include "s3lvol/s3_wal.h"

static int g_pass, g_fail;

#define CHECK(cond) do { \
	if (cond) { g_pass++; } else { g_fail++; \
		fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); } \
} while (0)

/* ---------------------------------------------------------------- clock */

static uint64_t g_ticks = 1000;

uint64_t spdk_get_ticks(void) { return g_ticks; }
uint64_t spdk_get_ticks_hz(void) { return 1000000; }   /* 1 tick = 1 us */

/* ---------------------------------------------------------------- poller */

struct spdk_poller {
	spdk_poller_fn fn;
	void *arg;
};

static struct spdk_poller *g_poller;
static int g_pollers_live;

struct spdk_poller *
spdk_poller_register_named(spdk_poller_fn fn, void *arg, uint64_t period,
			   const char *name)
{
	struct spdk_poller *p = calloc(1, sizeof(*p));

	(void)period; (void)name;
	p->fn = fn;
	p->arg = arg;
	g_poller = p;
	g_pollers_live++;
	return p;
}

void
spdk_poller_unregister(struct spdk_poller **pp)
{
	if (!pp || !*pp) {
		return;
	}
	if (*pp == g_poller) {
		g_poller = NULL;
	}
	free(*pp);
	*pp = NULL;
	g_pollers_live--;
}

/* One tick. Copies fn/arg first: the tick may unregister (free) the poller. */
static void
tick(void)
{
	spdk_poller_fn fn;
	void *arg;

	if (!g_poller) {
		return;
	}
	fn = g_poller->fn;
	arg = g_poller->arg;
	fn(arg);
}

/* ---------------------------------------------------------------- logging */

void
spdk_log(enum spdk_log_level level, const char *file, const int line,
	 const char *func, const char *format, ...)
{
	va_list ap;

	(void)level; (void)file; (void)line;
	fprintf(stderr, "  [log %s] ", func);
	va_start(ap, format);
	vfprintf(stderr, format, ap);
	va_end(ap);
}

void spdk_log_register_flag(const char *name, struct spdk_log_flag *flag)
{
	(void)name; (void)flag;
}

/* ---------------------------------------------------------------- fake WAL */

static struct {
	uint64_t next_seq;
	uint64_t truncated;
	int syncs;
	s3_wal_cb parked_cb;
	void *parked_arg;
} g_wal;

#define FAKE_WAL ((struct s3_wal *)&g_wal)

bool s3_wal_is_backpressured(const struct s3_wal *wal) { (void)wal; return false; }
uint64_t s3_wal_get_next_seq(const struct s3_wal *wal) { (void)wal; return g_wal.next_seq; }
void s3_wal_truncate_to_seq(struct s3_wal *wal, uint64_t s) { (void)wal; g_wal.truncated = s; }

void
s3_wal_sync_super(struct s3_wal *wal, uint64_t ckpt_seq, s3_wal_cb cb_fn,
		  void *cb_arg)
{
	(void)wal; (void)ckpt_seq;
	assert(g_wal.parked_cb == NULL);
	g_wal.syncs++;
	g_wal.parked_cb = cb_fn;
	g_wal.parked_arg = cb_arg;
}

static void
wal_complete_sync(int status)
{
	s3_wal_cb cb = g_wal.parked_cb;
	void *arg = g_wal.parked_arg;

	assert(cb);
	g_wal.parked_cb = NULL;
	g_wal.parked_arg = NULL;
	cb(arg, status);
}

/* ---------------------------------------------------------------- uploader */

#define MAXP 32

static struct {
	s3_flusher_upload_cb cb[MAXP];
	void *arg[MAXP];
	uint32_t n;
	uint32_t started;
} g_up;

static void
fake_upload(void *ctx, const struct s3_overlay_flush_view *view,
	    s3_flusher_upload_cb cb_fn, void *cb_arg)
{
	(void)ctx; (void)view;
	assert(g_up.n < MAXP);
	g_up.cb[g_up.n] = cb_fn;
	g_up.arg[g_up.n] = cb_arg;
	g_up.n++;
	g_up.started++;
}

/* Deliver every parked completion with \c status. Taken as a batch: a
 * completion kicks the flusher, which may park new rounds. */
static uint32_t
complete_all(int status)
{
	s3_flusher_upload_cb cb[MAXP];
	void *arg[MAXP];
	uint32_t n = g_up.n, i;

	memcpy(cb, g_up.cb, sizeof(cb[0]) * n);
	memcpy(arg, g_up.arg, sizeof(arg[0]) * n);
	g_up.n = 0;
	for (i = 0; i < n; i++) {
		cb[i](arg[i], status);
	}
	return n;
}

/* ---------------------------------------------------------------- helpers */

#define BS        4096u
#define CHUNK     (64u * 1024)
#define NBLOCKS   1024u

struct probe {
	int calls;
	int status;
	uint32_t in_flight_at_cb;
	struct s3_flusher *fl;
};

static void
probe_cb(void *arg, int status)
{
	struct probe *p = arg;

	p->calls++;
	p->status = status;
	if (p->fl) {
		struct s3_flusher_stats st;

		s3_flusher_get_stats(p->fl, &st);
		p->in_flight_at_cb = st.in_flight;
	}
}

static struct s3_overlay *
mk_overlay(void)
{
	struct s3_overlay *ov = NULL;
	struct s3_overlay_flush_policy pol = {
		.max_age_us = 3600ULL * 1000 * 1000, .full_pct = 100, .high_pct = 1,
	};

	/* 256 KiB cap: 1% high water is below one 4 KiB block, so any dirty
	 * block is flushable without a drain forcing it (same as s3_flush_test). */
	assert(s3_overlay_create(NBLOCKS, BS, CHUNK, 256 * 1024, &ov) == 0);
	s3_overlay_set_flush_policy(ov, &pol);
	return ov;
}

static void
dirty(struct s3_overlay *ov, uint64_t lba, uint64_t seq)
{
	static uint8_t buf[BS];

	memset(buf, (int)(seq & 0xff), sizeof(buf));
	assert(s3_overlay_write(ov, lba, 1, buf, seq) == 0);
}

static struct s3_flusher *
mk_flusher(struct s3_overlay *ov, struct s3_wal *wal)
{
	struct s3_flusher *fl = NULL;
	struct s3_flusher_opts o = { .max_concurrent = 1, .poll_period_us = 100 };

	assert(s3_flusher_create(ov, wal, fake_upload, NULL, &o, &fl) == 0);
	return fl;
}

static void
reset_fakes(void)
{
	memset(&g_up, 0, sizeof(g_up));
	memset(&g_wal, 0, sizeof(g_wal));
}

/* ======================================================================== */

/* A + B: deadline during an outage, late completion, backend return. */
static void
test_outage_deadline_and_late_completion(void)
{
	struct s3_overlay *ov;
	struct s3_flusher *fl;
	struct probe p = {0}, p2 = {0};
	uint32_t started_before;

	printf("[A/B] outage: drain deadline with an upload in flight\n");
	reset_fakes();
	ov = mk_overlay();
	fl = mk_flusher(ov, NULL);
	p.fl = fl;

	dirty(ov, 0, 10);
	s3_flusher_drain(fl, 5000, probe_cb, &p);      /* 5 ms deadline */
	CHECK(g_up.n == 1);                               /* upload started */
	CHECK(p.calls == 0);

	/* Outage: every completion fails, the chunk re-queues and a new round
	 * starts at once, so in_flight never drops to zero. */
	for (int i = 0; i < 5; i++) {
		complete_all(-EIO);
		CHECK(g_up.n == 1);
		g_ticks += 500;
		tick();
	}
	CHECK(p.calls == 0);                              /* before deadline */

	g_ticks += 5000;                                  /* past the deadline */
	tick();
	CHECK(p.calls == 1);
	CHECK(p.status == -ETIMEDOUT);
	CHECK(p.in_flight_at_cb == 1);                    /* reported WITH an upload in flight */

	/* Exactly once: more ticks and completions do not re-fire it. */
	g_ticks += 100000;
	tick();
	complete_all(-EIO);
	tick();
	CHECK(p.calls == 1);

	/* B: nothing was stopped. Retrying continues. */
	CHECK(!s3_flusher_is_retiring(fl));
	started_before = g_up.started;
	complete_all(-EIO);
	CHECK(g_up.started == started_before + 1);
	CHECK(g_up.n == 1);                       /* chunk claimed by the retry round */

	/* Backend returns: the late completion lands on a live flusher. */
	complete_all(0);
	CHECK(g_up.n == 0);
	CHECK(!s3_overlay_has_dirty(ov));

	p2.fl = fl;
	s3_flusher_drain(fl, 5000, probe_cb, &p2);
	CHECK(p2.calls == 1 && p2.status == 0);           /* later drain succeeds */

	/* Ordinary teardown: retire (idle) completes on the next tick. */
	{
		struct probe r = {0};

		s3_flusher_retire(fl, probe_cb, &r);
		CHECK(r.calls == 0);                      /* never inline */
		tick();
		CHECK(r.calls == 1 && r.status == 0);
	}
	s3_flusher_destroy(fl);
	s3_overlay_destroy(ov);
	CHECK(g_pollers_live == 0);
}

/* C + D: owner frees everything from the retire callback; a late failed
 * completion must have been absorbed before that, and nothing touches the
 * freed owner/flusher afterwards. */
struct owner {
	struct s3_flusher *fl;
	struct s3_overlay *ov;
	int drained_calls;
	int drain_status;
	int *retired_calls;
	int *retired_status;
	char *wal_sentinel;   /* stands in for the WAL/chunk map: freed in retire */
};

static void
owner_retired(void *arg, int status)
{
	struct owner *o = arg;

	(*o->retired_calls)++;
	*o->retired_status = status;
	s3_flusher_destroy(o->fl);
	s3_overlay_destroy(o->ov);
	free(o->wal_sentinel);
	free(o);                    /* any later access through o is a UAF */
}

static void
owner_drained(void *arg, int status)
{
	struct owner *o = arg;

	o->drained_calls++;
	o->drain_status = status;
	/* Exactly what the bs_dev teardown does: retire from the drain
	 * callback, which can run inside s3_flusher_kick(). */
	s3_flusher_retire(o->fl, owner_retired, o);
}

static void
test_retire_lifetime(int final_status)
{
	struct owner *o = calloc(1, sizeof(*o));
	int retired_calls = 0, retired_status = 12345;
	uint32_t started;

	printf("[C/D] retire with an upload in flight, late completion %d\n",
	       final_status);
	reset_fakes();
	o->ov = mk_overlay();
	o->fl = mk_flusher(o->ov, NULL);
	o->wal_sentinel = malloc(64);
	o->retired_calls = &retired_calls;
	o->retired_status = &retired_status;

	dirty(o->ov, 0, 1);
	s3_flusher_drain(o->fl, 1000, owner_drained, o);
	CHECK(g_up.n == 1);
	g_ticks += 2000;
	tick();                                   /* deadline -> drained -> retire */
	CHECK(o->drained_calls == 1 && o->drain_status == -ETIMEDOUT);
	CHECK(s3_flusher_is_retiring(o->fl));
	CHECK(retired_calls == 0);                /* upload still in flight */

	tick();
	tick();
	CHECK(retired_calls == 0);

	started = g_up.started;
	complete_all(final_status);               /* late completion: owner alive */
	CHECK(g_up.started == started);           /* retiring: no re-queue */
	CHECK(retired_calls == 0);                /* not inline from completion */

	tick();                                   /* frees owner + flusher */
	CHECK(retired_calls == 1);
	CHECK(retired_status == (final_status == 0 ? 0 : -EAGAIN));
	CHECK(g_pollers_live == 0);

	/* Nothing left that could call back. */
	CHECK(g_up.n == 0);
	tick();                                   /* no poller: no-op */
	CHECK(retired_calls == 1);
}

/* C: a WAL super sync in flight also holds retirement. */
static void
test_retire_waits_super_sync(void)
{
	struct s3_overlay *ov;
	struct s3_flusher *fl;
	struct probe r = {0};

	printf("[C] retire waits for an in-flight WAL super sync\n");
	reset_fakes();
	g_wal.next_seq = 100;
	ov = mk_overlay();
	fl = mk_flusher(ov, FAKE_WAL);

	dirty(ov, 0, 50);
	s3_flusher_kick(fl);
	CHECK(g_up.n == 1);
	complete_all(0);                          /* success -> truncate + sync */
	CHECK(g_wal.syncs == 1 && g_wal.parked_cb != NULL);

	s3_flusher_retire(fl, probe_cb, &r);
	tick();
	tick();
	CHECK(r.calls == 0);                      /* super sync still running */

	wal_complete_sync(0);
	CHECK(r.calls == 0);                      /* not inline */
	tick();
	CHECK(r.calls == 1 && r.status == 0);

	s3_flusher_destroy(fl);
	s3_overlay_destroy(ov);
	CHECK(g_pollers_live == 0);
}

/* E: API edges. */
static void
test_api_edges(void)
{
	struct s3_overlay *ov;
	struct s3_flusher *fl;
	struct probe d1 = {0}, d2 = {0}, d3 = {0}, r1 = {0}, r2 = {0};

	printf("[E] API edges\n");
	reset_fakes();
	ov = mk_overlay();
	fl = mk_flusher(ov, NULL);

	dirty(ov, 0, 1);
	s3_flusher_drain(fl, 100000, probe_cb, &d1);
	s3_flusher_drain(fl, 100000, probe_cb, &d2);
	CHECK(d2.calls == 1 && d2.status == -EBUSY);
	CHECK(d1.calls == 0);

	/* A pending drain is answered (once) when retirement starts. */
	s3_flusher_retire(fl, probe_cb, &r1);
	CHECK(d1.calls == 1 && d1.status == -ETIMEDOUT);
	s3_flusher_retire(fl, probe_cb, &r2);
	CHECK(r2.calls == 1 && r2.status == -EBUSY);
	s3_flusher_drain(fl, 100000, probe_cb, &d3);
	CHECK(d3.calls == 1 && d3.status == -ESHUTDOWN);

	tick();
	CHECK(r1.calls == 0);                     /* upload in flight */
	complete_all(-EIO);
	tick();
	CHECK(r1.calls == 1 && r1.status == -EAGAIN);
	CHECK(d1.calls == 1);                     /* still exactly once */

	s3_flusher_destroy(fl);
	s3_overlay_destroy(ov);

	/* NULL handling. */
	{
		struct probe n = {0};

		s3_flusher_retire(NULL, probe_cb, &n);
		CHECK(n.calls == 1 && n.status == -EINVAL);
	}
	CHECK(g_pollers_live == 0);
}

/* Regression: drain success path and the deadline when idle-but-held. */
static void
test_success_unchanged(void)
{
	struct s3_overlay *ov;
	struct s3_flusher *fl;
	struct probe d = {0};

	printf("[R] drain success path unchanged\n");
	reset_fakes();
	ov = mk_overlay();
	fl = mk_flusher(ov, NULL);
	d.fl = fl;

	dirty(ov, 0, 1);
	dirty(ov, 16, 2);                         /* second chunk */
	s3_flusher_drain(fl, 0, probe_cb, &d);    /* default deadline */
	while (g_up.n) {
		CHECK(d.calls == 0);
		complete_all(0);
	}
	CHECK(d.calls == 1 && d.status == 0 && d.in_flight_at_cb == 0);
	{
		struct probe r = {0};

		s3_flusher_retire(fl, probe_cb, &r);
		tick();
		CHECK(r.calls == 1 && r.status == 0);
	}
	s3_flusher_destroy(fl);
	s3_overlay_destroy(ov);
}

int
main(void)
{
	test_outage_deadline_and_late_completion();
	test_retire_lifetime(-EIO);
	test_retire_lifetime(0);
	test_retire_waits_super_sync();
	test_api_edges();
	test_success_unchanged();

	printf("s3_flusher_drain_lifetime_test: %d checks, %d failed\n",
	       g_pass + g_fail, g_fail);
	return g_fail ? 1 : 0;
}
