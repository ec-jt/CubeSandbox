/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_hedge -- hedged GET / PUT of immutable objects. See include/s3lvol/s3_hedge.h.
 *
 *   === One hedged request ===
 *
 *     submit:  attempt 0 is sent through the gated client entry point, and a
 *              one-shot poller is armed for hedge_ms on the submitting thread.
 *     timer:   if nothing has been delivered yet and the process-wide cap
 *              allows it, attempt 1 is sent. A GET duplicate reads into a
 *              private buffer; a PUT duplicate copies the caller's bytes.
 *     done:    every completion arrives on the submitting thread. The first
 *              success is delivered. Before a winning GET duplicate copies its
 *              bytes into the caller's buffer, attempt 0's gate is abandoned,
 *              which fences any write of attempt 0 that is still in progress.
 *              The other attempt is abandoned (CRT cancels it) and its
 *              completion is consumed here, never shown to the caller.
 *              A failure is only delivered once no attempt is left that could
 *              still succeed.
 *     free:    when the last attempt has completed, whichever order that is.
 *
 *   === Paths that are deliberately NOT hedged ===
 *
 *   Only objects named by a fresh uuid (create-once data chunks) and export
 *   chunks (written once per export uuid, never rewritten with other bytes)
 *   are safe to duplicate. Everything else stays on the plain client calls:
 *
 *     - conditional writes: s3_put(if_none_match=true), s3_put_if_match() and
 *       s3_delete_if_match(). The strict owner marker (s3_owner.c) is a
 *       compare-and-swap; a second attempt answers a different question than
 *       the first, and a delayed duplicate could overwrite a newer owner.
 *     - keys rewritten in place: export manifests (generation bumps), the
 *       export and pending-delete registries, checkpoints, recovery
 *       descriptors and source leases. A duplicate delayed past the next write
 *       would land last and silently roll the object back.
 *     - DELETE, HEAD, LIST, CopyObject: either not on the hot path, or not
 *       idempotent with respect to a concurrent rewrite.
 *     - the B19 demand-miss read of a guest I/O stays hedged through the same
 *       slot fetch as the prefetch, since it reads the same immutable objects.
 */

#include "spdk/stdinc.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/thread.h"

#include "s3lvol/s3_hedge.h"

#define HEDGE_DEFAULT_MIN_MS        1000
#define HEDGE_DEFAULT_MAX_INFLIGHT  16
#define HEDGE_P50_MULT              3
#define HEDGE_WINDOW                64      /* latency samples per kind */

/* ==========================================================================
 * Configuration, latency history, counters
 *
 * Shared by every SPDK thread that submits (flushes run on the lvstore owner
 * thread, export reads on the device home thread), so behind one mutex. It is
 * touched twice per request, which is noise next to an HTTP round trip.
 * ========================================================================== */

struct hedge_hist {
	uint64_t ms[HEDGE_WINDOW];
	uint32_t n;            /* samples held, <= HEDGE_WINDOW */
	uint32_t next;         /* ring position */
	uint32_t since_calc;   /* samples since p50 was last computed */
	uint64_t p50;
};

static pthread_mutex_t g_hedge_lock = PTHREAD_MUTEX_INITIALIZER;
static bool            g_hedge_loaded;
static bool            g_hedge_enabled = true;
static uint64_t        g_hedge_min_ms = HEDGE_DEFAULT_MIN_MS;
static uint64_t        g_hedge_max_inflight = HEDGE_DEFAULT_MAX_INFLIGHT;
static uint64_t        g_hedge_inflight;
static struct hedge_hist g_hist[2];       /* [0] GET, [1] PUT */
static uint64_t        g_issued[2], g_won[2], g_skipped;

static uint64_t
hedge_env_u64(const char *name, uint64_t dflt, uint64_t max)
{
	const char *v = getenv(name);
	char *end = NULL;
	unsigned long long n;

	if (!v || !*v) {
		return dflt;
	}
	errno = 0;
	n = strtoull(v, &end, 10);
	if (errno || !end || *end != '\0' || n > max) {
		SPDK_WARNLOG("ignoring %s='%s'; using %" PRIu64 "\n", name, v, dflt);
		return dflt;
	}
	return (uint64_t)n;
}

/* Under g_hedge_lock. */
static void
hedge_load_locked(void)
{
	g_hedge_enabled = hedge_env_u64("S3LVOL_S3_HEDGE", 1, 1) != 0;
	g_hedge_min_ms = hedge_env_u64("S3LVOL_S3_HEDGE_MIN_MS", HEDGE_DEFAULT_MIN_MS,
				       600000);
	if (g_hedge_min_ms == 0) {
		/* A zero delay would duplicate every request. */
		g_hedge_min_ms = 1;
	}
	g_hedge_max_inflight = hedge_env_u64("S3LVOL_S3_HEDGE_MAX_INFLIGHT",
					     HEDGE_DEFAULT_MAX_INFLIGHT, 4096);
	memset(g_hist, 0, sizeof(g_hist));
	g_hedge_loaded = true;
}

void
s3_hedge_config_load(void)
{
	pthread_mutex_lock(&g_hedge_lock);
	hedge_load_locked();
	pthread_mutex_unlock(&g_hedge_lock);
	SPDK_NOTICELOG("S3 hedging %s: delay max(%" PRIu64 " ms, %dx p50), at most %"
		       PRIu64 " duplicate(s) in flight\n",
		       g_hedge_enabled ? "on" : "off", g_hedge_min_ms, HEDGE_P50_MULT,
		       g_hedge_max_inflight);
}

static void
hedge_ensure_loaded(void)
{
	bool loaded;

	pthread_mutex_lock(&g_hedge_lock);
	loaded = g_hedge_loaded;
	pthread_mutex_unlock(&g_hedge_lock);
	if (!loaded) {
		s3_hedge_config_load();
	}
}

static int
hedge_cmp_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;

	return x < y ? -1 : (x > y ? 1 : 0);
}

/* Under g_hedge_lock. Recomputed every 8 samples: a 64-entry sort per request
 * would be harmless too, this just keeps it out of the common path. */
static uint64_t
hedge_delay_locked(int kind)
{
	struct hedge_hist *h = &g_hist[kind];
	uint64_t d;

	if (h->n > 0 && (h->p50 == 0 || h->since_calc >= 8)) {
		uint64_t tmp[HEDGE_WINDOW];

		memcpy(tmp, h->ms, h->n * sizeof(tmp[0]));
		qsort(tmp, h->n, sizeof(tmp[0]), hedge_cmp_u64);
		h->p50 = tmp[h->n / 2];
		h->since_calc = 0;
	}
	d = h->p50 * HEDGE_P50_MULT;
	return d > g_hedge_min_ms ? d : g_hedge_min_ms;
}

uint64_t
s3_hedge_delay_ms(bool put)
{
	uint64_t d;

	hedge_ensure_loaded();
	pthread_mutex_lock(&g_hedge_lock);
	d = hedge_delay_locked(put ? 1 : 0);
	pthread_mutex_unlock(&g_hedge_lock);
	return d;
}

static void
hedge_record(int kind, uint64_t ms)
{
	struct hedge_hist *h = &g_hist[kind];

	pthread_mutex_lock(&g_hedge_lock);
	h->ms[h->next] = ms;
	h->next = (h->next + 1) % HEDGE_WINDOW;
	if (h->n < HEDGE_WINDOW) {
		h->n++;
	}
	h->since_calc++;
	pthread_mutex_unlock(&g_hedge_lock);
}

void
s3_hedge_get_stats(struct s3_hedge_stats *out)
{
	hedge_ensure_loaded();
	pthread_mutex_lock(&g_hedge_lock);
	out->enabled = g_hedge_enabled;
	out->get_issued = g_issued[0];
	out->get_won = g_won[0];
	out->put_issued = g_issued[1];
	out->put_won = g_won[1];
	out->skipped_cap = g_skipped;
	out->inflight = g_hedge_inflight;
	out->max_inflight = g_hedge_max_inflight;
	out->get_delay_ms = hedge_delay_locked(0);
	out->put_delay_ms = hedge_delay_locked(1);
	pthread_mutex_unlock(&g_hedge_lock);
}

static uint64_t
hedge_now_ms(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/* ==========================================================================
 * Gated entry points, weak fallback
 *
 * The real ones live in s3_client_aws.c. These exist so that offline tests
 * which replace the client with an in-memory fake (and therefore keep
 * s3_client_aws.o out of the link) still link: they ignore the gate, which is
 * correct for a fake that never writes after completing. When s3_client_aws.o
 * is linked -- every real binary -- its strong definitions win.
 * ========================================================================== */

__attribute__((weak)) int
s3_get_range_gated(struct s3_client *client, const char *key,
		   uint64_t offset, uint64_t len, void *buf,
		   struct s3_gate *gate, s3_get_cb cb, void *cb_arg)
{
	(void)gate;
	return s3_get_range(client, key, offset, len, buf, cb, cb_arg);
}

__attribute__((weak)) int
s3_put_gated(struct s3_client *client, const char *key,
	     struct iovec *iov, int iovcnt, struct s3_gate *gate,
	     s3_op_cb cb, void *cb_arg)
{
	(void)gate;
	return s3_put(client, key, iov, iovcnt, false, cb, cb_arg);
}

/* ==========================================================================
 * The hedged request
 * ========================================================================== */

#define HEDGE_KEY_MAX 1024

struct hedge_ctx;

struct hedge_attempt {
	struct hedge_ctx *h;
	int               idx;
	bool              sent;
	bool              done;
	struct s3_gate   *gate;
	uint64_t          start_ms;
};

struct hedge_ctx {
	bool                      put;
	struct s3_client         *client;
	char                      key[HEDGE_KEY_MAX];

	/* GET */
	uint64_t                  offset;
	uint64_t                  len;
	void                     *buf;      /* the caller's */
	void                     *hbuf;     /* attempt 1's private buffer */
	s3_get_cb                 get_cb;

	/* PUT */
	struct iovec             *iov;      /* copy of the array, not the bytes */
	int                       iovcnt;
	s3_op_cb                  op_cb;

	void                     *cb_arg;
	struct s3_hedge_counters *counters;

	struct spdk_poller       *timer;
	struct hedge_attempt      a[2];
	uint32_t                  outstanding;
	bool                      delivered;
	int                       first_err;
};

static void
hedge_free(struct hedge_ctx *h)
{
	s3_gate_free(h->a[0].gate);
	s3_gate_free(h->a[1].gate);
	free(h->hbuf);
	free(h->iov);
	free(h);
}

static void
hedge_stop_timer(struct hedge_ctx *h)
{
	if (h->timer) {
		spdk_poller_unregister(&h->timer);
	}
}

static void hedge_get_done(void *cb_arg, uint64_t bytes_read, int status);
static void hedge_put_done(void *cb_arg, int status);

static int
hedge_send(struct hedge_ctx *h, int idx)
{
	struct hedge_attempt *a = &h->a[idx];
	int rc;

	a->h = h;
	a->idx = idx;
	a->gate = s3_gate_new();
	if (!a->gate) {
		return -ENOMEM;
	}
	a->start_ms = hedge_now_ms();
	a->sent = true;
	h->outstanding++;
	if (h->put) {
		rc = s3_put_gated(h->client, h->key, h->iov, h->iovcnt, a->gate,
				  hedge_put_done, a);
	} else {
		void *dst = idx == 0 ? h->buf : h->hbuf;

		rc = s3_get_range_gated(h->client, h->key, h->offset, h->len, dst,
					a->gate, hedge_get_done, a);
	}
	if (rc != 0) {
		h->outstanding--;
		a->sent = false;
		s3_gate_free(a->gate);
		a->gate = NULL;
	}
	return rc;
}

static int
hedge_timer_fire(void *arg)
{
	struct hedge_ctx *h = arg;
	int kind = h->put ? 1 : 0;
	bool allowed;
	int rc;

	hedge_stop_timer(h);
	if (h->delivered || h->a[0].done) {
		return SPDK_POLLER_BUSY;
	}

	pthread_mutex_lock(&g_hedge_lock);
	allowed = g_hedge_inflight < g_hedge_max_inflight;
	if (allowed) {
		g_hedge_inflight++;
		g_issued[kind]++;
	} else {
		g_skipped++;
	}
	pthread_mutex_unlock(&g_hedge_lock);

	if (!allowed) {
		/* The endpoint is degraded across the board; doubling every slow
		 * request would make it worse. This one waits for itself. */
		if (h->counters) {
			h->counters->skipped++;
		}
		return SPDK_POLLER_BUSY;
	}

	if (!h->put && !h->hbuf) {
		h->hbuf = malloc(h->len);
	}
	rc = (!h->put && !h->hbuf) ? -ENOMEM : hedge_send(h, 1);
	if (rc != 0) {
		pthread_mutex_lock(&g_hedge_lock);
		g_hedge_inflight--;
		g_issued[kind]--;
		pthread_mutex_unlock(&g_hedge_lock);
		SPDK_WARNLOG("S3 %s hedge of key=%s not sent: %s\n", h->put ? "PUT" : "GET",
			     h->key, spdk_strerror(-rc));
		return SPDK_POLLER_BUSY;
	}
	if (h->counters) {
		h->counters->issued++;
	}
	SPDK_NOTICELOG("S3 %s key=%s still running after %" PRIu64 " ms; sent a "
		       "hedged duplicate\n", h->put ? "PUT" : "GET", h->key,
		       hedge_now_ms() - h->a[0].start_ms);
	return SPDK_POLLER_BUSY;
}

/* A GET that failed for good: the longest in-order prefix either attempt left
 * behind, made to sit in the caller's buffer. Both attempts are finished here
 * (a failure is only delivered then), so neither can still write. The bytes
 * are valid because the key names an immutable object. 0 for PUT. */
static uint64_t
hedge_get_prefix(struct hedge_ctx *h)
{
	uint64_t p0 = 0, p1 = 0;

	if (h->put) {
		return 0;
	}
	if (h->a[0].sent && !s3_gate_is_abandoned(h->a[0].gate)) {
		p0 = s3_gate_partial(h->a[0].gate);
	}
	if (h->a[1].sent && h->hbuf && !s3_gate_is_abandoned(h->a[1].gate)) {
		p1 = s3_gate_partial(h->a[1].gate);
	}
	p0 = p0 > h->len ? h->len : p0;
	p1 = p1 > h->len ? h->len : p1;
	if (p1 > p0) {
		memcpy(h->buf, h->hbuf, p1);
		return p1;
	}
	return p0;
}

/* Common completion. \p bytes only for GET. Runs on the submitting thread. */
static void
hedge_attempt_done(struct hedge_attempt *a, uint64_t bytes, int status)
{
	struct hedge_ctx *h = a->h;
	struct hedge_attempt *other = &h->a[1 - a->idx];
	int kind = h->put ? 1 : 0;
	bool deliver = false;
	int out_status = status;
	uint64_t out_bytes = bytes;

	assert(h->outstanding > 0);
	h->outstanding--;
	a->done = true;
	if (a->idx == 1) {
		pthread_mutex_lock(&g_hedge_lock);
		g_hedge_inflight--;
		pthread_mutex_unlock(&g_hedge_lock);
	}

	if (!h->delivered) {
		if (status == 0) {
			deliver = true;
			/* Fence the other attempt before anything below touches the
			 * caller's buffer: once abandon returns, attempt 0 can no
			 * longer be writing into it. */
			if (other->sent && !other->done) {
				s3_gate_abandon(other->gate);
			}
			if (!h->put && a->idx == 1 && bytes > 0) {
				memcpy(h->buf, h->hbuf, bytes > h->len ? h->len : bytes);
			}
			hedge_record(kind, hedge_now_ms() - a->start_ms);
			if (a->idx == 1) {
				pthread_mutex_lock(&g_hedge_lock);
				g_won[kind]++;
				pthread_mutex_unlock(&g_hedge_lock);
				if (h->counters) {
					h->counters->won++;
				}
			}
		} else {
			if (h->first_err == 0 && status != -ECANCELED) {
				h->first_err = status;
			}
			/* Fail only when nothing can succeed any more: the other
			 * attempt is finished (or was never sent) and no hedge is
			 * still due. A primary failing before its hedge fired is
			 * delivered at once -- the caller has its own retry. */
			if (!(other->sent && !other->done)) {
				deliver = true;
				out_status = h->first_err ? h->first_err : status;
				out_bytes = hedge_get_prefix(h);
			}
		}
	}

	if (deliver) {
		h->delivered = true;
		hedge_stop_timer(h);
	}

	if (deliver) {
		bool free_now = h->outstanding == 0;

		/* After this the caller may reuse its buffer; the abandon above is
		 * what makes that safe while a loser is still outstanding. */
		if (h->put) {
			h->op_cb(h->cb_arg, out_status);
		} else {
			h->get_cb(h->cb_arg, out_bytes, out_status);
		}
		if (free_now) {
			hedge_free(h);
		}
		return;
	}
	if (h->delivered && h->outstanding == 0) {
		hedge_free(h);
	}
}

static void
hedge_get_done(void *cb_arg, uint64_t bytes_read, int status)
{
	hedge_attempt_done(cb_arg, bytes_read, status);
}

static void
hedge_put_done(void *cb_arg, int status)
{
	hedge_attempt_done(cb_arg, 0, status);
}

static bool
hedge_enabled(void)
{
	bool on;

	hedge_ensure_loaded();
	pthread_mutex_lock(&g_hedge_lock);
	on = g_hedge_enabled;
	pthread_mutex_unlock(&g_hedge_lock);
	return on;
}

static bool
hedge_usable(void)
{
	bool on;

	hedge_ensure_loaded();
	pthread_mutex_lock(&g_hedge_lock);
	on = g_hedge_enabled;
	pthread_mutex_unlock(&g_hedge_lock);
	/* The timer is a poller and completions must be bounced (the gate is
	 * freed from a callback that has to run after submit returned): both need
	 * an SPDK thread. */
	return on && spdk_get_thread() != NULL;
}

static struct hedge_ctx *
hedge_new(bool put, struct s3_client *client, const char *key,
	  struct s3_hedge_counters *counters, void *cb_arg)
{
	struct hedge_ctx *h;

	if (strlen(key) >= HEDGE_KEY_MAX) {
		return NULL;
	}
	h = calloc(1, sizeof(*h));
	if (!h) {
		return NULL;
	}
	h->put = put;
	h->client = client;
	snprintf(h->key, sizeof(h->key), "%s", key);
	h->counters = counters;
	h->cb_arg = cb_arg;
	return h;
}

static int
hedge_start(struct hedge_ctx *h)
{
	uint64_t delay_ms = s3_hedge_delay_ms(h->put);
	int rc;

	/* A guard reference for the duration of this function. The client
	 * bounces every callback, so attempt 0 cannot normally complete before
	 * the submit returns -- but a client (or test double) that completes
	 * inline must not be able to free h under us. */
	h->outstanding++;
	rc = hedge_send(h, 0);
	if (rc != 0) {
		h->outstanding--;
		hedge_free(h);
		return rc;
	}
	if (!h->delivered && hedge_enabled()) {
		h->timer = SPDK_POLLER_REGISTER(hedge_timer_fire, h, delay_ms * 1000);
		if (!h->timer) {
			/* No timer: the request simply runs unhedged. */
			SPDK_WARNLOG("S3 hedge timer unavailable for key=%s\n", h->key);
		}
	}
	h->outstanding--;
	if (h->delivered && h->outstanding == 0) {
		hedge_free(h);
	}
	return 0;
}

int
s3_get_range_hedged(struct s3_client *client, const char *key,
		    uint64_t offset, uint64_t len, void *buf,
		    struct s3_hedge_counters *counters,
		    s3_get_cb cb, void *cb_arg)
{
	struct hedge_ctx *h;

	if (!client || !key || !buf || !cb) {
		return -EINVAL;
	}
	/* With hedging off the context still runs (no timer, no duplicate):
	 * it is what reports the prefix of a failed GET for a ranged retry.
	 * Only off an SPDK thread is it a plain GET. */
	if (len == 0 || spdk_get_thread() == NULL) {
		return s3_get_range(client, key, offset, len, buf, cb, cb_arg);
	}
	h = hedge_new(false, client, key, counters, cb_arg);
	if (!h) {
		return -ENOMEM;
	}
	h->offset = offset;
	h->len = len;
	h->buf = buf;
	h->get_cb = cb;
	return hedge_start(h);
}

int
s3_put_hedged(struct s3_client *client, const char *key,
	      struct iovec *iov, int iovcnt,
	      struct s3_hedge_counters *counters,
	      s3_op_cb cb, void *cb_arg)
{
	struct hedge_ctx *h;

	if (!client || !key || !iov || iovcnt <= 0 || !cb) {
		return -EINVAL;
	}
	if (!hedge_usable()) {
		return s3_put(client, key, iov, iovcnt, false, cb, cb_arg);
	}
	h = hedge_new(true, client, key, counters, cb_arg);
	if (!h) {
		return -ENOMEM;
	}
	h->iov = calloc((size_t)iovcnt, sizeof(*iov));
	if (!h->iov) {
		free(h);
		return -ENOMEM;
	}
	memcpy(h->iov, iov, (size_t)iovcnt * sizeof(*iov));
	h->iovcnt = iovcnt;
	h->op_cb = cb;
	return hedge_start(h);
}
