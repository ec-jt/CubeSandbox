/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_flusher -- drains the overlay into S3, one read-modify-write per chunk
 *
 *   === Why this fixes a correctness bug, not just a performance one ===
 *
 *   The direct-to-S3 write path ran one read-modify-write per *incoming write*.
 *   Eight concurrent 128 KiB writes into the same 1 MiB chunk therefore ran
 *   eight RMW cycles against a chunk that did not exist yet, each PUTting an
 *   object holding only its own eighth. Seven eighths of the data was lost.
 *
 *   Here the writes have already been acknowledged from the WAL and accumulated
 *   in the overlay, and the flusher runs *one* RMW per chunk with a per-chunk
 *   single-flight rule. The race is gone by construction: there is never more
 *   than one writer of a given S3 object.
 *
 *   === Division of labour ===
 *
 *   This file owns queueing, concurrency, single flight and WAL truncation. It
 *   does *not* know about S3: the actual upload is a callback supplied by
 *   s3_bs_dev, which owns the client, the key layout and the chunk map. Keeping
 *   the split means the scheduling logic can be unit tested with a fake
 *   uploader, with no credentials and no network.
 *
 *   === Threading ===
 *
 *   Owner thread only, like every other per-lvstore component. The poller is
 *   registered on the thread that calls s3_flusher_create().
 */

#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/queue.h"
#include "spdk/thread.h"
#include "spdk/util.h"
#include "spdk/string.h"

#include "s3lvol/s3_flusher.h"
#include "s3lvol/s3_wal.h"

struct flush_req;

/* Diagnostics only (B16): how often each recently failed chunk has failed, so
 * the drain-timeout dump can say "attempt N". A small ring, not a map: only the
 * chunks that are failing right now matter, and losing an old entry costs a
 * log number, nothing else. */
#define FLUSHER_TRACE_FAIL_SLOTS 32

struct flusher_fail_slot {
	uint64_t chunk_index;
	uint32_t failures;
	bool     used;
};

struct s3_flusher {
	struct s3_overlay          *overlay;

	/* Uploads in flight, for the drain-timeout dump. Log only. */
	TAILQ_HEAD(, flush_req)     inflight_reqs;
	struct flusher_fail_slot    fail_slots[FLUSHER_TRACE_FAIL_SLOTS];
	uint32_t                    fail_next;

	/* Optional: without a WAL there is nothing to truncate, which is the
	 * mode the unit test uses. */
	struct s3_wal              *wal;

	s3_flusher_upload_fn        upload_fn;
	void                       *upload_ctx;

	uint32_t                    max_concurrent;
	uint32_t                    in_flight;

	struct spdk_poller *poller;

	/* Set by retire/destroy so no new upload is started. A drain timeout does
	 * NOT set it: a drain is an observation, and the flusher keeps retrying
	 * in the background afterwards (see s3_flusher_drain()). */
	bool                        stopping;

	s3_flusher_cb drain_cb;
	void *drain_arg;

	/* Tick at which a drain reports -ETIMEDOUT to its caller. Evaluated on
	 * every poller tick and every completion, whether or not an upload is in
	 * flight. */
	uint64_t drain_deadline;

	/* Retirement: the only state that makes s3_flusher_destroy() safe. Fires
	 * once nothing can call back into the flusher any more, and ONLY from the
	 * flusher's own poller tick (never inline from kick(), an upload
	 * completion or a super-sync completion), so the callback may destroy the
	 * flusher and tear down its owner without any caller frame still using
	 * either. */
	s3_flusher_cb retire_cb;
	void *retire_arg;
	bool retire_requested;

	/* Highest seq already reported to the WAL, so truncation is only
	 * attempted when it would actually make progress. */
	uint64_t                    truncated_seq;

	/* One super update at a time: they are not ordered against each other
	 * and the later one would win anyway. */
	bool                        super_sync_active;
	bool                        super_sync_again;

	struct s3_flusher_stats     stats;
};

/* One chunk being uploaded. The view has to outlive the call, so it lives
 * here rather than on the stack of the caller. */
struct flush_req {
	struct s3_flusher              *flusher;
	struct s3_overlay_flush_view    view;

	/* Diagnostics only. */
	uint64_t                        start_tsc;
	uint32_t                        attempt;
	TAILQ_ENTRY(flush_req)          link;
};

static struct flusher_fail_slot *
flusher_fail_slot(struct s3_flusher *f, uint64_t chunk_index, bool create)
{
	uint32_t i;

	for (i = 0; i < FLUSHER_TRACE_FAIL_SLOTS; i++) {
		if (f->fail_slots[i].used && f->fail_slots[i].chunk_index == chunk_index) {
			return &f->fail_slots[i];
		}
	}
	if (!create) {
		return NULL;
	}
	i = f->fail_next++ % FLUSHER_TRACE_FAIL_SLOTS;
	f->fail_slots[i].used = true;
	f->fail_slots[i].chunk_index = chunk_index;
	f->fail_slots[i].failures = 0;
	return &f->fail_slots[i];
}

static uint64_t
flusher_age_ms(uint64_t start_tsc)
{
	uint64_t now = spdk_get_ticks();

	return now > start_tsc ? (uint64_t)((now - start_tsc) * 1000 / spdk_get_ticks_hz()) : 0;
}

/* One line per upload still in flight when a drain gives up. Bounded by
 * max_concurrent (8 by default), so at most a handful of lines per timeout. */
static void
flusher_dump_inflight(struct s3_flusher *f)
{
	struct flush_req *req;

	TAILQ_FOREACH(req, &f->inflight_reqs, link) {
		SPDK_WARNLOG("  in-flight upload: chunk=%" PRIu64 " age_ms=%" PRIu64
			     " attempt=%u blocks=%u bytes<=%u covers_prefix=%d "
			     "max_seq=%" PRIu64 "\n", req->view.chunk_index,
			     flusher_age_ms(req->start_tsc), req->attempt,
			     req->view.nblocks, req->view.end_offset,
			     (int)req->view.covers_prefix, req->view.max_seq);
	}
}

static void flusher_check_drain(struct s3_flusher *f);
static void flusher_check_retire(struct s3_flusher *f);

/* ==========================================================================
 * WAL truncation
 * ========================================================================== */

static void
flusher_super_synced(void *cb_arg, int status)
{
	struct s3_flusher *f = cb_arg;

	f->super_sync_active = false;

	if (status != 0) {
		/* Not fatal: the positions stay in memory and the next attempt
		 * persists them. Recovery just rescans from an older point. */
		SPDK_WARNLOG("Failed to persist WAL positions after truncation: "
			     "%d\n", status);
	}

	if (f->super_sync_again && !f->stopping) {
		f->super_sync_again = false;
		f->super_sync_active = true;
		s3_wal_sync_super(f->wal, f->truncated_seq, flusher_super_synced, f);
	}

	/* A drain parked on super_sync_active can only resume here. The poller and
	 * every upload completion also re-check, but there is no reason to wait for
	 * the next one when the very state the drain was waiting on just changed. */
	flusher_check_drain(f);
}

/* Report progress to the WAL so it can release segments.
 *
 * The safe point is "one below the oldest seq the overlay still holds": every
 * entry older than that has been uploaded. When the overlay is empty the whole
 * log has been consumed, so everything issued so far is safe. */
static void
flusher_advance_wal(struct s3_flusher *f)
{
	uint64_t min_seq;
	uint64_t safe_seq;

	if (!f->wal) {
		return;
	}

	min_seq = s3_overlay_min_seq(f->overlay);
	if (min_seq == UINT64_MAX) {
		uint64_t next = s3_wal_get_next_seq(f->wal);

		safe_seq = next ? next - 1 : 0;
	} else {
		safe_seq = min_seq - 1;
	}

	if (safe_seq <= f->truncated_seq) {
		return;
	}
	f->truncated_seq = safe_seq;

	s3_wal_truncate_to_seq(f->wal, safe_seq);

	/* Persisting matters: the WAL may now reuse those segments, and replay
	 * starts from the position recorded in the super. If that position still
	 * pointed into a reused segment, recovery would scan bytes belonging to a
	 * later lap. */
	if (f->super_sync_active) {
		f->super_sync_again = true;
		return;
	}
	f->super_sync_active = true;
	s3_wal_sync_super(f->wal, safe_seq, flusher_super_synced, f);
}

/* ==========================================================================
 * Upload scheduling
 * ========================================================================== */

static void
flusher_upload_done(void *cb_arg, int status)
{
	struct flush_req *req = cb_arg;
	struct s3_flusher *f = req->flusher;
	uint64_t chunk_index = req->view.chunk_index;

	assert(f->in_flight > 0);
	f->in_flight--;
	TAILQ_REMOVE(&f->inflight_reqs, req, link);

	if (status == 0) {
		struct flusher_fail_slot *slot = flusher_fail_slot(f, chunk_index, false);

		f->stats.chunks_flushed++;
		f->stats.blocks_flushed += req->view.nblocks;
		if (slot) {
			if (slot->failures > 0) {
				SPDK_NOTICELOG("Chunk %" PRIu64 " flushed on attempt %u "
					       "after %u failure(s), %" PRIu64 " ms\n",
					       chunk_index, req->attempt, slot->failures,
					       flusher_age_ms(req->start_tsc));
			}
			slot->used = false;
		}
	} else {
		struct flusher_fail_slot *slot = flusher_fail_slot(f, chunk_index, true);

		slot->failures++;
		f->stats.failures++;
		/* The transport/HTTP cause is on the "S3 <op> failed" line logged
		 * by the client just before this one, with the same key. */
		SPDK_ERRLOG("Failed to flush chunk %" PRIu64 " to S3: %d (%s) after "
			    "%" PRIu64 " ms, attempt %u, %u failure(s) so far, %u "
			    "block(s) (kept dirty, will retry)\n", chunk_index, status,
			    spdk_strerror(-status), flusher_age_ms(req->start_tsc),
			    req->attempt, slot->failures, req->view.nblocks);
	}

	/* Releases the claimed blocks on success, or re-dirties and re-queues
	 * them on failure. Either way it must happen before truncation, which
	 * reads the overlay's oldest seq. */
	s3_overlay_flush_end(f->overlay, chunk_index, status == 0);

	free(req);

	if (status == 0) {
		flusher_advance_wal(f);
	}

	s3_flusher_kick(f);
}

static void
flusher_start_one(struct s3_flusher *f, uint64_t chunk_index)
{
	struct flush_req *req;
	int rc;

	req = calloc(1, sizeof(*req));
	if (!req) {
		/* The chunk is off the queue, so put it back by leaving it dirty
		 * and letting the next write or the poller re-queue it. Nothing
		 * was claimed yet, so no data is at risk. */
		SPDK_ERRLOG("Out of memory starting a chunk flush\n");
		return;
	}
	req->flusher = f;

	rc = s3_overlay_flush_begin(f->overlay, chunk_index, &req->view);
	if (rc != 0) {
		/* -ENOENT: the chunk was unmapped or already flushed between
		 * being queued and now. -EBUSY cannot happen -- a chunk with a
		 * live flush is never on the queue. */
		if (rc != -ENOENT) {
			SPDK_ERRLOG("Unexpected flush_begin(%" PRIu64 ") result: "
				    "%d\n", chunk_index, rc);
		}
		free(req);
		return;
	}

	f->in_flight++;
	f->stats.rounds++;
	req->start_tsc = spdk_get_ticks();
	{
		struct flusher_fail_slot *slot = flusher_fail_slot(f, chunk_index, false);

		req->attempt = 1 + (slot ? slot->failures : 0);
	}
	TAILQ_INSERT_TAIL(&f->inflight_reqs, req, link);

	f->upload_fn(f->upload_ctx, &req->view, flusher_upload_done, req);
}

void
s3_flusher_kick(struct s3_flusher *f)
{
	bool force;

	if (!f) {
		return;
	}

	/* Two reasons to set the overlay's hold-back policy aside.
	 *
	 * A drain, because "everything acknowledged is in S3" is not something
	 * waiting can satisfy -- and the drain deadline (30s) is shorter than the
	 * default hold-back age (45s), so honouring the policy here would make
	 * every unload, checkpoint and export time out instead of flushing.
	 *
	 * A backpressured WAL, because the log is only truncated once the data has
	 * reached S3 (flusher_advance_wal below). Holding a chunk back therefore
	 * holds on to log space, and the WAL refusing writes is a worse outcome
	 * than an early upload. In the shipped configuration the overlay's own
	 * high water mark is reached long first -- 4 GiB of RAM against 32 GiB of
	 * log -- so this is the guard for a configuration where it is not. */
	force = (f->drain_cb != NULL) || s3_wal_is_backpressured(f->wal);

	while (!f->stopping && f->in_flight < f->max_concurrent) {
		uint64_t chunk_index;

		if (s3_overlay_next_dirty(f->overlay, force, &chunk_index) != 0) {
			/* -ENOENT nothing dirty, -EAGAIN dirty but held back. The
			 * poller comes round again either way. */
			break;
		}
		flusher_start_one(f, chunk_index);
	}

	flusher_check_drain(f);
}

static int
flusher_poll(void *arg)
{
	struct s3_flusher *f = arg;

	if (f->retire_requested) {
		/* Retiring: nothing new starts, so all that is left is to notice
		 * the last completion. The callback may destroy f (and unregister
		 * this poller, which SPDK allows from inside it); f is not touched
		 * after it. */
		flusher_check_retire(f);
		return SPDK_POLLER_BUSY;
	}

	if (f->in_flight == 0 && !s3_overlay_has_dirty(f->overlay)) {
		flusher_check_drain(f);
		return SPDK_POLLER_IDLE;
	}

	/* A drain blocked on an unreachable S3 only makes progress through its
		* deadline, so it has to be re-checked on every tick, not just when an
		* upload completes -- including while uploads are in flight, which with S3
		* down is all the time (a failed upload re-queues at once). */
	s3_flusher_kick(f);
	return SPDK_POLLER_BUSY;
}

/* ==========================================================================
 * Drain
 * ========================================================================== */

/* A drain is an OBSERVATION with a deadline, not a resource transition.
 *
 * Success (0) means: nothing dirty, nothing in flight, no super sync running --
 * everything acknowledged so far is in S3 (INV2). -ETIMEDOUT means the deadline
 * passed first. It is reported at the deadline even while uploads are still in
 * flight: with S3 unreachable a failed upload re-queues immediately, so
 * in_flight never reaches zero and waiting for it made the deadline
 * unreachable (unload blocked for as long as S3 stayed down).
 *
 * Reporting early is safe ONLY because a timeout changes nothing about the
 * flusher's lifetime: it is not stopped, its uploads keep their pointers into
 * it, and it keeps retrying in the background. A caller that wants to free the
 * flusher (or anything an upload completion reaches: the WAL, the chunk map,
 * the bs_dev) must call s3_flusher_retire() and wait for THAT callback. */
static void
flusher_check_drain(struct s3_flusher *f)
{
	s3_flusher_cb cb_fn;
	void *cb_arg;
	int status;

	if (!f->drain_cb) {
		return;
	}

	if (f->in_flight == 0 && !f->super_sync_active &&
	    !s3_overlay_has_dirty(f->overlay)) {
		status = 0;
	} else if (spdk_get_ticks() >= f->drain_deadline) {
		status = -ETIMEDOUT;
		SPDK_WARNLOG("Flusher drain timed out (%u upload(s) in flight, "
			     "dirty=%d, super_sync=%d); the data stays in the WAL "
			     "and the flusher keeps retrying\n", f->in_flight,
			     (int)s3_overlay_has_dirty(f->overlay),
			     (int)f->super_sync_active);
		flusher_dump_inflight(f);
	} else {
		return;
	}

	/* Cleared before the call so the callback fires exactly once, even if it
	 * starts another drain or a late completion re-enters here. */
	cb_fn  = f->drain_cb;
	cb_arg = f->drain_arg;
	f->drain_cb  = NULL;
	f->drain_arg = NULL;

	cb_fn(cb_arg, status);
}

/* Retirement completes once nothing can call back into the flusher: no upload
 * in flight and no super sync running. stopping is already set, so a failed
 * upload is not re-queued and in_flight only goes down. Called only from
 * flusher_poll(), as its last touch of \c f: the callback may destroy it. */
static void
flusher_check_retire(struct s3_flusher *f)
{
	s3_flusher_cb cb_fn;
	void *cb_arg;
	int status;

	if (!f->retire_requested || f->in_flight > 0 || f->super_sync_active) {
		return;
	}

	status = s3_overlay_has_dirty(f->overlay) ? -EAGAIN : 0;
	cb_fn  = f->retire_cb;
	cb_arg = f->retire_arg;
	f->retire_requested = false;
	f->retire_cb  = NULL;
	f->retire_arg = NULL;

	if (cb_fn) {
		cb_fn(cb_arg, status);
	}
}

void
s3_flusher_retire(struct s3_flusher *f, s3_flusher_cb cb_fn, void *cb_arg)
{
	if (!f) {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}
	if (f->retire_requested) {
		/* One retirement: a second would race the first for the destroy. */
		if (cb_fn) {
			cb_fn(cb_arg, -EBUSY);
		}
		return;
	}

	f->stopping = true;
	f->retire_requested = true;
	f->retire_cb = cb_fn;
	f->retire_arg = cb_arg;

	/* A drain still waiting has to be answered now: no new round will start,
	 * so it could only time out. */
	if (f->drain_cb) {
		s3_flusher_cb d_fn = f->drain_cb;
		void *d_arg = f->drain_arg;

		f->drain_cb = NULL;
		f->drain_arg = NULL;
		d_fn(d_arg, (f->in_flight == 0 && !f->super_sync_active &&
			     !s3_overlay_has_dirty(f->overlay)) ? 0 : -ETIMEDOUT);
	}

	/* Deliberately NOT completed here, even when already idle: the retire
	 * callback is allowed to destroy the flusher and its owner, and this
	 * function may be on a stack that still uses them (e.g. from a drain
	 * callback inside s3_flusher_kick()). Completion happens on the next
	 * poller tick at the earliest -- see flusher_poll(). */
}

bool
s3_flusher_is_retiring(const struct s3_flusher *f)
{
	return f && f->stopping;
}

void
s3_flusher_drain(struct s3_flusher *f, uint64_t timeout_us,
		 s3_flusher_cb cb_fn, void *cb_arg)
{
	if (!f) {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}
	if (f->stopping) {
		/* Retiring: no round will be started again, so a drain could only
		 * report a timeout. Say so directly. */
		if (cb_fn) {
			cb_fn(cb_arg, -ESHUTDOWN);
		}
		return;
	}
	if (f->drain_cb) {
		/* One drain at a time. A second caller is told -EBUSY rather than
		 * queued: the drain already running is the one it wanted, so waiting
		 * for it and asking again gets the same answer as a waiter list
		 * would, without one. Export does exactly that (export_drained);
		 * shutdown has no competitor to wait for. */
		if (cb_fn) {
			cb_fn(cb_arg, -EBUSY);
		}
		return;
	}

	if (timeout_us == 0) {
		timeout_us = S3_FLUSHER_DRAIN_TIMEOUT_US;
	}
	f->drain_deadline = spdk_get_ticks() +
			    (timeout_us * spdk_get_ticks_hz()) / 1000000;

	f->drain_cb  = cb_fn;
	f->drain_arg = cb_arg;

	/* Push everything through rather than waiting for the poller tick. Cannot
	 * destroy f: retire_requested is false here (stopping implies it). */
	s3_flusher_kick(f);
}

/* ==========================================================================
 * Create / destroy
 * ========================================================================== */

int
s3_flusher_create(struct s3_overlay *overlay, struct s3_wal *wal,
		  s3_flusher_upload_fn upload_fn, void *upload_ctx,
		  const struct s3_flusher_opts *opts, struct s3_flusher **out)
{
	struct s3_flusher *f;
	uint32_t period_us;

	if (!overlay || !upload_fn || !out) {
		return -EINVAL;
	}

	f = calloc(1, sizeof(*f));
	if (!f) {
		return -ENOMEM;
	}

	f->overlay    = overlay;
	f->wal        = wal;
	TAILQ_INIT(&f->inflight_reqs);
	f->upload_fn  = upload_fn;
	f->upload_ctx = upload_ctx;

	f->max_concurrent = (opts && opts->max_concurrent) ?
			    opts->max_concurrent : S3_FLUSHER_DEFAULT_MAX_CONCURRENT;
	period_us = (opts && opts->poll_period_us) ?
		    opts->poll_period_us : S3_FLUSHER_DEFAULT_POLL_US;

	f->poller = SPDK_POLLER_REGISTER(flusher_poll, f, period_us);
	if (!f->poller) {
		free(f);
		return -ENOMEM;
	}

	SPDK_NOTICELOG("Flusher started: %u concurrent chunk uploads, %u us tick\n",
		       f->max_concurrent, period_us);

	*out = f;
	return 0;
}

void
s3_flusher_destroy(struct s3_flusher *f)
{
	if (!f) {
		return;
	}

	f->stopping = true;

	/* An upload completion would call back into a freed flusher. The caller
	 * has to RETIRE first (s3_flusher_retire()); a drain is not enough, since
	 * a timed-out drain returns with uploads still in flight. */
	assert(f->in_flight == 0);
	if (f->in_flight != 0) {
		SPDK_ERRLOG("s3_flusher_destroy() called with %u uploads in "
			    "flight; leaking the flusher to avoid "
			    "use-after-free\n", f->in_flight);
		/* The flusher is leaked, so the poller is of no use any more: it
		 * would spin forever on a flusher that never drains. */
		spdk_poller_unregister(&f->poller);
		return;
	}

	/* A super-sync completion also writes into the flusher. Drain waits for it,
	 * so assert it never leaks through here -- but leak rather than free if it
	 * somehow does, mirroring the upload check above. */
	assert(!f->super_sync_active);
	if (f->super_sync_active) {
		SPDK_ERRLOG("s3_flusher_destroy() called with a super-sync in "
			    "flight; leaking the flusher to avoid "
			    "use-after-free\n");
		spdk_poller_unregister(&f->poller);
		return;
	}

	if (s3_overlay_has_dirty(f->overlay)) {
		SPDK_WARNLOG("Flusher destroyed with dirty chunks left; that data "
			     "is still in the WAL and will be replayed\n");
	}

	spdk_poller_unregister(&f->poller);
	free(f);
}

void
s3_flusher_get_stats(const struct s3_flusher *f, struct s3_flusher_stats *out)
{
	if (!f || !out) {
		return;
	}
	*out = f->stats;
	out->in_flight = f->in_flight;
}

SPDK_LOG_REGISTER_COMPONENT(s3_flusher)
