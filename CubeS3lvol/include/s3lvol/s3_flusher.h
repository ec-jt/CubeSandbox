/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_flusher -- pushes overlay data to S3, one read-modify-write per chunk
 *
 *   Design notes are at the top of lib/s3bsdev/s3_flusher.c. The short version:
 *   user writes are acknowledged from the WAL and accumulate in the overlay, and
 *   this component turns them into S3 objects with *at most one writer per
 *   object at a time*. That per-chunk single-flight rule is what removes the
 *   concurrent read-modify-write race.
 *
 *   The upload itself is injected as a callback so this file does not depend on
 *   the S3 client or the chunk map, which keeps it unit testable without
 *   credentials.
 */

#ifndef S3LVOL_FLUSHER_H
#define S3LVOL_FLUSHER_H

#include "spdk/stdinc.h"

#include "s3lvol/s3_overlay.h"

/* Chunks uploaded at once. Each one is an S3 round trip measured in tens of
 * milliseconds, so some concurrency is needed to get any throughput at all. */
#define S3_FLUSHER_DEFAULT_MAX_CONCURRENT 8

/* Tick interval: roughly a millisecond. */
#define S3_FLUSHER_DEFAULT_POLL_US 1000

/* How long a drain waits before reporting -ETIMEDOUT to its caller.
 *
 * The deadline is honoured whether or not uploads are in flight. It used not to
 * be: the check returned early while any upload was running, and with S3
 * unreachable a failed upload re-queues at once, so the deadline was never
 * evaluated and an unload blocked for as long as the outage lasted.
 *
 * A drain timeout is a RESULT, not a state change: the flusher is not stopped,
 * uploads in flight keep running and their completions keep touching the
 * flusher (and whatever the upload callback reaches), and the flusher keeps
 * retrying. Freeing anything requires s3_flusher_retire(). */
#define S3_FLUSHER_DRAIN_TIMEOUT_US (30ULL * 1000 * 1000)

struct s3_flusher;
struct s3_wal;

typedef void (*s3_flusher_cb)(void *cb_arg, int status);

/**
 * Completion of one chunk upload. Must be called exactly once per upload_fn
 * invocation, and may be called before upload_fn returns.
 */
typedef void (*s3_flusher_upload_cb)(void *cb_arg, int status);

/**
 * Upload one chunk.
 *
 * \c view describes what to write and stays valid until the completion fires.
 * The implementation is expected to:
 *
 *   1. read the current object back when \c view->covers_prefix is false,
 *   2. merge with s3_overlay_flush_merge(),
 *   3. PUT a new object and publish it in the chunk map,
 *   4. *re-check s3_overlay_chunk_epoch() against view->epoch before
 *      publishing* -- a mismatch means the chunk was unmapped meanwhile and
 *      publishing would resurrect it.
 *
 * A non-zero status leaves the data dirty and it is retried later, so a
 * transient S3 failure costs nothing but time.
 */
typedef void (*s3_flusher_upload_fn)(void *ctx,
				     const struct s3_overlay_flush_view *view,
				     s3_flusher_upload_cb cb_fn, void *cb_arg);

struct s3_flusher_opts {
	uint32_t max_concurrent;   /* 0 selects the default */
	uint32_t poll_period_us;   /* 0 selects the default */
};

struct s3_flusher_stats {
	uint64_t rounds;           /* flush rounds started */
	uint64_t chunks_flushed;
	uint64_t blocks_flushed;
	uint64_t failures;
	uint32_t in_flight;
};

/**
 * Start the flusher. The poller is registered on the calling thread, which must
 * be the lvstore's owner thread.
 *
 * \param wal may be NULL, in which case no truncation is attempted. Used by the
 *            unit test, which has an overlay but no log.
 */
int s3_flusher_create(struct s3_overlay *overlay, struct s3_wal *wal,
		      s3_flusher_upload_fn upload_fn, void *upload_ctx,
		      const struct s3_flusher_opts *opts,
		      struct s3_flusher **out);

/**
 * Release. *The caller must retire first* (s3_flusher_retire(), and wait for its
 * callback) -- this asserts that no upload or super sync is in flight, because a
 * completion would otherwise touch freed memory. A completed drain is NOT
 * sufficient: a drain may report -ETIMEDOUT with uploads still in flight.
 */
void s3_flusher_destroy(struct s3_flusher *f);

/**
 * Stop starting uploads and report once nothing can call back into the flusher.
 *
 * Sets the flusher stopping (a failed upload is no longer re-queued, so the
 * in-flight count only goes down), answers a pending drain immediately (0 if
 * already clean, else -ETIMEDOUT), and fires \c cb_fn exactly once when no
 * upload and no WAL super sync is in flight:
 *
 *   0        nothing is dirty: everything acknowledged is in S3
 *   -EAGAIN  dirty data remains; it is durable only in the WAL
 *
 * The callback is only ever invoked from the flusher's poller tick -- never
 * inline from this call, from s3_flusher_kick(), or from an upload completion --
 * so it may call s3_flusher_destroy() and tear down the owner. Retirement is
 * irreversible and happens at most once; a second call gets -EBUSY. How long it
 * takes is bounded only by how long the in-flight upload_fn requests take to
 * complete (for s3_bs_dev: the S3 client's own timeouts); the flusher itself
 * adds no unbounded wait.
 */
void s3_flusher_retire(struct s3_flusher *f, s3_flusher_cb cb_fn, void *cb_arg);

/** True once s3_flusher_retire() was called. */
bool s3_flusher_is_retiring(const struct s3_flusher *f);

/**
 * Start uploads for whatever is dirty, up to the concurrency limit. Cheap and
 * idempotent; call it whenever new data lands rather than waiting for the tick.
 */
void s3_flusher_kick(struct s3_flusher *f);

/**
 * Observe, with a deadline, whether everything acknowledged so far reaches S3.
 * The callback fires exactly once:
 *
 *   0           nothing dirty, nothing in flight, no super sync (INV2 holds)
 *   -ETIMEDOUT  the deadline passed first. Reported at the deadline even with
 *               uploads in flight. NOTHING is released or stopped: the flusher
 *               keeps retrying in the background, so a later drain may succeed.
 *   -EBUSY      another drain is already waiting
 *   -ESHUTDOWN  the flusher is retiring
 *
 * \param timeout_us 0 selects S3_FLUSHER_DRAIN_TIMEOUT_US.
 *
 * A drain result never licenses freeing the flusher or anything an upload
 * completion can reach; use s3_flusher_retire() for that.
 */
void s3_flusher_drain(struct s3_flusher *f, uint64_t timeout_us,
		      s3_flusher_cb cb_fn, void *cb_arg);

void s3_flusher_get_stats(const struct s3_flusher *f, struct s3_flusher_stats *out);

#endif /* S3LVOL_FLUSHER_H */
