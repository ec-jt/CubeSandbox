/* SPDX-License-Identifier: Apache-2.0 */
/*
 * B16 export decisions, kept pure so they can be unit tested offline
 * (test/integration/export_b16_ctl_test.c). The asynchronous glue that feeds
 * them lives in vbdev_s3lvol_xfer.c; nothing here touches S3, a blobstore, a
 * registry, a clock or a thread.
 *
 * Three decisions:
 *
 *   1. export_drain_ctl_next(): an export's drain reported -ETIMEDOUT. Drain
 *      again while the flusher is still getting uploads through, fail only once
 *      it has made no progress for a whole window or the overall cap is spent.
 *
 *   2. export_reservation_ctl_drop(): an export failed. May its REF reservation
 *      (publication_pending) be dropped from the registry? Only when nothing
 *      that could name the export ever reached S3: no admission document write
 *      and no manifest write was attempted. Anything ambiguous keeps the
 *      reservation (fail closed), exactly as before B16.
 *
 *   3. export_abandon_ctl_verdict(): may an operator (or Cubelet) abandon a
 *      registry entry left publication_pending? Only with positive proof that
 *      no importer can hold it: no export of that uuid in flight, a 404 for the
 *      manifest AND a 404 for the admission document. Any other answer,
 *      including a transport error, refuses.
 */
#ifndef S3LVOL_EXPORT_B16_CTL_H
#define S3LVOL_EXPORT_B16_CTL_H

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>

/* ==========================================================================
 * 1. Progress-based drain
 * ========================================================================== */

/* Fail once the flusher has completed no upload for this long... */
#define EXPORT_DRAIN_NO_PROGRESS_MS (60ULL * 1000)
/* ...or once the export has spent this long draining in total. */
#define EXPORT_DRAIN_PROGRESS_CAP_MS (15ULL * 60 * 1000)

struct export_drain_progress {
	bool     started;
	uint64_t start_ms;          /* first drain attempt */
	uint64_t last_progress_ms;  /* last time chunks_flushed advanced */
	uint64_t last_flushed;      /* chunks_flushed at that time */
	uint32_t extensions;        /* re-drains granted so far */
};

enum export_drain_verdict {
	EXPORT_DRAIN_FAIL = 0,      /* report the -ETIMEDOUT */
	EXPORT_DRAIN_AGAIN,         /* drain again */
};

/* Record the counter when the first drain starts. Idempotent. */
static inline void
export_drain_ctl_start(struct export_drain_progress *p, uint64_t now_ms,
		       uint64_t chunks_flushed)
{
	if (p->started) {
		return;
	}
	p->started = true;
	p->start_ms = now_ms;
	p->last_progress_ms = now_ms;
	p->last_flushed = chunks_flushed;
}

/* A drain reported -ETIMEDOUT at \p now_ms with the flusher's monotonic
 * chunks_flushed counter at \p chunks_flushed. *progressed is set when the
 * counter moved since the previous look. */
static inline enum export_drain_verdict
export_drain_ctl_next(struct export_drain_progress *p, uint64_t now_ms,
		      uint64_t chunks_flushed, uint64_t no_progress_ms,
		      uint64_t cap_ms, bool *progressed)
{
	bool moved;

	if (!p->started) {
		export_drain_ctl_start(p, now_ms, chunks_flushed);
	}
	moved = chunks_flushed > p->last_flushed;
	if (moved) {
		p->last_flushed = chunks_flushed;
		p->last_progress_ms = now_ms;
	}
	if (progressed) {
		*progressed = moved;
	}
	if (now_ms - p->start_ms >= cap_ms) {
		return EXPORT_DRAIN_FAIL;
	}
	if (now_ms - p->last_progress_ms >= no_progress_ms) {
		return EXPORT_DRAIN_FAIL;
	}
	p->extensions++;
	return EXPORT_DRAIN_AGAIN;
}

/* ==========================================================================
 * 2. Reservation cleanup on failure
 * ========================================================================== */

struct export_attempts {
	/* An admission document write (release_safe exports) was submitted. */
	bool admission_write;
	/* A manifest or chunk upload was handed to S3, i.e. an engine run
	 * returned 0 and owns the completion. */
	bool publish_started;
};

/* True when a failed export's REF reservation may be removed from the
 * registry, so the uuid goes back to NONE and a resubmit is admitted. */
static inline bool
export_reservation_ctl_drop(const struct export_attempts *a, int status,
			    bool publication_pending)
{
	if (status == 0 || !publication_pending) {
		return false;
	}
	return !a->admission_write && !a->publish_started;
}

/* ==========================================================================
 * 3. Abandoning a stuck publication_pending entry
 * ========================================================================== */

struct export_abandon_facts {
	bool entry_exists;
	bool publication_pending;
	bool reaping;
	bool inflight;           /* an export of this uuid is running here */
	bool rewriting;          /* a release / rewrite of it is running */
	bool manifest_checked;
	int  manifest_head;      /* 0 found, -ENOENT absent, else error */
	bool admission_checked;
	int  admission_head;     /* 0 found, -ENOENT absent, else error */
};

/* Synchronous gate, before any S3 read. 0 to go on and read. */
static inline int
export_abandon_ctl_precheck(const struct export_abandon_facts *f)
{
	if (!f->entry_exists) {
		return -ENOENT;
	}
	if (!f->publication_pending) {
		return -EINVAL;   /* published: release it instead */
	}
	if (f->inflight || f->rewriting || f->reaping) {
		return -EBUSY;
	}
	return 0;
}

/* Full verdict once both objects were looked at: 0 means the entry may be
 * dropped. A present object is -EEXIST; a failed lookup passes its error on
 * (never "absent"); a lookup not done yet is -EAGAIN. */
static inline int
export_abandon_ctl_verdict(const struct export_abandon_facts *f)
{
	int rc = export_abandon_ctl_precheck(f);

	if (rc != 0) {
		return rc;
	}
	if (!f->manifest_checked) {
		return -EAGAIN;
	}
	if (f->manifest_head == 0) {
		return -EEXIST;
	}
	if (f->manifest_head != -ENOENT) {
		return f->manifest_head < 0 ? f->manifest_head : -EIO;
	}
	if (!f->admission_checked) {
		return -EAGAIN;
	}
	if (f->admission_head == 0) {
		return -EEXIST;
	}
	if (f->admission_head != -ENOENT) {
		return f->admission_head < 0 ? f->admission_head : -EIO;
	}
	return 0;
}

#endif /* S3LVOL_EXPORT_B16_CTL_H */
