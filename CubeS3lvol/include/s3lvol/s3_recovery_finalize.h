/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */

/**
 * \file
 * s3_recovery_finalize -- the publication ordering of a remote-only rebuild
 *
 * === The problem this exists to solve ===
 *
 * The recovery chain used to publish the local device *before* it loaded
 * blobstore:
 *
 *   checkpoint applied -> update_checkpoint -> commit_staged -> attach_wal
 *   -> spdk_lvs_load_ext -> open lvols -> register bdevs
 *
 * Everything after the commit can fail. A crash or a load failure in that
 * window left a device carrying a valid, attachable normal super block whose
 * blobstore was never loaded, whose lvols were never opened, and whose bdevs
 * were never registered. A later attach accepted it, because the block is
 * valid, and replayed from a checkpoint pointer describing a state that was
 * never fully rebuilt.
 *
 * === The ordering this module enforces ===
 *
 *   STAGE      format staged, write journal/WAL/checkpoint/uuid
 *   VALIDATE   load blobstore and open *every* lvol, registering nothing
 *   DRAIN      wait for every acknowledged write to reach S3
 *   PREFLUSH   flush the local device, so the prerequisites are durable
 *   COMMIT     publish the normal super block (its own preflush + postflush)
 *   FINALFLUSH flush again, so the publication itself is durable
 *   PUBLISH    register the bdevs, then report success
 *
 * The commit is the last *local* write, and publication (bdev registration)
 * happens only after it. A failure at any step before the commit leaves the
 * device STAGED, which every reader refuses. A failure at the commit or the
 * final flush is reported as an error with an indeterminate on-disk outcome:
 * the normal block may already be visible, and that is safe *only* because
 * every prerequisite was flushed before it was written.
 *
 * === What this module deliberately does not do ===
 *
 *   - It does not load blobstore, open lvols, drain the flusher or register
 *     bdevs itself. Those are the caller's, because they need the lvstore, the
 *     bs_dev and the bdev module. The caller drives them and reports each
 *     result through the step callbacks below.
 *   - It does not make the commit atomic against power loss. A torn commit
 *     block fails magic and CRC and is refused; that is detection, not
 *     atomicity.
 *   - It does not close the "blobstore load writes after the commit" window.
 *     The load runs *before* the commit here, so its writes are covered by the
 *     preflush, but a load that is itself only partly idempotent is still a
 *     limitation of blobstore, not of this ordering.
 *
 * === Why it is a separate translation unit ===
 *
 * The ordering is the thing under test. Keeping it in a small module with an
 * explicit step interface means the test harness drives the *same* state
 * machine the production chain drives, rather than a copy of it, and can
 * inject a failure at each step without an S3 client, a blobstore or a bdev
 * module.
 */

#ifndef S3LVOL_S3_RECOVERY_FINALIZE_H
#define S3LVOL_S3_RECOVERY_FINALIZE_H

#include "spdk/stdinc.h"

#include "s3lvol/s3_local_dev.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Where the state machine currently is. Reported in the failure log and
 * exposed for tests; the numeric order is the ordering contract. */
enum s3_recovery_finalize_step {
	S3_RF_STEP_INIT = 0,
	/* Blobstore loaded and every lvol opened. Nothing registered. */
	S3_RF_STEP_VALIDATED,
	/* Everything acknowledged has reached S3. */
	S3_RF_STEP_DRAINED,
	/* The local device has been flushed; prerequisites are durable. */
	S3_RF_STEP_PREFLUSHED,
	/* The normal super block is written and flushed. */
	S3_RF_STEP_COMMITTED,
	/* The publication itself has been flushed. */
	S3_RF_STEP_FINAL_FLUSHED,
	/* Bdevs registered; the lvstore is live. */
	S3_RF_STEP_PUBLISHED,
};

const char *s3_recovery_finalize_step_name(enum s3_recovery_finalize_step step);

/**
 * The caller's side of the state machine.
 *
 * Every callback is invoked exactly once per run, on the thread that started
 * the run, and only after the previous step reported success. A callback that
 * reports failure stops the run: no later callback is invoked, and the
 * completion callback receives that status.
 *
 * \c validate must load blobstore and open every lvol *without* registering a
 * bdev for any of them. A partial success is a failure: if any lvol cannot be
 * opened, \c validate must report an error, because the chain must not publish
 * a device whose lvol set is only partly usable.
 *
 * === The step contract, and why it is not enforced ===
 *
 * Each step must invoke \c done exactly once, and must never invoke it after
 * the run has reported completion. The run object is freed immediately before
 * the completion callback is invoked, so a second or late report is a
 * use-after-free rather than a recoverable error. That order is deliberate:
 * the caller is not expected to free an object it did not allocate, and the
 * alternative -- keeping the run alive forever -- would leak one per failed
 * recovery.
 *
 * A step that fails must route *every* failure it can produce through \c done.
 * Reporting a failure by any other route (calling back into the caller's own
 * setup funnel, for example) both loses the run object and frees the caller's
 * context while the run still points at it.
 */
struct s3_recovery_finalize_ops {
	/* Load blobstore, open all lvols, register nothing. */
	void (*validate)(void *cb_arg, void (*done)(void *done_arg, int status),
			 void *done_arg);
	/* Wait until everything acknowledged so far has reached S3. */
	void (*drain)(void *cb_arg, void (*done)(void *done_arg, int status),
		      void *done_arg);
	/* Register a bdev for every lvol opened by validate. Called only after
	 * the commit and the final flush succeeded. */
	void (*publish)(void *cb_arg, void (*done)(void *done_arg, int status),
			void *done_arg);
	void *cb_arg;
};

struct s3_recovery_finalize;

/**
 * Start a finalization run.
 *
 * \param dev   the staged local device. Required; the run refuses a device
 *              that is not staged, because publishing a normal block over a
 *              layout whose rebuild never ran is the failure this exists to
 *              prevent.
 * \param ops   the caller's steps. Required, and every member must be set.
 * \param cb_fn receives the final status. 0 means the lvstore is published and
 *              live. Any other value means it is not, and the caller must
 *              unwind. -EIO from the commit or the final flush specifically
 *              means the on-disk publication outcome is indeterminate.
 *
 * The callback may run before this function returns (argument validation, or
 * a step that completes inline).
 */
typedef void (*s3_recovery_finalize_cb)(void *cb_arg, int status);

void s3_recovery_finalize_start(struct s3_local_dev *dev,
				const struct s3_recovery_finalize_ops *ops,
				s3_recovery_finalize_cb cb_fn, void *cb_arg);

/**
 * The step the run stopped at, or S3_RF_STEP_INIT before it started.
 *
 * Valid until the completion callback returns. Exposed so a caller can log
 * "failed at DRAINED" rather than only an errno, and so tests can assert the
 * ordering directly.
 */
enum s3_recovery_finalize_step
s3_recovery_finalize_step(const struct s3_recovery_finalize *rf);

/**
 * True when the run reached S3_RF_STEP_PUBLISHED.
 *
 * The single question a caller should ask before treating the lvstore as live.
 */
bool s3_recovery_finalize_published(const struct s3_recovery_finalize *rf);

#ifdef __cplusplus
}
#endif

#endif /* S3LVOL_S3_RECOVERY_FINALIZE_H */
