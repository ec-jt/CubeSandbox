/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_recovery_finalize -- the publication ordering of a remote-only rebuild
 *
 *   The rationale lives in include/s3lvol/s3_recovery_finalize.h. This file is
 *   the state machine itself: one linear sequence, one step at a time, with a
 *   single completion callback.
 *
 *   === Why the sequence is written out rather than chained ad hoc ===
 *
 *   The previous chain interleaved the commit with the load: the commit ran
 *   from the checkpoint-recorded callback, and the load ran from the commit
 *   callback. That made "what has been published at this instant" a property of
 *   the call graph rather than of the code, and the answer was wrong -- the
 *   device was attachable before blobstore had been loaded.
 *
 *   Here the order is a list of steps in one function. Each step's completion
 *   advances the index, and nothing else can. A failure at step N leaves the
 *   index at N, which is what the caller logs and what the tests assert.
 *
 *   === Threading ===
 *
 *   Owner thread only, like every other per-lvstore component. Every step
 *   callback is expected to complete on that thread; the state machine does not
 *   bounce anything.
 */

#include "spdk/stdinc.h"
#include "spdk/log.h"
#include "spdk/string.h"

#include "s3lvol/s3_local_dev.h"
#include "s3lvol/s3_recovery_finalize.h"

struct s3_recovery_finalize {
	struct s3_local_dev                  *dev;
	struct s3_recovery_finalize_ops       ops;
	s3_recovery_finalize_cb              cb_fn;
	void                                *cb_arg;

	enum s3_recovery_finalize_step       step;

	/* Set once the run has stopped, so a step that re-enters the state
	 * machine (a nested completion, or a completion that arrives while the
	 * completion callback is running) cannot advance past the end.
	 *
	 * It is NOT a defence against a step that reports after the completion
	 * callback has returned: the run object is freed before that callback
	 * is invoked, so such a report would be a use-after-free. See the
	 * lifetime contract in the header -- each step must report exactly once,
	 * and never after the run has reported. */
	bool                                 finished;
};

const char *
s3_recovery_finalize_step_name(enum s3_recovery_finalize_step step)
{
	switch (step) {
	case S3_RF_STEP_INIT:
		return "INIT";
	case S3_RF_STEP_VALIDATED:
		return "VALIDATED";
	case S3_RF_STEP_DRAINED:
		return "DRAINED";
	case S3_RF_STEP_PREFLUSHED:
		return "PREFLUSHED";
	case S3_RF_STEP_COMMITTED:
		return "COMMITTED";
	case S3_RF_STEP_FINAL_FLUSHED:
		return "FINAL_FLUSHED";
	case S3_RF_STEP_PUBLISHED:
		return "PUBLISHED";
	}
	return "UNKNOWN";
}

enum s3_recovery_finalize_step
s3_recovery_finalize_step(const struct s3_recovery_finalize *rf)
{
	return rf ? rf->step : S3_RF_STEP_INIT;
}

bool
s3_recovery_finalize_published(const struct s3_recovery_finalize *rf)
{
	return rf && rf->step == S3_RF_STEP_PUBLISHED;
}

static void rf_advance(struct s3_recovery_finalize *rf);
static void rf_committed(void *cb_arg, int status);

/* Report the run as finished.
 *
 * Exactly one completion callback per run. \c finished makes a re-entrant call
 * a no-op (the reachable case: a step that completes inline from inside the
 * completion callback's own teardown), but it is not a substitute for the
 * caller honouring the step contract -- see the header.
 *
 * The run object is freed *before* the callback, because the caller is not
 * expected to free something it did not allocate. Everything the callback may
 * observe is therefore captured by value first, and no path in this file
 * touches the run object after the free. */
static void
rf_finish(struct s3_recovery_finalize *rf, int status)
{
	s3_recovery_finalize_cb cb_fn = rf->cb_fn;
	void *cb_arg = rf->cb_arg;
	enum s3_recovery_finalize_step step = rf->step;

	if (rf->finished) {
		return;
	}
	rf->finished = true;

	if (status != 0) {
		SPDK_ERRLOG("Recovery finalization failed at %s: %s\n",
			    s3_recovery_finalize_step_name(step),
			    spdk_strerror(-status));
	} else {
		SPDK_NOTICELOG("Recovery finalization completed: %s\n",
			       s3_recovery_finalize_step_name(step));
	}

	free(rf);

	if (cb_fn) {
		cb_fn(cb_arg, status);
	}
}

/* A step reported. Either stop, or move to the next one.
 *
 * The index is advanced *before* the next step runs, so a step that completes
 * inline cannot re-enter the one that just finished. Without this the state
 * machine would call the same step forever -- which is exactly what the
 * harness caught on its first run.
 */
static void
rf_step_done(void *cb_arg, int status)
{
	struct s3_recovery_finalize *rf = cb_arg;

	if (rf->finished) {
		/* A step completed after the run was already finished. Nothing
		 * may be advanced: the caller has been told the outcome. */
		SPDK_ERRLOG("Recovery finalization step completed after the run "
			    "finished; ignoring\n");
		return;
	}

	if (status != 0) {
		rf_finish(rf, status);
		return;
	}

	switch (rf->step) {
	case S3_RF_STEP_INIT:
		rf->step = S3_RF_STEP_VALIDATED;
		break;
	case S3_RF_STEP_VALIDATED:
		rf->step = S3_RF_STEP_DRAINED;
		break;
	case S3_RF_STEP_DRAINED:
		/* The preflush is the step that just completed. */
		rf->step = S3_RF_STEP_PREFLUSHED;
		break;
	case S3_RF_STEP_PREFLUSHED:
		/* Unreachable: the preflush has its own callback
		 * (rf_preflushed) because it must start the commit, whose
		 * completion is reported through rf_committed(). */
		SPDK_ERRLOG("Recovery finalization: the preflush completed "
			    "without its own callback; refusing to continue\n");
		rf_finish(rf, -EINVAL);
		return;
	case S3_RF_STEP_COMMITTED:
		/* The final flush is the step that just completed. */
		rf->step = S3_RF_STEP_FINAL_FLUSHED;
		break;
	case S3_RF_STEP_FINAL_FLUSHED:
		rf->step = S3_RF_STEP_PUBLISHED;
		break;
	case S3_RF_STEP_PUBLISHED:
		rf_finish(rf, 0);
		return;
	}

	rf_advance(rf);
}

/* The preflush completed. Only now is the commit started: the commit is the
	* point of no return, and it must not begin until the prerequisites are
	* durable. */
static void
rf_preflushed(void *cb_arg, int status)
{
	struct s3_recovery_finalize *rf = cb_arg;

	if (rf->finished) {
		return;
	}

	if (status != 0) {
		/* Nothing was published: the in-memory super block is still the
		 * staging one, because the commit has not been called. */
		SPDK_ERRLOG("Recovery finalization: the preflush failed (%s); "
			    "the device stays STAGED\n", spdk_strerror(-status));
		rf_finish(rf, status);
		return;
	}

	rf->step = S3_RF_STEP_PREFLUSHED;
	s3_local_dev_commit_staged(rf->dev, rf_committed, rf);
}

/* The commit's own callback. The commit is the point of no return: a failure
 * here means the normal block may already be on the device. */
static void
rf_committed(void *cb_arg, int status)
{
	struct s3_recovery_finalize *rf = cb_arg;

	if (rf->finished) {
		return;
	}

	if (status != 0) {
		SPDK_ERRLOG("Recovery finalization: the commit failed (%s); the "
			    "on-disk publication outcome is indeterminate. The "
			    "device may be attachable even though the rebuild is "
			    "not published\n", spdk_strerror(-status));
		rf_finish(rf, status);
		return;
	}

	rf->step = S3_RF_STEP_COMMITTED;
	rf_advance(rf);
}

/* The final flush's callback. Success here means the publication itself is
 * durable, and only now may bdevs be registered. */
static void
rf_final_flushed(void *cb_arg, int status)
{
	struct s3_recovery_finalize *rf = cb_arg;

	if (rf->finished) {
		return;
	}

	if (status != 0) {
		SPDK_ERRLOG("Recovery finalization: the final flush failed (%s); "
			    "the normal super block may be visible but is not "
			    "proven durable\n", spdk_strerror(-status));
		rf_finish(rf, status);
		return;
	}

	rf->step = S3_RF_STEP_FINAL_FLUSHED;
	rf_advance(rf);
}

/* Run the step at rf->step. Called only when the previous step succeeded. */
static void
rf_advance(struct s3_recovery_finalize *rf)
{
	switch (rf->step) {
	case S3_RF_STEP_INIT:
		/* Load blobstore and open every lvol, registering nothing. A
		 * partial success is a failure: the caller's validate step must
		 * report an error if any lvol could not be opened. */
		rf->ops.validate(rf->ops.cb_arg, rf_step_done, rf);
		return;

	case S3_RF_STEP_VALIDATED:
		rf->ops.drain(rf->ops.cb_arg, rf_step_done, rf);
		return;

	case S3_RF_STEP_DRAINED:
		/* Prerequisites are durable before the in-memory identity
		 * changes. The commit repeats this flush internally, but doing
		 * it here as well is what makes the ordering explicit at the
		 * chain level: the step name says what has been proven.
		 *
		 * rf_preflushed() starts the commit, rather than rf_step_done()
		 * advancing into it: the commit reports through its own
		 * callback and must not be entered from the generic step
		 * advance. */
		s3_local_dev_flush_durable(rf->dev,
					   "the recovery prerequisites",
					   rf_preflushed, rf);
		return;

	case S3_RF_STEP_PREFLUSHED:
		/* Unreachable: rf_preflushed() starts the commit directly. */
		SPDK_ERRLOG("Recovery finalization: reached PREFLUSHED without "
			    "starting the commit\n");
		rf_finish(rf, -EINVAL);
		return;

	case S3_RF_STEP_COMMITTED:
		s3_local_dev_flush_durable(rf->dev,
					   "the published super block",
					   rf_final_flushed, rf);
		return;

	case S3_RF_STEP_FINAL_FLUSHED:
		/* Registration is the publication. It runs only after the
		 * commit and the final flush, so a bdev can never be visible
		 * for a device that is not yet durably normal. */
		rf->ops.publish(rf->ops.cb_arg, rf_step_done, rf);
		return;

	case S3_RF_STEP_PUBLISHED:
		rf_finish(rf, 0);
		return;
	}

	/* Unreachable: every enum value is handled above. */
	SPDK_ERRLOG("Recovery finalization reached an unknown step %d\n",
		    (int)rf->step);
	rf_finish(rf, -EINVAL);
}

void
s3_recovery_finalize_start(struct s3_local_dev *dev,
			   const struct s3_recovery_finalize_ops *ops,
			   s3_recovery_finalize_cb cb_fn, void *cb_arg)
{
	struct s3_recovery_finalize *rf;

	if (!dev || !ops || !ops->validate || !ops->drain || !ops->publish) {
		SPDK_ERRLOG("Recovery finalization needs a staged device and "
			    "validate/drain/publish steps\n");
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}

	/* Publishing a normal block over a layout whose rebuild never ran is
	 * exactly the failure this module exists to prevent, so the staged
	 * state is checked here rather than assumed. */
	if (!s3_local_dev_is_staged(dev)) {
		SPDK_ERRLOG("Recovery finalization refused: the local device is "
			    "not staged\n");
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}

	rf = calloc(1, sizeof(*rf));
	if (!rf) {
		if (cb_fn) {
			cb_fn(cb_arg, -ENOMEM);
		}
		return;
	}
	rf->dev = dev;
	rf->ops = *ops;
	rf->cb_fn = cb_fn;
	rf->cb_arg = cb_arg;
	rf->step = S3_RF_STEP_INIT;

	rf_advance(rf);
}
