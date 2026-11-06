/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_recovery_unwind -- release ordering for a failed recovery rebuild
 *
 *   The rationale is in include/s3lvol/s3_recovery_unwind.h. This file is the
 *   barrier and the exactly-once report, nothing else: it never calls SPDK.
 *
 *   === Why the pump is a loop and not recursion ===
 *
 *   Every primitive is allowed to complete inline. A recursive design would
 *   then nest one stack frame per released item, and the depth would be a
 *   property of how many lvols a blobstore happens to hold. The loop below
 *   instead re-checks its own state after each call: an inline completion sets
 *   in_step back to false and is absorbed by the same iteration.
 *
 *   `pumping` is what keeps an inline completion from starting a second,
 *   nested run of the loop. The inline callback sees pumping == true, returns,
 *   and the loop that is already on the stack continues from the advanced
 *   index. Without it the item would be released twice.
 *
 *   === Threading ===
 *
 *   Owner thread only, like the rest of the per-lvstore code. A primitive that
 *   completes on another thread would race the index; the contract in the
 *   header says it must not.
 */

#include "spdk/stdinc.h"
#include "spdk/log.h"
#include "spdk/string.h"

#include "s3lvol/s3_recovery_unwind.h"

struct s3_recovery_unwind {
	struct s3_recovery_unwind_ops	 ops;

	/* Owned from the moment the unwind starts; freed before report. */
	struct s3_recovery_unwind_item	*items;
	size_t				 nitems;
	size_t				 next;

	/* The failure the caller is unwinding from. Never zero. */
	int				 status;
	/* First release/unload failure, for the log and for `released`. */
	int				 cleanup_status;

	/* A primitive owns the turn. Cleared by its completion. */
	bool				 in_step;
	/* The loop below is on the stack. An inline completion must not re-enter
	 * it. */
	bool				 pumping;
	bool				 unload_started;
	bool				 unload_done;
	bool				 reported;
};

static void uw_pump(struct s3_recovery_unwind *u);
static void uw_released(void *done_arg, int status);
static void uw_unloaded(void *done_arg, int status);

/* Finish and report. Called only from the pump, with nothing in flight. */
static void
uw_finish(struct s3_recovery_unwind *u)
{
	struct s3_recovery_unwind_ops ops = u->ops;
	int status = u->status;
	bool released = (u->cleanup_status == 0);

	if (u->reported) {
		return;
	}
	u->reported = true;

	if (!released) {
		SPDK_ERRLOG("recovery unwind: cleanup did not complete (%s); the "
			    "caller must retain the lvstore rather than free it\n",
			    spdk_strerror(-u->cleanup_status));
	} else {
		SPDK_NOTICELOG("recovery unwind: released %zu resource(s) and "
			       "unloaded the lvstore; reporting %s\n",
			       u->nitems, spdk_strerror(-status));
	}

	/* Everything the state machine needs has been captured by value, and the
	 * caller is entitled to free its context inside report. */
	free(u->items);
	free(u);

	ops.report(ops.cb_arg, status, released);
}

/* Run whatever is not currently owned by a primitive. */
static void
uw_pump(struct s3_recovery_unwind *u)
{
	if (u->reported) {
		return;
	}
	if (u->pumping) {
		/* A primitive completed inline. It has already cleared in_step;
		 * the loop that is on the stack picks the work up. */
		return;
	}

	u->pumping = true;

	for (;;) {
		if (u->in_step) {
			/* Asynchronous: its completion will pump again. */
			break;
		}

		if (u->next < u->nitems) {
			struct s3_recovery_unwind_item *it = &u->items[u->next];

			u->next++;
			u->in_step = true;

			/* The choice is made from the recorded ownership, never
			 * from the handle's current state: a registered lvol is
			 * released by the bdev layer's destructor, which closes
			 * it, and closing it here as well would be a double
			 * close. */
			if (it->registered) {
				u->ops.unregister(u->ops.cb_arg, it->handle,
						 uw_released, u);
			} else {
				u->ops.close(u->ops.cb_arg, it->handle,
					     uw_released, u);
			}
			continue;
		}

		if (!u->unload_started) {
			u->unload_started = true;
			u->in_step = true;
			u->ops.unload(u->ops.cb_arg, uw_unloaded, u);
			continue;
		}

		if (u->unload_done) {
			u->pumping = false;
			uw_finish(u);
			return;
		}

		/* Unload submitted and still in flight. */
		break;
	}

	u->pumping = false;
}

/* A release reported. */
static void
uw_released(void *done_arg, int status)
{
	struct s3_recovery_unwind *u = done_arg;

	if (!u->in_step) {
		/* A completion for a release that had already advanced the run.
		 * Nothing is advanced: the run has either moved on (so this
		 * callback is a duplicate) or is finished. Touching the state
		 * here would double-free the item list. */
		SPDK_ERRLOG("recovery unwind: a release completed after the run "
			    "had moved on; ignoring\n");
		return;
	}

	u->in_step = false;

	if (status != 0) {
		SPDK_ERRLOG("recovery unwind: releasing resource %zu failed "
			    "(%s); continuing with the rest\n",
			    u->next, spdk_strerror(-status));
		if (u->cleanup_status == 0) {
			u->cleanup_status = status;
		}
	}

	uw_pump(u);
}

/* The lvol_store unloaded (which also destroyed the backing device). */
static void
uw_unloaded(void *done_arg, int status)
{
	struct s3_recovery_unwind *u = done_arg;

	if (!u->in_step) {
		SPDK_ERRLOG("recovery unwind: the unload completed after the "
			    "run had moved on; ignoring\n");
		return;
	}

	u->in_step = false;
	u->unload_done = true;

	if (status != 0) {
		SPDK_ERRLOG("recovery unwind: the lvstore could not be unloaded "
			    "(%s); its lvols and backing device are still "
			    "allocated\n", spdk_strerror(-status));
		if (u->cleanup_status == 0) {
			u->cleanup_status = status;
		}
	}

	uw_pump(u);
}

int
s3_recovery_unwind_start(const struct s3_recovery_unwind_ops *ops,
			 struct s3_recovery_unwind_item *items,
			 size_t nitems, int status)
{
	struct s3_recovery_unwind *u;

	if (!ops || !ops->unregister || !ops->close || !ops->unload ||
	    !ops->report) {
		SPDK_ERRLOG("recovery unwind needs release, close, unload and "
			    "report primitives\n");
		return -EINVAL;
	}
	if (nitems > 0 && !items) {
		SPDK_ERRLOG("recovery unwind was given %zu release(s) and no "
			    "list\n", nitems);
		return -EINVAL;
	}
	if (status == 0) {
		/* An unwind reports a failure. Reporting 0 would make "rolled
		 * back" indistinguishable from "published". */
		SPDK_ERRLOG("recovery unwind was started with no failure to "
			    "report\n");
		return -EINVAL;
	}

	u = calloc(1, sizeof(*u));
	if (!u) {
		return -ENOMEM;
	}
	u->ops = *ops;
	u->items = items;
	u->nitems = nitems;
	u->status = status;

	uw_pump(u);
	return 0;
}
