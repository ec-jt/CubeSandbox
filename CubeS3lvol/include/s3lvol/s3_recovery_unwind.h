/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */

/**
 * \file
 * s3_recovery_unwind -- the release ordering of a failed recovery rebuild
 *
 * === The problem this exists to solve ===
 *
 * The recovery chain loads blobstore and opens every lvol before it commits the
 * normal super block. Until this module existed, a failure at or after that
 * point unwound through the *setup* funnel (lvs_setup_fail), which only tears
 * down the bs_dev. That leaves:
 *
 *   - every opened lvol still open (blobstore refuses to unload over them, and
 *     their blobs are never closed);
 *   - the lvol_store itself never unloaded, so its memory is leaked;
 *   - any bdev a partially completed publish step registered still in the
 *     global bdev table, pointing at an lvol whose backing device is about to
 *     be destroyed -- a use-after-free as soon as anything opens it;
 *   - and, because the bs_dev destroy is asynchronous, the setup context could
 *     be freed while blobstore still held references to the dead device.
 *
 * === The order this module enforces ===
 *
 *   RELEASE   one item at a time: unregister the bdev (whose destructor closes
 *             the lvol) if one was registered, otherwise close the lvol
 *   UNLOAD    unload the lvol_store, which destroys the bs_dev
 *   REPORT    hand the original failure to the caller, exactly once
 *
 * RELEASE must precede UNLOAD: spdk_lvs_unload() refuses with -EBUSY while any
 * lvol still has ref_count != 0 or an operation in progress. UNLOAD must
 * precede REPORT, because the caller frees the context that the bs_dev destroy
 * callback runs against.
 *
 * === What this module deliberately does not do ===
 *
 * It does not call SPDK. The primitives are the caller's, because they need the
 * bdev module, the lvol handles and the bs_dev. What it owns is the part that
 * is easy to get wrong and impossible to see in a review: the barrier count,
 * the exactly-once report, the inline-completion re-entry, and the decision
 * that "cleanup failed" must be reported rather than papered over.
 *
 * === Exactly once, and what that means for the caller ===
 *
 * Every release and the unload must invoke their completion callback exactly
 * once, and never after the run has reported. The run object is freed before
 * \c report is invoked, so nothing may touch it afterwards.
 */

#ifndef S3LVOL_S3_RECOVERY_UNWIND_H
#define S3LVOL_S3_RECOVERY_UNWIND_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

/* One resource the validate or publish step acquired.
 *
 * \c registered is the ownership fact that decides the release primitive, and
 * it is captured *before* any release runs. It is read-only here because the
 * two primitives are not interchangeable: the bdev layer's destructor closes
 * the lvol, so calling spdk_lvol_close() on a registered lvol as well is a
 * double close (spdk_lvol_close() answers -EINVAL once ref_count is 0), and
 * calling spdk_bdev_unregister() on an lvol without a bdev does nothing at all,
 * which would leave the lvol open.
 */
struct s3_recovery_unwind_item {
	void	*handle;
	bool	 registered;
};

/**
 * The caller's side of the unwind.
 *
 * All four callbacks are required.
 *
 * \c unregister and \c close are called once per item, sequentially, and the
 * next one is not started until the previous one has completed. Both must be
 * asynchronous-safe: \c done may run inline, before the call returns.
 *
 * \c unload is called once, after every item has been released.
 *
 * \c report is called exactly once. \c status is the failure the unwind was
 * started with, never zero. \c released is true only when every item was
 * released *and* the unload completed: when it is false the caller must not
 * free whatever owns the lvstore, because the caller's context is still
 * reachable from a live SPDK object. Reported cleanup failures are logged by
 * this module; \c released is the summary the caller acts on.
 */
struct s3_recovery_unwind_ops {
	void (*unregister)(void *cb_arg, void *handle,
			   void (*done)(void *done_arg, int status),
			   void *done_arg);
	void (*close)(void *cb_arg, void *handle,
		      void (*done)(void *done_arg, int status),
		      void *done_arg);
	void (*unload)(void *cb_arg,
		       void (*done)(void *done_arg, int status),
		       void *done_arg);
	void (*report)(void *cb_arg, int status, bool released);
	void *cb_arg;
};

/**
 * Start an unwind.
 *
 * \param ops     the caller's primitives. Required, and every member must be
 *                set.
 * \param items   the resources to release, in release order. Ownership passes
 *                to the unwind on success (it is freed before \c report); it
 *                is left with the caller when this function returns non-zero.
 *                May be NULL when \c nitems is 0.
 * \param nitems  number of items.
 * \param status  the failure that caused the unwind. Required to be non-zero:
 *                an unwind is not a success path, and reporting 0 as a failure
 *                would make "published" and "rolled back" indistinguishable.
 *
 * \return 0 when the unwind was started. \c report is then guaranteed unless a
 *         caller primitive never completes its callback. A negative errno means
 *         nothing was started and the caller still owns \c items.
 */
int s3_recovery_unwind_start(const struct s3_recovery_unwind_ops *ops,
			     struct s3_recovery_unwind_item *items,
			     size_t nitems, int status);

#ifdef __cplusplus
}
#endif

#endif /* S3LVOL_S3_RECOVERY_UNWIND_H */
