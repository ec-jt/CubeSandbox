/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   When may an lvstore give its S3 ownership marker back?
 *
 *   The marker says "this process is the writer". Releasing it tells every
 *   other process that nothing from here will touch the prefix again. So it
 *   may only be released once every writer this lvstore started has stopped
 *   AND stopped cleanly. "Stopped" alone is not enough: a flusher drain that
 *   timed out leaves acknowledged writes only in the local WAL, and a second
 *   process that rebuilds from S3 after the release would silently lose them.
 *
 *   This is cooperative quiescence of THIS process's writers. It is not a
 *   storage fence: a stalled remote request that the object store applies
 *   after the release is not prevented by anything here.
 *
 *   Header-only so the decisions are testable without S3 or SPDK, in the same
 *   way as pending_persist_ctl.h.
 */

#ifndef S3LVOL_OWNER_LIFECYCLE_CTL_H
#define S3LVOL_OWNER_LIFECYCLE_CTL_H

#include <errno.h>
#include <stdbool.h>

enum lvs_owner_action {
	LVS_OWNER_NOT_HELD = 0, /* nothing held: nothing to release or retain */
	LVS_OWNER_RELEASE,      /* every writer stopped cleanly: give it back */
	LVS_OWNER_RETAIN,       /* held, but a writer may remain or did not
				 * finish: keep the marker, report failure */
};

/* Inputs, gathered at the one point where the decision is taken. */
struct lvs_owner_teardown {
	bool owner_held;      /* this lvstore acquired the marker */
	bool resources_live;  /* SPDK still references the lvol_store/bs_dev */
	int  teardown_status; /* bs_dev destroy_cb status (flusher drain, WAL
			       * close). Non-zero: acknowledged writes may be
			       * only local. */
	int  quiesce_status;  /* writers of this lvstore outside the bs_dev
			       * (pending-delete registry PUTs) drained;
			       * non-zero: not proven */
};

static inline enum lvs_owner_action
lvs_owner_decide(const struct lvs_owner_teardown *t)
{
	if (!t->owner_held) {
		return LVS_OWNER_NOT_HELD;
	}
	if (t->resources_live || t->teardown_status != 0 ||
	    t->quiesce_status != 0) {
		return LVS_OWNER_RETAIN;
	}
	return LVS_OWNER_RELEASE;
}

/* Status an unload reports. The first failure wins, in the order the steps
 * ran. A release failure is never hidden behind a successful local teardown:
 * the caller must not believe the marker is free when it may not be. */
static inline int
lvs_unload_final_status(int lvs_status, int teardown_status,
			int quiesce_status, int release_status)
{
	if (lvs_status != 0) {
		return lvs_status;
	}
	if (teardown_status != 0) {
		return teardown_status;
	}
	if (quiesce_status != 0) {
		return quiesce_status;
	}
	return release_status;
}

/* Destroy never deletes the owner key directly, in either mode.
 *
 * It keeps the claim through every delete and gives it back LAST, through the
 * same owner protocol an unload uses (strict: conditional PUT of a free
 * tombstone; legacy: the legacy release, which deletes the marker). Deleting
 * the key as one more object would either drop the claim before the deletes
 * finished or, after a release, remove a held marker that a new owner wrote
 * in between. Kept as a function so the unit test pins the decision. */
static inline bool
lvs_destroy_deletes_owner_key(bool strict_mode)
{
	(void)strict_mode;
	return false;
}

/* May destroy issue ANY delete?
 *
 * Only when the unload completed cleanly (status 0: blobstore unloaded, the
 * bs_dev teardown drained and closed cleanly, the registry writer quiesced)
 * AND the unload handed this destroy the exclusive claim instead of releasing
 * it. Anything else means ownership or teardown is unproven: the lvstore may
 * still be loaded, acknowledged writes may exist only in the local WAL, or the
 * marker may no longer be ours. No delete is issued in any of those cases. */
static inline bool
lvs_destroy_may_delete(int unload_status, bool claim_transferred)
{
	return unload_status == 0 && claim_transferred;
}

/* May destroy release the claim it carried through the deletes?
 *
 * Only once every delete succeeded (404 counts as success), every key was
 * recorded, and the post-delete audit proved the recovery descriptor and the
 * checkpoint gone. Any failure or ambiguous outcome keeps the claim, so no
 * other process can acquire the prefix while objects of this lvstore may
 * remain. */
static inline bool
lvs_destroy_may_release(bool claim_transferred, int delete_status,
			bool lost_keys, int audit_status)
{
	return claim_transferred && delete_status == 0 && !lost_keys &&
	       audit_status == 0;
}

/* Status a destroy reports: the first failure in the order the steps ran.
 * 0 only when the unload, every delete, the audit AND the release succeeded.
 * An unload that completed without handing over a claim is -EPERM: the data
 * is intact, but the destroy did not happen. */
static inline int
lvs_destroy_final_status(int unload_status, bool claim_transferred,
			 int delete_status, bool lost_keys, int audit_status,
			 int release_status)
{
	if (unload_status != 0) {
		return unload_status;
	}
	if (!claim_transferred) {
		return -EPERM;
	}
	if (delete_status != 0) {
		return delete_status;
	}
	if (lost_keys) {
		return -ENOMEM;
	}
	if (audit_status != 0) {
		return audit_status;
	}
	return release_status;
}

/* Operator guidance when the marker is retained. \c strict_mode selects the
 * text; strict mode has no force and no takeover, so it must never suggest
 * either. \c destroying: the retention happened inside a destroy. */
static inline const char *
lvs_retained_guidance(int strict_mode, bool destroying)
{
	if (strict_mode) {
		return destroying ?
		       "The marker stays held so no other process can create or "
		       "attach this prefix while objects of the destroyed lvstore "
		       "may remain. There is no force and no in-band takeover: an "
		       "operator must verify this process no longer uses the "
		       "lvstore and finish or clean up the prefix out of band "
		       "before clearing the marker" :
		       "There is no force and no in-band takeover, and a later "
		       "attach is refused while the marker is held. Keep this "
		       "device (its WAL may hold acknowledged writes); an operator "
		       "must verify no writer remains and clear the marker out of "
		       "band, then attach this same device to replay it";
	}
	return destroying ?
	       "The legacy marker stays; a later create or attach of this name "
	       "needs a verified force=true after checking no writer remains" :
	       "A later attach of this device needs a verified force=true after "
	       "checking no other writer remains; attach this same device so its "
	       "WAL is replayed";
}

#endif /* S3LVOL_OWNER_LIFECYCLE_CTL_H */
