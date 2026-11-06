/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   Same-process retained strict owner claims.
 *
 *   === The defect this closes ===
 *
 *   An unload (or a destroy's unload) whose pre-drain succeeded can still fail
 *   AFTER blobstore unloaded: the bs_dev teardown drains the flusher again and
 *   retires it, and with the object store down that final drain times out.
 *   lvs_owner_decide() then says RETAIN -- correctly: acknowledged writes may
 *   exist only in the local WAL, so the S3 owner marker must stay held. But
 *   the in-memory lvstore was then freed, and with it the strict claim (nonce
 *   and ETag). The marker named this process forever, the only object that
 *   could release or re-use it was gone, and a later attach of the very same
 *   device in the very same process was refused with -EBUSY like any stranger.
 *   The WAL holding the acknowledged writes could not be replayed without an
 *   out-of-band operator action.
 *
 *   === What is kept, and when ===
 *
 *   At the instant the unload decides RETAIN after a post-drain teardown
 *   failure, and only if every former local writer of the lvstore has stopped
 *   (blobstore unloaded, bs_dev gone -- flusher retired, WAL closed -- and the
 *   pending-delete registry quiesced), a record is kept with:
 *
 *     - the strict claim exactly as acquired (nonce + ETag) and its caps;
 *     - the target it is bound to (endpoint, bucket, lvstore name / prefix);
 *     - the lvstore uuid;
 *     - the local device identity: WAL bdev name + bdev uuid, and the cache
 *       bdev name + uuid when it is a separate device.
 *
 *   The record holds copies only, never a pointer to the freed lvstore, the
 *   local device or any SPDK object.
 *
 *   === How it may be used ===
 *
 *   Only by an attach in THIS process, of the SAME device (name and bdev uuid),
 *   whose super block names the same lvstore name and uuid, against the same
 *   endpoint/bucket/prefix, and only after a read of the remote marker proves
 *   it is still the unchanged held document of this claim (nonce AND ETag).
 *   Then the attach proceeds with the retained claim instead of acquiring a
 *   new one, replays the local WAL, and the claim is released by the next
 *   clean unload through the ordinary strict release.
 *
 *   It is never: a force, a takeover, a weaker normal acquire (matching
 *   hostname/pid is not authority), a cross-process restart recovery, or a
 *   marker write. Create and remote recovery of a name with a retained record
 *   are refused locally.
 *
 *   === Exactly-once ===
 *
 *   take() marks the record busy before any asynchronous step, so a second
 *   concurrent attach is refused with -EBUSY. A failed attach puts the record
 *   back unchanged (busy cleared, failure counted); the claim is never
 *   released by a failed retained attach. Only a fully successful attach
 *   consumes (removes and frees) the record, at the moment the claim moves
 *   into the live lvstore -- so it is never held in two places.
 *
 *   Header-only so the bookkeeping runs unchanged under ASan/UBSan in a unit
 *   test (test/integration/retained_claim_ctl_test.c).
 */

#ifndef S3LVOL_RETAINED_CLAIM_CTL_H
#define S3LVOL_RETAINED_CLAIM_CTL_H

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>

#include "spdk/uuid.h"

#include "s3lvol/s3_owner.h"

/* Comfortably above SPDK's practical bdev name lengths; a longer name is
 * refused rather than truncated, since a truncated name could compare equal to
 * a different device. */
#define LVS_RETAINED_BDEV_NAME_MAX 256

struct lvs_retained_ident {
	char             lvs_name[S3_OWNER_LVS_NAME_MAX];
	char             endpoint[S3_OWNER_ENDPOINT_MAX];
	char             bucket[S3_OWNER_BUCKET_MAX];
	struct spdk_uuid lvs_uuid;

	char             wal_bdev[LVS_RETAINED_BDEV_NAME_MAX];
	struct spdk_uuid wal_bdev_uuid;

	/* Empty name: single-bdev layout (cache shares the WAL device). */
	char             cache_bdev[LVS_RETAINED_BDEV_NAME_MAX];
	struct spdk_uuid cache_bdev_uuid;
};

struct lvs_retained {
	struct lvs_retained_ident ident;
	struct s3_owner_caps      caps;
	struct s3_owner_claim     claim;

	/* An attach currently owns this record. */
	bool                      busy;
	/* Retained attaches that failed and put the record back. */
	uint32_t                  failed_attempts;

	TAILQ_ENTRY(lvs_retained) link;
};

TAILQ_HEAD(lvs_retained_list, lvs_retained);

/* Copy into a fixed buffer; -ENAMETOOLONG instead of truncating. */
static inline int
lvs_retained_copy(char *dst, size_t dst_len, const char *src)
{
	size_t n;

	if (!src) {
		return -EINVAL;
	}
	n = strlen(src);
	if (n >= dst_len) {
		return -ENAMETOOLONG;
	}
	memcpy(dst, src, n + 1);
	return 0;
}

/* A claim worth retaining: strict, acquired, with the ETag a conditional
 * release needs and a non-null nonce. */
static inline bool
lvs_retained_claim_usable(const struct s3_owner_claim *claim)
{
	return claim && claim->valid && claim->etag[0] != '\0' &&
	       !spdk_uuid_is_null(&claim->info.nonce);
}

/* Every identity field a record must carry. */
static inline bool
lvs_retained_ident_complete(const struct lvs_retained_ident *id)
{
	return id->lvs_name[0] != '\0' && id->endpoint[0] != '\0' &&
	       id->bucket[0] != '\0' && !spdk_uuid_is_null(&id->lvs_uuid) &&
	       id->wal_bdev[0] != '\0' && !spdk_uuid_is_null(&id->wal_bdev_uuid) &&
	       (id->cache_bdev[0] == '\0' ||
		!spdk_uuid_is_null(&id->cache_bdev_uuid));
}

/* Should the unload keep a record instead of dropping the claim?
 *
 * Only for a strict claim that is still held, after a teardown failure, and
 * only once every former local writer has stopped: blobstore unloaded, the
 * bs_dev gone (flusher retired, WAL closed by its teardown), and the registry
 * writer quiesced. A quiesce failure means a writer may remain, so nothing
 * here may make the claim usable again; that stays an operator decision. */
static inline bool
lvs_retained_eligible(bool strict, bool owner_held, bool lvs_done,
		      bool bs_dev_done, bool quiesce_done, int quiesce_status,
		      int teardown_status)
{
	return strict && owner_held && lvs_done && bs_dev_done && quiesce_done &&
	       quiesce_status == 0 && teardown_status != 0;
}

/* Compare what an attach found with what the record retained.
 *
 *   0             same target, same lvstore, same device(s)
 *   -EINVAL       incomplete identity on either side
 *   -EXDEV        different endpoint, bucket or lvstore name (prefix)
 *   -ESTALE       different lvstore uuid (a recreated lvstore of that name)
 *   -ENODEV       different WAL or cache device (name or bdev uuid), or a
 *                 different single/dual layout
 */
static inline int
lvs_retained_ident_check(const struct lvs_retained_ident *want,
			 const struct lvs_retained_ident *got)
{
	if (!want || !got || !lvs_retained_ident_complete(want) ||
	    !lvs_retained_ident_complete(got)) {
		return -EINVAL;
	}
	if (strcmp(want->lvs_name, got->lvs_name) != 0 ||
	    strcmp(want->endpoint, got->endpoint) != 0 ||
	    strcmp(want->bucket, got->bucket) != 0) {
		return -EXDEV;
	}
	if (spdk_uuid_compare(&want->lvs_uuid, &got->lvs_uuid) != 0) {
		return -ESTALE;
	}
	if (strcmp(want->wal_bdev, got->wal_bdev) != 0 ||
	    spdk_uuid_compare(&want->wal_bdev_uuid, &got->wal_bdev_uuid) != 0) {
		return -ENODEV;
	}
	if (strcmp(want->cache_bdev, got->cache_bdev) != 0) {
		return -ENODEV;
	}
	if (want->cache_bdev[0] != '\0' &&
	    spdk_uuid_compare(&want->cache_bdev_uuid, &got->cache_bdev_uuid) != 0) {
		return -ENODEV;
	}
	return 0;
}

static inline struct lvs_retained *
lvs_retained_find(struct lvs_retained_list *list, const char *lvs_name)
{
	struct lvs_retained *r;

	if (!lvs_name) {
		return NULL;
	}
	TAILQ_FOREACH(r, list, link) {
		if (strcmp(r->ident.lvs_name, lvs_name) == 0) {
			return r;
		}
	}
	return NULL;
}

/* Keep a claim. -EINVAL: not usable / incomplete identity; -EEXIST: a record
 * for this name already exists (never overwritten); -ENOMEM. */
static inline int
lvs_retained_add(struct lvs_retained_list *list,
		 const struct lvs_retained_ident *ident,
		 const struct s3_owner_caps *caps,
		 const struct s3_owner_claim *claim)
{
	struct lvs_retained *r;

	if (!ident || !caps || !lvs_retained_claim_usable(claim) ||
	    !lvs_retained_ident_complete(ident)) {
		return -EINVAL;
	}
	if (lvs_retained_find(list, ident->lvs_name)) {
		return -EEXIST;
	}
	r = calloc(1, sizeof(*r));
	if (!r) {
		return -ENOMEM;
	}
	r->ident = *ident;
	r->caps = *caps;
	r->claim = *claim;
	TAILQ_INSERT_TAIL(list, r, link);
	return 0;
}

/* Begin a retained attach: -ENOENT no record, -EBUSY another attach owns it.
 * On 0 the record is busy and *out points at it until put_back or consume. */
static inline int
lvs_retained_take(struct lvs_retained_list *list, const char *lvs_name,
		  struct lvs_retained **out)
{
	struct lvs_retained *r = lvs_retained_find(list, lvs_name);

	*out = NULL;
	if (!r) {
		return -ENOENT;
	}
	if (r->busy) {
		return -EBUSY;
	}
	r->busy = true;
	*out = r;
	return 0;
}

/* A retained attach failed. The record and its claim are unchanged and usable
 * by a later retry. -EINVAL if the record was not taken. */
static inline int
lvs_retained_put_back(struct lvs_retained *r)
{
	if (!r || !r->busy) {
		return -EINVAL;
	}
	r->busy = false;
	r->failed_attempts++;
	return 0;
}

/* A retained attach succeeded: the claim now lives in the lvstore. Remove and
 * free the record. -EINVAL if it was not taken (or not in this list). */
static inline int
lvs_retained_consume(struct lvs_retained_list *list, struct lvs_retained *r)
{
	struct lvs_retained *it;

	if (!r || !r->busy) {
		return -EINVAL;
	}
	TAILQ_FOREACH(it, list, link) {
		if (it == r) {
			TAILQ_REMOVE(list, r, link);
			free(r);
			return 0;
		}
	}
	return -EINVAL;
}

/* Free every record that is not busy; returns the number freed. */
static inline unsigned
lvs_retained_clear_idle(struct lvs_retained_list *list)
{
	struct lvs_retained *r, *tmp;
	unsigned n = 0;

	/* Open-coded safe iteration: glibc's sys/queue.h has no
	 * TAILQ_FOREACH_SAFE, and this header must build without SPDK's. */
	for (r = TAILQ_FIRST(list); r != NULL; r = tmp) {
		tmp = TAILQ_NEXT(r, link);
		if (!r->busy) {
			TAILQ_REMOVE(list, r, link);
			free(r);
			n++;
		}
	}
	return n;
}

#endif /* S3LVOL_RETAINED_CLAIM_CTL_H */
