/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */

/**
 * \file
 * s3_owner -- the lvstore occupancy marker
 *
 * An lvstore can have exactly one owner at any time. This module turns that
 * constraint into something checkable by means of a single object on S3,
 * `<lvs_name>/meta/owner`.
 *
 * === Why it is needed ===
 *
 * Since attach exists, the same lvstore can be attached twice -- two processes
 * each running their own flusher, overwriting each other on the same keys. That
 * does not error, it only **silently corrupts data**: the last writer of each
 * chunk wins, while both chunk maps believe they are right.
 *
 * Before attach existed this could not happen, so the check is the debt attach
 * brought with it.
 *
 * === Deliberately no lease ===
 *
 * The old 30 s renewal scheme has been removed. Do not "helpfully" add it
 * back:
 *
 *   1. Judging liveness by timestamp necessarily misjudges. A process that only
 *      stalled (long GC, network jitter) is declared dead while it is still
 *      writing -- which manufactures real concurrent writes, worse than not
 *      checking at all.
 *   2. A lease without fencing does not stop split brain. The old owner does
 *      not know its lease expired and keeps writing. Real safety needs a fence
 *      token, and the design principle is "one owner per lvstore, forever; do
 *      not invent lease/fence".
 *
 * So a marker left behind by a crash **deliberately does not expire on its
 * own**. Rather than guessing liveness by timeout, a human confirms explicitly
 * and clears the marker out of band (see "Crash-marker recovery" below).
 *
 * === Two modes, and what each one actually guarantees ===
 *
 * **strict** (the default, `S3LVOL_OWNER_MODE` unset, empty or `strict`;
 * on-object format v2):
 *
 *   The owner key is never deleted by strict code. It holds either a v2
 *   "held" document (nonce N of the claim) or a v2 "free" tombstone (fresh
 *   nonce R per release). Acquisition is `PUT If-None-Match: *` when the key
 *   is absent, or `PUT If-Match: <etag of the free tombstone>` (CAS) when it
 *   is free; a held or non-v2 document is -EBUSY with no write. If the
 *   backend honours the conditions, exactly one of two racing processes
 *   wins. A read-back then confirms the nonce and captures the claim ETag.
 *   Release is `PUT If-Match: <claim etag>` replacing held(N) with free(R),
 *   after a nonce check, so a retried or stale release cannot replace a
 *   marker that a newer owner wrote (ABA): every version carries a fresh
 *   random nonce, so ETags never repeat.
 *
 *   Strict v1 used `DELETE If-Match` for release. The pinned local MinIO
 *   (RELEASE.2025-09-07T16-13-09Z) was measured to ignore If-Match on DELETE
 *   while enforcing it on GET and PUT, so v2 does not depend on conditional
 *   DELETE at all.
 *
 *   Because a backend may accept and *ignore* conditional headers (Tencent
 *   COS ignores `If-None-Match`), strict mode refuses to run until a
 *   capability probe at a unique probe key has proved, against this
 *   endpoint/bucket/lvstore, that (a) a duplicate `If-None-Match: *` PUT is
 *   rejected with the original bytes unchanged, (b) a wrong-ETag and a
 *   stale-ETag `If-Match` PUT are rejected with bytes and ETag unchanged, and
 *   (c) a correct-ETag `If-Match` PUT is applied and changes the ETag.
 *   Anything else is `-ENOTSUP` **before** an ownership write, never a
 *   silent fallback to legacy.
 *
 *   Mixed versions: a legacy marker or a strict v1 marker is not a v2
 *   document and is treated as held by an unknown writer (-EBUSY). Strict v2
 *   never writes over it. A legacy or v1 writer, however, does not respect a
 *   v2 marker (legacy overwrites unconditionally; v1 would see an existing
 *   key as occupied, which is safe). Exclusive strict v2 on every writer is
 *   an operational precondition, not something this code can enforce.
 *
 * **legacy** (`S3LVOL_OWNER_MODE=legacy`, on-object format v1):
 *
 *   Two processes that acquire at the same time can both write a marker and
 *   both read their own nonce back. This guards against sequential
 *   misoperations (another live process, a crash leftover), not against
 *   concurrent scheduling. It is retained so an older deployment keeps
 *   working and so a legacy marker can still be decoded and reported. It is
 *   not the default, and the create/attach dispatch refuses an explicit
 *   legacy before any ownership or destructive mutation (no silent fallback).
 *   It must not be used as the basis of a recovery decision.
 *
 * The probe is evidence about an endpoint at a point in time, not a proof
 * about future behaviour: an endpoint or proxy change that stops enforcing the
 * headers can invalidate it. The capability record is bound to the endpoint,
 * bucket and lvstore name it was measured on, and an operator must re-enable
 * strict mode after such a change. Strict mode also only fences processes that
 * also use strict mode: a mixed deployment (legacy writers and strict writers
 * on one prefix) is not safe, which is why the operational cutover requires
 * all writers upgraded and exclusively strict before recovery is enabled.
 *
 * Neither mode replaces the process-local SPDK claim, and neither makes the
 * marker a lease. A crashed strict owner leaves a marker that denies everyone
 * until an operator removes it out of band, after ensuring the old writer is
 * stopped. There is deliberately no force token in the strict API: a force
 * override is itself a race.
 *
 * === Graceful handover, and what it is not ===
 *
 * A graceful two-process handover uses the existing flow: the retiring owner
 * drains and unloads cleanly, which releases the marker to a free tombstone
 * (strict) or deletes it (legacy); the successor then acquires normally. No
 * ceremonial handover API is needed: the free tombstone plus a fresh acquire
 * is the handover.
 *
 * This is cooperative retirement of a live owner. It is NOT:
 *   - local disk loss (the successor still needs the same local device),
 *   - cross-node VM migration, or
 *   - crash takeover (a crashed owner leaves a held marker that denies
 *     everyone until it is cleared out of band).
 *
 * === Crash-marker recovery (operator, out of band) ===
 *
 * A crashed strict owner leaves a v2 held document. Recovery is an operator
 * action, not an API option:
 *   1. Ensure the old writer is stopped (process gone, not merely stalled).
 *   2. Confirm no other writer is using the prefix.
 *   3. Clear the marker out of band (delete the owner key, or replace it with
 *      a free tombstone) so the next acquire can proceed.
 *
 * Deleting the key is sufficient against a stopped writer. It is NOT a fence
 * against a backend that may still apply a late write from the old owner:
 * for that, the old writer must be fenced (its requests rejected or drained)
 * before the marker is cleared. Until enforceable old-writer/request fencing
 * is demonstrated, crash recovery remains an operator prerequisite, not a
 * property of this module.
 */

#ifndef S3LVOL_S3_OWNER_H
#define S3LVOL_S3_OWNER_H

#include "spdk/stdinc.h"
#include "spdk/uuid.h"

#include "s3lvol/s3_client.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Upper bound for a host name. Big enough for Linux HOST_NAME_MAX (64). */
#define S3_OWNER_NODE_ID_MAX    64

/* Upper bound for the owner document. The real content is about 200 bytes; this
 * leaves generous headroom. It is also the length of the range GET used to read
 * the document, so it caps how much can be read back. */
#define S3_OWNER_DOC_MAX        1024

/**
 * The marker's content. Handed to the caller on a conflict, to put "who holds
 * it" into the error message -- otherwise an operator sees only -EBUSY and
 * cannot tell whom to go find.
 */
struct s3_owner_info {
	char     node_id[S3_OWNER_NODE_ID_MAX];
	int32_t  pid;

	/* Unix timestamp (seconds) of the acquire. **For display only**; it
	 * takes part in no liveness decision -- see "Deliberately no lease"
	 * in the file header. */
	uint64_t attach_ts;

	/* Which lvstore the holder believes it is attached to. Only has a true
	 * value after create, so it may be all zeroes. */
	struct spdk_uuid lvs_uuid;

	/* Generated randomly on every acquire, to recognise "this marker is
	 * mine" after writing. Not (node_id, pid): pids get reused. */
	struct spdk_uuid nonce;
};

/**
 * \param holder  non-NULL only when status == -EBUSY and the other side's info
 *                parsed successfully
 */
typedef void (*s3_owner_acquire_cb)(void *cb_arg,
				    const struct s3_owner_info *holder,
				    int status);

typedef void (*s3_owner_cb)(void *cb_arg, int status);

/**
 * Claim ownership of \c lvs_name.
 *
 * Flow: GET the existing marker -> if absent, PUT ours -> read it back and
 * verify the nonce.
 *
 * When a marker exists and \c force is false, the callback gets -EBUSY along
 * with the holder's info. With \c force true it is overwritten, and the
 * displaced holder is written to the log (not silently discarded --
 * overwriting somebody's marker is an action that must leave a trace).
 *
 * \param lvs_uuid  the lvstore uuid to record in the marker; pass all-zeroes
 *                  while create has not finished
 *
 * Asynchronous. The callback **may fire before this function returns** (param
 * validation or submission failure).
 */
void s3_owner_acquire(struct s3_client *client, const char *lvs_name,
		      const struct spdk_uuid *lvs_uuid, bool force,
		      s3_owner_acquire_cb cb_fn, void *cb_arg);

/**
 * Release ownership (delete the marker). Called on a clean unload so the next
 * attach does not need --force.
 *
 * Failure **must not block the unload**: a leftover marker only means the next
 * attach needs --force, and the unload is already done -- failing it would buy
 * nothing.
 */
void s3_owner_release(struct s3_client *client, const char *lvs_name,
		      s3_owner_cb cb_fn, void *cb_arg);

/* ==========================================================================
 * Strict ownership mode
 * ========================================================================== */

/* Environment variable selecting the mode. Only `legacy` and `strict` are
 * accepted; anything else is a configuration error and every ownership
 * operation refuses rather than guessing. Unset or empty means strict. */
#define S3_OWNER_MODE_ENV        "S3LVOL_OWNER_MODE"

enum s3_owner_mode {
	S3_OWNER_MODE_INVALID = -1,
	S3_OWNER_MODE_LEGACY  = 0,
	S3_OWNER_MODE_STRICT  = 1,
};

/* Read once per process and cached. Returns INVALID for an unrecognised
 * value (and logs it); no other value is treated as an alias for unsafe. */
enum s3_owner_mode s3_owner_mode_from_env(void);

#define S3_OWNER_ENDPOINT_MAX    256
#define S3_OWNER_BUCKET_MAX      256
#define S3_OWNER_LVS_NAME_MAX    128
#define S3_OWNER_ETAG_MAX        128
#define S3_OWNER_PROBE_KEY_MAX   320

/**
 * The result of the strict-mode capability probe against one endpoint, bucket
 * and lvstore prefix.
 *
 * \c probed plus both capability bits must all be true before any strict
 * acquire or release is attempted. An all-zero structure means "not probed"
 * and is not accepted.
 */
struct s3_owner_caps {
	bool     probed;
	bool     atomic_create;       /* PUT If-None-Match rejected a duplicate */
	bool     cas_replace;         /* PUT If-Match: wrong/stale rejected,
				       * correct applied with a new ETag */

	char     endpoint[S3_OWNER_ENDPOINT_MAX];
	char     bucket[S3_OWNER_BUCKET_MAX];
	char     lvs_name[S3_OWNER_LVS_NAME_MAX];

	uint64_t probed_at;
};

/**
 * An acquired strict claim. The ETag is required for the conditional release;
 * a claim without one is refused by the release path rather than falling back
 * to an unconditional write or delete.
 */
struct s3_owner_claim {
	struct s3_owner_info info;
	char                 etag[S3_OWNER_ETAG_MAX];
	bool                 valid;
};

/**
 * Strict acquire callback. \c claim is non-NULL only on success.
 */
typedef void (*s3_owner_claim_cb)(void *cb_arg,
				  const struct s3_owner_info *self,
				  const struct s3_owner_claim *claim,
				  int status);

/**
 * Probe whether this endpoint/bucket/lvstore honours conditional create and
 * compare-and-swap replace, and fill \c caps on success.
 *
 * The probe writes only to a unique key under `<lvs_name>/meta/`, never to the
 * owner marker. Steps:
 *
 *   1. PUT `If-None-Match: *` (A)            -> must succeed
 *   2. PUT `If-None-Match: *` (B)            -> must be 412/409
 *   3. GET                                   -> A, ETag EA
 *   4. PUT `If-Match: <wrong>` (C)           -> must be 412/409
 *   5. GET                                   -> still A, still EA
 *   6. PUT `If-Match: EA` (D)                -> must succeed
 *   7. GET                                   -> D, ETag ED != EA
 *   8. PUT `If-Match: EA` (E, stale)         -> must be 412/409
 *   9. GET                                   -> still D, still ED
 *  10. DELETE the unique probe key (unconditional; only this probe wrote it)
 *
 * Any accepted write that should have been rejected, or any byte/ETag change
 * after a rejection, is \c -ENOTSUP. Other failures are the underlying error.
 * Conditional DELETE is deliberately not probed and not used.
 *
 * Asynchronous, single completion.
 */
void s3_owner_probe(struct s3_client *client, const char *lvs_name,
		    struct s3_owner_caps *caps, s3_owner_cb cb_fn, void *cb_arg);

/**
 * Validate a capability record against the client and lvstore it is about.
 */
bool s3_owner_caps_valid(const struct s3_owner_caps *caps,
			 const struct s3_client *client, const char *lvs_name);

/**
 * Claim ownership atomically.
 *
 * There is no `force` parameter. Whatever the caller wants, strict mode
 * refuses to displace an existing marker: taking over a crashed owner is an
 * out-of-band operator action (stop the old writer, delete the marker), not an
 * API option.
 *
 * If the response to the conditional PUT is lost, the implementation reads the
 * marker back and proceeds only when the nonce is ours; a read that cannot
 * prove ownership is `-EIO`, never "probably mine".
 *
 * Requires caps that passed s3_owner_caps_valid(); otherwise `-ENOTSUP`
 * completes synchronously and no write is submitted.
 */
void s3_owner_acquire_strict(struct s3_client *client, const char *lvs_name,
			     const struct spdk_uuid *lvs_uuid,
			     const struct s3_owner_caps *caps,
			     s3_owner_claim_cb cb_fn, void *cb_arg);

/**
 * Release only the exact claim that was acquired.
 *
 * Reads the marker, requires it to be a v2 held document with this claim's
 * nonce and ETag, then replaces it with a fresh free tombstone via
 * `PUT If-Match: <claim etag>`. A marker that changed (including a new owner
 * after an ABA interleaving) is `-EBUSY` and is not written. If the CAS
 * acknowledgement is lost, the marker is read back: our tombstone (or any
 * state that no longer holds this claim) is 0; the unchanged held claim is
 * `-EIO` (not applied, claim still valid); an unreadable marker is `-EIO`.
 * The owner key is never deleted.
 *
 * Requires caps that passed s3_owner_caps_valid(); otherwise `-ENOTSUP`
 * completes synchronously and no request is submitted.
 */
void s3_owner_release_strict(struct s3_client *client, const char *lvs_name,
			     const struct s3_owner_claim *claim,
			     const struct s3_owner_caps *caps,
			     s3_owner_cb cb_fn, void *cb_arg);

/**
 * Verify, read-only, that the owner marker is still exactly \c claim.
 *
 * One GET of the owner key. Success (0) requires ALL of: the marker exists,
 * it is a v2 held document, its nonce is the claim's nonce, its ETag is the
 * claim's ETag, its recorded lvstore uuid is exactly the one the claim wrote
 * (all zeroes for a claim taken on the create path, before the uuid existed),
 * a claim that did record a uuid records \c lvs_uuid (when non-NULL and
 * non-null), and \c caps is valid for this client/bucket/lvstore. It never
 * writes and never deletes anything.
 *
 * Failure, all fail closed and leave the marker and the claim untouched:
 *   -ENOTSUP  caps invalid for this target (no request issued)
 *   -EINVAL   claim not usable (no request issued)
 *   -ENOENT   the marker is absent
 *   -EBUSY    the marker is free, foreign, or held by another nonce/ETag
 *   -ESTALE   held by this claim but naming a different lvstore uuid
 *   other     the read failed (transport, permission, ...)
 *
 * This is the authority check for reusing a claim that this process kept
 * after a failed teardown. It is deliberately not "the marker names my
 * hostname and pid": those are display fields and pids are reused.
 *
 * Asynchronous, single completion; the callback may run before return.
 */
void s3_owner_verify_claim_strict(struct s3_client *client, const char *lvs_name,
				  const struct s3_owner_claim *claim,
				  const struct spdk_uuid *lvs_uuid,
				  const struct s3_owner_caps *caps,
				  s3_owner_cb cb_fn, void *cb_arg);

/**
 * Shared policy check: strict mode refuses `force`. Returns -EINVAL when
 * \c force is true, 0 otherwise. Exposed so the create/attach paths implement
 * the refusal in one place.
 */
int s3_owner_strict_check_force(bool force);

/**
 * Format the holder info into one line, for error messages. \c out is always
 * NUL-terminated.
 */
void s3_owner_info_str(const struct s3_owner_info *info, char *out,
		       size_t out_len);

#ifdef __cplusplus
}
#endif

#endif /* S3LVOL_S3_OWNER_H */
