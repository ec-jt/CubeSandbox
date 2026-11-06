/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   vbdev_s3lvol_lvstore -- the lvstore and lvol lifecycle
 *
 *   Chain: s3_client_get_or_create -> s3_bs_dev_create -> spdk_lvs_init ->
 *   spdk_lvol_create -> vbdev_s3lvol_bdev_register
 *
 *   === Why maintain our own lvstore list ===
 *
 *   The upstream lvstore<->bdev pairing table g_spdk_lvol_pairs is static
 *   private (vbdev_lvol.c); no external API can insert entries, so the
 *   built-in bdev_lvol_* RPCs are unavailable to us. We keep our own copy for
 *   the bespoke RPCs to look up.
 *
 *   === Why not use the upstream vbdev_lvs_create() ===
 *
 *   It assumes the lvstore sits on a real bdev: it builds its own bs_dev with
 *   spdk_bdev_create_bs_dev_ext() and then unconditionally dereferences
 *   bs_dev->get_base_bdev() (vbdev_lvol.c). Our bs_dev has no bdev
 *   underneath; get_base_bdev is NULL and it would segfault. spdk_lvs_init()
 *   itself only needs a struct spdk_bs_dev *, so the wrapper is bypassed and
 *   it is called directly.
 */

#include "spdk/stdinc.h"
#include "spdk/bdev_module.h"
#include "spdk/blob.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/lvol.h"
#include "spdk/string.h"
#include "spdk/thread.h"

#include "spdk_internal/lvolstore.h"

#include "s3lvol/s3_bs_dev.h"
#include "s3lvol/s3_checkpoint.h"
#include "s3lvol/s3_chunk_map.h"
#include "s3lvol/s3_client.h"
#include "s3lvol/s3_export.h"
#include "s3lvol/s3_owner.h"
#include "s3lvol/s3_journal.h"
#include "s3lvol/s3_local_dev.h"
#include "s3lvol/s3_recovery.h"
#include "s3lvol/s3_recovery_finalize.h"
#include "s3lvol/s3_recovery_unwind.h"
#include "s3lvol/s3_types.h"
#include "s3lvol/s3_wal.h"

#include "vbdev_s3lvol.h"
#include "source_control_ticket.h"
#include "owner_lifecycle_ctl.h"
#include "retained_claim_ctl.h"

struct scoped_source;
struct source_gate_entry { struct source_gate_entry *next; struct spdk_uuid uuid; struct s3_producer_gate gate; };
struct s3lvol_lvstore {
	struct scoped_source *scoped_sources;
	struct source_gate_entry *source_gates;
	struct s3_producer_gate producers;
	uint64_t control_inflight; /* owner-thread only */
	struct spdk_thread *source_thread;
	struct spdk_uuid source_operation, source_uuid;
	uint64_t source_generation;
	struct spdk_poller *source_poller;
	uint64_t source_deadline;
	spdk_lvs_op_complete source_cb;
	void *source_arg;
	int source_phase, source_status;
	bool source_callback_pending;
	bool source_pending_loaded;
	struct spdk_lvol_store  *lvs;
	struct spdk_bs_dev      *bs_dev;
	struct s3_client        *client;
	char                    *name;

	/* Copy of lvs->uuid, taken once the blobstore is open.
	 *
	 * Needed because the unload path clears lvs->lvs before
	 * s3lvol_lvstore_free() runs, and the free is where this lvstore's
	 * pending-delete marks are dropped -- reading the uuid off lvs->lvs
	 * there would read NULL and skip the cleanup. Stays all-zero on the
	 * setup error paths, which never got as far as an open blobstore. */
	struct spdk_uuid         uuid;

	/* The namespace this lvstore was created in. An import defaults to the
	 * same namespace, which is the common case. The COS target is resolved
	 * from this name through rcow_namespace_to_target(). */
	char                    *ns_name;

	/* Auto-generated blobstore name (bstore_XXXXXXXX). Used for spdk_lvs_init
	 * and spdk_lvs_load_ext; the user never sees it. */
	char                    *bs_name;

	/* Local device carrying the metadata journal and the WAL.
	 *
	 * All NULL when the lvstore was created without a wal_bdev, which leaves
	 * it in the direct-to-S3 mode: no crash safety and no protection against
	 * concurrent partial-chunk writes. Only useful for smoke testing against
	 * S3 without a local disk. */
	struct s3_local_dev     *local_dev;
	struct s3_journal       *journal;
	struct s3_wal           *wal;

	/* True once<name>/meta/owner in S3 says this process owns the lvstore.
	 *
	 * Tracked rather than assumed because the marker must only be deleted by
	 * whoever actually wrote it. Releasing one we never acquired would clear
	 * another process's claim -- the exact thing this is meant to prevent. */
	bool                    owner_held;

	/* An unload (ordinary or destroy) owns this lvstore's teardown slots: the
	 * bs_dev destroy callback (single slot, overwritten by a second setter)
	 * and, for destroy, the reap callback. A second unload/destroy started
	 * meanwhile would steal them and strand the first one, so it is refused
	 * with -EBUSY. Cleared when an unload fails and the lvstore stays loaded;
	 * a completed unload frees the lvstore. */
	bool                    unload_in_progress;
	bool                    exports_ready;
	/* False until create or a complete attach establishes the dependency view.
	 * Failed opens leave durable blobs outside lvols; never infer completeness
	 * from lvol_count or clear this after an unrelated successful operation. */
	bool                    dependency_view_complete;
	/* Diagnostic values survive caller notification without borrowing ctx. */
	bool                    unload_timeout_reported;
	bool                    owner_release_pending;

	/* Ownership policy selected by S3LVOL_OWNER_MODE. Strict is the default
	 * (unset or empty means strict); an explicit legacy is refused before any
	 * ownership or destructive mutation, and strict refuses to run without a
	 * successful capability probe and never falls back. */
	enum s3_owner_mode      owner_mode;
	struct s3_owner_caps    owner_caps;
	struct s3_owner_claim   owner_claim;

	TAILQ_ENTRY(s3lvol_lvstore) link;
};

struct s3_producer_gate *
s3lvol_source_gate_alloc(struct spdk_lvol *v, struct s3_producer_gate *store_gate)
{
 struct s3lvol_lvstore *lvs;
 struct source_gate_entry *e;
 if(!v || !store_gate)return NULL;
 lvs=SPDK_CONTAINEROF(store_gate, struct s3lvol_lvstore, producers);
 for(e=lvs->source_gates;e;e=e->next)if(!spdk_uuid_compare(&e->uuid,&v->uuid))return &e->gate;
 e=calloc(1,sizeof(*e));if(!e)return NULL;
 e->uuid=v->uuid;e->next=lvs->source_gates;lvs->source_gates=e;return &e->gate;
}

void
s3lvol_source_pending_loaded(struct s3lvol_lvstore *lvs, int status)
{
	lvs->source_pending_loaded = status == 0;
}

struct source_pending_check {
	const struct spdk_uuid *uuid;
	bool found;
};
static void
source_pending_check(void *arg, const struct s3lvol_pending_entry *e)
{
	struct source_pending_check *c = arg;
	if (!spdk_uuid_compare(c->uuid, &e->lvs_uuid)) c->found = true;
}

struct s3_producer_gate *
s3lvol_lvstore_producer_gate(struct s3lvol_lvstore *lvs)
{
	return lvs ? &lvs->producers : NULL;
}

bool
s3lvol_lvstore_source_closed(struct s3lvol_lvstore *lvs)
{
	return lvs && s3_producer_closed(&lvs->producers);
}

int
s3lvol_control_enter(struct s3lvol_lvstore *lvs)
{
	if (!lvs || !lvs->lvs) return -EINVAL;
	if (spdk_get_thread() != lvs->source_thread) return -EXDEV;
	if (lvs->unload_in_progress || s3lvol_lvstore_source_closed(lvs)) return -ESHUTDOWN;
	/* Freeze metadata membership while a scoped source retains native objects.
	 * This also blocks imports admitted after prepare; data writes are gated
	 * separately per volume and remain open until drain. */
	if (s3lvol_scoped_source_held(lvs)) return -EBUSY;
	lvs->control_inflight++;
	return 0;
}

void
s3lvol_control_leave(struct s3lvol_lvstore *lvs, int status)
{
	int zero = 0;
	assert(lvs->control_inflight && spdk_get_thread() == lvs->source_thread);
	if (status) atomic_compare_exchange_strong(&lvs->producers.error, &zero, status);
	lvs->control_inflight--;
}

/* Phases: 0 waiting for accepted I/O, 1 WAL drain, 2 checkpoint publication,
 * 3 complete. Callbacks borrow the retained store, NEVER the waiter's context.
 * The poller alone calls the waiter and detaches it before delivery. */
static void
source_checkpoint_done(void *arg, int status)
{
	struct s3lvol_lvstore *lvs = arg;
	lvs->source_callback_pending = false;
	lvs->source_status = status;
	lvs->source_phase = 3;
}

static void
source_wal_done(void *arg, int status)
{
	struct s3lvol_lvstore *lvs = arg;
	lvs->source_callback_pending = false;
	if (status) {
		lvs->source_status = status;
		lvs->source_phase = 3;
		return;
	}
	lvs->source_phase = 2;
	lvs->source_callback_pending = true;
	s3_bs_dev_checkpoint(lvs->bs_dev, source_checkpoint_done, lvs);
}

static int
source_drain_poll(void *arg)
{
	struct s3lvol_lvstore *lvs = arg;
	spdk_lvs_op_complete cb;
	void *cb_arg;
	int status;

	if (lvs->source_cb && spdk_get_ticks() >= lvs->source_deadline) {
		cb = lvs->source_cb; cb_arg = lvs->source_arg;
		lvs->source_cb = NULL; lvs->source_arg = NULL;
		cb(cb_arg, -ETIMEDOUT);
	}
	if (lvs->source_phase == 0 && !s3_producer_count(&lvs->producers) &&
	    !lvs->control_inflight && !s3_bs_dev_source_busy(lvs->bs_dev) &&
	    !s3lvol_export_work_busy(lvs) && !s3lvol_export_registry_busy(lvs) &&
	    !s3lvol_pending_persist_busy(&lvs->uuid)) {
		status = atomic_load(&lvs->producers.error);
		if (status) {
			lvs->source_status = status; lvs->source_phase = 3;
		} else {
			lvs->source_phase = 1; lvs->source_callback_pending = true;
			s3_bs_dev_drain(lvs->bs_dev, source_wal_done, lvs);
		}
	}
	if (lvs->source_phase != 3 || lvs->source_callback_pending ||
	    s3_bs_dev_source_busy(lvs->bs_dev)) return SPDK_POLLER_IDLE;
	spdk_poller_unregister(&lvs->source_poller);
	cb = lvs->source_cb; cb_arg = lvs->source_arg;
	lvs->source_cb = NULL; lvs->source_arg = NULL;
	if (cb) cb(cb_arg, lvs->source_status);
	return SPDK_POLLER_BUSY;
}

int
s3lvol_source_drain(struct s3lvol_lvstore *lvs,
	const struct s3lvol_source_binding *b, uint64_t timeout_us,
	spdk_lvs_op_complete cb, void *arg)
{
	const char *enabled = getenv("S3LVOL_INTERNAL_SOURCE_DRAIN");
	struct spdk_lvol *v;
	struct s3lvol_export *exp;
	size_t count = 0, i, j;
	bool closed;
	struct source_pending_check pending = {.uuid = lvs ? &lvs->uuid : NULL};
	if (!enabled || strcmp(enabled, "1")) return -ENOTSUP;
	if (!lvs || !lvs->lvs || !b || !cb || !timeout_us || timeout_us > 3600000000ULL ||
	    !b->exclusive_store || !b->volumes || !b->volume_count ||
	    b->volume_count > 65536 || !b->source_generation ||
	    spdk_uuid_is_null(&b->source) || spdk_uuid_is_null(&b->operation) ||
	    !b->endpoint || !b->bucket || !b->prefix) return -EINVAL;
	if (spdk_get_thread() != lvs->source_thread) return -EXDEV;
	if (!lvs->wal || !lvs->local_dev || !lvs->owner_held ||
	    lvs->owner_mode != S3_OWNER_MODE_STRICT || !lvs->owner_claim.valid ||
	    !lvs->dependency_view_complete || !lvs->exports_ready) return -ENOTSUP;
	/* Imports own remote lease renewal and admission callbacks not covered by
	 * this store's write completion accounting. Do not silently omit them. */
	if (s3lvol_import_first(lvs)) return -ENOTSUP;
	if (!lvs->source_pending_loaded) return -EAGAIN;
	s3lvol_pending_foreach(source_pending_check, &pending);
	if (pending.found) return -EBUSY;
	/* Bind the live client, not a mutable namespace configuration. */
	if (strcmp(b->endpoint, s3_client_endpoint(lvs->client)) ||
	    strcmp(b->bucket, s3_client_bucket(lvs->client)) || strcmp(b->prefix, lvs->name) ||
	    spdk_uuid_compare(&b->store_uuid, &lvs->uuid) ||
	    spdk_uuid_compare(&b->owner_nonce, &lvs->owner_claim.info.nonce)) return -ESTALE;
	closed = s3lvol_lvstore_source_closed(lvs);
	if (closed && (spdk_uuid_compare(&b->operation, &lvs->source_operation) ||
	    spdk_uuid_compare(&b->source, &lvs->source_uuid) ||
	    b->source_generation != lvs->source_generation)) return -EEXIST;
	/* Namespace-changing callbacks must finish BEFORE membership is validated.
	 * No partial closure on refusal. Data writers are handled below instead. */
	if (lvs->unload_in_progress || lvs->control_inflight || lvs->source_cb)
		return -EBUSY;
	for (i = 0; i < b->volume_count; i++) {
		if (spdk_uuid_is_null(&b->volumes[i])) return -EINVAL;
		for (j = 0; j < i; j++)
			if (!spdk_uuid_compare(&b->volumes[i], &b->volumes[j])) return -EINVAL;
	}
	TAILQ_FOREACH(v, &lvs->lvs->lvols, link) {
		if (v->action_in_progress || s3lvol_lvol_decouple_pending(v) ||
		    s3lvol_snapshot_pending_test(&lvs->uuid, &v->uuid)) return -EBUSY;
		for (i = 0; i < b->volume_count; i++)
			if (!spdk_uuid_compare(&v->uuid, &b->volumes[i])) break;
		if (i == b->volume_count) return -ESTALE;
		count++;
	}
	if (count != b->volume_count) return -ESTALE;
	/* Existing export work can include destructive rewrites. Refuse rather
	 * than pretend its absence from a transient list proves publication. */
	if (s3lvol_export_work_busy(lvs) || s3lvol_export_registry_busy(lvs) ||
	    s3lvol_pending_persist_busy(&lvs->uuid)) return -EBUSY;
	for (exp = s3lvol_export_first(lvs); exp; exp = s3lvol_export_next(exp)) {
		struct s3lvol_export_entry info;
		s3lvol_export_get(exp, &info);
		if (info.publication_pending || info.reaping) return -EBUSY;
	}
	if (closed && lvs->source_phase == 3 && !lvs->source_poller) {
		cb(arg, lvs->source_status);
		return 0;
	}
	if (!lvs->source_poller) {
		lvs->source_poller = SPDK_POLLER_REGISTER(source_drain_poll, lvs, 1000);
		if (!lvs->source_poller) return -ENOMEM;
	}
	if (!closed) {
		spdk_uuid_copy(&lvs->source_operation, &b->operation);
		spdk_uuid_copy(&lvs->source_uuid, &b->source);
		lvs->source_generation = b->source_generation;
		s3_producer_close(&lvs->producers);
		s3_bs_dev_source_hold(lvs->bs_dev);
	}
	lvs->source_cb = cb; lvs->source_arg = arg;
	lvs->source_deadline = spdk_get_ticks() + timeout_us * spdk_get_ticks_hz() / 1000000;
	return 0;
}

#include "source_scoped.h"

static TAILQ_HEAD(, s3lvol_lvstore) g_lvstores = TAILQ_HEAD_INITIALIZER(g_lvstores);

/* Strict claims kept by THIS process after a post-drain teardown failure, so
 * the same device can be re-attached and its local WAL replayed under the
 * claim that still holds the marker (retained_claim_ctl.h). Copies only; no
 * record points at a freed lvstore or at any SPDK object. */
static struct lvs_retained_list g_retained = TAILQ_HEAD_INITIALIZER(g_retained);

/* State carried across the create *or* attach chain. The lvstore is only
 * published in g_lvstores once blobstore is up.
 *
 * Both chains share this, and with it the whole unwind (lvs_setup_fail and
 * friends). That is not just to save code: the unwind is the part that is easy to
 * get wrong -- what has to be released depends on how far the chain got, and the
 * bs_dev teardown is asynchronous -- and having two copies of it would mean the
 * rarely exercised one silently rots.
 *
 * The caller's opts do not outlive the first callback, so whatever later steps
 * need is copied in here. */
struct lvs_setup_ctx {
	struct s3lvol_lvstore   *lvs;
	s3lvol_lvs_op_cb         cb_fn;
	void                    *cb_arg;

	struct s3_lvs_opts       opts;
	char                    *wal_bdev_name;
	char                    *cache_bdev_name;
	uint64_t                 journal_size;
	uint64_t                 wal_size;

	/* Attach only. */
	bool                     attaching;
	/* Recover only: rebuild from the remote checkpoint onto a blank device.
	 * Implies attaching, because the tail of the chain is shared. */
	bool                     recovering;
	/* Take the lvstore over even if S3 says somebody else holds it.
	 * Attach/create only: recovery refuses force explicitly, because the
	 * marker is not mutual exclusion (see s3_owner.h). Strict mode refuses
	 * it as well: a force override is itself a race. */
	bool                     force;

	/* Strict mode: the uuid recorded in the atomic marker. */
	struct spdk_uuid         owner_uuid;
	bool                     owner_uuid_valid;

	/* Recover only: what the descriptor said, kept so the checkpoint that
	 * arrives can be checked against it. */
	struct spdk_uuid         recover_uuid;
	uint64_t                 recover_gen;
	uint64_t                 recover_lsn;
	uint64_t                 recover_journal_size;
	uint64_t                 recover_wal_size;

	/* Recover only: the checkpoint bytes that passed the preflight, held
	 * until they are applied. Applying these exact bytes (rather than
	 * downloading again) is what makes the preflight mean something. */
	void                    *recover_ckpt;
	uint64_t                 recover_ckpt_len;
	/* Recover only: the exclusive claim on the WAL bdev, held from before the
	 * blank proof until the format has finished. The format consumes the
	 * descriptor it owns, so the device is never unclaimed in between. */
	struct s3_recovery_claim *recover_wal_claim;
	/* Recover only: the claim on the cache bdev, when it is a distinct
	 * device. Both claims are taken before either device is scanned, so a
	 * failure on the second releases the first and nothing is formatted. */
	struct s3_recovery_claim *recover_cache_claim;
	/* True once the WAL bdev's zero scan passed; the cache bdev is scanned
	 * next when it is a different device. */
	bool                     recover_wal_proven_blank;
	/* Journal LSN to replay from, i.e. what the last checkpoint covered. */
	uint64_t                 from_lsn;
	/* uuid recorded on the local device; all-zeroes when the create never got
	 * as far as writing it back. */
	struct spdk_uuid         expect_uuid;
	/* Create only: s3_head() writes the object size here. Unused -- only the
	 * status matters -- but it has to outlive the async call, so it cannot be
	 * a local in lvs_create_prefix_check(). */
	uint64_t                 prefix_probe_size;
	/* Attach only: outstanding spdk_lvol_open() calls, plus one self
	 * reference so an inline completion cannot finish the round early. */
	uint32_t                 lvols_pending;
	uint32_t                 lvols_ok;
	uint32_t                 lvols_failed;

	/* Recover only: the finalization state machine that owns the ordering
	 * from "blobstore loaded, every lvol open" through "bdevs registered".
	 * NULL on the attach and create paths, which keep their own semantics:
	 * attach tolerates a partial lvol failure and registers as it goes. */
	struct s3_recovery_finalize *recover_finalize;
	/* Recover only: set while the validate step is opening lvols. The open
	 * callback must not register a bdev in this mode -- registration is the
	 * publication, and it happens only after the commit and final flush. */
	bool                     recover_validating;
	/* Recover only: the validate step's completion, called once every lvol
	 * open has completed. */
	void                   (*recover_validate_done)(void *arg, int status);
	void                    *recover_validate_arg;
	/* Recover only: the publish step's completion. */
	void                   (*recover_publish_done)(void *arg, int status);
	void                    *recover_publish_arg;

	/* Recover only: set once spdk_lvs_load_ext() has succeeded, i.e. once
	 * lvs->lvs is live. From that point a failure must be torn down by the
	 * ordered unwind (lvols, then the lvol_store, then the bs_dev), not by
	 * the setup funnel, which only knows about the bs_dev. */
	bool                     recover_loaded;
	/* Recover only: the unwind could not finish (a release, the unload, or
	 * the allocation of the release list failed). The lvol_store and the
	 * bs_dev are still reachable from live SPDK objects, so lvs_setup_report()
	 * must not free lvs and must not release the S3 owner marker: doing
	 * either would free memory SPDK still references and advertise the
	 * lvstore as free while it is not. The user's callback still runs with
	 * the failure, so the rebuild is reported as not published. */
	bool                     retain_lvs;

	/* Status of the bs_dev / WAL teardown that ran during the unwind. Non-zero
	 * means acknowledged writes may exist only in the local WAL, so the S3
	 * owner marker is kept rather than released (lvs_owner_decide()). */
	int                      teardown_status;

	/* Recover only: rendezvous of the unwind's spdk_lvs_unload() completion
	 * and the bs_dev destroy callback. The unwind's unload step completes
	 * only when both have arrived, because the bs_dev's background teardown
	 * still writes to S3 and to the local device. */
	bool                     unwind_in_submit;
	bool                     unwind_lvs_done;
	bool                     unwind_bs_dev_done;
	int                      unwind_lvs_status;
	void                   (*unwind_done)(void *arg, int status);
	void                    *unwind_done_arg;

	/* Carried through the unwind: the bs_dev teardown is asynchronous, so the
	 * failure that started it has to be remembered somewhere. */
	int                      status;

	/* Attach only: a retained claim record this attach has taken (busy).
	 * Put back unchanged on any failure, consumed only on success. */
	struct lvs_retained     *retained;
};

/* ==========================================================================
 * Destroy (rcow_delete_lvstore)
 *
 * Unloads the lvstore and then deletes the S3 objects it owned, as opposed to
 * unload, which keeps everything and expects a later attach.
 *
 * The object list is derived rather than listed: s3_list_objects() is still
 * -ENOTSUP, so the data objects come from walking the chunk map (which names
 * every chunk this lvstore has written) and the metadata objects are the four
 * fixed keys, plus one manifest per registered export.
 *
 * What it does *not* cover is orphans -- an object whose mapping never made it
 * into the chunk map, e.g. a create-once delete that failed, or a flush that
 * was cut short before its journal record landed. Those are exactly what GC is
 * for, and until GC exists a destroy can leave some behind. Said plainly here
 * rather than implied, because "delete" reads like it reclaims everything.
 *
 * === Why the unload comes first===
 *
 * The obvious order -- delete, then unload -- is wrong twice over, and the first
 * version of this code had both faults:
 *
 *   - Unload *reads* what it is about to rewrite. Deleting first turns those
 *     metadata reads into 404s, the chunk map is then reported as inconsistent
 *     ("the chunk map entry is wrong"), and spdk_lvs_unload() fails with
 *     -ENOENT. The lvstore is then left in the registry with its data already
 *     gone: unloadable, unattachable, and still listed.
 *
 *   - Unload *writes*. Blobstore flushes its final metadata during unload, those
 *     writes go through create-once like any other, and each one lands as a new
 *     object with a new uuid. Anything enumerated before the unload therefore
 *     misses them -- one leaked object per destroy, every time.
 *
 * So: unload first, take the object list from the final chunk map through
 * s3_bs_dev_set_reap_cb() (the one instant it is both complete and still
 * readable), and delete afterwards. Two consequences follow from the unload
 * having already finished when the deletes are issued:
 *
 *   - The lvstore is freed by then, so nothing here may touch it afterwards.
 *     Everything needed later -- its name, its client, its ownership claim --
 *     is taken up front or handed over, and the client is held through a
 *     reference of our own.
 *
 *   - A failed unload is now recoverable: nothing has been deleted yet, so the
 *     destroy simply reports the failure and leaves the lvstore intact.
 *
 * === Ownership through the deletes ===
 *
 * The unload does NOT release the S3 owner claim for a destroy; it hands it
 * over (struct lvs_owner_handoff) at the point where an ordinary unload would
 * release. The destroy then deletes, in order: (1) the recovery descriptor,
 * alone; (2) the checkpoint, the other fixed metadata, the export manifests and
 * every data object of the final chunk map; (3) audits that the descriptor and
 * the checkpoint are gone (HEAD), so a checkpoint or descriptor written after
 * the list was taken is reported rather than called a complete destroy; and
 * only then (4) releases the claim through the owner protocol. A second
 * process cannot acquire the prefix while any of this runs, so it can never
 * create or attach and then lose its objects to these deletes. Any failure or
 * ambiguous outcome stops before the release: the claim is retained, the
 * bstore.json entry is kept, and the caller gets the error exactly once.
 *
 * The owner key itself is never deleted as an object. Export chunk objects
 * under <name>/exports/<uuid>/ and orphans not named by the chunk map are
 * still left for GC; this is not a general GC.
 * ========================================================================== */

/* Same bound as the one s3_bs_dev.c uses for the keys it builds. */
#define LVS_DESTROY_KEY_MAX 512

/* The S3 claim, handed from the unload to the destroy instead of being
 * released by the unload.
 *
 * Ordinary unload releases the claim once its writers stopped cleanly: the data
 * stays and the next owner is welcome. Destroy cannot do that. Releasing before
 * the deletes would let a second process acquire the free marker, create or
 * attach, and then have its objects deleted by the deletes this process issues
 * afterwards. So destroy asks the unload for the claim instead: at the exact
 * point the unload would release (every writer stopped AND stopped cleanly,
 * lvs_owner_decide() == RELEASE) the claim moves here, with no release and no
 * re-acquire in between. The destroy releases it only after its deletes and the
 * audit succeeded; any failure keeps it. */
struct lvs_owner_handoff {
	bool                  transferred;
	enum s3_owner_mode    mode;
	struct s3_owner_caps  caps;
	struct s3_owner_claim claim;
};

struct lvs_destroy_ctx {
	/* Copies, because the unload frees the lvstore before the deletes run. */
	char                   *lvs_name;
	struct s3_client       *client;

	spdk_lvs_op_complete    cb_fn;
	void                   *cb_arg;

	/* The keys to delete in phase 2, collected in two stages: the fixed
	 * metadata and the export manifests up front (the export registry is
	 * freed with the lvstore), the data objects from the final chunk map
	 * during the unload, i.e. after the final drain. */
	char                  **keys;
	uint32_t                n_keys;
	uint32_t                cap_keys;

	/* Phase 1 key: the recovery descriptor. Deleted first and alone, so an
	 * interrupted destroy never leaves a descriptor that names a checkpoint
	 * whose data is already partly gone. */
	char                    recovery_key[LVS_DESTROY_KEY_MAX];
	char                    checkpoint_key[LVS_DESTROY_KEY_MAX];

	/* Post-delete audit: HEAD of the recovery descriptor and the checkpoint,
	 * so a late write of either (a checkpoint published after the list was
	 * taken) is reported instead of a "complete" destroy. */
	uint64_t                audit_size;
	uint32_t                audit_idx;

	uint32_t                inflight;
	bool                    issued_all;

	/* The claim from the unload. Valid while owner.transferred is true; still
	 * true at the end means the marker is RETAINED and still names this
	 * process. */
	struct lvs_owner_handoff owner;
	/* owner.transferred as it was when the unload completed: whether the
	 * deletes were allowed to start at all. */
	bool                    claimed;

	/* Set once the unload succeeded and the claim was transferred, which is
	 * what makes the deletes safe to issue. */
	bool                    unloaded;

	/* Set when a key could not be recorded: an object no retry of this
	 * destroy will find. Treated as a delete failure (claim retained). */
	bool                    lost_keys;

	int                     unload_status;
	int                     delete_status;
	int                     audit_status;
	int                     release_status;
	bool                    finished;

	/* Test-only stage gate (S3LVOL_RECOVERY_TEST_BUILD). */
	void                  (*gate_next)(struct lvs_destroy_ctx *ctx);
	struct spdk_poller     *gate_poller;
	char                    gate_hold[LVS_DESTROY_KEY_MAX];
};

static void lvs_destroy_finish(struct lvs_destroy_ctx *ctx);
static void lvs_unload_start(struct s3lvol_lvstore *lvs,
			     struct lvs_owner_handoff *handoff,
			     spdk_lvs_op_complete cb_fn, void *cb_arg,
			     spdk_lvs_op_complete timeout_fn);

#ifdef S3LVOL_RECOVERY_TEST_BUILD
/* TEST-ONLY fault injection and stage gates for the destroy ownership tests.
 * Compiled only into the isolated test harness (the production build does not
 * define S3LVOL_RECOVERY_TEST_BUILD and contains none of this).
 *
 *   S3LVOL_TEST_DESTROY_FAULT=unload|delete|release
 *       unload:  the destroy's unload teardown reports -EIO (as a failed
 *                final flusher drain / WAL close would)
 *       delete:  phase-2 deletes are issued for real, then reported -EIO
 *       release: the release is not attempted and is reported -EIO
 *   S3LVOL_TEST_DESTROY_GATE_DIR=<dir>
 *       at stage S ("deletes": after phase 1, before phase 2; "release": after
 *       the audit, before the release), if <dir>/hold-S exists the destroy
 *       writes <dir>/at-S and waits, polling, until hold-S is removed. */
static bool
lvs_test_destroy_fault(const char *what)
{
	const char *v = getenv("S3LVOL_TEST_DESTROY_FAULT");

	return v && strcmp(v, what) == 0;
}

static int
lvs_destroy_gate_poll(void *arg)
{
	struct lvs_destroy_ctx *ctx = arg;

	if (access(ctx->gate_hold, F_OK) == 0) {
		return SPDK_POLLER_IDLE;
	}
	spdk_poller_unregister(&ctx->gate_poller);
	SPDK_NOTICELOG("lvstore '%s': TEST gate %s released\n", ctx->lvs_name,
		       ctx->gate_hold);
	ctx->gate_next(ctx);
	return SPDK_POLLER_BUSY;
}
#endif

/* Continue with \c next, after an optional test-only hold at \c stage. */
static void
lvs_destroy_gate(struct lvs_destroy_ctx *ctx, const char *stage,
		 void (*next)(struct lvs_destroy_ctx *ctx))
{
#ifdef S3LVOL_RECOVERY_TEST_BUILD
	const char *dir = getenv("S3LVOL_TEST_DESTROY_GATE_DIR");
	char at[LVS_DESTROY_KEY_MAX];
	int fd;

	if (dir && dir[0] != '\0') {
		snprintf(ctx->gate_hold, sizeof(ctx->gate_hold), "%s/hold-%s",
			 dir, stage);
		if (access(ctx->gate_hold, F_OK) == 0) {
			snprintf(at, sizeof(at), "%s/at-%s", dir, stage);
			fd = open(at, O_CREAT | O_WRONLY | O_TRUNC, 0600);
			if (fd >= 0) {
				close(fd);
			}
			ctx->gate_next = next;
			ctx->gate_poller = SPDK_POLLER_REGISTER(
						   lvs_destroy_gate_poll, ctx, 50 * 1000);
			if (ctx->gate_poller) {
				SPDK_NOTICELOG("lvstore '%s': TEST gate holding at "
					       "'%s'\n", ctx->lvs_name, stage);
				return;
			}
		}
	}
#else
	(void)stage;
#endif
	next(ctx);
}

/* Record one key for later deletion. */
static int
lvs_destroy_add_key(struct lvs_destroy_ctx *ctx, const char *key)
{
	char *dup;

	if (ctx->n_keys == ctx->cap_keys) {
		uint32_t cap = ctx->cap_keys ? ctx->cap_keys * 2 : 64;
		char **grown = realloc(ctx->keys, cap * sizeof(*grown));

		if (!grown) {
			return -ENOMEM;
		}
		ctx->keys = grown;
		ctx->cap_keys = cap;
	}

	dup = strdup(key);
	if (!dup) {
		return -ENOMEM;
	}
	ctx->keys[ctx->n_keys++] = dup;
	return 0;
}

static void
lvs_destroy_add_key_checked(struct lvs_destroy_ctx *ctx, const char *key)
{
	if (lvs_destroy_add_key(ctx, key) != 0) {
		ctx->lost_keys = true;
	}
}

/* ---- release (last) ---- */

static void
lvs_destroy_released(void *cb_arg, int status)
{
	struct lvs_destroy_ctx *ctx = cb_arg;

	ctx->release_status = status;
	if (status == 0) {
		ctx->owner.transferred = false;
	}
	lvs_destroy_finish(ctx);
}

static void
lvs_destroy_release(struct lvs_destroy_ctx *ctx)
{
#ifdef S3LVOL_RECOVERY_TEST_BUILD
	if (lvs_test_destroy_fault("release")) {
		SPDK_ERRLOG("lvstore '%s': TEST fault: release not attempted, "
			    "reporting -EIO\n", ctx->lvs_name);
		ctx->release_status = -EIO;
		lvs_destroy_finish(ctx);
		return;
	}
#endif
	/* The same release the unload would have issued, with the claim and
	 * caps it handed over and this context's own client reference. Strict:
	 * conditional PUT of a free tombstone against the claim's ETag. Legacy:
	 * the legacy release (DELETE of the marker), which also removes the
	 * owner key the legacy destroy used to delete directly. */
	if (ctx->owner.mode == S3_OWNER_MODE_STRICT) {
		s3_owner_release_strict(ctx->client, ctx->lvs_name,
					&ctx->owner.claim, &ctx->owner.caps,
					lvs_destroy_released, ctx);
		return;
	}
	s3_owner_release(ctx->client, ctx->lvs_name, lvs_destroy_released, ctx);
}

/* ---- audit ---- */

static void lvs_destroy_audit_next(struct lvs_destroy_ctx *ctx);

static void
lvs_destroy_audit_done(void *cb_arg, int status)
{
	struct lvs_destroy_ctx *ctx = cb_arg;
	const char *key = ctx->audit_idx == 0 ? ctx->recovery_key :
			  ctx->checkpoint_key;

	if (status == 0) {
		/* Present again: written after it was deleted (or the delete
		 * did not take). Not a complete destroy. */
		SPDK_ERRLOG("lvstore '%s': %s still exists after the destroy "
			    "deleted it\n", ctx->lvs_name, key);
		if (ctx->audit_status == 0) {
			ctx->audit_status = -EEXIST;
		}
	} else if (status != -ENOENT) {
		SPDK_ERRLOG("lvstore '%s': cannot confirm %s is gone (%s)\n",
			    ctx->lvs_name, key, spdk_strerror(-status));
		if (ctx->audit_status == 0) {
			ctx->audit_status = status;
		}
	}
	ctx->audit_idx++;
	lvs_destroy_audit_next(ctx);
}

static void
lvs_destroy_audit_next(struct lvs_destroy_ctx *ctx)
{
	const char *key;
	int rc;

	if (ctx->audit_idx >= 2) {
		if (!lvs_destroy_may_release(ctx->claimed, ctx->delete_status,
					     ctx->lost_keys, ctx->audit_status)) {
			lvs_destroy_finish(ctx);
			return;
		}
		lvs_destroy_gate(ctx, "release", lvs_destroy_release);
		return;
	}

	key = ctx->audit_idx == 0 ? ctx->recovery_key : ctx->checkpoint_key;
	rc = s3_head(ctx->client, key, &ctx->audit_size,
		     lvs_destroy_audit_done, ctx);
	if (rc != 0) {
		if (ctx->audit_status == 0) {
			ctx->audit_status = rc;
		}
		lvs_destroy_finish(ctx);
	}
}

/* ---- phase 2: everything else ---- */

static void
lvs_destroy_phase2_complete(struct lvs_destroy_ctx *ctx)
{
#ifdef S3LVOL_RECOVERY_TEST_BUILD
	if (lvs_test_destroy_fault("delete") && ctx->delete_status == 0) {
		SPDK_ERRLOG("lvstore '%s': TEST fault: phase-2 deletes reported "
			    "-EIO\n", ctx->lvs_name);
		ctx->delete_status = -EIO;
	}
#endif
	if (ctx->delete_status != 0 || ctx->lost_keys) {
		lvs_destroy_finish(ctx);
		return;
	}
	ctx->audit_idx = 0;
	lvs_destroy_audit_next(ctx);
}

static void
lvs_destroy_delete_done(void *cb_arg, int status)
{
	struct lvs_destroy_ctx *ctx = cb_arg;

	/* 404 is success for DELETE (s3_delete()); any other error, including
	 * an ambiguous transport failure, leaves the object possibly present. */
	if (status != 0 && status != -ENOENT && ctx->delete_status == 0) {
		ctx->delete_status = status;
	}

	assert(ctx->inflight > 0);
	ctx->inflight--;

	if (ctx->issued_all && ctx->inflight == 0) {
		lvs_destroy_phase2_complete(ctx);
	}
}

static void
lvs_destroy_phase2(struct lvs_destroy_ctx *ctx)
{
	uint32_t i;

	if (ctx->n_keys == 0) {
		lvs_destroy_phase2_complete(ctx);
		return;
	}

	/* Not stopped by the first error: the claim is still held, so issuing
	 * the rest cannot touch anybody else's objects, and stopping would only
	 * leave more behind. The claim is retained if anything failed. */
	for (i = 0; i < ctx->n_keys; i++) {
		int rc;

		ctx->inflight++;
		rc = s3_delete(ctx->client, ctx->keys[i],
			       lvs_destroy_delete_done, ctx);
		if (rc != 0) {
			/* Never queued, so no callback is coming for it. */
			SPDK_ERRLOG("lvstore '%s': could not submit DELETE %s: "
				    "%s\n", ctx->lvs_name, ctx->keys[i],
				    spdk_strerror(-rc));
			if (ctx->delete_status == 0) {
				ctx->delete_status = rc;
			}
			ctx->inflight--;
		}
	}

	/* Set last: until every delete is queued, a completion that drains
	 * inflight to zero must not finish the phase. */
	ctx->issued_all = true;
	if (ctx->inflight == 0) {
		lvs_destroy_phase2_complete(ctx);
	}
}

/* ---- phase 1: the recovery descriptor ---- */

static void
lvs_destroy_phase1_done(void *cb_arg, int status)
{
	struct lvs_destroy_ctx *ctx = cb_arg;

	if (status != 0 && status != -ENOENT) {
		SPDK_ERRLOG("lvstore '%s': deleting the recovery descriptor %s "
			    "failed (%s); no other object was deleted\n",
			    ctx->lvs_name, ctx->recovery_key, spdk_strerror(-status));
		ctx->delete_status = status;
		lvs_destroy_finish(ctx);
		return;
	}
	lvs_destroy_gate(ctx, "deletes", lvs_destroy_phase2);
}

/* Notification only: reap and handoff still own this context until retirement.
 * Consume the RPC callback before calling it. No later path may delete after
 * this notification, even if the old writer eventually finishes cleanly. */
static void
lvs_destroy_unload_timeout(void *cb_arg, int status)
{
	struct lvs_destroy_ctx *ctx = cb_arg;
	spdk_lvs_op_complete cb_fn = ctx->cb_fn;
	void *user_arg = ctx->cb_arg;

	ctx->unload_status = status;
	ctx->cb_fn = NULL;
	ctx->cb_arg = NULL;
	if (cb_fn) {
		cb_fn(user_arg, status);
	}
}

static void
lvs_destroy_unload_done(void *cb_arg, int lvserrno)
{
	struct lvs_destroy_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *still = s3lvol_lvstore_find(ctx->lvs_name);
	int rc;

	if (ctx->unload_status != 0) {
		lvserrno = ctx->unload_status;
	}
	ctx->unload_status = lvserrno;
	ctx->claimed = ctx->owner.transferred;

	if (still && still->bs_dev) {
		/* The unload failed before the bs_dev went away, so the reap
		 * callback is still armed with this context. Disarm it: the
		 * context is freed below, and a later teardown must not call
		 * into it. */
		s3_bs_dev_set_reap_cb(still->bs_dev, NULL, NULL);
	}

	if (!lvs_destroy_may_delete(lvserrno, ctx->claimed)) {
		/* Nothing is deleted. Either the lvstore is still loaded and
		 * intact, or it was unloaded locally but its writers did not
		 * stop cleanly (the unload retained the marker and logged why),
		 * or the claim was not this process's to hand over. Deleting in
		 * any of these would destroy data this process cannot prove it
		 * alone owns, or that may exist only in the local WAL. */
		lvs_destroy_finish(ctx);
		return;
	}

	ctx->unloaded = true;

	/* From here only ctx->lvs_name, ctx->client and ctx->owner are valid:
	 * the lvstore is freed. The claim stays held through every delete. */
	rc = s3_delete(ctx->client, ctx->recovery_key, lvs_destroy_phase1_done,
		       ctx);
	if (rc != 0) {
		ctx->delete_status = rc;
		lvs_destroy_finish(ctx);
	}
}

/* Terminal step: report exactly once, drop the client reference and free.
 *
 * The bstore.json entry is removed only on complete success: it is the record
 * that something needs attention whenever the destroy stopped short. */
static void
lvs_destroy_finish(struct lvs_destroy_ctx *ctx)
{
	int status;
	uint32_t i;

	assert(!ctx->finished);
	ctx->finished = true;

	status = lvs_destroy_final_status(ctx->unload_status, ctx->claimed,
					  ctx->delete_status, ctx->lost_keys,
					  ctx->audit_status,
					  ctx->release_status);

	if (status == 0) {
		bstore_remove_entry(ctx->lvs_name);
		SPDK_NOTICELOG("lvstore '%s' destroyed: recovery descriptor and "
			       "%" PRIu32 " objects deleted, owner marker "
			       "released last. Export chunk objects and orphans "
			       "not named by the chunk map are GC's to reclaim\n",
			       ctx->lvs_name, ctx->n_keys);
	} else if (ctx->unload_status != 0) {
		SPDK_ERRLOG("lvstore '%s' not destroyed: the unload failed (%s); "
			    "no object was deleted. See the unload messages above "
			    "for whether it is still loaded and for the owner "
			    "marker state\n", ctx->lvs_name,
			    spdk_strerror(-ctx->unload_status));
	} else if (!ctx->claimed) {
		SPDK_ERRLOG("lvstore '%s' unloaded but NOT destroyed: this "
			    "process held no S3 ownership claim to carry through "
			    "the deletes, so none were issued and its data is "
			    "intact\n", ctx->lvs_name);
	} else {
		/* Numeric statuses: spdk_strerror() returns one shared per-thread
		 * buffer, so several calls in one argument list all print the
		 * last message. */
		SPDK_ERRLOG("lvstore '%s': destroy incomplete (status %d; deletes "
			    "%d%s, audit %d, release %d). %s\n", ctx->lvs_name,
			    status, ctx->delete_status,
			    ctx->lost_keys ? ", some keys unrecorded" : "",
			    ctx->audit_status, ctx->release_status,
			    lvs_retained_guidance(
				    ctx->owner.mode == S3_OWNER_MODE_STRICT, true));
	}
	if (ctx->owner.transferred) {
		SPDK_ERRLOG("lvstore '%s': the S3 owner marker is RETAINED: %s\n",
			    ctx->lvs_name, ctx->release_status != 0 ?
			    "the release failed or its outcome is unknown, so "
			    "the marker may still name this process" :
			    "no release was attempted and the marker still names "
			    "this process");
	}

	if (ctx->cb_fn) {
		ctx->cb_fn(ctx->cb_arg, status);
	}

	for (i = 0; i < ctx->n_keys; i++) {
		free(ctx->keys[i]);
	}
	free(ctx->keys);
	if (ctx->client) {
		s3_client_put(ctx->client);
	}
	free(ctx->lvs_name);
	free(ctx);
}

/* Called from the bs_dev teardown with the final chunk map, just before it is
 * freed, i.e. after the final drain. Only collects: the deletes cannot be
 * issued until the unload has finished, and this runs in the middle of it. */
static int
lvs_destroy_reap_one(void *cb_arg, uint64_t chunk_index,
		     const struct spdk_uuid *uuid, uint32_t valid_bytes,
		     uint32_t flags, uint64_t gen)
{
	struct lvs_destroy_ctx *ctx = cb_arg;
	char key[LVS_DESTROY_KEY_MAX];

	s3_chunk_data_key(ctx->lvs_name, uuid, key, sizeof(key));
	lvs_destroy_add_key_checked(ctx, key);
	return 0;
}

static void
lvs_destroy_reap(void *cb_arg, struct s3_chunk_map *map)
{
	struct lvs_destroy_ctx *ctx = cb_arg;

	if (!map) {
		return;
	}
	s3_chunk_map_foreach(map, lvs_destroy_reap_one, ctx);
}

void
s3lvol_lvstore_destroy(struct s3lvol_lvstore *lvs,
		       spdk_lvs_op_complete cb_fn, void *cb_arg)
{
	/* The owner key is never listed: the claim is released through the
	 * owner protocol, last, after everything below is gone. The recovery
	 * descriptor is phase 1 (ctx->recovery_key), not listed here. */
	static const char * const meta_suffix[] = {
		"checkpoint", "pending-deletes.json", "exports.json",
		"imports.json",
	};
	struct lvs_destroy_ctx *ctx;
	const struct s3_target *target;

	size_t n;
	int rc;

	if (!lvs || !lvs->lvs) {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}
	if (lvs->unload_in_progress || lvs->control_inflight ||
	    s3_producer_count(&lvs->producers) || s3lvol_lvstore_source_closed(lvs) ||
	    s3lvol_scoped_source_held(lvs)) {
		/* Checked before anything is armed: the reap callback below is a
		 * single slot the running unload/destroy may be relying on. */
		SPDK_ERRLOG("lvstore '%s': an unload or destroy is already in "
			    "progress; refusing destroy\n", lvs->name);
		if (cb_fn) {
			cb_fn(cb_arg, -EBUSY);
		}
		return;
	}

	/* Refuse before arming reap, draining, changing the owner, or deleting any
	 * object. Every published export (including dense) owns a durable obligation.
	 * A local reader count or an expired/missing heartbeat cannot discharge it.
	 * No asynchronous gap separates this check from lvs_unload_start(), which
	 * closes admission on the same reactor thread. */
	if (!lvs->exports_ready || s3lvol_export_first(lvs) ||
	    s3lvol_export_work_busy(lvs) || s3lvol_export_registry_busy(lvs)) {
		SPDK_ERRLOG("lvstore '%s': export dependencies are present or unknown; "
			    "destroy refused without changing source state\n", lvs->name);
		if (cb_fn) {
			cb_fn(cb_arg, -EBUSY);
		}
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		if (cb_fn) {
			cb_fn(cb_arg, -ENOMEM);
		}
		return;
	}

	ctx->cb_fn  = cb_fn;
	ctx->cb_arg = cb_arg;
	ctx->lvs_name = strdup(lvs->name);
	if (!ctx->lvs_name) {
		free(ctx);
		if (cb_fn) {
			cb_fn(cb_arg, -ENOMEM);
		}
		return;
	}

	rc = snprintf(ctx->recovery_key, sizeof(ctx->recovery_key),
		      "%s/meta/recovery", lvs->name);
	if (rc < 0 || (size_t)rc >= sizeof(ctx->recovery_key)) {
		rc = -ENAMETOOLONG;
		goto err;
	}
	rc = snprintf(ctx->checkpoint_key, sizeof(ctx->checkpoint_key),
		      S3_CKPT_KEY_FMT, lvs->name);
	if (rc < 0 || (size_t)rc >= sizeof(ctx->checkpoint_key)) {
		rc = -ENAMETOOLONG;
		goto err;
	}

	/* A reference of our own, because the deletes and the release outlive
	 * the lvstore: the unload frees it, and with it its reference to this
	 * same pooled client. Resolved through the namespace while the lvstore
	 * is still here to name it. */
	target = rcow_namespace_to_target(lvs->ns_name);
	if (!target) {
		SPDK_ERRLOG("lvstore '%s': namespace '%s' is no longer "
			    "configured, so its objects cannot be deleted\n",
			    lvs->name, lvs->ns_name ? lvs->ns_name : "(null)");
		rc = -ENODEV;
		goto err;
	}
	rc = s3_client_get_or_create(target, &ctx->client);
	if (rc != 0) {
		SPDK_ERRLOG("lvstore '%s': could not hold an S3 client for the "
			    "deletes: %s\n", lvs->name, spdk_strerror(-rc));
		goto err;
	}

	for (n = 0; n < SPDK_COUNTOF(meta_suffix); n++) {
		char key[LVS_DESTROY_KEY_MAX];

		snprintf(key, sizeof(key), "%s/meta/%s", lvs->name,
			 meta_suffix[n]);
		lvs_destroy_add_key_checked(ctx, key);
	}

	/* The data objects come later, from the final chunk map. Registered before
	 * the unload because that is what triggers it. */
	if (lvs->bs_dev) {
		s3_bs_dev_set_reap_cb(lvs->bs_dev, lvs_destroy_reap, ctx);
	}

	lvs_unload_start(lvs, &ctx->owner, lvs_destroy_unload_done, ctx,
			 lvs_destroy_unload_timeout);
	return;

err:
	if (ctx->client) {
		s3_client_put(ctx->client);
	}
	free(ctx->lvs_name);
	free(ctx);
	if (cb_fn) {
		cb_fn(cb_arg, rc);
	}
}

/* ==========================================================================
 * Lookup and iteration
 * ========================================================================== */

struct s3lvol_lvstore *
s3lvol_lvstore_pick_one(void)
{
	struct s3lvol_lvstore *lvs, *found = NULL;

	TAILQ_FOREACH(lvs, &g_lvstores, link) {
		if (found) {
			return NULL;	/* more than one */
		}
		found = lvs;
	}
	return found;
}

unsigned
s3lvol_lvstore_count(void)
{
	struct s3lvol_lvstore *lvs;
	unsigned n = 0;

	TAILQ_FOREACH(lvs, &g_lvstores, link) {
		n++;
	}
	return n;
}

struct s3lvol_lvstore *
s3lvol_lvstore_find(const char *name)
{
	struct s3lvol_lvstore *lvs;

	if (!name) {
		return NULL;
	}
	TAILQ_FOREACH(lvs, &g_lvstores, link) {
		if (strcmp(lvs->name, name) == 0) {
			return lvs;
		}
	}
	return NULL;
}

struct s3lvol_lvstore *
s3lvol_lvstore_first(void)
{
	return TAILQ_FIRST(&g_lvstores);
}

struct s3lvol_lvstore *
s3lvol_lvstore_next(struct s3lvol_lvstore *prev)
{
	return prev ? TAILQ_NEXT(prev, link) : NULL;
}

struct s3lvol_lvstore *
s3lvol_lvstore_find_by_lvs(struct spdk_lvol_store *store)
{
	struct s3lvol_lvstore *lvs;

	if (!store) {
		return NULL;
	}
	/* Returns NULL while an lvstore is still loading -- lvs->lvs is only filled
	 * in once spdk_lvs_load_ext() completes, and the esnap callback fires before
	 * that. Callers that run during load must not depend on this; it is here so
	 * they can name the lvstore in a message when it happens to be known. */
	TAILQ_FOREACH(lvs, &g_lvstores, link) {
		if (lvs->lvs == store) {
			return lvs;
		}
	}
	return NULL;
}

struct spdk_lvol_store *
s3lvol_lvstore_get_lvs(struct s3lvol_lvstore *lvs)
{
	return lvs ? lvs->lvs : NULL;
}

bool
s3lvol_lvstore_exports_ready(const struct s3lvol_lvstore *lvs)
{
	return lvs && lvs->exports_ready;
}

void
s3lvol_lvstore_exports_set_ready(struct s3lvol_lvstore *lvs, bool ready)
{
	lvs->exports_ready = ready;
}

bool
s3lvol_lvstore_xfer_allowed(const struct s3lvol_lvstore *lvs)
{
	return lvs && lvs->lvs && lvs->exports_ready && !lvs->unload_in_progress &&
		!s3lvol_lvstore_source_closed((struct s3lvol_lvstore *)lvs) &&
		!s3lvol_scoped_source_held((struct s3lvol_lvstore *)lvs);
}

bool
s3lvol_lvstore_dependency_view_complete(const struct s3lvol_lvstore *lvs)
{
	return s3lvol_lvstore_xfer_allowed(lvs) && lvs->dependency_view_complete;
}

void
s3lvol_lvstore_get_lifecycle(const struct s3lvol_lvstore *lvs,
			    struct s3lvol_lifecycle_status *out)
{
	memset(out, 0, sizeof(*out));
	out->uuid = lvs->uuid;
	out->unload_in_progress = lvs->unload_in_progress;
	out->timeout_reported = lvs->unload_timeout_reported;
	out->release_pending = lvs->owner_release_pending;
	out->owner_claim_local = lvs->owner_held;
	/* Do not inspect native internals while asynchronous unload owns them. */
	out->native_available = !lvs->unload_in_progress && lvs->lvs &&
		lvs->lvs->blobstore;
	out->write_path_available = out->native_available && lvs->bs_dev;
}

void
s3lvol_lvstore_foreach_retained_status(s3lvol_retained_status_cb cb, void *arg)
{
	const struct lvs_retained *r;

	TAILQ_FOREACH(r, &g_retained, link) {
		cb(arg, r->ident.lvs_name, &r->ident.lvs_uuid, r->busy,
		   r->failed_attempts);
	}
}

struct s3_client *
s3lvol_lvstore_get_client(struct s3lvol_lvstore *lvs)
{
	return lvs ? lvs->client : NULL;
}

struct spdk_bs_dev *
s3lvol_lvstore_get_bs_dev(struct s3lvol_lvstore *lvs)
{
	return lvs ? lvs->bs_dev : NULL;
}

const char *
s3lvol_lvstore_get_namespace(struct s3lvol_lvstore *lvs)
{
	return lvs ? lvs->ns_name : NULL;
}

const char *
s3lvol_lvstore_get_name(struct s3lvol_lvstore *lvs)
{
	return lvs ? lvs->name : NULL;
}

/* ==========================================================================
 * lvstore creation
 *
 * The chain, when a wal_bdev was given:
 *
 *   s3_client_get_or_create
 *   -> s3_local_dev_format ... super block and region layout
 *   -> s3_journal_create ... format the metadata journal
 *   -> s3_wal_create ... format the write-ahead log
 *   -> s3_bs_dev_create
 *   -> s3_bs_dev_attach_journal ... chunk map changes become durable
 *   -> s3_bs_dev_attach_wal ... writes are acknowledged from the log
 *   -> spdk_lvs_init
 *
 * Without a wal_bdev the middle steps are skipped and the bs_dev writes
 * straight to S3 (the direct-to-S3 behaviour).
 *
 * Every step is asynchronous, hence the callback chain. The whole thing runs on
 * the thread that called s3lvol_lvstore_create(), which becomes the lvstore's
 * owner thread -- the journal, the WAL, the overlay and the flusher all assume
 * that and hold no locks.
 * ========================================================================== */

/* Release the local device stack.
 *
 * The order is forced by who writes to what:
 *
 *   bs_dev (its flusher writes the WAL, its chunk map writes the journal)
 *   -> journal
 *   -> local_dev
 *
 * So this may only run once the bs_dev is completely gone. On the WAL path that
 * is *not* when spdk_lvs_unload() completes -- see s3_bs_dev_set_destroy_cb(). */
static void
s3lvol_lvstore_release_local(struct s3lvol_lvstore *lvs)
{
	/* Closed by the bs_dev, which owns the flusher that writes it. */
	lvs->wal = NULL;

	if (lvs->journal) {
		s3_journal_destroy(lvs->journal);
		lvs->journal = NULL;
	}
	if (lvs->local_dev) {
		s3_local_dev_close(lvs->local_dev);
		lvs->local_dev = NULL;
	}
}

static void
s3lvol_lvstore_free(struct s3lvol_lvstore *lvs)
{
	if (!lvs) {
		return;
	}

 assert(!lvs->scoped_sources);
 while(lvs->source_gates){struct source_gate_entry *e=lvs->source_gates;lvs->source_gates=e->next;free(e);}

	/* Pending-delete marks belong to this lvstore's lvols, all of which are
	 * gone by now: unload closed them, delete destroyed them. Leaving the
	 * marks behind is what would let --retry-pending act on a same-named
	 * snapshot in the lvstore that gets attached next. The single funnel for
	 * unload, delete and the setup error paths, so one call covers all.
	 *
	 * Keyed off the copy rather than lvs->lvs, which the unload path has
	 * already cleared by the time it gets here. All-zero means the setup
	 * never reached an open blobstore, so there is nothing to clear. */
	if (!spdk_uuid_is_null(&lvs->uuid)) {
		s3lvol_snapshot_pending_clear_lvs(&lvs->uuid);
	}

	/* Cached import manifests go with the lvstore. The registry object in S3
	 * stays: it belongs to the lvstore, and the next attach needs it. */
	s3lvol_xfer_lvstore_fini(lvs);
	s3lvol_xfer_exports_fini(lvs);
	s3lvol_lvstore_release_local(lvs);
	if (lvs->client) {
		s3_client_put(lvs->client);
	}
	/* The bstore.json entry deliberately stays.
	 *
	 * This runs on unload as well as on teardown, and an unload keeps the S3
	 * data: the lvstore is meant to be attachable again afterwards. The entry
	 * is what tells a recovery script that it exists and which namespace and
	 * WAL bdev to attach it with, so dropping it here would leave the data
	 * with nothing pointing at it. Only rcow_delete_lvstore, which destroys
	 * the data, removes the entry. */
	free(lvs->bs_name);
	free(lvs->ns_name);
	free(lvs->name);
	free(lvs);
}

static void
lvs_setup_ctx_free(struct lvs_setup_ctx *ctx)
{
	struct s3_local_dev *dev = ctx->lvs ? ctx->lvs->local_dev : NULL;

	s3_recovery_ckpt_free(ctx->recover_ckpt);

	/* Release only the claims the local device did not adopt.
	 *
	 * s3_local_dev_format() adopts both claims at the instant it reports
	 * success, because from that point the journal, the WAL and the cache
	 * submit I/O through those descriptors. Ownership must therefore last
	 * until s3_local_dev_close() has put the device's channels -- which is
	 * later than this function on both the success path and every failure
	 * after the format. Closing a descriptor here, before those channels are
	 * drained, is the defect this guard exists to prevent.
	 *
	 * Before a successful format the device owns nothing, so a scan failure,
	 * a format failure, or a second-claim refusal releases here exactly once.
	 * The pointer is cleared either way; the device holds its own copy. */
	/* Defensive: every terminal path resolves the retained record before
	 * this; never leave it busy (that would block every later retry). */
	if (ctx->retained) {
		lvs_retained_put_back(ctx->retained);
		ctx->retained = NULL;
	}

	if (!s3_local_dev_owns_claim(dev, ctx->recover_cache_claim)) {
		s3_recovery_claim_release(ctx->recover_cache_claim);
	}
	ctx->recover_cache_claim = NULL;
	if (!s3_local_dev_owns_claim(dev, ctx->recover_wal_claim)) {
		s3_recovery_claim_release(ctx->recover_wal_claim);
	}
	ctx->recover_wal_claim = NULL;

	free(ctx->wal_bdev_name);
	free(ctx->cache_bdev_name);
	free(ctx);
}

static void lvs_setup_report(struct lvs_setup_ctx *ctx);
static void lvs_recover_validate_complete(struct lvs_setup_ctx *ctx, int status);
static void lvs_recover_unwind(struct lvs_setup_ctx *ctx, int status);

/* Defined with the ownership dispatch below; the setup teardown at the top of
 * this file calls it before that section. */
static void lvs_owner_release(struct s3lvol_lvstore *lvs, s3_owner_cb cb_fn,
			      void *cb_arg);

/* The owner marker is gone; carry on failing. */
static void
lvs_setup_owner_released(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	/* The setup is failing either way and the original error is what the
	 * caller is told. A failed release is still not "released": the marker
	 * may still name this process, so it is reported rather than dropped. */
	if (status != 0) {
		/* Numeric: two spdk_strerror() calls share one buffer. */
		SPDK_ERRLOG("lvstore '%s': setup failed (%d) and the S3 owner "
			    "marker could not be released (%d); it may still name "
			    "this process and refuse the next acquire\n",
			    ctx->lvs->name, ctx->status, status);
	}

	lvs_setup_report(ctx);
}

/* Terminal step of both chains: publish the lvstore, or tear everything down. */
static void
lvs_setup_report(struct lvs_setup_ctx *ctx)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	s3lvol_lvs_op_cb cb_fn = ctx->cb_fn;
	void *cb_arg = ctx->cb_arg;
	int status = ctx->status;
	bool ctx_attaching = ctx->attaching;
	bool retain_lvs = ctx->retain_lvs;

	/* A retained claim moves into the live lvstore only on success, and is
	 * never released by a failed retained attach: the record goes back
	 * unchanged so the same device can be retried, and owner_held is cleared
	 * so the generic failure path below does not release it. Clean or not,
	 * this attach may have replayed and uploaded part of the WAL; the marker
	 * keeps naming this claim until a later clean unload releases it. */
	if (ctx->retained) {
		if (status == 0) {
			lvs_retained_consume(&g_retained, ctx->retained);
			SPDK_NOTICELOG("lvstore '%s': retained claim consumed by the "
				       "attach; a clean unload releases it\n",
				       lvs->name);
		} else {
			lvs->owner_held = false;
			lvs_retained_put_back(ctx->retained);
			SPDK_ERRLOG("lvstore '%s': attach with the retained claim "
				    "failed (%d); the claim stays retained and "
				    "unreleased, the local WAL is kept, and the same "
				    "device may be attached again\n", lvs->name,
				    status);
		}
		ctx->retained = NULL;
	}

	/* Claimed the lvstore in S3 but could not bring it up. Give the claim back
	 * before unwinding: leaving it behind would make the next attach demand
	 * force=true for no reason, and force=true is supposed to mean "I have
	 * checked that the other owner is really gone".
	 *
	 * Clearing owner_held first is what stops this from recursing: the release
	 * completion comes back here and takes the normal path. */
	if (status != 0 && lvs->owner_held) {
		struct lvs_owner_teardown td = {
			.owner_held      = true,
			.resources_live  = retain_lvs,
			.teardown_status = ctx->teardown_status,
		};

		if (lvs_owner_decide(&td) == LVS_OWNER_RELEASE) {
			/* Every writer this setup started is gone: this runs from
			 * the bs_dev destroy callback, from the WAL close, or from
			 * a failure before either existed. What the free below
			 * does is local only (journal, local device, memory), so
			 * releasing first advertises nothing that is untrue. */
			lvs->owner_held = false;
			lvs_owner_release(lvs, lvs_setup_owner_released, ctx);
			return;
		}
		if (!retain_lvs) {
			SPDK_ERRLOG("lvstore '%s': setup failed and its teardown "
				    "did not finish cleanly (%s). Acknowledged "
				    "writes may exist only in the local WAL, so the "
				    "S3 owner marker is retained. %s\n", lvs->name,
				    spdk_strerror(-ctx->teardown_status),
				    lvs_retained_guidance(
					    lvs->owner_mode == S3_OWNER_MODE_STRICT,
					    false));
		} else {
			/* The recovery unwind could not release everything it
			 * acquired. The lvol_store and the bs_dev are still live,
			 * so this process is still the writer: releasing the
			 * marker would invite a second process to attach the same
			 * data and corrupt it. The marker is deliberately left in
			 * place (it does not expire), and an operator has to
			 * intervene. See the retain_lvs comment in the context. */
			SPDK_ERRLOG("lvstore '%s': recovery cleanup did not complete; "
				    "retaining the S3 owner marker as well as the "
				    "live lvol_store. Recovery runs in strict mode "
				    "only, which has no force and no in-band "
				    "takeover: stop this instance, and an operator "
				    "must verify no writer remains before clearing "
				    "the marker out of band\n", lvs->name);
		}
	}

	lvs_setup_ctx_free(ctx);

	if (status != 0) {
		if (retain_lvs) {
			/* Not freed, on purpose. SPDK still points at it (an
			 * lvol, the lvol_store, or the bs_dev), so freeing it
			 * here would be a use-after-free; dropping the pointer
			 * silently would hide a leak the operator has to see.
			 * The failure is still reported, so the caller does not
			 * treat the lvstore as published. */
			SPDK_ERRLOG("lvstore '%s' is retained after a failed "
				    "rebuild: its lvols and backing device are "
				    "still allocated\n", lvs->name);
		} else {
			s3lvol_lvstore_free(lvs);
		}
		if (cb_fn) {
			cb_fn(cb_arg, NULL, status);
		}
		return;
	}

	TAILQ_INSERT_TAIL(&g_lvstores, lvs, link);

	/* Deletes that were asked for before the last restart and could not be
	 * carried out. Started here rather than earlier in the chain because the
	 * queue is keyed by lvstore uuid, which only exists once blobstore is up,
	 * and because it must not be able to hold the attach up: it is fire and
	 * forget. Callbacks re-resolve this wrapper by uuid, so an unload that
	 * wins the race drops the body instead of using a freed pointer. */
	s3lvol_pending_load(lvs);

	SPDK_NOTICELOG("lvstore '%s' %s: cluster=%" PRIu64 " bytes, "
		       "%" PRIu64 " free clusters, write path=%s\n",
		       lvs->name, ctx_attaching ? "attached" : "ready",
		       spdk_bs_get_cluster_size(lvs->lvs->blobstore),
		       spdk_bs_free_cluster_count(lvs->lvs->blobstore),
		       lvs->wal ? "WAL" : "direct-to-S3");

	if (cb_fn) {
		cb_fn(cb_arg, lvs, 0);
	}
}

/* The bs_dev finished tearing itself down after a failed create or attach. Only
 * now is it safe to release the journal and the local device. */
static void
lvs_setup_bs_dev_gone(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	/* Non-zero: the flusher drain timed out or the WAL close failed, so
	 * acknowledged writes may exist only locally. Remembered for the owner
	 * decision in lvs_setup_report(). */
	if (status != 0 && ctx->teardown_status == 0) {
		ctx->teardown_status = status;
	}
	ctx->lvs->bs_dev = NULL;
	/* The bs_dev owned the WAL and has just closed it. Clearing the pointer
	 * is what keeps the failure funnel from closing it a second time: that
	 * branch exists for a WAL that was never attached. */
	ctx->lvs->wal = NULL;

	/* During the recovery validate step this completion belongs to the
	 * finalization state machine, which owns the run object and reports
	 * exactly once. Calling the user's callback from here instead would
	 * leave the run object pointing at a freed setup context. */
	if (ctx->recover_validating) {
		lvs_recover_validate_complete(ctx, ctx->status);
		return;
	}

	lvs_setup_report(ctx);
}

static void
lvs_setup_wal_closed(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("Failed to close the WAL while unwinding: %d\n", status);
		if (ctx->teardown_status == 0) {
			ctx->teardown_status = status;
		}
	}
	ctx->lvs->wal = NULL;
	lvs_setup_report(ctx);
}

/* Failure anywhere in the chain.
 *
 * Three cases, by how far we got:
 *   - a bs_dev exists: it owns the WAL now, and its teardown is asynchronous
 *   - a WAL exists but was never attached: close it here
 *   - neither: report directly
 */
static void
lvs_setup_fail(struct lvs_setup_ctx *ctx, int status)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;

	ctx->status = status;

	if (lvs->bs_dev) {
		s3_bs_dev_set_destroy_cb(lvs->bs_dev, lvs_setup_bs_dev_gone, ctx);
		lvs->bs_dev->destroy(lvs->bs_dev);
		return;
	}
	if (lvs->wal) {
		s3_wal_close(lvs->wal, lvs_setup_wal_closed, ctx);
		return;
	}

	lvs_setup_report(ctx);
}

/* Deliver the recovery validate step's outcome, exactly once.
	*
	* The validate step is driven by s3_recovery_finalize's state machine, which
	* owns the run object and frees it as it reports. So every failure inside that
	* step -- the imports or exports registry load, the blobstore load, a uuid
	* mismatch, a partially opened lvol set -- has to come back through here.
	* Routing one of them straight to lvs_setup_fail() instead frees this setup
	* context while the run object still points at it, and leaves the run never
	* completed: the finalizer's own object, and its completion callback, are
	* leaked.
	*
	* Clearing the callback first is what makes a second call a no-op rather than a
	* double completion, which would report to a caller that has already been told
	* the outcome. */
static void
lvs_recover_validate_complete(struct lvs_setup_ctx *ctx, int status)
{
	void (*done)(void *arg, int status) = ctx->recover_validate_done;
	void *done_arg = ctx->recover_validate_arg;

	ctx->recover_validate_done = NULL;
	ctx->recover_validate_arg = NULL;
	ctx->recover_validating = false;

	if (!done) {
		/* Not reachable from the chain: the validate step installs the
		 * callback before it starts anything that can fail. Failing
		 * loudly beats leaving the caller waiting for a completion that
		 * cannot arrive. */
		SPDK_ERRLOG("lvstore '%s': recovery validate outcome (%d) has no "
			    "completion to report to\n", ctx->lvs->name, status);
		lvs_setup_fail(ctx, status != 0 ? status : -EINVAL);
		return;
	}

	done(done_arg, status);
}

/* A failure inside whichever chain is currently running.
	*
	* Identical to lvs_setup_fail() except during the recovery validate step, where
	* the outcome belongs to the finalization state machine. Keeping the decision
	* here is what makes "every failure of that step reaches done" a property of
	* the code rather than of a review. */
static void
lvs_setup_fail_step(struct lvs_setup_ctx *ctx, int status)
{
	if (ctx->recover_validating) {
		lvs_recover_validate_complete(ctx, status);
		return;
	}
	if (ctx->recover_loaded) {
		/* Blobstore is live, so the setup funnel would destroy the
		 * bs_dev underneath an lvol_store and a set of open lvols. */
		lvs_recover_unwind(ctx, status);
		return;
	}
	lvs_setup_fail(ctx, status);
}

/* Last step of a successful create: record which lvstore this local device
 * belongs to, so a later attach can refuse to pair it with a different one.
 *
 * A failure here does not fail the create. The lvstore is up and correct; only
 * the attach-time check degrades from "name and uuid" to "name only". Unwinding
 * a working lvstore over a bookkeeping write would be the worse trade. */
static void
lvs_create_uuid_recorded(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_WARNLOG("lvstore '%s': could not record its uuid on the "
			     "local device (%d); a later attach will only be able "
			     "to check the name\n", ctx->lvs->name, status);
	}

	ctx->status = 0;
	lvs_setup_report(ctx);
}

static void
s3lvol_lvs_init_cb(void *cb_arg, struct spdk_lvol_store *lvol_store, int lvserrno)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;

	if (lvserrno != 0) {
		SPDK_ERRLOG("Failed to init lvstore '%s': %s\n",
			    lvs->name, spdk_strerror(-lvserrno));
		/* spdk_lvs_init's failure path already called bs_dev->destroy(),
		 * so it must not be called again. But on the WAL path that
		 * teardown is asynchronous, and the destroy callback registered
		 * before spdk_lvs_init() is what tells us when the
		 * journal and the local device are free. */
		ctx->status = lvserrno;
		if (lvs->bs_dev) {
			return;
		}
		lvs_setup_report(ctx);
		return;
	}

	lvs->lvs = lvol_store;
	lvs->exports_ready = true;
	lvs->dependency_view_complete = true; /* New, empty blobstore. */

	/* Kept for s3lvol_lvstore_free(): the unload clears lvs->lvs before the
	 * free runs, and the free is where this lvstore's pending-delete marks
	 * are dropped. */
	spdk_uuid_copy(&lvs->uuid, &lvol_store->uuid);

	/* The create context is about to go away, so drop its destroy callback;
	 * the unload path registers its own. */
	s3_bs_dev_set_destroy_cb(lvs->bs_dev, NULL, NULL);

	/* Record the auto-generated blobstore name in /data/cubelet/rcow/bstore.json, so a
	 * recovery script can find it later. A failure here is logged but not
	 * fatal: the lvstore is up. */
	bstore_save_entry(lvs->name, lvs->bs_name, lvs->ns_name,
			  ctx->wal_bdev_name);

	if (lvs->local_dev) {
		/* Only knowable now: blobstore generates the uuid and refuses to
		 * take one from outside, so the disk was formatted with zeroes
		 * there. */
		s3_local_dev_set_lvs_uuid(lvs->local_dev, &lvol_store->uuid,
					  lvs_create_uuid_recorded, ctx);
		return;
	}

	ctx->status = 0;
	lvs_setup_report(ctx);
}

static void
lvs_create_start_blobstore(struct lvs_setup_ctx *ctx)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	struct spdk_lvs_opts lvs_opts;
	int rc;

	rc = s3_bs_dev_create(&ctx->opts, NULL, NULL, lvs->client,
			      ctx->opts.capacity_bytes, &lvs->bs_dev);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to create bs_dev for '%s': %d\n", lvs->name, rc);
		lvs_setup_fail(ctx, rc);
		return;
	}

	if (lvs->journal) {
		rc = s3_bs_dev_attach_journal(lvs->bs_dev, lvs->journal,
					      lvs->local_dev);
		if (rc != 0) {
			SPDK_ERRLOG("Failed to attach the journal to '%s': %d\n",
				    lvs->name, rc);
			lvs_setup_fail(ctx, rc);
			return;
		}

		/* Deliberately not checked: a missing or unusable cache region
		 * means reads go to S3, which is how this worked before the cache
		 * existed. It is not a reason to fail creating the lvstore. */
		(void)s3_bs_dev_attach_cache(lvs->bs_dev);
	}

	if (lvs->wal) {
		/* From here on the bs_dev owns closing the WAL. */
		rc = s3_bs_dev_attach_wal(lvs->bs_dev, lvs->wal, NULL);
		if (rc != 0) {
			SPDK_ERRLOG("Failed to attach the WAL to '%s': %d\n",
				    lvs->name, rc);
			lvs_setup_fail(ctx, rc);
			return;
		}
	}

	spdk_lvs_opts_init(&lvs_opts);

	/* The cluster size follows the device's chunk size unless it was asked for
	 * explicitly.
	 *
	 * Two reasons, and the second one is why the default had to change. A cluster
	 * that spans several chunks makes copy-on-write read every one of them: a
	 * write to an unallocated cluster pulls the whole cluster through the back
	 * device, which at a 4 MiB cluster over 1 MiB chunks is four GETs and four
	 * PUTs for one write. And a zero-copy export needs one blob cluster to be
	 * exactly one chunk map entry, so any other ratio sends every handoff down
	 * the copying path -- which is what SPDK's 4 MiB default silently did, while
	 * the device defaulted to 1 MiB chunks.
	 *
	 * An explicit cluster size is still honoured, because an lvstore that will
	 * never be exported may legitimately want a coarser one; it just gets told
	 * what it is giving up. */
	if (ctx->opts.cluster_size) {
		uint32_t chunk_size = s3_bs_dev_get_chunk_size(lvs->bs_dev);

		lvs_opts.cluster_sz = ctx->opts.cluster_size;
		if (chunk_size != 0 && lvs_opts.cluster_sz != chunk_size) {
			SPDK_WARNLOG("lvstore '%s' has a %u-byte cluster over %u-byte "
				     "chunks. Exports of it cannot be zero-copy, and every "
				     "copy-on-write will touch %" PRIu64 " chunk(s).\n", lvs->name,
				     lvs_opts.cluster_sz, chunk_size,
				     spdk_divide_round_up(lvs_opts.cluster_sz, chunk_size));
		}
	} else {
		lvs_opts.cluster_sz = s3_bs_dev_get_chunk_size(lvs->bs_dev);
	}
	/* The blobstore name is auto-generated so the user never has to pick one
	 * that does not already exist on the local device. The user-visible name
	 * (lvs->name) stays as-is for RPC addressing, S3 prefix, and bdev naming. */
	char generated[32];

	bstore_generate_bs_name(generated, sizeof(generated));
	lvs->bs_name = strdup(generated);
	if (!lvs->bs_name) {
		lvs_setup_fail(ctx, -ENOMEM);
		return;
	}
	snprintf(lvs_opts.name, sizeof(lvs_opts.name), "%s", generated);

	/* Registered on the create path too, not only on load. A brand new lvstore
	 * has no esnap clones, but it can acquire one the moment an import runs
	 * against it, and blobstore asks this callback for the parent while creating
	 * the clone -- not only while loading it. spdk_lvs_init() passes it straight
	 * through to spdk_bs_opts, with esnap_ctx set to the lvol store, so the
	 * callback sees the same arguments as on the load path.
	 *
	 * One asymmetry upstream, in case someone reaches for those APIs later: init
	 * does *not* also record it in lvs->esnap_bs_dev_create, which is what
	 * spdk_lvol_set_external_parent() and spdk_lvs_notify_hotplug() call. Neither
	 * is used here; calling one against a freshly created lvstore would find a
	 * NULL there. */
	lvs_opts.esnap_bs_dev_create = s3lvol_esnap_dev_create;

	/* Registered before spdk_lvs_init() because *both* of its failure paths
	 * destroy the bs_dev, and that teardown is asynchronous. */
	s3_bs_dev_set_destroy_cb(lvs->bs_dev, lvs_setup_bs_dev_gone, ctx);

	rc = spdk_lvs_init(lvs->bs_dev, &lvs_opts, s3lvol_lvs_init_cb, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("spdk_lvs_init failed for '%s': %d\n", lvs->name, rc);
		/* Synchronous failure: spdk_lvs_init never took the bs_dev, so we
		 * destroy it ourselves. */
		lvs_setup_fail(ctx, rc);
	}
}

static void
lvs_create_wal_done(void *cb_arg, struct s3_wal *wal, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("Failed to format the WAL on '%s': %d\n",
			    ctx->wal_bdev_name, status);
		lvs_setup_fail(ctx, status);
		return;
	}

	ctx->lvs->wal = wal;
	lvs_create_start_blobstore(ctx);
}

static void
lvs_create_journal_done(void *cb_arg, struct s3_journal *journal, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("Failed to format the journal on '%s': %d\n",
			    ctx->wal_bdev_name, status);
		lvs_setup_fail(ctx, status);
		return;
	}

	ctx->lvs->journal = journal;

	s3_wal_create(ctx->lvs->local_dev, NULL, lvs_create_wal_done, ctx);
}

static void
lvs_create_local_dev_done(void *cb_arg, struct s3_local_dev *dev, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("Failed to format the local device '%s': %d\n",
			    ctx->wal_bdev_name, status);
		lvs_setup_fail(ctx, status);
		return;
	}

	ctx->lvs->local_dev = dev;

	s3_journal_create(dev, lvs_create_journal_done, ctx);
}

/* Common prologue of both chains: allocate the lvstore and the setup context,
 * take a copy of everything the callbacks will still need, and get the S3 client.
 *
 * Nothing here is asynchronous, so a failure is reported through the return value
 * and neither the lvstore nor the context exists afterwards. On success the
 * caller owns *ctx and must finish through lvs_setup_report() or
 * lvs_setup_fail(). */
static int
lvs_setup_begin(const struct s3_lvs_opts *opts, s3lvol_lvs_op_cb cb_fn,
		void *cb_arg, struct lvs_setup_ctx **out)
{
	struct s3lvol_lvstore *lvs;
	struct lvs_setup_ctx *ctx;
	int rc;

	lvs = calloc(1, sizeof(*lvs));
	if (lvs) {
		atomic_init(&lvs->producers.state, 0);
		atomic_init(&lvs->producers.error, 0);
		lvs->source_thread = spdk_get_thread();
	}
	ctx = calloc(1, sizeof(*ctx));
	if (!lvs || !ctx) {
		free(ctx);
		free(lvs);
		return -ENOMEM;
	}
	ctx->lvs = lvs;
	ctx->cb_fn = cb_fn;
	ctx->cb_arg = cb_arg;

	lvs->name = strdup(opts->lvs_name);
	if (!lvs->name) {
		free(ctx);
		free(lvs);
		return -ENOMEM;
	}

	/* Copied here rather than read back from the client later: the caller's
	 * opts do not survive the first callback, and an import that defaults to
	 * the same namespace needs it long afterwards. */
	lvs->ns_name = strdup(opts->ns_name ? opts->ns_name : "");
	if (!lvs->ns_name) {
		rc = -ENOMEM;
		goto err;
	}

	/* The chain outlives the caller's opts, so copy what later steps need.
	 * s3_bs_dev_create() only reads lvs_name and chunk_size, and lvs_name has
	 * to point at our own copy. */
	ctx->opts = *opts;
	ctx->opts.lvs_name = lvs->name;
	ctx->opts.wal_bdev_name = NULL;
	ctx->opts.cache_bdev_name = NULL;

	if (opts->wal_bdev_name) {
		ctx->wal_bdev_name = strdup(opts->wal_bdev_name);
		if (!ctx->wal_bdev_name) {
			rc = -ENOMEM;
			goto err;
		}
	}
	if (opts->cache_bdev_name) {
		ctx->cache_bdev_name = strdup(opts->cache_bdev_name);
		if (!ctx->cache_bdev_name) {
			rc = -ENOMEM;
			goto err;
		}
	}
	ctx->journal_size = (uint64_t)opts->journal_size_mb * 1024 * 1024;
	ctx->wal_size = (uint64_t)opts->wal_size_mb * 1024 * 1024;

	rc = s3_client_get_or_create(&opts->target, &lvs->client);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to get S3 client for %s: %d\n",
			    opts->target.endpoint, rc);
		goto err;
	}

	*out = ctx;
	return 0;

err:
	lvs_setup_ctx_free(ctx);
	s3lvol_lvstore_free(lvs);
	return rc;
}

/* Defined with the attach chain below; both chains resume through the shared
 * owner callback. */
static void lvs_attach_after_owner(struct lvs_setup_ctx *ctx);

/* Everything the create chain does after it owns the lvstore. */
static void
lvs_create_after_owner(struct lvs_setup_ctx *ctx)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	struct s3_local_dev_format_opts fopts = {0};

	if (!ctx->wal_bdev_name) {
		/* No local device: the direct-to-S3 mode. Loud about it, because
		 * that mode loses concurrent partial-chunk writes and survives
		 * no crash. It is a smoke-test mode, not a usable
		 * configuration. */
		SPDK_WARNLOG("lvstore '%s' created without a wal_bdev: writes go "
			     "straight to S3, which is neither crash safe nor "
			     "correct under concurrent partial-chunk writes\n",
			     lvs->name);
		lvs_create_start_blobstore(ctx);
		return;
	}

	/* The lvstore uuid does not exist yet -- blobstore generates it and will
	 * not accept one from outside -- so the disk is formatted with zeroes
	 * there and the real value is written back in s3lvol_lvs_init_cb().
	 * Capacity and name *are* recorded now: the attach path reads them back
	 * instead of asking the operator to repeat them. */
	fopts.wal_bdev_name   = ctx->wal_bdev_name;
	fopts.cache_bdev_name = ctx->cache_bdev_name;
	fopts.lvs_name        = lvs->name;
	fopts.capacity_bytes  = ctx->opts.capacity_bytes;
	fopts.chunk_size      = ctx->opts.chunk_size ? ctx->opts.chunk_size :
				S3LVOL_DEFAULT_CHUNK_SIZE;
	fopts.journal_size    = ctx->journal_size;
	fopts.wal_size        = ctx->wal_size;

	s3_local_dev_format(&fopts, lvs_create_local_dev_done, ctx);
}

/* Does this prefix already hold a blobstore?
 *
 * The owner marker answers "is somebody writing to it right now", which is not the
 * same question: a clean unload releases the marker (lvs_unload_maybe_finish), so
 * an lvstore that was stopped properly leaves no trace in it at all. Creating then
 * proceeds and lays a fresh blobstore super block over live data. Two ways in:
 *
 *   - the local bstore.json is gone or was never written on this host, so
 *     rcow_start.sh cannot tell that the prefix is in use and falls back to create;
 *   - two nodes derive the same lvstore name, which is a key prefix, and the first
 *     one to have used it was cleanly stopped.
 *
 * Neither is exotic. The first one happened here.
 *
 * `<prefix>/meta/checkpoint` is the marker used because it is the only object with
 * a deterministic name that outlives a clean shutdown: data objects are uuid-named
 * (and s3_list_objects() is still -ENOTSUP, so they cannot be enumerated), and
 * meta/owner is by design transient.
 *
 * What this does not cover: an lvstore created and cleanly stopped before its first
 * checkpoint ever ran. Its prefix holds nothing but uuid-named data objects, and
 * nothing here can find them. Closing that needs either a working list or an
 * identity object written at create time -- the latter is a small change, but it
 * adds an object whose lifecycle has to be right in every delete path, and getting
 * that wrong turns "create refuses over live data" into "create refuses forever".
 * Left alone deliberately; the window is a create followed by a clean stop with no
 * checkpoint in between.
 *
 * --force skips the check, and has to: it is the documented way to take over a
 * prefix, and it is also the only escape if a checkpoint object is somehow left
 * behind by a prefix that is genuinely dead.
 */
static void
lvs_create_prefix_checked(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status == 0) {
		SPDK_ERRLOG("refusing to create lvstore '%s': the key prefix "
			    "'%s/' in this bucket already holds a blobstore "
			    "(found %s/meta/checkpoint)\n",
			    ctx->lvs->name, ctx->lvs->name, ctx->lvs->name);
		SPDK_ERRLOG("creating over it would write a fresh super block on "
			    "top of live data. Attach it instead, or pick another "
			    "lvstore name; pass force=true only if you know this "
			    "prefix is dead\n");
		lvs_setup_fail(ctx, -EEXIST);
		return;
	}

	if (status != -ENOENT) {
		/* Anything other than a clean 404 leaves the question open, and
		 * the destructive answer is the wrong default: a 403 on a bucket
		 * that does hold a blobstore would otherwise be read as "empty".
		 * force=true still gets through, since that is what it is for. */
		SPDK_ERRLOG("refusing to create lvstore '%s': cannot tell whether "
			    "the prefix '%s/' is already in use (HEAD "
			    "%s/meta/checkpoint failed: %s)\n",
			    ctx->lvs->name, ctx->lvs->name, ctx->lvs->name,
			    spdk_strerror(-status));
		lvs_setup_fail(ctx, status);
		return;
	}

	lvs_create_after_owner(ctx);
}

static void
lvs_create_prefix_check(struct lvs_setup_ctx *ctx)
{
	char key[S3_LVS_NAME_MAX + sizeof("/meta/checkpoint")];
	int rc;

	if (ctx->force) {
		SPDK_WARNLOG("lvstore '%s': force=true, so not checking whether the "
			     "key prefix '%s/' already holds a blobstore\n",
			     ctx->lvs->name, ctx->lvs->name);
		lvs_create_after_owner(ctx);
		return;
	}

	rc = snprintf(key, sizeof(key), "%s/meta/checkpoint", ctx->lvs->name);
	if (rc < 0 || (size_t)rc >= sizeof(key)) {
		/* lvs_name_ok() and the RPC layer bound the name well below this,
		 * so reaching here means one of those grew a hole. */
		SPDK_ERRLOG("lvstore '%s': name too long to build the checkpoint "
			    "key\n", ctx->lvs->name);
		lvs_setup_fail(ctx, -ENAMETOOLONG);
		return;
	}

	rc = s3_head(ctx->lvs->client, key, &ctx->prefix_probe_size,
		     lvs_create_prefix_checked, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("lvstore '%s': cannot probe the prefix: %s\n",
			    ctx->lvs->name, spdk_strerror(-rc));
		lvs_setup_fail(ctx, rc);
	}
}

/* ==========================================================================
 * Ownership mode dispatch
 *
 * Strict is the default and the only mode that acquires: it probes the backend
 * for conditional create and compare-and-swap replace, then acquires atomically
 * and records the claim so the release can be fenced. An explicit legacy is
 * refused before any ownership or destructive mutation (no silent fallback),
 * and an unrecognised value is a configuration error that refuses before
 * touching S3.
 * ========================================================================== */

static void lvs_setup_owner_acquired(void *cb_arg,
				     const struct s3_owner_info *holder,
				     int status);
static void lvs_setup_owner_probe_done(void *cb_arg, int status);
static void lvs_setup_owner_claim_acquired(void *cb_arg,
					   const struct s3_owner_info *self,
					   const struct s3_owner_claim *claim,
					   int status);
/* Defined with the recovery chain; the strict recovery acquire must resume
 * there, not in the attach branch of lvs_setup_owner_acquired(). */
static void lvs_recover_owner_acquired(void *cb_arg,
				       const struct s3_owner_info *holder,
				       int status);
static void lvs_recover_probe_done(void *cb_arg, int status);
static void lvs_recover_owner_claim_acquired(void *cb_arg,
					     const struct s3_owner_info *self,
					     const struct s3_owner_claim *claim,
					     int status);

static void
lvs_setup_owner_probe_done(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		/* Probe failure is -ENOTSUP for an unsupported backend. There is
		 * deliberately no legacy fallback: a deployment that asked for
		 * strict must not silently get the weaker guarantee. */
		SPDK_ERRLOG("lvstore '%s': strict ownership probe failed (%s); "
			    "refusing to attach or create without an atomic "
			    "ownership fence\n", ctx->lvs->name,
			    spdk_strerror(-status));
		lvs_setup_fail(ctx, status);
		return;
	}

	s3_owner_acquire_strict(ctx->lvs->client, ctx->lvs->name,
				ctx->owner_uuid_valid ? &ctx->owner_uuid : NULL,
				&ctx->lvs->owner_caps,
				lvs_setup_owner_claim_acquired, ctx);
}

static void
lvs_setup_owner_claim_acquired(void *cb_arg, const struct s3_owner_info *self,
			       const struct s3_owner_claim *claim, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		lvs_setup_fail(ctx, status);
		return;
	}

	/* Kept for the fenced release. A claim without this cannot be released
	 * safely, and s3_owner_release_strict() refuses rather than issuing an
	 * unconditional delete. */
	ctx->lvs->owner_claim = *claim;
	lvs_setup_owner_acquired(cb_arg, self, 0);
}

/* Claim the lvstore under the configured policy.
 *
 * \c uuid is NULL while create has not produced one yet; it is only a
 * diagnostic field in the marker. */
static void
lvs_owner_acquire(struct lvs_setup_ctx *ctx, const struct spdk_uuid *uuid,
		  bool force)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	int rc;

	lvs->owner_mode = s3_owner_mode_from_env();

	switch (lvs->owner_mode) {
	case S3_OWNER_MODE_STRICT:
		rc = s3_owner_strict_check_force(force);
		if (rc != 0) {
			lvs_setup_fail(ctx, rc);
			return;
		}
		if (uuid) {
			spdk_uuid_copy(&ctx->owner_uuid, uuid);
			ctx->owner_uuid_valid = true;
		}
		s3_owner_probe(lvs->client, lvs->name, &lvs->owner_caps,
			       lvs_setup_owner_probe_done, ctx);
		return;
	case S3_OWNER_MODE_LEGACY:
		/* Explicit legacy is refused before any ownership or destructive
		 * mutation: the read-write-readback marker is not an atomic
		 * exclusion, and a silent fallback to it would downgrade the
		 * guarantee the candidate default (strict) provides. */
		SPDK_ERRLOG("lvstore '%s': %s=legacy is not supported by this "
			    "candidate; strict is the default. Remove the setting "
			    "(or set it to strict) and retry. No ownership write "
			    "was issued.\n", lvs->name, S3_OWNER_MODE_ENV);
		lvs_setup_fail(ctx, -ENOTSUP);
		return;
	default:
		SPDK_ERRLOG("lvstore '%s': %s has an unrecognised value; "
			    "ownership operations refuse rather than guessing\n",
			    lvs->name, S3_OWNER_MODE_ENV);
		lvs_setup_fail(ctx, -EINVAL);
		return;
	}
}

/* Recovery path: strict ownership is mandatory.
 *
 * Recovery rewrites the lvstore from a remote checkpoint, so it is the operation
 * that most needs a real fence. It refuses legacy mode outright: the legacy
 * marker is a sequential-misoperation guard, and enabling a destructive rebuild
 * on top of it is exactly the assumption this task removes. A mixed deployment
 * (legacy writers plus a strict recovery process) is also refused, because the
 * legacy writer is not fenced by any conditional write.
 */
static void
lvs_owner_acquire_strict_only(struct lvs_setup_ctx *ctx,
			      const struct spdk_uuid *uuid)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;

	lvs->owner_mode = s3_owner_mode_from_env();
	if (lvs->owner_mode != S3_OWNER_MODE_STRICT) {
		SPDK_ERRLOG("lvstore '%s': remote recovery requires strict "
			    "ownership. Set %s=strict on every writer, stop "
			    "all legacy writers, and retry. No fallback is "
			    "offered because a legacy marker cannot fence a "
			    "concurrent writer.\n", lvs->name,
			    S3_OWNER_MODE_ENV);
		lvs_setup_fail(ctx, -ENOTSUP);
		return;
	}
	if (uuid) {
		spdk_uuid_copy(&ctx->owner_uuid, uuid);
		ctx->owner_uuid_valid = true;
	}
	s3_owner_probe(lvs->client, lvs->name, &lvs->owner_caps,
		       lvs_recover_probe_done, ctx);
}

static void
lvs_recover_probe_done(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("lvstore '%s': strict ownership probe failed (%s); "
			    "recovery refuses to run without an atomic ownership "
			    "fence\n", ctx->lvs->name, spdk_strerror(-status));
		lvs_setup_fail(ctx, status);
		return;
	}

	s3_owner_acquire_strict(ctx->lvs->client, ctx->lvs->name,
				ctx->owner_uuid_valid ? &ctx->owner_uuid : NULL,
				&ctx->lvs->owner_caps,
				lvs_recover_owner_claim_acquired, ctx);
}

static void
lvs_recover_owner_claim_acquired(void *cb_arg,
				 const struct s3_owner_info *self,
				 const struct s3_owner_claim *claim, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		lvs_setup_fail(ctx, status);
		return;
	}

	ctx->lvs->owner_claim = *claim;
	lvs_recover_owner_acquired(cb_arg, self, 0);
}

/* Release under the same policy that acquired. */
static void
lvs_owner_release(struct s3lvol_lvstore *lvs, s3_owner_cb cb_fn, void *cb_arg)
{
	if (lvs->owner_mode == S3_OWNER_MODE_STRICT) {
		s3_owner_release_strict(lvs->client, lvs->name,
					&lvs->owner_claim, &lvs->owner_caps,
					cb_fn, cb_arg);
		return;
	}
	s3_owner_release(lvs->client, lvs->name, cb_fn, cb_arg);
}

/* Fill a retained-claim identity from live objects. Every field must be
 * present (lvs_retained_ident_complete); -ENAMETOOLONG/-EINVAL otherwise. The
 * local device is described by its bdev name AND bdev uuid, so a different
 * device that happens to reuse the name does not match. */
static int
lvs_retained_ident_fill(struct lvs_retained_ident *id, const char *lvs_name,
			const struct s3_client *client,
			const struct spdk_uuid *lvs_uuid,
			struct s3_local_dev *dev)
{
	struct spdk_bdev_desc *wd, *cd;
	struct spdk_bdev *wb, *cb;
	int rc;

	memset(id, 0, sizeof(*id));
	if (!lvs_name || !client || !lvs_uuid || !dev) {
		return -EINVAL;
	}
	wd = s3_local_dev_get_desc(dev, S3_REGION_WAL);
	cd = s3_local_dev_get_desc(dev, S3_REGION_CACHE);
	wb = wd ? spdk_bdev_desc_get_bdev(wd) : NULL;
	if (!wb) {
		return -EINVAL;
	}
	rc = lvs_retained_copy(id->lvs_name, sizeof(id->lvs_name), lvs_name);
	rc = rc ? rc : lvs_retained_copy(id->endpoint, sizeof(id->endpoint),
					 s3_client_endpoint(client));
	rc = rc ? rc : lvs_retained_copy(id->bucket, sizeof(id->bucket),
					 s3_client_bucket(client));
	rc = rc ? rc : lvs_retained_copy(id->wal_bdev, sizeof(id->wal_bdev),
					 spdk_bdev_get_name(wb));
	if (rc != 0) {
		return rc;
	}
	spdk_uuid_copy(&id->lvs_uuid, lvs_uuid);
	spdk_uuid_copy(&id->wal_bdev_uuid, spdk_bdev_get_uuid(wb));
	if (cd && cd != wd) {
		cb = spdk_bdev_desc_get_bdev(cd);
		if (!cb) {
			return -EINVAL;
		}
		rc = lvs_retained_copy(id->cache_bdev, sizeof(id->cache_bdev),
				       spdk_bdev_get_name(cb));
		if (rc != 0) {
			return rc;
		}
		spdk_uuid_copy(&id->cache_bdev_uuid, spdk_bdev_get_uuid(cb));
	}
	return lvs_retained_ident_complete(id) ? 0 : -EINVAL;
}

/* The remote marker was read back: still exactly the retained claim (or not). */
static void
lvs_attach_retained_verified(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	struct lvs_retained *r = ctx->retained;

	if (status != 0) {
		/* Changed, replaced, free, absent or unreadable: fail closed.
		 * Nothing local was mutated yet (the WAL is not opened before
		 * this point), and the claim is not released. */
		SPDK_ERRLOG("lvstore '%s': retained claim not usable (verify %d); "
			    "refusing the attach without touching the local "
			    "device\n", lvs->name, status);
		lvs_setup_fail(ctx, status);
		return;
	}

	/* The claim moves into the lvstore here, but the record stays (busy)
	 * until the attach finishes: consumed on success, put back unchanged on
	 * failure (lvs_setup_report), so it is never held in two places. */
	lvs->owner_mode = S3_OWNER_MODE_STRICT;
	lvs->owner_caps = r->caps;
	lvs->owner_claim = r->claim;
	lvs->owner_held = true;
	SPDK_NOTICELOG("lvstore '%s': marker verified unchanged; attaching the "
		       "same device under the claim this process retained after "
		       "its failed teardown (WAL replay follows)\n", lvs->name);
	lvs_attach_after_owner(ctx);
}

/* An attach of a name this process retains a claim for. Same process only. */
static void
lvs_attach_with_retained(struct lvs_setup_ctx *ctx,
			 const struct s3_super_block *sb)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	struct lvs_retained_ident got;
	struct lvs_retained *r;
	int rc;

	/* Take first: exclusivity before any asynchronous step, so a concurrent
	 * retry is refused here and not after a second verify. */
	rc = lvs_retained_take(&g_retained, lvs->name, &r);
	if (rc != 0) {
		SPDK_ERRLOG("lvstore '%s': a retained-claim attach is already in "
			    "progress (%d); refusing\n", lvs->name, rc);
		lvs_setup_fail(ctx, rc == -ENOENT ? -EINVAL : rc);
		return;
	}
	ctx->retained = r;

	if (s3_owner_mode_from_env() != S3_OWNER_MODE_STRICT ||
	    s3_owner_strict_check_force(ctx->force) != 0) {
		SPDK_ERRLOG("lvstore '%s': the retained claim is strict and is "
			    "only reused by a strict, non-force attach\n",
			    lvs->name);
		lvs_setup_fail(ctx, -EINVAL);
		return;
	}

	rc = lvs_retained_ident_fill(&got, lvs->name, lvs->client,
				     &sb->lvs_uuid, lvs->local_dev);
	if (rc == 0) {
		rc = lvs_retained_ident_check(&r->ident, &got);
	}
	if (rc != 0) {
		SPDK_ERRLOG("lvstore '%s': attach does not match the retained "
			    "claim's target/lvstore/device (%d: %s); refusing. The "
			    "retained device '%s' must be attached to replay its "
			    "WAL\n", lvs->name, rc,
			    rc == -ENODEV ? "different local device" :
			    rc == -ESTALE ? "different lvstore uuid" :
			    rc == -EXDEV ? "different endpoint/bucket/prefix" :
			    "incomplete identity", r->ident.wal_bdev);
		lvs_setup_fail(ctx, rc);
		return;
	}

	s3_owner_verify_claim_strict(lvs->client, lvs->name, &r->claim,
				     &r->ident.lvs_uuid, &r->caps,
				     lvs_attach_retained_verified, ctx);
}

/* Unload decided RETAIN. Keep the claim (copies only) when every former local
 * writer has stopped, so the same device can be re-attached in this process. */
static void
lvs_unload_keep_retained(struct s3lvol_lvstore *lvs, bool lvs_done,
			 bool bs_dev_done, bool quiesce_done,
			 int quiesce_status, int teardown_status)
{
	struct lvs_retained_ident id;
	int rc;

	if (!lvs_retained_eligible(lvs->owner_mode == S3_OWNER_MODE_STRICT,
				   lvs->owner_held, lvs_done, bs_dev_done,
				   quiesce_done, quiesce_status,
				   teardown_status)) {
		return;
	}
	rc = lvs_retained_ident_fill(&id, lvs->name, lvs->client, &lvs->uuid,
				     lvs->local_dev);
	if (rc == 0) {
		rc = lvs_retained_add(&g_retained, &id, &lvs->owner_caps,
				      &lvs->owner_claim);
	}
	if (rc != 0) {
		SPDK_ERRLOG("lvstore '%s': could not retain the owner claim for a "
			    "same-device re-attach (%d); the marker stays held "
			    "and needs operator action\n", lvs->name, rc);
		return;
	}
	SPDK_NOTICELOG("lvstore '%s': owner claim retained in this process for "
		       "a same-device re-attach of '%s' (teardown status %d); "
		       "local WAL kept\n", lvs->name, id.wal_bdev,
		       teardown_status);
}

/* The lvstore is ours (or it is not, and we stop here).
 *
 * Shared by both chains, which is why it dispatches on ctx->attaching: the check
 * itself and its failure handling are identical, only what comes next differs. */
static void
lvs_setup_owner_acquired(void *cb_arg, const struct s3_owner_info *holder,
			 int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		/* The ownership acquire (strict or legacy) has already logged who
		 * holds it and what to do about it, in more detail than is
		 * available here. */
		(void)holder;
		lvs_setup_fail(ctx, status);
		return;
	}

	ctx->lvs->owner_held = true;

	if (ctx->attaching) {
		lvs_attach_after_owner(ctx);
	} else {
		lvs_create_prefix_check(ctx);
	}
}

/* An lvstore's name is its key prefix in the bucket, so it cannot collide with the
 * one place that is addressed without a prefix: export manifests live at
 * `exports/<uuid>.json`, which is what lets an importer find one knowing only the
 * uuid (see s3_export_manifest_key()). An lvstore called "exports" would write its
 * own metadata into that space.
 *
 * Checked on attach as well as create. A bucket written by an older build could
 * hold such an lvstore, and attaching it would be the same collision. */
static int
lvs_name_ok(const char *name)
{
	if (strcmp(name, S3_EXPORTS_DIR) == 0) {
		SPDK_ERRLOG("'%s' is reserved: export manifests live under that "
			    "prefix so that an importer needs nothing but the export "
			    "uuid to find one\n", S3_EXPORTS_DIR);
		return -EINVAL;
	}
	return 0;
}

int
s3lvol_lvstore_create(const struct s3_lvs_opts *opts,
		      s3lvol_lvs_op_cb cb_fn, void *cb_arg)
{
	struct lvs_setup_ctx *ctx;
	int rc;

	if (!opts || !opts->lvs_name || opts->capacity_bytes == 0) {
		return -EINVAL;
	}
	rc = lvs_name_ok(opts->lvs_name);
	if (rc != 0) {
		return rc;
	}
	if (s3lvol_lvstore_find(opts->lvs_name)) {
		SPDK_ERRLOG("lvstore '%s' already exists\n", opts->lvs_name);
		return -EEXIST;
	}
	if (lvs_retained_find(&g_retained, opts->lvs_name)) {
		/* This process still holds the marker for that prefix and its
		 * local WAL may hold acknowledged writes. Creating over it would
		 * discard them; only an attach of the same device may proceed. */
		SPDK_ERRLOG("lvstore '%s': this process retains its owner claim "
			    "after a failed teardown; refusing create. Attach the "
			    "same wal_bdev to replay its WAL\n", opts->lvs_name);
		return -EBUSY;
	}

	rc = lvs_setup_begin(opts, cb_fn, cb_arg, &ctx);
	if (rc != 0) {
		return rc;
	}
	ctx->force = opts->force;

	/* Claim the lvstore before touching anything in S3.
	 *
	 * Create checks this too, not just attach: an existing marker under this
	 * name means another process is actively writing to the same key prefix,
	 * and creating over it would have blobstore lay a fresh super block on top
	 * of live data. The uuid is all-zeroes because blobstore has not generated
	 * one yet -- it is a diagnostic field in the marker, nothing keys off it. */
	lvs_owner_acquire(ctx, NULL, ctx->force);
	return 0;
}

/* ==========================================================================
 * lvstore attach -- re-hang an existing lvstore (including crash recovery)
 *
 * The chain mirrors create, with _open in place of _create and two replays
 * inserted:
 *
 *   s3_client_get_or_create
 *   -> s3_local_dev_open ....... read the layout, capacity and identity back
 *   -> s3_journal_open ......... synchronous, reads checkpoint_lsn only
 *   -> s3_wal_open ............. picks the newer of the two super slots
 *   -> s3_bs_dev_create ........ geometry comes from the super block, not the RPC
 *   -> s3_bs_dev_attach_journal
 *   -> s3_journal_replay ....... rebuild chunk_index -> S3 object
 *   -> s3_bs_dev_attach_wal .... the overlay has to exist before the WAL replay
 *   -> s3_wal_replay ........... re-apply writes that never reached S3
 *   -> spdk_lvs_load_ext
 *   -> register a bdev for every lvol that was found
 *
 * === Why the order is exactly this ===
 *
 * *Both replays must precede spdk_lvs_load_ext().* blobstore reads its super
 * block as its very first action, and that read has to already see the recovered
 * state -- there is no way to tell blobstore "reload, the device changed".
 *
 * *The journal must precede the WAL.* The journal says which S3 objects exist;
 * the WAL then re-applies the writes that had not reached them yet. Replaying in
 * the other order would let a journal record overwrite a newer mapping.
 *
 * *attach_wal must precede the WAL replay* because replayed entries land in the
 * overlay, which attach_wal creates.
 *
 * === Identity checking, and what it cannot cover ===
 *
 * Pairing the wrong local device with an lvstore is destructive: replay would
 * write another lvstore's data into this one. Two checks:
 *
 *   1. lvs_name, compared right after s3_local_dev_open, i.e. *before* any
 *      replay. This catches the plausible operator error (pointing attach at a
 *      leftover image from a different lvstore) at zero cost.
 *   2. lvs_uuid, compared once spdk_lvs_load_ext() has run -- the uuid only
 *      exists inside blobstore, so it cannot be checked earlier. This catches
 *      the case the name misses: the lvstore was destroyed and recreated under
 *      the same name while this disk was kept.
 *
 * The second check is therefore *late*: by the time it fires, the WAL has been
 * replayed into the overlay and the flusher may already have uploaded some of
 * it. Closing that window needs identity metadata in S3 that can be read before
 * blobstore is loaded (the meta/owner object is the natural place), which does
 * not exist yet. Until then a uuid mismatch is reported as a failed attach with
 * a loud log, not as silent tolerance.
 * ========================================================================== */

/* Everything that had to be opened has been. Finish the attach. */
static void
lvs_attach_lvols_done(struct lvs_setup_ctx *ctx)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;

	SPDK_NOTICELOG("lvstore '%s': %u lvol(s) exported as bdevs%s\n",
		       lvs->name, ctx->lvols_ok,
		       ctx->lvols_failed ? " (some could not be opened or "
		       "registered)" : "");

	if (spdk_uuid_is_null(&ctx->expect_uuid) && lvs->local_dev) {
		/* Repair the pairing record left incomplete by an interrupted
		 * create, so the next attach gets the stronger check. Failure is
		 * not fatal for the same reason it is not on the create path. */
		s3_local_dev_set_lvs_uuid(lvs->local_dev, &lvs->lvs->uuid,
					  lvs_create_uuid_recorded, ctx);
		return;
	}

	ctx->status = 0;
	lvs_setup_report(ctx);
}

/* One lvol's blob is open (or failed to open). Give it a bdev.
 *
 * A failure here does not fail the attach. An lvol without a bdev is invisible
 * to nvmf, which matters -- but refusing the whole lvstore over one bad lvol
 * would take the healthy ones down with it, so log, count and carry on. Upstream
 * vbdev_lvol makes the same call in _vbdev_lvs_examine_finish. */
static void
lvs_attach_lvol_opened(void *cb_arg, struct spdk_lvol *lvol, int lvolerrno)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	int rc;

	/* NULL lvol means this is the self reference being released by
	 * lvs_attach_open_lvols() after it finished submitting, not a real
	 * completion. Without this it would register a NULL lvol and count a
	 * phantom failure. */
	if (lvol == NULL) {
		goto out;
	}

	if (lvolerrno != 0) {
		SPDK_ERRLOG("lvstore '%s': could not open lvol '%s': %s\n",
			    lvs->name, lvol->name, spdk_strerror(-lvolerrno));

		/* Drop it from the lvstore rather than leaving it behind.
		 *
		 * This is not tidiness: an lvol whose blob never opened has
		 * lvol->blob == NULL, and the unload path would call
		 * spdk_lvol_close() on it, which fails with -EINVAL and makes the
		 * whole lvstore impossible to unload. Upstream removes and frees
		 * it for the same reason.
		 *
		 * Upstream also retries -ENOMEM via retry_open_lvols. Not doing
		 * that here: it would mean draining a retry queue inside the
		 * attach chain, and an lvol short of memory at attach time is
		 * better reported than silently deferred. */
		TAILQ_REMOVE(&lvs->lvs->lvols, lvol, link);
		if (lvs->lvs->lvol_count > 0) {
			lvs->lvs->lvol_count--;
		}
		free(lvol);
		ctx->lvols_failed++;
		goto out;
	}

	/* Recovery: opening is *validation*, not publication. Registering a bdev
	 * here would make the lvol visible to nvmf before the local device has
	 * been committed, which is exactly the ordering this chain exists to
	 * prevent. The blob is open and stays open; registration happens in the
	 * publish step, after the commit and the final flush. */
	if (ctx->recovering) {
		ctx->lvols_ok++;
		goto out;
	}

	/* Only now is this safe: vbdev_s3lvol_bdev_register() needs lvol->blob for
	 * the block count, and spdk_lvs_load() leaves every blob closed -- it
	 * walks them with the blobstore iterator, which closes each one as it
	 * moves on. Registering straight after the load is what returned -EINVAL
	 * on the first live run. */
	rc = vbdev_s3lvol_bdev_register(lvol, lvs->name, &lvs->producers);
	if (rc != 0) {
		SPDK_ERRLOG("lvstore '%s': could not register a bdev for lvol "
			    "'%s': %d\n", lvs->name, lvol->name, rc);
		/* Left in the list on purpose: the blob *is* open, so the unload
		 * path's spdk_lvol_close() is both correct and necessary. */
		ctx->lvols_failed++;
		goto out;
	}

	ctx->lvols_ok++;

out:
	assert(ctx->lvols_pending > 0);
	if (--ctx->lvols_pending > 0) {
		return;
	}

	/* Only the completed attach can establish this proof. A failed open was
	 * removed from lvols, but its durable blob still needs the import registry
	 * and source objects. Keep healthy bdevs available without allowing ack. */
	lvs->dependency_view_complete = ctx->lvols_failed == 0;

	/* Recovery's validate step is strict: any lvol that could not be opened
	 * fails the whole rebuild. Attach keeps its tolerant behaviour, where a
	 * single bad lvol is logged and the healthy ones are still exported.
	 *
	 * This is the last of the opens, so the outcome is reported once, here,
	 * through the step completion. The lvols that did open stay open: the
	 * caller's unwind closes them, and closing them here would race the
	 * finalization state machine's own teardown. */
	if (ctx->recovering) {
		if (ctx->lvols_failed > 0) {
			SPDK_ERRLOG("lvstore '%s': recovery opened %u lvol(s) but "
				    "%u could not be opened; refusing to publish a "
				    "partially rebuilt lvstore\n",
				    lvs->name, ctx->lvols_ok, ctx->lvols_failed);
			lvs_recover_validate_complete(ctx, -EIO);
			return;
		}
		SPDK_NOTICELOG("lvstore '%s': recovery validated %u lvol(s); "
			       "nothing registered yet\n", lvs->name,
			       ctx->lvols_ok);
		lvs_recover_validate_complete(ctx, 0);
		return;
	}

	lvs_attach_lvols_done(ctx);
}

/* Open every lvol the load found, then export each as a bdev.
 *
 * spdk_lvs_load() only rebuilds the lvol *list*; it leaves the blobs closed.
 * So this is not an optional extra step -- without it every lvol has a NULL
 * blob and nothing can be exported. */
static void
lvs_attach_open_lvols(struct lvs_setup_ctx *ctx)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	struct spdk_lvol *lvol, *tmp;

	/* Self reference, so an open that completes inline cannot run the
	 * finish before the loop below has submitted everything. */
	lvs->dependency_view_complete = false;
	ctx->lvols_pending = 1;

	/* _SAFE because the failure path above removes entries from this list. */
	TAILQ_FOREACH_SAFE(lvol, &lvs->lvs->lvols, link, tmp) {
		ctx->lvols_pending++;
		spdk_lvol_open(lvol, lvs_attach_lvol_opened, ctx);
	}

	lvs_attach_lvol_opened(ctx, NULL, 0);
}

/* spdk_lvs_unload() calls its callback unconditionally on the -EBUSY paths, so a
 * NULL one would segfault. Used where there is genuinely nothing to do with the
 * result: the unwind is driven by the bs_dev destroy callback instead. */
static void
lvs_unload_ignored(void *cb_arg, int lvserrno)
{
	(void)cb_arg;

	if (lvserrno != 0) {
		SPDK_ERRLOG("Failed to unload the lvstore while unwinding: %s\n",
			    spdk_strerror(-lvserrno));
	}
}

static void
s3lvol_lvs_load_cb(void *cb_arg, struct spdk_lvol_store *lvol_store, int lvserrno)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;

	if (lvserrno != 0) {
		SPDK_ERRLOG("Failed to load lvstore '%s': %s\n",
			    lvs->name, spdk_strerror(-lvserrno));
		/* Every failure path inside spdk_lvs_load_ext() ends in
		 * spdk_bs_load()/spdk_bs_unload() destroying the bs_dev, exactly
		 * like spdk_lvs_init(). So do not destroy it again; wait for the
		 * destroy callback, which is what says the journal and the local
		 * device may be released. */
		ctx->status = lvserrno;
		if (lvs->bs_dev) {
			return;
		}
		/* No bs_dev means no teardown completion is coming. During
		 * recovery validation this still has to complete the finalizer
		 * step, not the setup funnel. */
		lvs_setup_fail_step(ctx, lvserrno);
		return;
	}

	/* Second identity check. It can only happen here: the uuid lives in
	 * blobstore's own metadata.
	 *
	 * All-zeroes means the create crashed between formatting the disk and
	 * recording the uuid, which is a legitimate state -- accept it and repair
	 * it below rather than refuse an otherwise healthy pair. */
	if (!spdk_uuid_is_null(&ctx->expect_uuid) &&
	    spdk_uuid_compare(&ctx->expect_uuid, &lvol_store->uuid) != 0) {
		char want[SPDK_UUID_STRING_LEN], got[SPDK_UUID_STRING_LEN];

		spdk_uuid_fmt_lower(want, sizeof(want), &ctx->expect_uuid);
		spdk_uuid_fmt_lower(got, sizeof(got), &lvol_store->uuid);

		SPDK_ERRLOG("local device '%s' belongs to lvstore uuid %s, but "
			    "'%s' in the bucket is %s -- the lvstore was most "
			    "likely destroyed and recreated under the same name "
			    "while this device was kept. Refusing to attach; the "
			    "log on this device has already been replayed into "
			    "memory but the device itself is untouched.\n",
			    ctx->wal_bdev_name, want, lvs->name, got);

		lvs->lvs = NULL;
		ctx->status = -EINVAL;
		/* Unloading is what destroys the bs_dev, and the destroy callback
		 * is still ours, so the unwind continues from there.
		 * spdk_lvs_unload() also frees lvol_store, hence not storing it.
		 *
		 * The setup funnel, not lvs_setup_fail_step(): the lvstore is
		 * being unloaded here, so the ordered unwind would try to unload
		 * it a second time. */
		if (spdk_lvs_unload(lvol_store, lvs_unload_ignored, NULL) != 0) {
			/* It refused, so no bs_dev teardown is coming from it and
			 * nothing else will ever complete the unwind. During the
			 * recovery validate step the outcome still belongs to the
			 * finalization state machine, so it must go through the
			 * step completion -- the setup funnel would free this
			 * context while the run object still points at it. */
			lvs_setup_fail_step(ctx, -EINVAL);
		}
		return;
	}

	lvs->lvs = lvol_store;

	/* Kept for s3lvol_lvstore_free(): the unload clears lvs->lvs before the
	 * free runs, and the free is where this lvstore's pending-delete marks
	 * are dropped. */
	spdk_uuid_copy(&lvs->uuid, &lvol_store->uuid);

	/* Recovery only: the lvstore is now live in memory, so a later failure
	 * in this chain (an lvol that will not open, a registry load) must be
	 * torn down by the ordered unwind rather than by the setup funnel. The
	 * funnel only knows about the bs_dev and would leave the lvol_store and
	 * every opened lvol behind. Attach and create keep the funnel: they
	 * never set this, so their behaviour is unchanged. */
	if (ctx->recovering) {
		ctx->recover_loaded = true;
	}

	/* The create context is about to go away, so drop its destroy callback;
	 * the unload path registers its own. */
	s3_bs_dev_set_destroy_cb(lvs->bs_dev, NULL, NULL);

	/* Recover the generated blobstore name from blobstore's own metadata: an
	 * attach never generated one, and spdk_lvs_load_ext() has just read it
	 * back from the super block. Without this lvs->bs_name stays NULL for the
	 * whole life of an attached lvstore. */
	if (!lvs->bs_name) {
		lvs->bs_name = strdup(lvol_store->name);
	}

	/* Re-record the entry rather than assume one is there.
	 *
	 * An attach is the point where this process learns that the lvstore exists
	 * and which namespace and WAL bdev reach it, which is exactly what the
	 * entry holds -- so writing it here makes the file self-healing if it was
	 * lost, and refreshes wal_bdev when an attach uses a different one from
	 * the create. */
	bstore_save_entry(lvs->name, lvs->bs_name, lvs->ns_name,
			  ctx->wal_bdev_name);

	/* Opening the lvols is asynchronous, so the rest of the attach continues
	 * from lvs_attach_lvols_done(). */
	lvs_attach_open_lvols(ctx);
}

/* The imports registry is in memory; blobstore may now ask for esnap parents. */
static void lvs_attach_exports_loaded(void *cb_arg, int status);

static void
lvs_attach_imports_loaded(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	int rc;

	if (status != 0) {
		SPDK_ERRLOG("Failed to read the imports registry of '%s': %s. Not "
			    "loading: an esnap clone whose manifest is unknown would "
			    "either fail to open or, worse, be opened against nothing.\n",
			lvs->name, spdk_strerror(-status));
		lvs_setup_fail_step(ctx, status);
		return;
	}

	/* And what this lvstore has published. Not needed by blobstore, so the
	 * ordering here is arbitrary -- but it has to be in memory before any RPC can
	 * delete an lvol, because it is what says which snapshots are spoken for. */
	rc = s3lvol_xfer_exports_load(lvs, lvs_attach_exports_loaded, ctx);
	if (rc != 0) {
		lvs_setup_fail_step(ctx, rc);
	}
}

static void
lvs_attach_exports_loaded(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	struct spdk_lvs_opts lvs_opts;

	if (status != 0) {
		SPDK_ERRLOG("Failed to read the imports registry of '%s': %s. Not "
			    "loading: an esnap clone whose manifest is unknown would "
			    "either fail to open or, worse, be opened against nothing.\n",
			lvs->name, spdk_strerror(-status));
		lvs_setup_fail_step(ctx, status);
		return;
	}

	/* Registered before spdk_lvs_load_ext() because all of its failure paths
	 * destroy the bs_dev, and that teardown is asynchronous. */
	s3_bs_dev_set_destroy_cb(lvs->bs_dev, lvs_setup_bs_dev_gone, ctx);

	/* The only member that matters on load: the name, the uuid and the cluster
	 * size all come from blobstore's own metadata, and lvs_load() reads nothing
	 * else out of these opts.
	 *
	 * esnap_bs_dev_create is *not* optional. Without it blobstore refuses to
	 * open any esnap clone it finds, which is the whole lvstore failing to load
	 * -- and that is the good outcome; the alternative would be clones that
	 * read as zeroes. */
	spdk_lvs_opts_init(&lvs_opts);
	lvs_opts.esnap_bs_dev_create = s3lvol_esnap_dev_create;

	spdk_lvs_load_ext(lvs->bs_dev, &lvs_opts, s3lvol_lvs_load_cb, ctx);
}

static void
lvs_attach_start_blobstore(struct lvs_setup_ctx *ctx)
{
	int rc;

	/* Fetch the manifests *first*. blobstore asks for the parent of each esnap
	 * clone synchronously while loading, and that can only be answered out of
	 * memory: waiting there for an S3 GET would deadlock the very thread that
	 * has to poll for its completion. */
	rc = s3lvol_xfer_imports_load(ctx->lvs, lvs_attach_imports_loaded, ctx);
	if (rc != 0) {
		lvs_setup_fail_step(ctx, rc);
	}
}

static void
lvs_attach_wal_replayed(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3_wal_stats st = {0};

	if (status != 0) {
		SPDK_ERRLOG("WAL replay failed on '%s': %d\n",
			    ctx->wal_bdev_name, status);
		lvs_setup_fail(ctx, status);
		return;
	}

	s3_wal_get_stats(ctx->lvs->wal, &st);
	SPDK_NOTICELOG("lvstore '%s': replayed %" PRIu64 " WAL entries, "
		       "dropped %" PRIu64 " unclosed batch(es)\n",
		       ctx->lvs->name, st.replayed_entries,
		       st.dropped_batches);

	lvs_attach_start_blobstore(ctx);
}

static void
lvs_attach_journal_replayed(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	int rc;

	if (status != 0) {
		SPDK_ERRLOG("Journal replay failed on '%s': %d\n",
			    ctx->wal_bdev_name, status);
		lvs_setup_fail(ctx, status);
		return;
	}

	/* From here on the bs_dev owns closing the WAL. It also has to happen
	 * before the replay below: replayed entries go into the overlay, which
	 * this call creates. */
	rc = s3_bs_dev_attach_wal(lvs->bs_dev, lvs->wal, NULL);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to attach the WAL to '%s': %d\n",
			    lvs->name, rc);
		lvs_setup_fail(ctx, rc);
		return;
	}

	s3_wal_replay(lvs->wal, (s3_wal_replay_cb)s3_bs_dev_wal_apply, lvs->bs_dev,
		      lvs_attach_wal_replayed, ctx);
}

static void lvs_attach_checkpoint_loaded(void *cb_arg, uint64_t lsn,
					 uint64_t gen, int status);

static void
lvs_attach_wal_opened(void *cb_arg, struct s3_wal *wal, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	int rc;

	if (status != 0) {
		SPDK_ERRLOG("Failed to open the WAL on '%s': %d\n",
			    ctx->wal_bdev_name, status);
		lvs_setup_fail(ctx, status);
		return;
	}

	lvs->wal = wal;

	rc = s3_bs_dev_create(&ctx->opts, NULL, NULL, lvs->client,
			      ctx->opts.capacity_bytes, &lvs->bs_dev);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to create bs_dev for '%s': %d\n", lvs->name, rc);
		lvs_setup_fail(ctx, rc);
		return;
	}

	rc = s3_bs_dev_attach_journal(lvs->bs_dev, lvs->journal,
					      lvs->local_dev);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to attach the journal to '%s': %d\n",
			    lvs->name, rc);
		lvs_setup_fail(ctx, rc);
		return;
	}

	/* Deliberately not checked; see the create path. Doing it here rather than
	 * after replay only means the cache is available to the very first read,
	 * which is blobstore's super block. */
	(void)s3_bs_dev_attach_cache(lvs->bs_dev);

	/* Rebuild the table from the snapshot before replaying anything.
	 *
	 * *Skipping this loses data as soon as any checkpoint has ever run.* Once
	 * checkpoint_lsn > 0 the journal no longer holds the records at or below
	 * it -- they were truncated -- so those mappings exist only in the
	 * snapshot. Replaying the journal alone would come up with a table missing
	 * everything older than the last checkpoint, and every S3 object behind
	 * those mappings would be unreachable. */
	s3_checkpoint_load(lvs->client, lvs->name,
			   s3_bs_dev_get_chunk_map(lvs->bs_dev),
			   lvs_attach_checkpoint_loaded, ctx);
}

static void
lvs_attach_checkpoint_loaded(void *cb_arg, uint64_t lsn, uint64_t gen,
			     int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;

	if (status != 0) {
		SPDK_ERRLOG("Failed to load the checkpoint for '%s': %d\n",
			    lvs->name, status);
		lvs_setup_fail(ctx, status);
		return;
	}

	/* Replay resumes from what the snapshot actually covers, not from what the
	 * super block claims.
	 *
	 * They agree in normal operation, because a checkpoint uploads the snapshot
	 * before updating the super block. A disagreement means a crash landed
	 * between those two steps, and then the *snapshot* is the authority on what
	 * has been restored -- resuming from the larger of the two would skip
	 * journal records the snapshot never captured. */
	if (ctx->from_lsn != lsn) {
		SPDK_NOTICELOG("lvstore '%s': the super block records checkpoint "
			       "LSN %" PRIu64 " but the snapshot in S3 covers %"
			       PRIu64 "; replaying from the snapshot's value\n",
			       lvs->name, ctx->from_lsn, lsn);
	}
	if (lsn > ctx->from_lsn) {
		/* The snapshot is *ahead* of the super block: the upload landed and
		 * the super block update did not. Harmless -- the journal still
		 * holds everything from the super block's LSN onwards, so replaying
		 * from the snapshot's higher LSN skips nothing. */
		SPDK_NOTICELOG("lvstore '%s': checkpoint gen=%" PRIu64 " is newer "
			       "than the super block; a crash interrupted the last "
			       "checkpoint after the upload\n", lvs->name, gen);
	}
	ctx->from_lsn = lsn;

	/* The journal first: it says which S3 objects exist, and the WAL replay
	 * then re-applies whatever had not reached them. */
	s3_journal_replay(lvs->journal, ctx->from_lsn,
			  (s3_journal_apply_cb)s3_bs_dev_journal_apply,
			  lvs->bs_dev, lvs_attach_journal_replayed, ctx);
}

/* Everything the attach chain does after it owns the lvstore. */
static void
lvs_attach_after_owner(struct lvs_setup_ctx *ctx)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	int rc;

	rc = s3_journal_open(lvs->local_dev, &lvs->journal);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to open the journal on '%s': %d\n",
			    ctx->wal_bdev_name, rc);
		lvs_setup_fail(ctx, rc);
		return;
	}

	s3_wal_open(lvs->local_dev, lvs_attach_wal_opened, ctx);
}

static void
lvs_attach_local_dev_opened(void *cb_arg, struct s3_local_dev *dev, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	const struct s3_super_block *sb;

	if (status != 0) {
		SPDK_ERRLOG("Failed to open the local device '%s': %d\n",
			    ctx->wal_bdev_name, status);
		lvs_setup_fail(ctx, status);
		return;
	}

	lvs->local_dev = dev;
	sb = s3_local_dev_get_super(dev);

	/* First identity check, and the only one that happens before any replay.
	 * The S3 key prefix is derived from the name, so a mismatch means this
	 * disk's log describes a different lvstore and replaying it would write
	 * foreign data into this one. */
	if (strcmp(sb->lvs_name, lvs->name) != 0) {
		SPDK_ERRLOG("local device '%s' belongs to lvstore '%s', not '%s' "
			    "-- refusing to attach\n",
			    ctx->wal_bdev_name, sb->lvs_name, lvs->name);
		lvs_setup_fail(ctx, -EINVAL);
		return;
	}
	if (sb->capacity_bytes == 0) {
		SPDK_ERRLOG("local device '%s' records no capacity; it was "
			    "formatted by an older build and cannot be attached\n",
			    ctx->wal_bdev_name);
		lvs_setup_fail(ctx, -EPROTO);
		return;
	}

	/* Geometry comes from the disk, never from the RPC. Handing blobstore a
	 * different capacity than it was created with either fails the load
	 * (-EILSEQ, when smaller) or quietly invites a grow (when larger), and a
	 * typo in an RPC parameter should not be able to do either. */
	ctx->opts.capacity_bytes = sb->capacity_bytes;
	ctx->opts.chunk_size     = sb->chunk_size;
	ctx->from_lsn            = sb->checkpoint_lsn;
	spdk_uuid_copy(&ctx->expect_uuid, &sb->lvs_uuid);

	/* This process retains the claim for this name after a failed teardown:
	 * the ordinary acquire would (correctly) see its own held marker as busy.
	 * Reuse it only for the same device, after verifying the marker. */
	if (lvs_retained_find(&g_retained, lvs->name)) {
		lvs_attach_with_retained(ctx, sb);
		return;
	}

	/* Claim the lvstore now: after the local device has been read, because the
	 * marker records which lvstore uuid we think we are attaching, but *before*
	 * the WAL is attached and the flusher starts uploading. Anything later
	 * would mean a second owner had already written to S3 by the time it was
	 * told it was not the owner. */
	lvs_owner_acquire(ctx, &sb->lvs_uuid, ctx->force);
}

int
s3lvol_lvstore_attach(const struct s3_lvs_opts *opts, s3lvol_lvs_op_cb cb_fn,
		      void *cb_arg)
{
	struct lvs_setup_ctx *ctx;
	int rc;

	if (!opts || !opts->lvs_name) {
		return -EINVAL;
	}
	rc = lvs_name_ok(opts->lvs_name);
	if (rc != 0) {
		return rc;
	}
	/* A local device is not optional here. Without one there is no journal,
	 * so the chunk map cannot be rebuilt, and every object in the bucket is
	 * an orphan: the data is there but nothing maps an LBA to it. Attaching
	 * "without a WAL" would produce an lvstore that reads as zeroes. */
	if (!opts->wal_bdev_name) {
		SPDK_ERRLOG("attaching lvstore '%s' needs the wal_bdev it was "
			    "created with: the chunk map lives in that device's "
			    "journal and cannot be reconstructed from S3\n",
			    opts->lvs_name);
		return -EINVAL;
	}
	if (s3lvol_lvstore_find(opts->lvs_name)) {
		SPDK_ERRLOG("lvstore '%s' is already attached\n", opts->lvs_name);
		return -EEXIST;
	}

	rc = lvs_setup_begin(opts, cb_fn, cb_arg, &ctx);
	if (rc != 0) {
		return rc;
	}
	ctx->attaching = true;
	ctx->force = opts->force;

	s3_local_dev_open(ctx->wal_bdev_name, ctx->cache_bdev_name,
			  lvs_attach_local_dev_opened, ctx);
	return 0;
}

/* ==========================================================================
 * lvstore recovery -- rebuild the local state from the remote checkpoint
 *
 * The chain, and why each step is where it is:
 *
 *   s3_client_get_or_create
 *   -> s3_recovery_load ........ descriptor: geometry, identity, checkpoint gen
 *   -> s3_recovery_ckpt_preflight ... download + validate the *named* checkpoint
 *   -> s3_owner_acquire ........ claim before touching anything (force refused)
 *   -> s3_recovery_claim_open .. exclusive local claim on wal (and cache) bdev
 *   -> s3_recovery_blank_scan_claimed .. positive zero scan through that claim
 *   -> s3_local_dev_format_staged ... STAGING layout, on the *same* descriptor
 *   -> s3_local_dev_set_lvs_uuid ... identity, while still staged
 *   -> s3_journal_create ....... empty journal: there is nothing to replay
 *   -> s3_wal_create ........... empty WAL: same
 *   -> s3_bs_dev_create
 *   -> s3_bs_dev_attach_journal
 *   -> s3_checkpoint_apply ..... the bytes validated by the preflight
 *   -> s3_local_dev_update_checkpoint ... record what was restored
 *   -> s3_recovery_finalize_start ... the ordering from here on:
 *        validate  s3_bs_dev_attach_wal, spdk_lvs_load_ext, open every lvol
 *                  (nothing registered)
 *        drain     s3_bs_dev_drain
 *        preflush  s3_local_dev_flush_durable
 *        commit    s3_local_dev_commit_staged (preflush + write + postflush)
 *        final     s3_local_dev_flush_durable
 *        publish   vbdev_s3lvol_bdev_register for every lvol
 *
 * === Why the commit is not the end of the chain ===
 *
 * It used to be. The commit ran from the checkpoint-recorded callback and the
 * load ran from the commit callback, so a failure after the commit left a
 * valid, attachable super block on a device whose blobstore had never been
 * loaded. A later attach accepted it and replayed from a checkpoint pointer
 * describing a state that was never rebuilt.
 *
 * The commit is now the *last local write*, and publication (bdev
 * registration) happens only after it and after a final flush. Everything that
 * can fail -- the load, every lvol open, the drain, the preflushes -- happens
 * before the commit, so a failure there leaves the device STAGED and refused
 * by every reader.
 *
 * === Why the layout is staged rather than formatted normally ===
 *
 * The normal formatter writes an attachable super block as its *first* write.
 * On this chain that is the one thing that must not happen: every step after it
 * can fail, and a crash between them would leave a device carrying a valid
 * normal super block whose journal, WAL, checkpoint pointer and uuid were never
 * rebuilt. A later attach would accept it and replay from a checkpoint pointer
 * that describes data that is not there -- silently, because the block is
 * valid.
 *
 * The staged formatter writes a block with a *different magic and version*
 * instead. Every reader refuses it: this build reports -EAGAIN ("staged, retry
 * or recover"), an older build fails its magic check with -EILSEQ. Only after
 * every prerequisite write has completed and been flushed does
 * s3_local_dev_commit_staged() overwrite that block with the normal one and
 * flush it. The commit is the last local write, and its callback is the first
 * point at which the device is attachable.
 *
 * What that does not buy: atomicity of the 4 KiB commit overwrite against
 * arbitrary power loss. A torn commit block fails both its magic and its CRC
 * and is refused, which is detection, not atomicity. See the header comment on
 * s3_local_dev_commit_staged().
 *
 * === Why the checkpoint is downloaded before the owner is claimed ===
 *
 * The descriptor and the checkpoint are two objects, published at two different
 * moments. A descriptor can name a generation the checkpoint no longer has (or
 * name an object whose bytes were replaced). Validating the pair *before* the
 * claim means a mismatch fails with nothing written anywhere -- no marker to
 * release, no device touched. The validated bytes are kept and applied later,
 * so the restore cannot silently use a different object than the one checked.
 *
 * === Why the local device is proven blank before it is formatted ===
 *
 * Formatting overwrites the super block and zeroes the journal head. If the
 * device already carries a layout, that journal may hold mappings that no
 * checkpoint covers, and destroying them to "recover" would be the worst
 * possible outcome.
 *
 * The proof is a *positive* one: every byte of the designated device (and of a
 * separate cache bdev, when there is one) is read and required to be zero. The
 * earlier version of this chain called s3_local_dev_open() and treated -EILSEQ
 * as "no layout" -- but that errno is also what a corrupt super block with the
 * right magic, or a valid layout from a newer build, produces. A parse failure
 * is not evidence of blankness, so no parse is involved any more.
 *
 * === What the local claim closes, and what it does not ===
 *
 * The blank scan and the format used to be two independent opens: the scan
 * closed its descriptor before the callback, and the formatter opened its own.
 * Between them the device was unclaimed, so any other writer in this process
 * could open it, write, and leave the format running over a device that was no
 * longer blank. That window is now closed: both steps run on one descriptor per
 * device, with an exclusive claim held from before the scan until after the
 * format (s3_recovery_claim_open / s3_recovery_blank_scan_claimed /
 * fopts.wal_desc).
 *
 * What that claim is worth, precisely:
 *
 *   - it refuses to be granted while another descriptor in this process has
 *     the bdev open for writing (SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE,
 *     claim_verify_rwo in bdev.c), so a pre-existing writer is detected rather
 *     than ignored;
 *   - once held, a later writable open fails with -EPERM and a second claim
 *     fails with -EPERM, so no new writer can appear in this process.
 *
 * What it is not:
 *
 *   - it is not process-external exclusion. Another process, or the host, can
 *     still write the backing file of an aio bdev. SPDK has no fence for that.
 *   - the owner marker is a sequential-misoperation guard, not mutual
 *     exclusion (s3_owner.h), so it does not add any of that back.
 *
 * A dual-bdev layout takes a second claim on the cache device. The formatter
 * adopts that claim's descriptor as well: reopening the cache for writing while
 * it is claimed would fail with -EPERM, so consuming the retained descriptor is
 * required for dual-bdev recovery to work at all, not just an optimisation.
 * Both claims move into the local device at format completion and are released
 * by s3_local_dev_close() after its channels are put.
 *
 * That is why the API is still gated off behind -ENOTSUP in
 * s3lvol_lvstore_recover(): the local claim removes the in-process TOCTOU, and
 * the remaining threats are exactly the ones a local claim cannot address.
 *
 * === Why the checkpoint is loaded before blobstore ===
 *
 * Same reason as attach: blobstore reads its super block as its first action,
 * and that read has to already see the recovered mapping. There is no way to
 * tell it "reload, the device changed".
 *
 * === Why the journal and WAL are created empty rather than replayed ===
 *
 * There is nothing to replay. The local device is new, so its journal and WAL
 * are empty by construction. Creating them (rather than opening) is what makes
 * that explicit: an open would scan a region that was never written.
 * ========================================================================== */

static void lvs_recover_after_owner(struct lvs_setup_ctx *ctx);
static void lvs_recover_checkpoint_recorded(void *cb_arg, int status);
static void lvs_recover_uuid_recorded(void *cb_arg, int status);
static void lvs_recover_owner_acquired(void *cb_arg,
				       const struct s3_owner_info *holder,
				       int status);

/* One designated device passed its zero scan. The cache bdev is a second
 * designated device when it is distinct, and it is scanned too: format can
 * overwrite it, so "the WAL device is blank" is not enough.
 *
 * Both devices are claimed *before* either is scanned (see
 * lvs_recover_owner_acquired), so a failure here leaves both claims held and
 * the unwind releases them; nothing is formatted. */
static void
lvs_recover_blank_scanned(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		/* Not zero-filled, unreadable, or not a usable bdev. None of
		 * those may be formatted over. Note that this is reached only
		 * when the positive scan failed: a parse error is never an
		 * input to this decision. */
		SPDK_ERRLOG("lvstore '%s': refusing to recover -- the designated "
			    "local device is not provably blank (%s)\n",
			    ctx->lvs->name, spdk_strerror(-status));
		lvs_setup_fail(ctx, status);
		return;
	}

	if (!ctx->recover_wal_proven_blank) {
		ctx->recover_wal_proven_blank = true;

		if (ctx->recover_cache_claim) {
			/* Same claim, same descriptor, same proof -- only the
			 * device differs. */
			s3_recovery_blank_scan_claimed(ctx->recover_cache_claim,
						       lvs_recover_blank_scanned,
						       ctx);
			return;
		}
	}

	lvs_recover_after_owner(ctx);
}

/* The descriptor and the checkpoint agree with each other. Only now is the
 * owner marker claimed. */
static void
lvs_recover_ckpt_validated(void *cb_arg, void *ckpt, uint64_t ckpt_len,
			   int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("lvstore '%s': the remote checkpoint does not match "
			    "the recovery descriptor (%s); nothing has been "
			    "claimed or written\n", ctx->lvs->name,
			    spdk_strerror(-status));
		lvs_setup_fail(ctx, status);
		return;
	}

	/* Kept until the chain applies it, so the state restored is the state
	 * that passed the preflight rather than a second download. */
	ctx->recover_ckpt = ckpt;
	ctx->recover_ckpt_len = ckpt_len;

	/* force=false, deliberately: recovery refuses force outright, and
	 * strict mode refuses it too. */
	lvs_owner_acquire_strict_only(ctx, &ctx->recover_uuid);
}

static void
lvs_recover_checkpoint_loaded(void *cb_arg, uint64_t lsn, uint64_t gen,
			      int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;

	if (status != 0) {
		SPDK_ERRLOG("lvstore '%s': could not load the remote checkpoint: "
			    "%s\n", lvs->name, spdk_strerror(-status));
		lvs_setup_fail(ctx, status);
		return;
	}

	/* The descriptor named a generation and LSN; the checkpoint that arrived
	 * must be that one. A mismatch means the descriptor and the checkpoint
	 * are from different points in time -- a publication that raced, or an
	 * object replaced underneath us -- and restoring it would silently give
	 * the caller a different state than the one they validated. */
	if (gen != ctx->recover_gen || lsn != ctx->recover_lsn) {
		SPDK_ERRLOG("lvstore '%s': the recovery descriptor names "
			    "checkpoint gen=%" PRIu64 " lsn=%" PRIu64 ", but the "
			    "checkpoint in the bucket is gen=%" PRIu64
			    " lsn=%" PRIu64 "; refusing to restore a state the "
			    "descriptor does not describe\n",
			    lvs->name, ctx->recover_gen, ctx->recover_lsn, gen,
			    lsn);
		lvs_setup_fail(ctx, -EINVAL);
		return;
	}

	/* Record what was restored in the new super block, so a later attach
	 * replays from here rather than from zero. This is the step that makes
	 * the recovered device a normal device. */
	s3_local_dev_update_checkpoint(lvs->local_dev, gen, lsn,
				       lvs_recover_checkpoint_recorded, ctx);
}

/* ==========================================================================
 * Recovery finalization -- the ordering from "loaded" to "published"
 *
 * The chain used to commit the local device *before* loading blobstore, so a
 * failure after the commit left a valid, attachable super block on a device
 * whose lvstore had never been loaded. The ordering now lives in
 * s3_recovery_finalize.c, which the tests drive directly:
 *
 *   validate (load blobstore, open every lvol, register nothing)
 *   -> drain (everything acknowledged reaches S3)
 *   -> preflush (local prerequisites durable)
 *   -> commit (normal super block, its own preflush + postflush)
 *   -> final flush (the publication itself is durable)
 *   -> publish (register the bdevs)
 *
 * The three callbacks below are the caller's side of that state machine. They
 * are the *only* place the recovery chain touches the local device's
 * publication state, and none of them registers a bdev before the commit.
 * ========================================================================== */

/* ==========================================================================
 * Recovery unwind -- releasing what a failed rebuild acquired
 *
 * The setup funnel (lvs_setup_fail) is the right teardown for a chain that
 * never got as far as loading blobstore: it destroys the bs_dev and releases
 * the local device, and there is nothing else to release.
 *
 * It is the wrong teardown once blobstore is loaded. At that point the chain
 * owns a set of open lvols, a live lvol_store, and possibly some registered
 * bdevs. Destroying the bs_dev underneath them leaves every lvol open (so the
 * lvstore can never be unloaded), leaks the lvol_store, and leaves any
 * registered bdev pointing at a device that is about to be freed.
 *
 * So the failure path below releases in the order SPDK requires:
 *
 *   every lvol (unregister if it has a bdev, else close)
 *   -> spdk_lvs_unload, which destroys the bs_dev
 *   -> report the original failure
 *
 * The ordering, the barrier and the exactly-once report live in
 * s3_recovery_unwind.c, which the tests drive directly. What is here is only
 * the SPDK side: which primitive each item needs, and what to do with the
 * result.
 * ========================================================================== */

/* The unwind finished. `released` is false when a release or the unload
 * failed, in which case the lvstore is still reachable from a live SPDK object
 * and must not be freed. */
static void
lvs_recover_unwound(void *cb_arg, int status, bool released)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;

	if (!released) {
		/* Reported rather than papered over. The caller is told the
		 * rebuild failed, and the lvstore is deliberately retained: it
		 * is still referenced by SPDK, so freeing it here would be a
		 * use-after-free, and dropping the reference silently would
		 * hide a leak that an operator needs to see. */
		SPDK_ERRLOG("lvstore '%s': recovery failed (%s) and its cleanup "
			    "did not complete; the lvstore is retained, not "
			    "freed. Its lvols and backing device are still "
			    "allocated\n",
			    lvs->name, spdk_strerror(-status));
		ctx->retain_lvs = true;
		ctx->status = status;
		/* The user's callback still has to run: the rebuild failed and
		 * the caller must not treat the lvstore as published. */
		lvs_setup_report(ctx);
		return;
	}

	/* Everything is gone: the lvols are closed, the lvstore is unloaded and
	 * the bs_dev with it. Only the local device, the journal and the WAL
	 * remain, and lvs_setup_report() releases those. */
	lvs->lvs = NULL;
	lvs->bs_dev = NULL;
	lvs->wal = NULL;

	ctx->status = status;
	lvs_setup_report(ctx);
}

static void
lvs_recover_unwind_unregister(void *cb_arg, void *handle,
			      void (*done)(void *arg, int status),
			      void *done_arg)
{
	struct spdk_lvol *lvol = handle;

	/* The bdev layer's destructor closes the lvol, so this is the only
	 * close this lvol gets. */
	vbdev_s3lvol_bdev_unregister(lvol, done, done_arg);
}

static void
lvs_recover_unwind_close(void *cb_arg, void *handle,
			 void (*done)(void *arg, int status),
			 void *done_arg)
{
	struct spdk_lvol *lvol = handle;

	/* Never registered, so nothing else will close it. spdk_lvol_close()
	 * answers -EINVAL when ref_count is already 0, which the unwind records
	 * as a cleanup failure rather than ignoring. */
	spdk_lvol_close(lvol, done, done_arg);
}

/* Complete the unwind's unload step once it is really over.
 *
 * spdk_lvs_unload()'s completion is not the end: it destroys the bs_dev, whose
 * teardown (deferred deletes, cache fill, flusher drain, WAL close) continues in
 * the background and still writes to S3 and to the local device. Completing
 * the step before that would let lvs_setup_report() close the local device
 * under live I/O and release the owner marker while this process is still
 * writing. A failed unload destroys nothing, so it completes on its own. */
static void
lvs_recover_unwind_finish_unload(struct lvs_setup_ctx *ctx)
{
	void (*done)(void *arg, int status);
	void *done_arg;

	if (ctx->unwind_in_submit || !ctx->unwind_lvs_done) {
		return;
	}
	if (ctx->unwind_lvs_status == 0 && !ctx->unwind_bs_dev_done) {
		return;
	}
	done = ctx->unwind_done;
	done_arg = ctx->unwind_done_arg;
	ctx->unwind_done = NULL;
	ctx->unwind_done_arg = NULL;
	if (done) {
		/* A teardown failure is not reported here: the resources are
		 * gone either way. It is kept in ctx->teardown_status, which
		 * makes lvs_setup_report() retain the owner marker. */
		done(done_arg, ctx->unwind_lvs_status);
	}
}

static void
lvs_recover_unwind_bs_dev_gone(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0 && ctx->teardown_status == 0) {
		ctx->teardown_status = status;
	}
	ctx->unwind_bs_dev_done = true;
	lvs_recover_unwind_finish_unload(ctx);
}

static void
lvs_recover_unwind_lvs_unloaded(void *cb_arg, int lvserrno)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (ctx->unwind_lvs_done) {
		/* spdk_lvs_unload() reports -EBUSY through both the callback
		 * and its return value; the first report wins. */
		return;
	}
	ctx->unwind_lvs_done = true;
	ctx->unwind_lvs_status = lvserrno;
	if (lvserrno != 0 && ctx->lvs->bs_dev) {
		/* Nothing was torn down, so no destroy callback is coming; if
		 * the bs_dev is destroyed later it must not reach this context. */
		s3_bs_dev_set_destroy_cb(ctx->lvs->bs_dev, NULL, NULL);
	}
	lvs_recover_unwind_finish_unload(ctx);
}

static void
lvs_recover_unwind_unload(void *cb_arg,
			  void (*done)(void *arg, int status),
			  void *done_arg)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	int rc;

	ctx->unwind_done = done;
	ctx->unwind_done_arg = done_arg;
	ctx->unwind_lvs_done = false;
	ctx->unwind_lvs_status = 0;
	ctx->unwind_bs_dev_done = (lvs->bs_dev == NULL);

	/* Registered before the unload, which is what destroys the bs_dev. */
	if (lvs->bs_dev) {
		s3_bs_dev_set_destroy_cb(lvs->bs_dev,
					 lvs_recover_unwind_bs_dev_gone, ctx);
	}

	/* Every lvol has been released, so this cannot fail with -EBUSY for
	 * that reason. Completions that arrive while it is on the stack are
	 * recorded and acted on after it returns: acting inline could free
	 * this context underneath the call. */
	ctx->unwind_in_submit = true;
	rc = spdk_lvs_unload(lvs->lvs, lvs_recover_unwind_lvs_unloaded, ctx);
	ctx->unwind_in_submit = false;

	if (rc != 0) {
		SPDK_ERRLOG("lvstore '%s': spdk_lvs_unload refused during the "
			    "recovery unwind (%s)\n", lvs->name, spdk_strerror(-rc));
		if (!ctx->unwind_lvs_done) {
			ctx->unwind_lvs_done = true;
			ctx->unwind_lvs_status = rc;
		}
		if (lvs->bs_dev) {
			s3_bs_dev_set_destroy_cb(lvs->bs_dev, NULL, NULL);
		}
	}
	lvs_recover_unwind_finish_unload(ctx);
}

/* Start the unwind for a failed rebuild that got as far as loading blobstore.
 *
 * `status` is the failure being unwound from; it is what the caller is told.
 * The item list is built here from the lvols the validate step opened, so the
 * ownership decision (registered or not) is taken from the lvol itself at the
 * moment the unwind starts, not from a flag that a failed publish step may have
 * left half-updated. */
static void
lvs_recover_unwind(struct lvs_setup_ctx *ctx, int status)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	struct s3_recovery_unwind_ops ops = {
		.unregister = lvs_recover_unwind_unregister,
		.close      = lvs_recover_unwind_close,
		.unload     = lvs_recover_unwind_unload,
		.report     = lvs_recover_unwound,
		.cb_arg     = ctx,
	};
	struct s3_recovery_unwind_item *items = NULL;
	struct spdk_lvol *lvol;
	size_t count = 0;
	size_t i = 0;
	int rc;

	if (!lvs->lvs) {
		/* Blobstore never loaded, so there is nothing this unwind owns.
		 * The setup funnel is the correct teardown. */
		lvs_setup_fail(ctx, status);
		return;
	}

	TAILQ_FOREACH(lvol, &lvs->lvs->lvols, link) {
		count++;
	}

	if (count > 0) {
		items = calloc(count, sizeof(*items));
		if (!items) {
			/* Cannot build the release list. Retaining the lvstore is
			 * the only safe outcome: freeing it would leave SPDK
			 * objects pointing at freed memory. */
			SPDK_ERRLOG("lvstore '%s': out of memory building the "
				    "recovery unwind list; retaining the "
				    "lvstore\n", lvs->name);
			ctx->retain_lvs = true;
			ctx->status = status;
			lvs_setup_report(ctx);
			return;
		}
	}

	TAILQ_FOREACH(lvol, &lvs->lvs->lvols, link) {
		items[i].handle = lvol;
		items[i].registered = (lvol->bdev != NULL);
		i++;
	}

	rc = s3_recovery_unwind_start(&ops, items, count, status);
	if (rc != 0) {
		/* Nothing was started, so the list is still ours. */
		free(items);
		SPDK_ERRLOG("lvstore '%s': could not start the recovery unwind "
			    "(%s); retaining the lvstore\n",
			    lvs->name, spdk_strerror(-rc));
		ctx->retain_lvs = true;
		ctx->status = status;
		lvs_setup_report(ctx);
	}
}

/* The finalization run finished. Success means the lvstore is live. */
static void
lvs_recover_finalized(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;

	ctx->recover_finalize = NULL;

	if (status == 0 && lvs->bs_dev) {
		/* Published: the device is durable and every bdev is
		 * registered. Only now may automatic checkpoints run again. */
		s3_bs_dev_set_checkpoint_suppressed(lvs->bs_dev, false, NULL);
	}

	if (status != 0) {
		/* Two distinct outcomes, and the difference matters to the
		 * operator:
		 *
		 *   - before the commit: the device is still STAGED, every
		 *     reader refuses it, and a retry is safe;
		 *   - at or after the commit: the normal block may already be
		 *     visible. That is safe *only* because every prerequisite
		 *     was flushed before it was written, but it is not
		 *     something this code can prove after the fact, so it is
		 *     reported as an indeterminate publication rather than as
		 *     "nothing happened".
		 *
		 * Either way the lvstore is not published, and by this point
		 * blobstore is loaded and lvols are open, so the teardown is the
		 * ordered unwind rather than the setup funnel. */
		SPDK_ERRLOG("lvstore '%s': recovery finalization failed (%s); "
			    "the lvstore is NOT published. If the failure was at "
			    "or after the commit, the local device may already "
			    "carry a normal super block\n",
			    lvs->name, spdk_strerror(-status));
		lvs_recover_unwind(ctx, status);
		return;
	}

	ctx->status = 0;
	lvs_setup_report(ctx);
}

/* Step 1: load blobstore and open every lvol, registering nothing.
 *
 * This is the step the old chain got wrong: it registered bdevs from the lvol
 * open callback, which ran after the commit. Here the same callback only
 * counts, and a single unopenable lvol fails the whole rebuild.
 *
 * === Why every failure below goes through done ===
 *
 * The run object that owns this step is freed by the state machine as it
 * reports. A failure reported by any other route -- lvs_setup_fail(), which is
 * the setup funnel -- would free the setup context while the run still points
 * at it, and would never complete the run. So the callback is installed
 * *before* the first thing that can fail, and the attach chain's own failure
 * paths are redirected to it by lvs_setup_fail_step() while
 * ctx->recover_validating is set. */
static void
lvs_recover_validate(void *cb_arg, void (*done)(void *arg, int status),
		     void *done_arg)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	int rc;

	ctx->recover_validate_done = done;
	ctx->recover_validate_arg = done_arg;
	ctx->recover_validating = true;
	ctx->lvols_ok = 0;
	ctx->lvols_failed = 0;

	/* The WAL is attached before the load: it creates the overlay, and the
	 * overlay must exist before blobstore writes anything. Nothing is
	 * replayed into it -- it is empty -- but the write path needs it. */
	rc = s3_bs_dev_attach_wal(lvs->bs_dev, lvs->wal, NULL);
	if (rc != 0) {
		SPDK_ERRLOG("lvstore '%s': failed to attach the WAL during "
			    "recovery: %d\n", lvs->name, rc);
		lvs_recover_validate_complete(ctx, rc);
		return;
	}

	/* Same entry point as attach, but lvs_attach_lvol_opened() branches on
	 * ctx->recovering and does not register. */
	lvs_attach_start_blobstore(ctx);
}

/* Step 2: wait until everything acknowledged so far has reached S3.
 *
 * The WAL is empty by construction (recovery creates it, it does not replay
 * it), so this normally completes immediately. It is still a real step: the
 * checkpoint apply and the journal writes are acknowledged from the local
 * device, and the drain is what makes "nothing is still in flight" a checked
 * fact rather than an assumption before the commit.
 *
 * s3_bs_dev_drain() takes an s3_bs_dev_cb, whose signature differs from the
 * finalize step's done callback, so it is adapted here rather than cast. */
struct lvs_recover_drain_ctx {
	void (*done)(void *arg, int status);
	void *done_arg;
};

static void
lvs_recover_drained(void *cb_arg, int status)
{
	struct lvs_recover_drain_ctx *d = cb_arg;
	void (*done)(void *arg, int status) = d->done;
	void *done_arg = d->done_arg;

	free(d);
	done(done_arg, status);
}

static void
lvs_recover_drain(void *cb_arg, void (*done)(void *arg, int status),
		  void *done_arg)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	struct lvs_recover_drain_ctx *d;

	d = calloc(1, sizeof(*d));
	if (!d) {
		done(done_arg, -ENOMEM);
		return;
	}
	d->done = done;
	d->done_arg = done_arg;

	/* Completes inline when there is no flusher, which is the case here. */
	s3_bs_dev_drain(lvs->bs_dev, lvs_recover_drained, d);
	(void)ctx;
}

/* Step 3: register a bdev for every lvol the validate step opened.
 *
 * Runs only after the commit and the final flush succeeded, so a bdev can
 * never be visible for a device that is not yet durably normal.
 *
 * === Transient visibility, and why it is accepted here ===
 *
 * Registration is sequential, so between the first successful
 * spdk_bdev_register() and the last one the lvstore is *partly* visible: some
 * bdevs exist, others do not. There is no SPDK API that publishes a set of
 * bdevs atomically, and building one would mean a staging table in the bdev
 * layer -- a much larger change than this increment. So the window is real and
 * is documented rather than claimed away:
 *
 *   - during the window the lvstore is not in g_lvstores, so no RPC can find
 *     it and no new namespace can be attached to it;
 *   - a bdev that is visible is fully backed by a committed, durable device,
 *     so reading it is correct;
 *   - if a later registration fails, every bdev this step registered is
 *     unregistered again, in reverse order, and the run does not report until
 *     all of those unregisters have completed. Completion therefore means
 *     "all or none", even though the intermediate state was partial.
 *
 * === Why the rollback is here and not in the caller's unwind ===
 *
 * The caller's unwind releases *every* lvol, registered or not, and then
 * unloads the lvstore. That is the right teardown for a failed rebuild, but it
 * is not a rollback of this step: it would also close the lvols that were never
 * registered, which is correct only because the whole rebuild is being
 * abandoned. Keeping the rollback local means the publish step's own failure
 * leaves exactly the state it found -- nothing registered -- before the caller
 * decides what to do with the device. */
struct lvs_recover_publish_ctx {
	struct lvs_setup_ctx *ctx;
	void (*done)(void *arg, int status);
	void *done_arg;

	/* Every lvol this step registered, in registration order. */
	struct spdk_lvol **registered;
	size_t             nregistered;
	size_t             next_unregister;
	int                status;
};

static void lvs_recover_publish_unregister_next(struct lvs_recover_publish_ctx *p);

static void
lvs_recover_publish_unregistered(void *cb_arg, int bdeverrno)
{
	struct lvs_recover_publish_ctx *p = cb_arg;

	if (bdeverrno != 0) {
		/* The bdev layer refused to unregister. It stays visible, so
		 * the caller must not free the lvstore: the unwind's own
		 * unregister of the same lvol will report the same failure and
		 * retain ownership. */
		SPDK_ERRLOG("lvstore '%s': rolling back a recovery publication "
			    "failed to unregister a bdev (%s); the lvstore stays "
			    "partly visible\n",
			    p->ctx->lvs->name, spdk_strerror(-bdeverrno));
		if (p->status == 0) {
			p->status = bdeverrno;
		}
	}

	lvs_recover_publish_unregister_next(p);
}

static void
lvs_recover_publish_unregister_next(struct lvs_recover_publish_ctx *p)
{
	if (p->next_unregister > 0) {
		struct spdk_lvol *lvol = p->registered[--p->next_unregister];

		/* Asynchronous: the bdev's destructor closes the lvol, and the
		 * completion is what says that finished. */
		vbdev_s3lvol_bdev_unregister(lvol,
					     lvs_recover_publish_unregistered, p);
		return;
	}

	{
		void (*done)(void *arg, int status) = p->done;
		void *done_arg = p->done_arg;
		int status = p->status;

		free(p->registered);
		free(p);
		done(done_arg, status);
	}
}

static void
lvs_recover_publish(void *cb_arg, void (*done)(void *arg, int status),
		    void *done_arg)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	struct lvs_recover_publish_ctx *p;
	struct spdk_lvol *lvol;
	size_t count = 0;
	int rc;

	TAILQ_FOREACH(lvol, &lvs->lvs->lvols, link) {
		count++;
	}

	p = calloc(1, sizeof(*p));
	if (p && count > 0) {
		p->registered = calloc(count, sizeof(*p->registered));
		if (!p->registered) {
			free(p);
			p = NULL;
		}
	}
	if (!p) {
		/* Nothing has been registered yet, so there is nothing to roll
		 * back. */
		done(done_arg, -ENOMEM);
		return;
	}
	p->ctx = ctx;
	p->done = done;
	p->done_arg = done_arg;

	TAILQ_FOREACH(lvol, &lvs->lvs->lvols, link) {
		rc = vbdev_s3lvol_bdev_register(lvol, lvs->name, &lvs->producers);
		if (rc != 0) {
			SPDK_ERRLOG("lvstore '%s': could not register a bdev for "
				    "lvol '%s' after recovery: %d. The local "
				    "device is fully rebuilt and durable; "
				    "unregistering the %zu bdev(s) already "
				    "published so the lvstore is not left partly "
				    "visible\n",
				    lvs->name, lvol->name, rc, p->nregistered);
			p->status = rc;
			p->next_unregister = p->nregistered;
			lvs_recover_publish_unregister_next(p);
			return;
		}
		p->registered[p->nregistered++] = lvol;
	}

	SPDK_NOTICELOG("lvstore '%s': recovery published %zu bdev(s)\n",
		       lvs->name, p->nregistered);
	free(p->registered);
	free(p);
	done(done_arg, 0);
}

static void
lvs_recover_checkpoint_recorded(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	struct s3_recovery_finalize_ops ops = {
		.validate = lvs_recover_validate,
		.drain    = lvs_recover_drain,
		.publish  = lvs_recover_publish,
		.cb_arg   = ctx,
	};

	if (status != 0) {
		SPDK_ERRLOG("lvstore '%s': the checkpoint loaded but the new "
			    "super block could not record it (%d); the device "
			    "would replay from zero on the next attach\n",
			    lvs->name, status);
		lvs_setup_fail(ctx, status);
		return;
	}

	/* Every prerequisite local write is now done:
	 *
	 *   - the staging super block (flushed by the staged formatter),
	 *   - the journal head clear (flushed with it),
	 *   - the journal itself (s3_journal_create),
	 *   - the WAL (s3_wal_create),
	 *   - the checkpoint pointer (s3_local_dev_update_checkpoint, whose
	 *     callback is the one that got us here),
	 *   - the lvstore uuid (s3_local_dev_set_lvs_uuid, earlier in the
	 *     chain).
	 *
	 * The remote state was validated by the preflight before anything was
	 * claimed, and the checkpoint applied here is the exact byte range that
	 * passed it.
	 *
	 * What has NOT happened yet, and must happen before publication:
	 * blobstore has not been loaded, no lvol has been opened, and nothing
	 * has been registered. The finalization state machine does all three,
	 * and it flushes the prerequisites before it commits. */
	if (!s3_local_dev_is_staged(lvs->local_dev)) {
		/* Should be impossible: the chain formatted staged and nothing
		 * commits before this point. Fail loudly rather than publish a
		 * device whose state does not match the protocol. */
		SPDK_ERRLOG("lvstore '%s': the local device is not staged at "
			    "the finalization point; refusing to publish it\n",
			    lvs->name);
		lvs_setup_fail(ctx, -EINVAL);
		return;
	}

	SPDK_NOTICELOG("lvstore '%s': all local prerequisites are written; "
		       "loading blobstore and validating every lvol before "
		       "publication\n", lvs->name);

	ctx->recover_finalize = NULL;
	s3_recovery_finalize_start(lvs->local_dev, &ops,
				   lvs_recover_finalized, ctx);
}

static void
lvs_recover_wal_created(void *cb_arg, struct s3_wal *wal, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	int rc;

	if (status != 0) {
		SPDK_ERRLOG("lvstore '%s': failed to format the WAL on '%s': "
			    "%d\n", lvs->name, ctx->wal_bdev_name, status);
		lvs_setup_fail(ctx, status);
		return;
	}

	lvs->wal = wal;

	rc = s3_bs_dev_create(&ctx->opts, NULL, NULL, lvs->client,
			      ctx->opts.capacity_bytes, &lvs->bs_dev);
	if (rc != 0) {
		SPDK_ERRLOG("lvstore '%s': failed to create bs_dev: %d\n",
			    lvs->name, rc);
		lvs_setup_fail(ctx, rc);
		return;
	}

	/* The next steps attach the journal (which starts the checkpoint
	 * poller) and apply the remote checkpoint. A checkpoint that fired in
	 * that window would snapshot a map that is still being rebuilt and
	 * overwrite the checkpoint pointer the recovery descriptor names. The
	 * gate is cleared only after publication, in lvs_recover_finalized(). */
	s3_bs_dev_set_checkpoint_suppressed(lvs->bs_dev, true,
		"remote-only recovery is rebuilding this device");

	rc = s3_bs_dev_attach_journal(lvs->bs_dev, lvs->journal, lvs->local_dev);
	if (rc != 0) {
		SPDK_ERRLOG("lvstore '%s': failed to attach the journal: %d\n",
			    lvs->name, rc);
		lvs_setup_fail(ctx, rc);
		return;
	}

	(void)s3_bs_dev_attach_cache(lvs->bs_dev);

	/* The restore itself, from the bytes the preflight validated. The map is
	 * empty, so this is a rebuild rather than a merge. */
	{
		struct s3_ckpt_expect expect = {0};

		expect.chunk_size = ctx->opts.chunk_size;
		expect.lvs_uuid = &ctx->recover_uuid;
		expect.check_point = true;
		expect.gen = ctx->recover_gen;
		expect.lsn = ctx->recover_lsn;
		expect.num_chunks_limit = ctx->opts.capacity_bytes /
					  ctx->opts.chunk_size;

		rc = s3_checkpoint_apply(s3_bs_dev_get_chunk_map(lvs->bs_dev),
					 ctx->recover_ckpt,
					 ctx->recover_ckpt_len, &expect,
					 lvs_recover_checkpoint_loaded, ctx);
		if (rc != 0) {
			/* The callback ran inline and has already failed the
			 * setup; do not fail it a second time. */
			return;
		}
	}
}

static void
lvs_recover_journal_created(void *cb_arg, struct s3_journal *journal,
			    int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("lvstore '%s': failed to format the journal on '%s': "
			    "%d\n", ctx->lvs->name, ctx->wal_bdev_name, status);
		lvs_setup_fail(ctx, status);
		return;
	}

	ctx->lvs->journal = journal;

	s3_wal_create(ctx->lvs->local_dev, NULL, lvs_recover_wal_created, ctx);
}

static void
lvs_recover_formatted(void *cb_arg, struct s3_local_dev *dev, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("lvstore '%s': failed to format the local device "
			    "'%s': %d\n", ctx->lvs->name, ctx->wal_bdev_name,
			    status);
		lvs_setup_fail(ctx, status);
		return;
	}

	ctx->lvs->local_dev = dev;

	/* The uuid is known from the descriptor, so unlike create there is no
	 * need to wait for blobstore to hand one out: record it now, before
	 * anything else can fail, so the device identifies itself even if the
	 * rest of the chain does not complete. */
	s3_local_dev_set_lvs_uuid(dev, &ctx->recover_uuid,
				  lvs_recover_uuid_recorded, ctx);
}

static void
lvs_recover_uuid_recorded(void *cb_arg, int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		/* Not fatal on its own, but on this path it is: the whole point
		 * of recovery is to reproduce the identity, and a device that
		 * cannot record it would fail the attach-time uuid check. */
		SPDK_ERRLOG("lvstore '%s': could not record the recovered uuid "
			    "on the new local device (%d)\n", ctx->lvs->name,
			    status);
		lvs_setup_fail(ctx, status);
		return;
	}

	s3_journal_create(ctx->lvs->local_dev, lvs_recover_journal_created, ctx);
}

/* Everything recovery does after it owns the lvstore. */
static void
lvs_recover_after_owner(struct lvs_setup_ctx *ctx)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	struct s3_local_dev_format_opts fopts = {0};

	/* Geometry comes from the descriptor, never from the RPC. The caller
	 * did not supply it and must not be able to: a wrong capacity either
	 * fails blobstore's load or invites a grow, and a wrong chunk size
	 * reinterprets every chunk_index in the checkpoint. */
	fopts.wal_bdev_name   = ctx->wal_bdev_name;
	fopts.cache_bdev_name = ctx->cache_bdev_name;
	fopts.lvs_name        = lvs->name;
	fopts.capacity_bytes  = ctx->opts.capacity_bytes;
	fopts.chunk_size      = ctx->opts.chunk_size;
	fopts.journal_size    = ctx->recover_journal_size;
	fopts.wal_size        = ctx->recover_wal_size;

	/* The descriptors the blank proof was taken on, with the claims still
	 * held. The formatter adopts both instead of reopening either device, so
	 * there is no instant at which a device is unclaimed between the proof
	 * and the first write, and a dual-bdev layout does not fail by trying to
	 * open its claimed cache bdev for writing. */
	fopts.wal_claim       = ctx->recover_wal_claim;
	fopts.cache_claim     = ctx->recover_cache_claim;

	SPDK_NOTICELOG("lvstore '%s': recovering from checkpoint gen=%" PRIu64
		       " lsn=%" PRIu64 " onto blank device '%s' "
		       "(capacity=%" PRIu64 ", chunk=%u)\n",
		       lvs->name, ctx->recover_gen, ctx->recover_lsn,
		       ctx->wal_bdev_name, ctx->opts.capacity_bytes,
		       ctx->opts.chunk_size);

	/* STAGED, not normal. The normal formatter writes an attachable super
	 * block as its first write, which is exactly the state this chain must
	 * not expose: everything after it -- journal, WAL, checkpoint, uuid --
	 * can still fail, and a crash in between would leave a device that a
	 * later attach accepts and then replays from a checkpoint pointer that
	 * describes data that is not there. The staged formatter writes a block
	 * that every reader refuses, so the device is not attachable until
	 * lvs_recover_checkpoint_recorded() commits it at the very end. */
	s3_local_dev_format_staged(&fopts, lvs_recover_formatted, ctx);
}

static void
lvs_recover_owner_acquired(void *cb_arg, const struct s3_owner_info *holder,
			   int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;
	struct spdk_bdev_module *module;
	bool dual;
	int rc;

	if (status != 0) {
		/* The ownership acquire has already logged who holds it. */
		(void)holder;
		lvs_setup_fail(ctx, status);
		return;
	}

	ctx->lvs->owner_held = true;

	/* Claim both designated devices *before* either is scanned, and keep
	 * the claims until the format has finished. This is the step that
	 * removes the close/reopen window: the scan and the format now run on
	 * one descriptor per device, with the exclusive claim held throughout.
	 *
	 * The claim is process-local. It stops another opener in this process
	 * from writing the device, and it refuses to be granted while another
	 * descriptor already has it open for writing. It does not stop another
	 * process from writing the backing file, and it does not fence the
	 * host. See s3_recovery.h for the full statement. */
	module = vbdev_s3lvol_get_module();

	rc = s3_recovery_claim_open(ctx->wal_bdev_name, module,
				    &ctx->recover_wal_claim);
	if (rc != 0) {
		SPDK_ERRLOG("lvstore '%s': cannot claim the WAL device '%s' "
			    "for recovery (%s); refusing to format a device "
			    "this process cannot prove is exclusively ours\n",
			    ctx->lvs->name, ctx->wal_bdev_name,
			    spdk_strerror(-rc));
		lvs_setup_fail(ctx, rc);
		return;
	}

	dual = ctx->cache_bdev_name &&
	       strcmp(ctx->cache_bdev_name, ctx->wal_bdev_name) != 0;
	if (dual) {
		rc = s3_recovery_claim_open(ctx->cache_bdev_name, module,
					    &ctx->recover_cache_claim);
		if (rc != 0) {
			/* The WAL claim is already held. lvs_setup_fail ->
			 * lvs_setup_ctx_free releases it, so a failure on the
			 * second device leaves nothing claimed and nothing
			 * formatted. */
			SPDK_ERRLOG("lvstore '%s': cannot claim the cache device "
				    "'%s' for recovery (%s); releasing the WAL "
				    "claim and formatting nothing\n",
				    ctx->lvs->name, ctx->cache_bdev_name,
				    spdk_strerror(-rc));
			lvs_setup_fail(ctx, rc);
			return;
		}
	}

	/* Prove the designated device(s) blank before formatting, through the
	 * claims just taken. See the section comment for why this is a positive
	 * zero scan and not a parser result. */
	s3_recovery_blank_scan_claimed(ctx->recover_wal_claim,
				       lvs_recover_blank_scanned, ctx);
}

static void
lvs_recover_desc_loaded(void *cb_arg, struct s3_recovery_desc *desc,
			int status)
{
	struct lvs_setup_ctx *ctx = cb_arg;

	if (status != 0) {
		if (status == -ENOENT) {
			SPDK_ERRLOG("lvstore '%s': no recovery descriptor in the "
				    "bucket. Remote-only recovery needs one, and "
				    "it is only written by a checkpoint taken by a "
				    "build that has it. This lvstore cannot be "
				    "recovered without its local device\n",
				    ctx->lvs->name);
		} else {
			SPDK_ERRLOG("lvstore '%s': the recovery descriptor is "
				    "unusable (%s); refusing to guess the "
				    "geometry or identity\n", ctx->lvs->name,
				    spdk_strerror(-status));
		}
		lvs_setup_fail(ctx, status);
		return;
	}

	/* Adopt the descriptor's geometry and identity. From here nothing reads
	 * them from the RPC or from a default. */
	ctx->opts.capacity_bytes = desc->capacity_bytes;
	ctx->opts.chunk_size     = desc->chunk_size;
	spdk_uuid_copy(&ctx->recover_uuid, &desc->lvs_uuid);
	ctx->recover_gen          = desc->checkpoint_gen;
	ctx->recover_lsn          = desc->checkpoint_lsn;
	ctx->recover_journal_size = desc->journal_size;
	ctx->recover_wal_size     = desc->wal_size;

	/* Check the checkpoint the descriptor names before claiming anything or
	 * touching a device. The preflight copies the descriptor, so freeing
	 * ours after the call is safe even when its callback runs inline. */
	s3_recovery_ckpt_preflight(ctx->lvs->client, ctx->lvs->name, desc,
				   lvs_recover_ckpt_validated, ctx);
	s3_recovery_desc_free(desc);
}

int
s3lvol_lvstore_recover(const struct s3_lvs_opts *opts,
		       s3lvol_lvs_op_cb cb_fn, void *cb_arg)
{
	struct lvs_setup_ctx *ctx;
	int rc;

	/* Recovery is not safe to expose yet. Fail before owner acquisition or
	 * any device writes.
	 *
	 * Closed in this increment:
	 *   - crash-safe local publication. The chain formats a STAGING super
	 *     block first and publishes the normal one last, after every
	 *     prerequisite write has been flushed. A device interrupted at any
	 *     earlier point is refused by s3_local_dev_open() (-EAGAIN here,
	 *     -EILSEQ on a reader that predates the staging magic), so a
	 *     partially rebuilt device can no longer be attached as if complete.
	 *   - descriptor-to-checkpoint integrity: the preflight binds the
	 *     downloaded bytes to the descriptor's identity, generation, LSN,
	 *     geometry and size, and the chain applies exactly those bytes.
	 *   - publication ordering. The commit is now the last local write, and
	 *     bdev registration happens only after it and after a final flush.
	 *     Blobstore is loaded and *every* lvol is opened before the commit,
	 *     and a single unopenable lvol fails the rebuild. A failure at any
	 *     step before the commit leaves the device STAGED. The ordering lives
	 *     in s3_recovery_finalize.c so the tests drive the same state machine
	 *     the chain does.
	 *
	 * Still blocking the gate:
	 *   - the owner marker is a sequential-misoperation guard, not mutual
	 *     exclusion (s3_owner.h). Two processes can both believe they own the
	 *     lvstore; the local claim is process-local only.
	 *   - a failed commit or final flush has an indeterminate on-disk
	 *     outcome: the normal block may already be visible. That is safe only
	 *     because every prerequisite was flushed before it was written, and
	 *     this code cannot prove it after the fact -- so it reports an error
	 *     rather than claiming either "published" or "nothing happened".
	 *   - checkpoint GC / object retention: a checkpoint object deleted by a
	 *     clean unload makes recovery impossible, and nothing here prevents
	 *     that.
	 *   - the RPO is the last checkpoint. Everything acknowledged after it but
	 *     not yet checkpointed is lost. The drain step proves the *local*
	 *     device has nothing in flight; it does not make the remote state
	 *     newer than the checkpoint.
	 *
	 * Remove this gate only with regressions covering those boundaries.
	 *
	 * S3LVOL_RECOVERY_TEST_BUILD exists only so an isolated test binary can
	 * exercise the chain end to end. It is never defined by the production
	 * build, and the RPC remains present and gated in both builds. Even in
	 * the test build the function is not a supported operator entry point:
	 * it reports the same limitations in its result. */
#ifndef S3LVOL_RECOVERY_TEST_BUILD
	SPDK_ERRLOG("remote-only recovery is disabled pending safety validation\n");
	return -ENOTSUP;
#else
	SPDK_WARNLOG("remote-only recovery is running in a TEST BUILD; this "
	      "binary must not be deployed\n");
#endif

	if (!opts || !opts->lvs_name) {
		return -EINVAL;
	}
	/* Recovery never takes over another owner's marker. The marker is a
	 * sequential-misoperation guard, not mutual exclusion (s3_owner.h):
	 * accepting force here would turn a claim this API cannot verify into
	 * "the other side is gone". Attach and create keep their force
	 * semantics unchanged.
	 *
	 * Unreachable while the -ENOTSUP gate above stands. It is here so that
	 * removing the gate does not silently re-introduce force. */
	if (opts->force) {
		SPDK_ERRLOG("recover_lvstore does not accept force: the owner "
			    "marker is not mutual exclusion\n");
		return -EINVAL;
	}
	rc = lvs_name_ok(opts->lvs_name);
	if (rc != 0) {
		return rc;
	}
	/* A local device is required: recovery formats one. It is not optional
	 * in the way it is on create, because the result has to be attachable
	 * afterwards and an lvstore without a journal cannot be. */
	if (!opts->wal_bdev_name) {
		SPDK_ERRLOG("recovering lvstore '%s' needs a blank wal_bdev to "
			    "format; without one the result could not be "
			    "attached again\n", opts->lvs_name);
		return -EINVAL;
	}
	if (s3lvol_lvstore_find(opts->lvs_name)) {
		SPDK_ERRLOG("lvstore '%s' is already attached\n", opts->lvs_name);
		return -EEXIST;
	}
	if (lvs_retained_find(&g_retained, opts->lvs_name)) {
		/* A blank-device rebuild from S3 would lose what the retained
		 * WAL holds; only the same-device attach may use that claim. */
		SPDK_ERRLOG("lvstore '%s': this process retains its owner claim "
			    "and local WAL; refusing remote recovery\n",
			    opts->lvs_name);
		return -EBUSY;
	}

	rc = lvs_setup_begin(opts, cb_fn, cb_arg, &ctx);
	if (rc != 0) {
		return rc;
	}
	ctx->attaching = true;
	ctx->recovering = true;
	/* ctx->force stays false: recovery refuses the option above. */

	s3_recovery_load(ctx->lvs->client, ctx->lvs->name,
			 lvs_recover_desc_loaded, ctx);
	return 0;
}

/* ==========================================================================
 * lvstore unload
 *
 * Two things have to happen in order, and neither is optional on the WAL path:
 *
 *   1. everything acknowledged so far reaches S3 (INV2), and
 *   2. the journal and the local device are only released once the bs_dev is
 *      completely done with them.
 *
 * The subtlety is that spdk_lvs_unload() writes blobstore's final metadata,
 * *those writes are acknowledged from the log*, and bs_dev->destroy() cannot
 * block. So destroy() finishes in the background and reports through its own
 * callback -- spdk_lvs_unload()'s completion is not the end of the story.
 *
 * The two completions can arrive in either order, so this rendezvouses on both.
 * ========================================================================== */

struct lvs_unload_ctx {
	struct s3lvol_lvstore *lvs;
	spdk_lvs_op_complete cb_fn;
	void *cb_arg;

	/* cb_fn is retirement for destroy, caller completion for ordinary unload.
	 * timeout_fn only notifies destroy; it must not free its context. */
	spdk_lvs_op_complete timeout_fn;
	bool timed_out;
	bool release_pending;
	bool quiesce_started;
	struct spdk_poller *deadline_poller;
	uint64_t deadline;

	bool lvs_done;
	bool bs_dev_done;
	int status;

	/* Owner-marker decision inputs and outcome (owner_lifecycle_ctl.h).
	 * teardown_status: the bs_dev destroy callback's status; non-zero means
	 * the final flusher drain or the WAL close failed, so acknowledged
	 * writes may exist only in the local WAL. quiesce_status: the
	 * pending-delete registry PUT for this lvstore did not drain. Either
	 * keeps the marker held. release_status: the release itself. */
	int teardown_status;
	int quiesce_status;
	int release_status;
	bool quiesce_done;
	bool owner_decided;

	/* Outstanding lvol bdev unregisters, plus one self reference so an inline
	 * completion cannot finish the round early. */
	uint32_t lvols_pending;

	/* Set while spdk_lvs_unload() is on the stack. It reports its -EBUSY
	 * failures *both* through the callback and through the return value, so
	 * without this the error would be handled twice -- which is exactly the
	 * double free that segfaulted the target on the first live run. */
	bool in_submit;
	bool cb_ran;

	/* Destroy only (NULL for an ordinary unload). When the decision below is
	 * RELEASE, the claim is moved here instead of being released, so the
	 * destroy holds it continuously through its deletes and releases it
	 * last. A RETAIN decision leaves it untouched (transferred stays false)
	 * and the destroy issues no delete. Owned by the destroy context, which
	 * outlives this one. */
	struct lvs_owner_handoff *handoff;

#ifdef S3LVOL_RECOVERY_TEST_BUILD
	/* Test-only "postdrain" stage gate (S3LVOL_TEST_UNLOAD_GATE_DIR). */
	struct spdk_poller *gate_poller;
	char gate_hold[LVS_DESTROY_KEY_MAX];
#endif
};

static void lvs_unload_maybe_finish(struct lvs_unload_ctx *ctx);

/* The release completed, successfully or not. */
static void
lvs_unload_owner_released(void *cb_arg, int status)
{
	struct lvs_unload_ctx *ctx = cb_arg;

	/* A failed release is an unload failure. The local teardown is complete,
	 * but the marker may still name this process, and reporting success
	 * would tell the caller the lvstore is free when it may not be. In
	 * strict mode nothing can clear it except this claim or an operator, so
	 * the caller has to know. */
	if (status != 0) {
		SPDK_ERRLOG("lvstore '%s': unloaded locally, but releasing the S3 "
			    "owner marker failed (%s); the marker may still name "
			    "this process\n", ctx->lvs->name, spdk_strerror(-status));
	}
	ctx->release_status = status;
	ctx->release_pending = false;
	ctx->lvs->owner_release_pending = false;
	lvs_unload_maybe_finish(ctx);
}

/* Caller bound: 30 seconds after successful pre-drain, subject to reactor
 * scheduling. This is NOT cancellation or a resource retirement deadline.
 * The same poller survives timeout and waits for registry quiescence without
 * a second allocation. Hung I/O intentionally retains its entire owner graph. */
#define LVS_UNLOAD_POSTDRAIN_TIMEOUT_US (30ULL * 1000 * 1000)
#define LVS_UNLOAD_DEADLINE_POLL_US (10 * 1000)

static void
lvs_unload_check_deadline(struct lvs_unload_ctx *ctx)
{
	spdk_lvs_op_complete cb_fn;
	void *user_arg;

	if (!ctx->deadline_poller || ctx->timed_out ||
	    spdk_get_ticks() < ctx->deadline) {
		return;
	}
	ctx->timed_out = true;
	ctx->lvs->unload_timeout_reported = true;
	SPDK_ERRLOG("lvstore '%s': unload deadline expired; retirement is still "
		    "in progress, local resources and exclusion remain held. %s\n",
		    ctx->lvs->name, ctx->release_pending ?
		    "Owner release already submitted; outcome is pending." :
		    "Owner claim retained; no release or destroy deletes will start.");
	user_arg = ctx->cb_arg;
	if (ctx->timeout_fn) {
		cb_fn = ctx->timeout_fn;
		ctx->timeout_fn = NULL;
	} else {
		cb_fn = ctx->cb_fn;
		ctx->cb_fn = NULL;
		ctx->cb_arg = NULL;
	}
	if (cb_fn) {
		cb_fn(user_arg, -ETIMEDOUT);
	}
}

static int
lvs_unload_deadline_poll(void *arg)
{
	struct lvs_unload_ctx *ctx = arg;

	lvs_unload_check_deadline(ctx);
	if (ctx->quiesce_started && !ctx->quiesce_done &&
	    !s3lvol_pending_persist_busy(&ctx->lvs->uuid)) {
		ctx->quiesce_done = true;
		lvs_unload_maybe_finish(ctx);
	}
	return SPDK_POLLER_BUSY;
}

/* Stop the writers that live outside the bs_dev before the marker is released.
 *
 * The bs_dev teardown already waited for its own: checkpoints, deferred
 * deletes, cache fills, the flusher and the WAL. What it does not cover is the
 * pending-delete registry, which PUTs `<lvstore>/meta/pending-deletes.json` on
 * its own schedule. Clearing this lvstore's marks here (rather than only in
 * s3lvol_lvstore_free(), after the release) issues its last write now, and the
 * poll waits for it. Export and import lease renewals are not waited for: they
 * write keys under the exports/ prefix and the source's lease keys, never this
 * lvstore's own prefix, and s3lvol_lvstore_free() stops them. */
static void
lvs_unload_quiesce_start(struct lvs_unload_ctx *ctx)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;

	ctx->quiesce_started = true;
	if (!spdk_uuid_is_null(&lvs->uuid)) {
		s3lvol_snapshot_pending_clear_lvs(&lvs->uuid);
	}
	if (!s3lvol_pending_persist_busy(&lvs->uuid)) {
		ctx->quiesce_done = true;
		lvs_unload_maybe_finish(ctx);
		return;
	}
	/* The deadline poller was allocated before any destructive transition.
	 * Never equate a reporting timeout with registry I/O completion. */

}

static void
lvs_unload_maybe_finish(struct lvs_unload_ctx *ctx)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	spdk_lvs_op_complete cb_fn = ctx->cb_fn;
	void *user_arg = ctx->cb_arg;
	int status = ctx->status;

	if (ctx->in_submit || !ctx->lvs_done || !ctx->bs_dev_done ||
	    ctx->release_pending) {
		return;
	}

	lvs_unload_check_deadline(ctx);
	/* Notification may have consumed the ordinary caller callback. */
	cb_fn = ctx->cb_fn;
	user_arg = ctx->cb_arg;

	/* Blobstore has unloaded and the bs_dev is completely gone, which means
	 * its checkpoint, deferred deletes, cache fills, flusher and WAL close
	 * all finished (s3_bs_dev_teardown). Now the writers outside it. */
	if (!ctx->quiesce_done) {
		if (!ctx->quiesce_started) {
			lvs_unload_quiesce_start(ctx);
		}
		return;
	}

	/* Give the claim back only if every writer stopped AND stopped cleanly.
	 * Releasing while acknowledged writes exist only in the local WAL would
	 * let another process rebuild from S3 without them. This is cooperative
	 * quiescence of this process, not a fence against a request the object
	 * store applies late.
	 *
	 * Clearing owner_held first is what stops this from recursing. */
	if (!ctx->owner_decided) {
		struct lvs_owner_teardown td = {
			.owner_held      = lvs->owner_held,
			.resources_live  = false,
			.teardown_status = ctx->timed_out ? -ETIMEDOUT : ctx->teardown_status,
			.quiesce_status  = ctx->quiesce_status,
		};

		ctx->owner_decided = true;
		switch (lvs_owner_decide(&td)) {
		case LVS_OWNER_RELEASE:
			lvs->owner_held = false;
			if (ctx->handoff) {
				/* Destroy: hand the live claim over, no release
				 * and no re-acquire. The marker keeps naming this
				 * process until the destroy's deletes are done. */
				ctx->handoff->mode = lvs->owner_mode;
				ctx->handoff->caps = lvs->owner_caps;
				ctx->handoff->claim = lvs->owner_claim;
				ctx->handoff->transferred = true;
				break;
			}
			ctx->release_pending = true;
			lvs->owner_release_pending = true;
			lvs_owner_release(lvs, lvs_unload_owner_released, ctx);
			return;
		case LVS_OWNER_RETAIN:
			/* Numeric statuses: two spdk_strerror() calls in one
			 * argument list share one buffer and print the same
			 * text. */
			SPDK_ERRLOG("lvstore '%s': unload did not drain cleanly "
				    "(teardown %d, registry %d); the S3 owner marker "
				    "is RETAINED. Acknowledged writes may exist only "
				    "in the local WAL. %s\n", lvs->name,
				    ctx->timed_out ? -ETIMEDOUT : ctx->teardown_status,
				    ctx->quiesce_status,
				    lvs_retained_guidance(
					    lvs->owner_mode == S3_OWNER_MODE_STRICT,
					    false));
			/* Before the free below drops the claim with the
			 * lvstore. The local device is still open here. */
			lvs_unload_keep_retained(lvs, ctx->lvs_done,
						 ctx->bs_dev_done,
						 ctx->quiesce_done,
						 ctx->quiesce_status,
						 ctx->timed_out ? -ETIMEDOUT :
						 ctx->teardown_status);
			break;
		case LVS_OWNER_NOT_HELD:
			break;
		}
	}

	status = lvs_unload_final_status(ctx->timed_out ? -ETIMEDOUT : status, ctx->teardown_status,
					 ctx->quiesce_status, ctx->release_status);
	spdk_poller_unregister(&ctx->deadline_poller);
	free(ctx);

	if (status == 0) {
		SPDK_NOTICELOG("lvstore '%s' unloaded\n", lvs->name);
	} else {
		SPDK_ERRLOG("lvstore '%s' unloaded locally with an error (%s); "
			    "see above for the S3 owner marker state\n",
			    lvs->name, spdk_strerror(-status));
	}

	TAILQ_REMOVE(&g_lvstores, lvs, link);
	/* Frees the journal and closes the local device, which is safe now that
	 * the bs_dev is gone. */
	s3lvol_lvstore_free(lvs);

	if (cb_fn) {
		cb_fn(user_arg, status);
	}
}

static void
lvs_unload_bs_dev_gone(void *cb_arg, int status)
{
	struct lvs_unload_ctx *ctx = cb_arg;

#ifdef S3LVOL_RECOVERY_TEST_BUILD
	/* TEST-ONLY: make a destroy's teardown look like a failed final drain
	 * / WAL close. Ordinary unloads (handoff == NULL) are never affected. */
	if (ctx->handoff && status == 0 &&
	    lvs_test_destroy_fault("unload")) {
		SPDK_ERRLOG("lvstore '%s': TEST fault: destroy teardown reported "
			    "-EIO\n", ctx->lvs->name);
		status = -EIO;
	}
#endif
	/* Non-zero: the final flusher drain timed out or the WAL close failed.
	 * The bs_dev is gone either way, but the marker must not be released. */
	ctx->teardown_status = status;
	ctx->lvs->bs_dev = NULL;
	ctx->bs_dev_done = true;
	lvs_unload_maybe_finish(ctx);
}

/* Nothing was torn down, so no destroy callback is coming. Drop it anyway: if
 * the bs_dev does get destroyed later it must not call into a freed context. */
static void
lvs_unload_report_error(struct lvs_unload_ctx *ctx, int status)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	spdk_lvs_op_complete cb_fn = ctx->cb_fn;
	void *user_arg = ctx->cb_arg;

	if (lvs->bs_dev) {
		s3_bs_dev_set_destroy_cb(lvs->bs_dev, NULL, NULL);
	}

	/* The lvstore stays in the registry: an unload that failed leaves it
	 * usable, and removing it would turn it into an unreachable object. A
	 * later unload/destroy may be attempted. */
	lvs->unload_in_progress = false;
	if (ctx->timed_out) {
		status = -ETIMEDOUT;
	}
	spdk_poller_unregister(&ctx->deadline_poller);
	free(ctx);

	if (cb_fn) {
		cb_fn(user_arg, status);
	}
}

static void
s3lvol_lvs_unload_cb(void *cb_arg, int lvserrno)
{
	struct lvs_unload_ctx *ctx = cb_arg;
	struct s3lvol_lvstore *lvs = ctx->lvs;

	if (lvserrno != 0) {
		SPDK_ERRLOG("Failed to unload lvstore '%s': %s\n",
			    lvs->name, spdk_strerror(-lvserrno));

		/* spdk_lvs_unload() reports its -EBUSY failures through *both* the
		 * callback and the return value, so the submitting frame is still
		 * on the stack and owns the cleanup. Handling it here as well is
		 * what double-freed the context on the first live run. */
		if (ctx->in_submit) {
			ctx->cb_ran = true;
			ctx->status = lvserrno;
			return;
		}

		lvs_unload_report_error(ctx, lvserrno);
		return;
	}

	lvs->lvs = NULL;
	ctx->lvs_done = true;
	lvs_unload_maybe_finish(ctx);
}

static void
lvs_unload_do_unload(struct lvs_unload_ctx *ctx)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	int rc;

	if (ctx->status != 0) {
		/* An lvol failed to close, so the lvstore is still busy and
		 * unloading it would only fail with -EBUSY. */
		SPDK_ERRLOG("lvstore '%s': not every lvol could be closed (%s); "
			    "not unloading\n", lvs->name, spdk_strerror(-ctx->status));
		if (ctx->status == -EINVAL) {
			/* From spdk_lvol_close() with ref_count already 0. The
			 * "has no bdev" notices just above name the candidates. */
			SPDK_ERRLOG("lvstore '%s': -EINVAL here means an lvol was "
				    "already closed; see the 'has no bdev' notices "
				    "above for which\n", lvs->name);
		}
		lvs_unload_report_error(ctx, ctx->status);
		return;
	}

	ctx->in_submit = true;
	rc = spdk_lvs_unload(lvs->lvs, s3lvol_lvs_unload_cb, ctx);
	ctx->in_submit = false;

	if (rc == 0) {
		lvs_unload_maybe_finish(ctx);
		return;
	}

	SPDK_ERRLOG("spdk_lvs_unload failed for '%s': %d\n", lvs->name, rc);
	lvs_unload_report_error(ctx, ctx->cb_ran ? ctx->status : rc);
}

/* One lvol bdev is gone. Its destruct closed the lvol, which is what releases
 * the reference spdk_lvs_unload() refuses to unload over. */
static void
lvs_unload_lvol_unregistered(void *cb_arg, int bdeverrno)
{
	struct lvs_unload_ctx *ctx = cb_arg;

	if (bdeverrno != 0 && ctx->status == 0) {
		ctx->status = bdeverrno;
	}

	assert(ctx->lvols_pending > 0);
	if (--ctx->lvols_pending > 0) {
		return;
	}

	lvs_unload_do_unload(ctx);
}

/* Step 2: close every lvol.
 *
 * spdk_lvs_unload() rejects an lvstore with open lvols, and an lvol stays open
 * for as long as its bdev is registered. Upstream vbdev_lvol does the same
 * (_vbdev_lvs_remove). The first live run failed here: the script deleted the
 * nvmf namespace but the bdev was still registered, so unload returned -EBUSY. */
static void
lvs_unload_close_lvols(struct lvs_unload_ctx *ctx)
{
	struct s3lvol_lvstore *lvs = ctx->lvs;
	struct spdk_lvol *lvol, *tmp;

	lvs_unload_check_deadline(ctx);
	if (ctx->timed_out) {
		/* No destructive work was started (e.g. a test gate was held). */
		lvs_unload_report_error(ctx, -ETIMEDOUT);
		return;
	}

	/* Self reference, so an unregister that completes inline cannot run the
	 * next step before the loop has finished submitting. */
	/* A failed unload may leave closed volumes in a still-loaded store.
	 * Only a complete reattach can restore the dependency proof. */
	lvs->dependency_view_complete = false;
	ctx->lvols_pending = 1;

	TAILQ_FOREACH_SAFE(lvol, &lvs->lvs->lvols, link, tmp) {
		ctx->lvols_pending++;

		if (lvol->bdev != NULL) {
			spdk_bdev_unregister(lvol->bdev,
					     lvs_unload_lvol_unregistered, ctx);
		} else {
			/* No bdev but possibly still open (a failed registration,
			 * or a future internal lvol). Same callback shape.
			 *
			 * Logged because this is the branch that produces the -22
			 * below: spdk_lvol_close() answers -EINVAL when ref_count is
			 * already 0, i.e. when the lvol is closed as well as
			 * bdev-less. Without the name, that error identifies nothing
			 * and the only way to find the lvol is by elimination. */
			SPDK_NOTICELOG("lvstore '%s': lvol '%s' has no bdev; closing "
				       "it directly\n", lvs->name, lvol->name);
			spdk_lvol_close(lvol, lvs_unload_lvol_unregistered, ctx);
		}
	}

	lvs_unload_lvol_unregistered(ctx, 0);
}

/* Step 1: get as much as possible into S3 while blobstore is still up. What
 * unloading itself writes is handled by the bs_dev teardown. */
/* Pre-drain failure leaves the store loaded and retryable (30-second flusher
 * observation deadline). After pre-drain succeeds, a separate 30-second caller
 * deadline covers lvol closure, blobstore unload, bsdev retirement, registry
 * quiescence and ordinary owner release. Allocation failure fails closed before
 * closing any lvol. Both bounds require reactor progress.
 *
 * Timeout reports -ETIMEDOUT exactly once, NOT cancellation or proof that the
 * store is detached. The context, local resources and unload exclusion survive
 * until their last callbacks. A hung writer can retain them forever. No release
 * is started after timeout. A release already in flight is not cancelled and
 * its actual outcome is waited for. Otherwise late clean/error retirement keeps
 * the strict claim via the existing same-device retained-record path.
 *
 * Destroy's timeout notification consumes only its caller callback; the separate
 * retirement callback owns the destroy context (including reap and handoff).
 * Timed-out destroy never starts deletes later. If unload finishes before the
 * deadline, destroy's subsequent deletes/audit/release are outside this bound.
 * This is cooperative local quiescence, not a storage fence.
 */
#ifdef S3LVOL_RECOVERY_TEST_BUILD
/* TEST-ONLY stage gate for the post-drain window (compiled only into the
 * isolated test harness; the production build contains none of this).
 *
 *   S3LVOL_TEST_UNLOAD_GATE_DIR=<dir>
 *       after a SUCCESSFUL pre-unload drain and before any lvol is closed, if
 *       <dir>/hold-postdrain exists the unload writes <dir>/at-postdrain and
 *       waits, polling, until hold-postdrain is removed. Lets a test take the
 *       object store away exactly between the bounded pre-drain and the
 *       blobstore unload / bs_dev teardown. If the poller cannot be
 *       registered the unload simply continues (no hold). */
static int
lvs_unload_gate_poll(void *arg)
{
	struct lvs_unload_ctx *ctx = arg;

	if (access(ctx->gate_hold, F_OK) == 0) {
		return SPDK_POLLER_IDLE;
	}
	spdk_poller_unregister(&ctx->gate_poller);
	SPDK_NOTICELOG("lvstore '%s': TEST gate %s released\n", ctx->lvs->name,
		       ctx->gate_hold);
	lvs_unload_close_lvols(ctx);
	return SPDK_POLLER_BUSY;
}

static bool
lvs_unload_gate_hold(struct lvs_unload_ctx *ctx)
{
	const char *dir = getenv("S3LVOL_TEST_UNLOAD_GATE_DIR");
	char at[LVS_DESTROY_KEY_MAX];
	int fd;

	if (!dir || dir[0] == '\0') {
		return false;
	}
	snprintf(ctx->gate_hold, sizeof(ctx->gate_hold), "%s/hold-postdrain",
		 dir);
	if (access(ctx->gate_hold, F_OK) != 0) {
		return false;
	}
	snprintf(at, sizeof(at), "%s/at-postdrain", dir);
	fd = open(at, O_CREAT | O_WRONLY | O_TRUNC, 0600);
	if (fd >= 0) {
		close(fd);
	}
	ctx->gate_poller = SPDK_POLLER_REGISTER(lvs_unload_gate_poll, ctx,
						50 * 1000);
	if (!ctx->gate_poller) {
		return false;
	}
	SPDK_NOTICELOG("lvstore '%s': TEST gate holding at 'postdrain'\n",
		       ctx->lvs->name);
	return true;
}
#endif

static void
lvs_unload_drained(void *cb_arg, int status)
{
	struct lvs_unload_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("lvstore '%s': acknowledged writes did not reach S3 "
			    "before unload (%s); NOT unloading. The lvstore stays "
			    "loaded, the S3 owner marker stays held, and the "
			    "flusher keeps retrying; retry the unload once the "
			    "object store is reachable\n", ctx->lvs->name,
			    spdk_strerror(-status));
		lvs_unload_report_error(ctx, status);
		return;
	}

	/* Allocate the reporting/retirement poller before closing any lvol.
	 * Allocation failure leaves the store loaded and retryable. */
	ctx->deadline = spdk_get_ticks() +
		(LVS_UNLOAD_POSTDRAIN_TIMEOUT_US * spdk_get_ticks_hz()) / 1000000;
	ctx->deadline_poller = SPDK_POLLER_REGISTER(lvs_unload_deadline_poll,
		ctx, LVS_UNLOAD_DEADLINE_POLL_US);
	if (!ctx->deadline_poller) {
		lvs_unload_report_error(ctx, -ENOMEM);
		return;
	}
#ifdef S3LVOL_RECOVERY_TEST_BUILD
	if (lvs_unload_gate_hold(ctx)) {
		return;
	}
#endif
	lvs_unload_close_lvols(ctx);
}

void
s3lvol_lvstore_unload(struct s3lvol_lvstore *lvs,
		      spdk_lvs_op_complete cb_fn, void *cb_arg)
{
	lvs_unload_start(lvs, NULL, cb_fn, cb_arg, NULL);
}

/* The unload proper. \c handoff is NULL for an ordinary unload (release the
 * claim when clean, exactly as before) and non-NULL only for destroy. */
static void
lvs_unload_start(struct s3lvol_lvstore *lvs, struct lvs_owner_handoff *handoff,
		 spdk_lvs_op_complete cb_fn, void *cb_arg,
		 spdk_lvs_op_complete timeout_fn)
{
	struct lvs_unload_ctx *ctx;

	if (!lvs || !lvs->lvs) {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}
	if (lvs->unload_in_progress || lvs->control_inflight ||
	    s3_producer_count(&lvs->producers) || s3lvol_lvstore_source_closed(lvs) ||
	    s3lvol_scoped_source_held(lvs)) {
		/* The destroy callback is a single slot; a second setter would
		 * strand the running unload's context. */
		SPDK_ERRLOG("lvstore '%s': an unload or destroy is already in "
			    "progress; refusing\n", lvs->name);
		if (cb_fn) {
			cb_fn(cb_arg, -EBUSY);
		}
		return;
	}

	if (s3lvol_export_work_busy(lvs) || s3lvol_export_registry_busy(lvs)) {
		if (cb_fn) {
			cb_fn(cb_arg, -EBUSY);
		}
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		if (cb_fn) {
			cb_fn(cb_arg, -ENOMEM);
		}
		return;
	}
	ctx->lvs = lvs;
	ctx->cb_fn = cb_fn;
	ctx->cb_arg = cb_arg;
	ctx->handoff = handoff;
	ctx->timeout_fn = timeout_fn;
	lvs->unload_in_progress = true;
	lvs->unload_timeout_reported = false;
	lvs->owner_release_pending = false;

	/* Registered before anything else: spdk_lvs_unload() destroys the bs_dev
	 * internally, and this is the only signal for when that finished. */
	s3_bs_dev_set_destroy_cb(lvs->bs_dev, lvs_unload_bs_dev_gone, ctx);

	s3_bs_dev_drain(lvs->bs_dev, lvs_unload_drained, ctx);
}

/* Push everything acknowledged so far into S3 without unloading.
 *
 * Exists for testing: after this returns the overlay is empty, so a subsequent
 * read has to come from S3 rather than from RAM. That is the difference between
 * "the data is in the process" and "the data is in the object store". */
void
s3lvol_lvstore_flush(struct s3lvol_lvstore *lvs, spdk_lvs_op_complete cb_fn,
		     void *cb_arg)
{
	struct source_control_ticket *ticket;
	int rc = s3lvol_control_enter(lvs);
	if (rc) { if (cb_fn) cb_fn(cb_arg, rc); return; }
	ticket = calloc(1, sizeof(*ticket));
	if (!ticket) {
		s3lvol_control_leave(lvs, 0);
		if (cb_fn) cb_fn(cb_arg, -ENOMEM);
		return;
	}
	ticket->lvs = lvs; ticket->cb = cb_fn; ticket->arg = cb_arg;
	s3_bs_dev_drain(lvs->bs_dev, source_control_done, ticket);
}

void
s3lvol_lvstore_checkpoint(struct s3lvol_lvstore *lvs, spdk_lvs_op_complete cb_fn,
			  void *cb_arg)
{
	struct source_control_ticket *ticket;
	int rc = s3lvol_control_enter(lvs);
	if (rc) { if (cb_fn) cb_fn(cb_arg, rc); return; }
	ticket = calloc(1, sizeof(*ticket));
	if (!ticket) {
		s3lvol_control_leave(lvs, 0);
		if (cb_fn) cb_fn(cb_arg, -ENOMEM);
		return;
	}
	ticket->lvs = lvs; ticket->cb = cb_fn; ticket->arg = cb_arg;
	s3_bs_dev_checkpoint(lvs->bs_dev, source_control_done, ticket);
}

void
s3lvol_lvstore_get_stats(struct s3lvol_lvstore *lvs, struct s3_bs_dev_stats *out)
{
	if (!lvs || !lvs->bs_dev || !out) {
		return;
	}
	s3_bs_dev_get_stats(lvs->bs_dev, out);
}

/* ==========================================================================
 * lvol creation
 * ========================================================================== */

struct lvol_create_ctx {
	struct s3lvol_lvstore   *lvs;
	s3lvol_lvol_op_cb        cb_fn;
	void                    *cb_arg;
};

static void
s3lvol_lvol_create_cb(void *cb_arg, struct spdk_lvol *lvol, int lvolerrno)
{
	struct lvol_create_ctx *ctx = cb_arg;
	s3lvol_lvol_op_cb cb_fn = ctx->cb_fn;
	void *user_arg = ctx->cb_arg;
	const char *lvs_name = ctx->lvs->name;
	struct s3_producer_gate *gate = &ctx->lvs->producers;
	int rc;

	free(ctx);

	if (lvolerrno != 0) {
		SPDK_ERRLOG("Failed to create lvol: %s\n", spdk_strerror(-lvolerrno));
		if (cb_fn) {
			cb_fn(user_arg, NULL, lvolerrno);
		}
		return;
	}

	/* Register as a bdev -- this step is the whole point of the layer; nvmf
	 * only recognises names in the global bdev table. */
	rc = vbdev_s3lvol_bdev_register(lvol, lvs_name, gate);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to register bdev for lvol '%s': %d\n",
			    lvol->name, rc);
		/* Destroy the lvol on registration failure, or an inaccessible
		 * lvol would be left behind. */
		spdk_lvol_destroy(lvol, (spdk_lvol_op_complete)NULL, NULL);
		if (cb_fn) {
			cb_fn(user_arg, NULL, rc);
		}
		return;
	}

	if (cb_fn) {
		cb_fn(user_arg, lvol, 0);
	}
}

static int
s3lvol_lvol_create_admitted(struct s3lvol_lvstore *lvs, const char *name,
		   uint64_t size_bytes, bool thin_provision,
		   s3lvol_lvol_op_cb cb_fn, void *cb_arg)
{
	struct lvol_create_ctx *ctx;
	int rc;

	if (!lvs || !lvs->lvs || !name || size_bytes == 0) {
		return -EINVAL;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		return -ENOMEM;
	}
	ctx->lvs    = lvs;
	ctx->cb_fn  = cb_fn;
	ctx->cb_arg = cb_arg;

	/* clear_method is UNMAP: on an S3 backend, "clear to zero" means
	 * unmap the chunk -- an unallocated region reads as zero, with no need
	 * to write zero bytes. */
	rc = spdk_lvol_create(lvs->lvs, name, size_bytes, thin_provision,
			      LVOL_CLEAR_WITH_UNMAP,
			      s3lvol_lvol_create_cb, ctx);
	if (rc != 0) {
		free(ctx);
		return rc;
	}

	return 0;
}

/* ==========================================================================
 * Snapshots and clones
 *
 * On an S3 backend neither of these **moves data**: a snapshot freezes the
 * origin blob's cluster list into a read-only blob and the origin becomes
 * CoW; a clone's new blob points straight at the parent's clusters. The
 * existing chunk objects stay referenced, and only new writes produce new
 * objects.
 *
 * === Why CoW cannot rewrite a shared chunk ===
 *
 * This was verified in the blobstore before touching it, because guessing
 * wrong silently corrupts snapshots:
 *
 *   blob_can_copy() requires `blob->bs->dev->copy != NULL` **and**
 *   `translate_lba()` to return true. We satisfy neither (copy left NULL,
 *   translate_lba always returns false; the reasons are in the
 *   s3_bs_dev_translate_lba comment), so the "move data inside the device"
 *   fast path can never be entered.
 *
 * The remaining regular path allocates a cluster_sz buffer, reads the whole
 * cluster from the back_bs_dev, then writes to
 *   `bs_cluster_to_lba(bs, ctx->new_cluster)` --
 *   **the write target is a freshly allocated cluster**, landing on a new LBA,
 *   a new chunk, a new uuid, a new S3 object. Rewriting the parent's chunk in
 *   place is impossible on this path.
 *
 * The cost is amplification: a write landing on an unallocated cluster first
 * reads a whole cluster. At 1 MiB chunks and 4 MiB clusters that is 4 GETs
 * plus 4 PUTs. Correct but not cheap; worth benchmarking on a clone before
 * discussing performance.
 *
 * === I/O on the origin volume is frozen during the snapshot ===
 *
 * `spdk_bs_create_snapshot()` freezes the origin blob's I/O internally. If an
 * S3 request is stuck and never returns at that moment, the snapshot stalls
 * with it. This is not solvable here, but it should be the first thing
 * considered when diagnosing.
 * ========================================================================== */

/* Completion callback for snapshot / clone.
 *
 * The only difference from create is what happens when registration fails --
 * and that difference is mandatory. A failed create deletes the lvol; that
 * must not happen here: once the snapshot exists, **the origin is already its
 * clone**, and the blobstore refuses to delete a snapshot that still has a
 * clone. Copying create's behaviour would only produce a failing cleanup plus
 * a misleading error log.
 *
 * So the snapshot is kept and the registration failure is reported honestly.
 * The data is intact; only a bdev is missing, and re-attaching the lvstore
 * registers it. */
static void
s3lvol_lvol_derive_cb(void *cb_arg, struct spdk_lvol *lvol, int lvolerrno)
{
	struct lvol_create_ctx *ctx = cb_arg;
	s3lvol_lvol_op_cb cb_fn = ctx->cb_fn;
	void *user_arg = ctx->cb_arg;
	const char *lvs_name = ctx->lvs->name;
	struct s3_producer_gate *gate = &ctx->lvs->producers;
	struct s3lvol_lvstore *lvs = ctx->lvs;
	int rc;

	free(ctx);

	if (lvolerrno != 0) {
		SPDK_ERRLOG("Failed to create snapshot/clone: %s\n",
			    spdk_strerror(-lvolerrno));
		if (cb_fn) {
			cb_fn(user_arg, NULL, lvolerrno);
		}
		return;
	}

	/* Say so while there is still room to act.
	 *
	 * Deriving is the only thing that lengthens a chain, so this is the one
	 * place the crossing can be caught as it happens. Past
	 * S3LVOL_DEFAULT_MAX_CHAIN_DEPTH an export of this lvol silently stops being
	 * zero-copy and duplicates the whole volume into an export that is then
	 * never reaped -- correct, expensive, and permanent. The soft threshold
	 * leaves eight snapshots' worth of warning before that.
	 *
	 * A warning and not a refusal: the snapshot is the caller's to keep, and
	 * failing an ordinary create_snapshot to avoid a cost they may be willing to
	 * pay would be the worse trade. What the message has to carry is therefore
	 * what to *do* -- deleting an intermediate snapshot shortens the chain and is
	 * pure metadata, no S3 traffic, because blobstore merges the deleted layer
	 * into its single clone.
	 *
	 * Logged after the bdev registration below would be tidier, but this runs
	 * before it deliberately: a registration failure keeps the snapshot, so the
	 * chain is longer either way and the caller should hear about it either way. */
	if (lvs) {
		uint32_t depth = s3lvol_lvol_chain_depth(lvs, lvol);

		if (depth > S3LVOL_DEFAULT_MAX_CHAIN_DEPTH) {
			SPDK_WARNLOG("lvol '%s/%s' is %u snapshot(s) deep, past the "
				     "limit of %u: exporting it can no longer name "
				     "the objects it already has and will copy the "
				     "whole volume instead, into an export nothing "
				     "reclaims. Delete an intermediate snapshot to "
				     "shorten the chain -- that is metadata only, no "
				     "S3 traffic.\n",
				     lvs_name, lvol->name, depth,
				     S3LVOL_DEFAULT_MAX_CHAIN_DEPTH);
		} else if (depth >= S3LVOL_DEFAULT_SOFT_CHAIN_DEPTH) {
			SPDK_WARNLOG("lvol '%s/%s' is %u snapshot(s) deep; at %u an "
				     "export stops being zero-copy and duplicates the "
				     "volume. Deleting an intermediate snapshot "
				     "shortens the chain and moves no data.\n",
				     lvs_name, lvol->name, depth,
				     S3LVOL_DEFAULT_MAX_CHAIN_DEPTH + 1);
		}
	}

	rc = vbdev_s3lvol_bdev_register(lvol, lvs_name, gate);
	if (rc != 0) {
		SPDK_ERRLOG("lvol '%s' was created but its bdev could not be "
			    "registered (%d). The data is intact -- re-attach the "
			    "lvstore to expose it.\n", lvol->name, rc);
		if (cb_fn) {
			cb_fn(user_arg, NULL, rc);
		}
		return;
	}

	if (cb_fn) {
		cb_fn(user_arg, lvol, 0);
	}
}

/* The lvol must belong to lvs: the caller looked up both separately, and
 * without this check a cross-lvstore combination would travel all the way to
 * the blobstore before exploding. */
static int
derive_check(struct s3lvol_lvstore *lvs, struct spdk_lvol *lvol, const char *name)
{
	if (!lvs || !lvs->lvs || !lvol || !name || name[0] == '\0') {
		return -EINVAL;
	}
	if (lvol->lvol_store != lvs->lvs) {
		SPDK_ERRLOG("lvol '%s' does not belong to lvstore '%s'\n",
			    lvol->name, lvs->name);
		return -EINVAL;
	}

	/* A decouple in flight is the case this is really about. Snapshotting an lvol
	 * halfway through one hands the esnap parent to the snapshot, and the decouple
	 * would then go on materialising and clearing the parent of a blob that no
	 * longer has it -- while the snapshot keeps reading an export everybody has
	 * stopped accounting for. blobstore's own locked_operation_in_progress does
	 * not cover this: a decouple deliberately does not hold it, so that the volume
	 * stays usable while the data is copied. */
	if (lvol->action_in_progress) {
		SPDK_ERRLOG("another operation is in progress on lvol '%s'\n",
			    lvol->name);
		return -EBUSY;
	}

	/* A queued decouple deliberately does not hold action_in_progress -- a
	 * queued volume must remain deletable -- but snapshotting it is just as
	 * destructive: the snapshot takes the external snapshot identity with it,
	 * and the queued decouple then fails its detach with "blob is not a clone
	 * of an external snapshot" after materialising the data. */
	if (s3lvol_lvol_decouple_pending(lvol)) {
		SPDK_ERRLOG("lvol '%s' is queued to be decoupled; a snapshot or "
			    "clone would take its external snapshot while the "
			    "decouple still reads through it\n", lvol->name);
		return -EBUSY;
	}
	return 0;
}

static struct lvol_create_ctx *
derive_ctx_alloc(struct s3lvol_lvstore *lvs, s3lvol_lvol_op_cb cb_fn, void *cb_arg)
{
	struct lvol_create_ctx *ctx = calloc(1, sizeof(*ctx));

	if (ctx) {
		ctx->lvs    = lvs;
		ctx->cb_fn  = cb_fn;
		ctx->cb_arg = cb_arg;
	}
	return ctx;
}

/* Everything create_snapshot needs to resume after a cancelled decouple.
 *
 * snapshot_name is copied rather than kept: the caller's string belongs to the
 * RPC request, which frees it as soon as the dispatching function returns -- and
 * with a cancellation in flight that happens long before the name is used. */
struct snapshot_after_cancel_ctx {
	struct s3lvol_lvstore *lvs;
	struct spdk_lvol      *lvol;
	char                   snapshot_name[SPDK_LVOL_NAME_MAX];
	s3lvol_lvol_op_cb      cb_fn;
	void                  *cb_arg;
};

static void
snapshot_after_cancel(void *cb_arg, int status)
{
	struct snapshot_after_cancel_ctx *c = cb_arg;
	int rc;

	/* The cancellation is what clears action_in_progress and empties the queue,
	 * so by here the volume must look untouched to derive_check. If it does not,
	 * the retry below would cancel nothing and refuse -- better to state the
	 * expectation than to loop on it. */
	assert(!s3lvol_lvol_decouple_pending(c->lvol));

	rc = s3lvol_lvol_create_snapshot(c->lvs, c->lvol, c->snapshot_name, NULL,
					 c->cb_fn, c->cb_arg);
	if (rc != 0 && c->cb_fn) {
		c->cb_fn(c->cb_arg, NULL, rc);
	}
	free(c);
}

static int
s3lvol_lvol_create_snapshot_admitted(struct s3lvol_lvstore *lvs, struct spdk_lvol *lvol,
			    const char *snapshot_name,
			    bool *out_cancelled_decouple,
			    s3lvol_lvol_op_cb cb_fn, void *cb_arg)
{
	struct lvol_create_ctx *ctx;
	int rc;

	if (out_cancelled_decouple) {
		*out_cancelled_decouple = false;
	}

	/* Before derive_check, because a decouple in flight is exactly what it
	 * refuses -- and for the default import it always is one. Cancelling here
	 * turns "import, then snapshot" from impossible into ordinary; what it costs
	 * is that the volume keeps reading the export, which is the reference
	 * snapshot this is for. Reported through out_cancelled_decouple so the
	 * caller learns that the decouple it asked for is not going to happen. */
	if (s3lvol_lvol_decouple_pending(lvol)) {
		struct snapshot_after_cancel_ctx *c;

		c = calloc(1, sizeof(*c));
		if (!c) {
			return -ENOMEM;
		}
		c->lvs    = lvs;
		c->lvol   = lvol;
		c->cb_fn  = cb_fn;
		c->cb_arg = cb_arg;
		spdk_strcpy_pad(c->snapshot_name, snapshot_name,
				sizeof(c->snapshot_name) - 1, '\0');

		rc = s3lvol_decouple_cancel(lvol, snapshot_after_cancel, c);
		if (rc < 0) {
			free(c);
			return rc;
		}
		if (out_cancelled_decouple) {
			*out_cancelled_decouple = true;
		}
		if (rc == 1) {
			/* Accepted; snapshot_after_cancel() carries on from here. */
			return 0;
		}
		/* Cancelled synchronously (it was only queued), so fall through and
		 * take the snapshot now rather than bouncing through the callback. */
		free(c);
	}

	rc = derive_check(lvs, lvol, snapshot_name);
	if (rc != 0) {
		return rc;
	}

	/* Snapshotting a read-only blob is pointless: its content will never
	 * change again, and a clone is how to get another name. The blobstore
	 * would refuse too, but refusing here gives a reason one can
	 * articulate. */
	if (lvol->blob && spdk_blob_is_read_only(lvol->blob)) {
		SPDK_ERRLOG("lvol '%s' is already read-only (a snapshot?); "
			    "clone it instead\n", lvol->name);
		return -EPERM;
	}

	ctx = derive_ctx_alloc(lvs, cb_fn, cb_arg);
	if (!ctx) {
		return -ENOMEM;
	}

	spdk_lvol_create_snapshot(lvol, snapshot_name, s3lvol_lvol_derive_cb, ctx);
	return 0;
}

static int
s3lvol_lvol_create_clone_admitted(struct s3lvol_lvstore *lvs, struct spdk_lvol *lvol,
			 const char *clone_name,
			 s3lvol_lvol_op_cb cb_fn, void *cb_arg)
{
	struct lvol_create_ctx *ctx;
	int rc;

	rc = derive_check(lvs, lvol, clone_name);
	if (rc != 0) {
		return rc;
	}

	/* Only snapshots can be cloned. The blobstore's
	 * spdk_bs_create_clone() also requires the parent to be read-only --
	 * otherwise clone and parent are simultaneously writable, anyone can
	 * modify a shared cluster, and neither side's data is defined any more.
	 * Blocking here is what lets the error message say why. */
	if (!lvol->blob || !spdk_blob_is_read_only(lvol->blob)) {
		SPDK_ERRLOG("lvol '%s' is not a snapshot; only a read-only lvol "
			    "can be cloned\n", lvol->name);
		return -EINVAL;
	}

	ctx = derive_ctx_alloc(lvs, cb_fn, cb_arg);
	if (!ctx) {
		return -ENOMEM;
	}

	spdk_lvol_create_clone(lvol, clone_name, s3lvol_lvol_derive_cb, ctx);
	return 0;
}

/* ==========================================================================
 * lvol deletion
 * ========================================================================== */

struct lvol_destroy_ctx {
	struct spdk_lvol        *lvol;

	/* Where to look afterwards, and for what. All three are read *before* the
	 * blob goes away: once the destroy succeeds there is no lvol left to ask
	 * its name or what it used to read through to, and the registry entry that
	 * has to be dropped is keyed by exactly that. */
	struct s3lvol_lvstore   *owner;
	char                     name[SPDK_LVOL_NAME_MAX];
	char                     esnap_uuid[SPDK_UUID_STRING_LEN];

	/* The pending-delete mark is keyed by these, and both are unreadable by
	 * the time the completion runs -- the lvol is freed on success. Copied
	 * here so the callback can clear (or record) the right mark. */
	struct spdk_uuid         lvs_uuid;
	struct spdk_uuid         lvol_uuid;

	/* Cluster counts read before the destroy: the blob must still be open
	 * to read them, and is gone by the time the success callback runs, so
	 * the reclaimed counts can only be reported from there. */
	uint64_t                 allocated_clusters;
	uint64_t                 total_clusters;
	bool                     have_cluster_counts;
	bool                     is_snapshot;
	bool                     release_chain;

	spdk_lvol_op_complete    cb_fn;
	void                    *cb_arg;
};

/* Record the "a delete was asked for and refused" mark.
 *
 * Called only from the refusals in s3lvol_lvol_destroy() that clear on their own
 * -- an export pin (published or in flight), more than one clone, a running
 * decouple -- which is what makes the mark useful: the blocker goes away without
 * the caller doing anything, and --retry-pending comes back to finish the delete.
 * Keyed by uuid, so it cannot survive into a same-named object.
 *
 * Deliberately does not test snapshot-ness first. It cannot be answered reliably
 * with the blob closed, and these refusals fire before any blob check: an export
 * pin or a queued decouple is reported for a deactivated volume just the same.
 * spdk_blob_is_read_only() needs the blob open, and spdk_blob_get_clones() only
 * finds blobs that something still references as a parent -- after a re-attach a
 * snapshot whose last clone was deleted has no entry at all
 * (bs_blob_list_add() at blobstore.c:3780 rebuilds the registry from each blob's
 * parent_id), so it would answer "not a snapshot" for exactly the case the mark
 * is for.
 *
 * The asymmetry decides it: marking something that is not a snapshot costs one
 * retried delete the caller did ask for, while not marking loses the request
 * entirely. Only the three blockers above reach here anyway -- an ordinary
 * volume's "deactivate it first" refusal is answered in the RPC layer and never
 * gets this far. */
static void
destroy_mark_pending(struct spdk_lvol *lvol, enum s3lvol_pending_reason reason)
{
	struct s3lvol_lvstore *owner;

	if (!lvol || !lvol->lvol_store) {
		return;
	}
	owner = s3lvol_lvstore_find_by_lvs(lvol->lvol_store);
	s3lvol_snapshot_pending_set(&lvol->lvol_store->uuid, &lvol->uuid,
				    owner ? s3lvol_lvstore_get_name(owner) : NULL,
				    lvol->name, reason);
}

/* Deleting the last volume that read an export is what ends this node's
 * dependency on it. The imports registry has to hear about that here, while the
 * lvstore is still loaded -- see s3lvol_imports_recheck(). */
static void
s3lvol_lvol_destroyed(void *cb_arg, int lvolerrno)
{
	struct lvol_destroy_ctx *ctx = cb_arg;

	if (lvolerrno == 0) {
		/* A delete that finally went through clears any pending mark. */
		s3lvol_snapshot_pending_clear(&ctx->lvs_uuid, &ctx->lvol_uuid);
		/* The one line a search can find for a successful delete. Nothing else
		 * prints it: the unregister is silent on success, the object deletes
		 * that follow are fire-and-forget and log only their failures, and the
		 * RPC answer travels to the caller, not the log. So "did the delete
		 * actually go through" was previously answerable only by asking again
		 * or counting objects; a production post-mortem needs better. The
		 * reclaimed cluster counts are printed here, not before the delete,
		 * so the claim only ever accompanies an actual success. */
		if (ctx->have_cluster_counts) {
			SPDK_NOTICELOG("Deleted lvol '%s/%s', reclaimed %" PRIu64
				       " of %" PRIu64 " clusters\n",
				       ctx->owner ? s3lvol_lvstore_get_name(ctx->owner) : "(null)",
				       ctx->name,
				       ctx->allocated_clusters, ctx->total_clusters);
		} else {
			SPDK_NOTICELOG("Deleted lvol '%s/%s'\n",
				       ctx->owner ? s3lvol_lvstore_get_name(ctx->owner) : "(null)",
				       ctx->name);
		}

		if (ctx->esnap_uuid[0] != '\0') {
			s3lvol_imports_recheck(ctx->owner, ctx->esnap_uuid);
		}
	} else {
		if (ctx->release_chain && ctx->lvol) {
			ctx->lvol->action_in_progress = false;
			s3lvol_decouple_kick_queue();
		}
		/* The refusal paths in s3lvol_lvol_destroy() log before returning, but
		 * an asynchronous failure -- the unregister, or spdk_lvol_destroy --
		 * lands here instead and would otherwise be silent. A blocked
		 * snapshot delete records the intent so --ls can show it.
		 *
		 * Recorded as FAILED rather than as one of the reference blockers:
		 * the pre-checks all passed, so whatever stopped this is not a
		 * reference count and would stop it again. The poller leaves such
		 * an entry alone and an explicit retry is needed. */
		if (ctx->is_snapshot) {
			s3lvol_snapshot_pending_set(&ctx->lvs_uuid,
						    &ctx->lvol_uuid,
						    ctx->owner
						    ? s3lvol_lvstore_get_name(ctx->owner)
						    : NULL,
						    ctx->name,
						    S3LVOL_PENDING_FAILED);
		}
		SPDK_ERRLOG("Failed to delete lvol '%s/%s': %s\n",
			    ctx->owner ? s3lvol_lvstore_get_name(ctx->owner) : "(null)",
			    ctx->name, spdk_strerror(-lvolerrno));
	}

	if (ctx->cb_fn) {
		ctx->cb_fn(ctx->cb_arg, lvolerrno);
	}
	free(ctx);
}

static void
s3lvol_lvol_bdev_unregistered(void *cb_arg, int bdeverrno)
{
	struct lvol_destroy_ctx *ctx = cb_arg;

	if (bdeverrno != 0) {
		if (ctx->release_chain && ctx->lvol) {
			ctx->lvol->action_in_progress = false;
			s3lvol_decouple_kick_queue();
		}
		if (ctx->is_snapshot) {
			s3lvol_snapshot_pending_set(&ctx->lvs_uuid, &ctx->lvol_uuid,
						    ctx->owner
						    ? s3lvol_lvstore_get_name(ctx->owner)
						    : NULL,
						    ctx->name,
						    S3LVOL_PENDING_FAILED);
		}
		SPDK_ERRLOG("Failed to unregister bdev: %d\n", bdeverrno);
		if (ctx->cb_fn) {
			ctx->cb_fn(ctx->cb_arg, bdeverrno);
		}
		free(ctx);
		return;
	}

	/* The bdev is unregistered; the lvol can be deleted now. */
	spdk_lvol_destroy(ctx->lvol, s3lvol_lvol_destroyed, ctx);
}

struct snapshot_export_release_ctx {
	struct spdk_lvol      *lvol;
	spdk_lvol_op_complete  cb_fn;
	void                  *cb_arg;
};

static int
snapshot_destroy_check_clones(struct spdk_lvol *lvol)
{
	struct spdk_blob_store *bs;
	size_t clone_count = 0;
	int rc;

	if (!lvol->blob) {
		return 0;
	}
	bs = lvol->lvol_store->blobstore;
	rc = spdk_blob_get_clones(bs, lvol->blob_id, NULL, &clone_count);
	if (rc != 0 && rc != -ENOMEM) {
		SPDK_ERRLOG("cannot tell how many clones snapshot '%s' has (%s); "
			    "refusing to delete it\n", lvol->name,
			    spdk_strerror(-rc));
		return -EBUSY;
	}
	if (clone_count > 1) {
		SPDK_ERRLOG("lvol '%s' is a snapshot with %zu clones; blobstore can "
			    "only merge a snapshot into a single clone\n",
			    lvol->name, clone_count);
		destroy_mark_pending(lvol, S3LVOL_PENDING_CLONE_COUNT);
		return -EBUSY;
	}
	return 0;
}

static int s3lvol_lvol_destroy_impl(struct spdk_lvol *lvol,
				    spdk_lvol_op_complete cb_fn, void *cb_arg,
				    bool release_chain);

static void
snapshot_export_released(void *cb_arg, int status)
{
	struct snapshot_export_release_ctx *ctx = cb_arg;
	spdk_lvol_op_complete cb_fn = ctx->cb_fn;
	void *op_arg = ctx->cb_arg;
	struct spdk_lvol *lvol = ctx->lvol;
	int rc;

	if (status != 0) {
		lvol->action_in_progress = false;
		s3lvol_decouple_kick_queue();
		destroy_mark_pending(lvol, S3LVOL_PENDING_FAILED);
		free(ctx);
		if (cb_fn) {
			cb_fn(op_arg, status);
		}
		return;
	}

	/* There may be several exports of one snapshot. Re-entering releases the
	 * next one; once none remain it follows the ordinary destroy path. */
	free(ctx);
	rc = s3lvol_lvol_destroy_impl(lvol, cb_fn, op_arg, true);
	if (rc != 0) {
		lvol->action_in_progress = false;
		s3lvol_decouple_kick_queue();
		if (cb_fn) {
			cb_fn(op_arg, rc);
		}
	}
}

static int
s3lvol_lvol_destroy_impl(struct spdk_lvol *lvol,
			 spdk_lvol_op_complete cb_fn, void *cb_arg,
			 bool release_chain)
{
	struct s3lvol_lvstore *owner;
	struct s3lvol_export *pin;
	int rc;

	if (!lvol) {
		return -EINVAL;
	}

	/* A snapshot another machine is reading through must not go away. Deleting it
	 * would release its clusters, and the objects behind them would then be
	 * deleted by an overwrite or by GC -- leaving an importer reading holes that
	 * are not holes, and unable to open its clone at all after a restart.
	 *
	 * A live lease therefore defers this explicit delete. Once no reader remains,
	 * the path below releases every export before destroying the snapshot; export
	 * lifetime never ends merely because wall-clock time passed. */
	owner = s3lvol_lvstore_find_by_lvs(lvol->lvol_store);
	if (s3lvol_lvol_source_held(lvol) || !s3lvol_lvstore_xfer_allowed(owner)) {
		return -EBUSY;
	}

	/* An export that has not published its manifest yet is not in the registry,
	 * so the pin check below cannot see it -- and it is holding a bare
	 * spdk_lvol pointer it dereferences after the drain completes.
	 *
	 * That window existed while rcow_export_snapshot answered only on
	 * completion, but the caller was blocked on the reply and had no chance to
	 * act on it. Now that the uuid comes back immediately, the caller is
	 * explicitly free to do other things while the export runs, so the window
	 * has to be closed here rather than merely reported by
	 * rcow_get_snapshot_status. */
	if (owner && s3lvol_export_inflight_pinning(owner, lvol->name)) {
		SPDK_ERRLOG("lvol '%s' is being exported right now; wait for "
			    "rcow_get_snapshot_status to stop reporting INPROGRESS "
			    "before deleting it\n", lvol->name);
		return -EBUSY;
	}

	pin = owner ? s3lvol_export_pinning(owner, lvol->name) : NULL;
	if (pin) {
		struct s3lvol_export_entry info;

		s3lvol_export_get(pin, &info);
		SPDK_ERRLOG("lvol '%s' is the snapshot behind export %s, which another "
			    "node may still be reading through; delete is deferred "
			    "until its durable dependency is discharged\n",
			    lvol->name, info.export_uuid);
		return -EBUSY;
	}

	/* Do not mistake another operation's action flag for the release chain's
	 * own serialization. Only a continuation may bypass it. */
	if (lvol->action_in_progress && !release_chain) {
		SPDK_ERRLOG("another operation is in progress on lvol '%s'; it cannot "
			    "be deleted yet\n", lvol->name);
		destroy_mark_pending(lvol, S3LVOL_PENDING_DECOUPLE);
		return -EBUSY;
	}
	rc = snapshot_destroy_check_clones(lvol);
	if (rc != 0) {
		return rc;
	}

	/* An explicit snapshot delete also revokes every export of that snapshot.
	 * The release is deliberately internal: callers own snapshot lifetime, not
	 * the ref-export/lease/esnap dependency graph below it.
	 *
	 * A live remote lease was refused above. A same-process esnap reader is
	 * checked by s3lvol_export_release(); if its background decouple has not
	 * finished yet, record the delete and let the pending poller retry. */
	if (owner) {
		struct s3lvol_export *exp =
			s3lvol_export_first_ref_for_snapshot(owner, lvol->name);

		if (exp) {
			struct snapshot_export_release_ctx *release_ctx;
			struct s3lvol_export_entry info;
			int rc;

			s3lvol_export_get(exp, &info);
			release_ctx = calloc(1, sizeof(*release_ctx));
			if (!release_ctx) {
				return -ENOMEM;
			}
			release_ctx->lvol = lvol;
			release_ctx->cb_fn = cb_fn;
			release_ctx->cb_arg = cb_arg;

			/* Hold action_in_progress across the entire release chain and
			 * final unregister. The delete-specific release entry point knows
			 * that this flag belongs to its own operation. Drop a queued
			 * decouple now: it does not hold the flag, and the drain would
			 * otherwise start it during the S3 round-trips below. */
			lvol->action_in_progress = true;
			s3lvol_decouple_dequeue_lvol(lvol);
			rc = s3lvol_export_release_for_delete(
				owner, info.export_uuid, snapshot_export_released,
				release_ctx);
			if (rc != 0) {
				lvol->action_in_progress = false;
				s3lvol_decouple_kick_queue();
				free(release_ctx);
				if (rc == -EBUSY) {
					destroy_mark_pending(lvol,
							 S3LVOL_PENDING_EXPORT);
				}
				return rc;
			}

			return 0;
		}
	}

	struct lvol_destroy_ctx *ctx;

	/* A snapshot with *more than one* clone cannot be deleted, and that has to be
	 * established here rather than left to blobstore.
	 *
	 * The order further down is: unregister the bdev, and destroy the lvol from
	 * the unregister's callback. blobstore's own refusal therefore arrives after
	 * the bdev is already gone and the lvol already closed, with no way back --
	 * the delete reports failure, and what it leaves behind is a snapshot that
	 * cannot be activated any more, in an lvstore that can no longer be unloaded
	 * at all, because spdk_lvol_close() on an already-closed lvol answers -EINVAL
	 * and lvs_unload_close_lvols() treats that as "not every lvol could be
	 * closed".
	 *
	 * A failed delete has to leave the volume exactly as it was. Checking first
	 * is what does that; rolling the unregister back afterwards would mean
	 * re-registering a bdev whose consumers have already been told it is gone.
	 * This mirrors the export pin check above, which refuses before touching
	 * anything for the same reason.
	 *
	 * The threshold is > 1, not > 0, and that distinction was originally wrong
	 * here. bs_is_blob_deletable() (blobstore.c:8714) refuses unconditionally
	 * only above one clone; at exactly one it sets *update_clone and *merges* the
	 * snapshot into that clone, and spdk_lvol_destroy() cooperates by looking the
	 * clone's lvol up beforehand so it can be fixed up afterwards. Refusing at
	 * one clone therefore rejected something the layers below implement, and
	 * since every snapshot of a live volume has exactly one clone, it made
	 * snapshots undeletable until the volume itself was deleted -- turning a
	 * chain of N snapshots into something that could only be dismantled from the
	 * leaf end. That was a regression, not a safety property.
	 *
	 * Everything else bs_is_blob_deletable() can refuse depends on open_ref,
	 * which is not knowable from here and which the normal path handles by
	 * closing the lvol first. Only the unconditional case is worth pre-empting.
	 *
	 * Querying with ids == NULL asks for the count alone: blobstore then always
	 * takes its -ENOMEM branch after filling *count in, and answers 0 with
	 * count == 0 when the blob is not a snapshot at all. So no separate
	 * spdk_blob_is_snapshot() test is needed. lvol->blob_id rather than
	 * spdk_blob_get_id(lvol->blob), because blob_id stays valid even once the
	 * blob is closed -- which is what upstream does at lvol.c:1584. */
	/* snapshot_destroy_check_clones() runs before any export is released, so a
	 * synchronous clone-count refusal leaves every manifest intact. Once the
	 * release chain has started there is no rollback: an asynchronous blob
	 * failure leaves the snapshot without the export that was already revoked. */

	/* Past the check above, so this volume is not materialising anything -- but it
	 * may be *waiting* to. A queued decouple deliberately does not hold
	 * action_in_progress, since it can sit behind a run that takes minutes, so the
	 * delete is allowed and the queue entry has to go instead. Done here rather
	 * than in the completion path because the queue holds this lvol pointer, and
	 * by then it is freed. */
	s3lvol_decouple_dequeue_lvol(lvol);

	/* Section 12.0.4 constraint 1 requires -EBUSY while the bdev is still
	 * claimed, and never a forced removal.
	 *
	 * But SPDK has no public interface to query claim state (enum
	 * spdk_bdev_claim_type lives only in bdev_module.h, with no getter), and
	 * the upstream vbdev_lvol simply calls spdk_bdev_unregister
	 * (vbdev_lvol.c:686).
	 *
	 * Today we rely on the bdev layer's own protection:
	 * spdk_bdev_unregister() waits for every open descriptor to close before
	 * actually destructing, so an unreleased claim never silently loses
	 * data. Giving an explicit -EBUSY plus a "please
	 * nvmf_subsystem_remove_ns" hint at the RPC layer would require keeping
	 * our own record of namespace attachments -- deferred to the RPC-layer
	 * implementation.
	 */
	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		return -ENOMEM;
	}
	ctx->lvol   = lvol;
	ctx->owner  = owner;
	ctx->cb_fn  = cb_fn;
	ctx->cb_arg = cb_arg;
	ctx->release_chain = true;
	lvol->action_in_progress = true;

	/* Copied now: the destroy frees the lvol and its name with it. */
	snprintf(ctx->name, sizeof(ctx->name), "%s", lvol->name);

	/* Snapshot (read-only blob) deletions carry the pending-delete mark when
	 * they cannot complete; recorded here because the blob is gone by the
	 * time the completion callback runs. */
	ctx->is_snapshot = lvol->blob != NULL && spdk_blob_is_read_only(lvol->blob);

	/* The mark's key, for the same reason: on success the lvol is freed, so
	 * the completion cannot read either uuid off it any more. */
	if (lvol->lvol_store) {
		spdk_uuid_copy(&ctx->lvs_uuid, &lvol->lvol_store->uuid);
	}
	spdk_uuid_copy(&ctx->lvol_uuid, &lvol->uuid);

	/* Recorded now, for the same reason the owner is: after the destroy the blob
	 * is gone and the esnap id with it. An id that is not a NUL-terminated uuid
	 * string is not one of ours, so it is left empty rather than guessed at. */
	if (lvol->blob && spdk_blob_is_esnap_clone(lvol->blob)) {
		const void *esnap_id = NULL;
		size_t id_len = 0;

		if (spdk_blob_get_esnap_id(lvol->blob, &esnap_id, &id_len) == 0 &&
		    id_len < sizeof(ctx->esnap_uuid)) {
			memcpy(ctx->esnap_uuid, esnap_id, id_len);
			ctx->esnap_uuid[id_len] = '\0';
		}
	}

	/* Capture the cluster counts before the destroy: the blob must still be
	 * open to read them, and is gone by the time s3lvol_lvol_destroyed()
	 * runs. Printing them from the success callback (rather than here) keeps
	 * "reclaimed" honest when an asynchronous destroy fails. */
	if (lvol->blob != NULL) {
		ctx->allocated_clusters =
			spdk_blob_get_num_allocated_clusters(lvol->blob);
		ctx->total_clusters =
			spdk_blob_get_num_clusters(lvol->blob);
		ctx->have_cluster_counts = true;
	}

	if (lvol->bdev) {
		vbdev_s3lvol_bdev_unregister(lvol, s3lvol_lvol_bdev_unregistered, ctx);
		return 0;
	}

	spdk_lvol_destroy(lvol, s3lvol_lvol_destroyed, ctx);
	return 0;
}

int
s3lvol_lvol_destroy(struct spdk_lvol *lvol,
		    spdk_lvol_op_complete cb_fn, void *cb_arg)
{
	return s3lvol_lvol_destroy_impl(lvol, cb_fn, cb_arg, false);
}

/* ==========================================================================
 * lvol lookup
 * ========================================================================== */

struct spdk_lvol *
s3lvol_lvol_find(struct s3lvol_lvstore *lvs, const char *name)
{
	struct spdk_lvol *lvol;

	if (!lvs || !lvs->lvs || !name) {
		return NULL;
	}

	/* The blobstore's lvols list holds **every opened** lvol, snapshots and
	 * clones included (they differ from ordinary volumes only in the blob's
	 * read-only bit and parent/child relationships). So this single lookup
	 * serves resize / delete / snapshot / clone alike; no separate tables
	 * needed. */
	TAILQ_FOREACH(lvol, &lvs->lvs->lvols, link) {
		if (strcmp(lvol->name, name) == 0) {
			return lvol;
		}
	}

	return NULL;
}

struct spdk_lvol *
s3lvol_lvol_find_any(const char *name)
{
	struct s3lvol_lvstore *lvs;
	struct spdk_lvol *found = NULL;

	if (!name) {
		return NULL;
	}

	TAILQ_FOREACH(lvs, &g_lvstores, link) {
		struct spdk_lvol *lvol = s3lvol_lvol_find(lvs, name);

		if (lvol) {
			if (found) {
				return NULL; /* ambiguous */
			}
			found = lvol;
		}
	}
	return found;
}

struct s3lvol_lvstore *
s3lvol_lvstore_of_lvol(struct spdk_lvol *lvol)
{
	struct s3lvol_lvstore *lvs;

	if (!lvol || !lvol->lvol_store) {
		return NULL;
	}

	TAILQ_FOREACH(lvs, &g_lvstores, link) {
		if (lvs->lvs == lvol->lvol_store) {
			return lvs;
		}
	}
	return NULL;
}

/* ==========================================================================
 * lvol resize
 *
 * **Only growth is supported.** Shrinking is doable at the blobstore layer,
 * but is actively refused here:
 *
 *   1. It irreversibly loses data, and an RPC is a too-easy entry point for a
 *      mistyped argument;
 *   2. whether the chunks past the new boundary should have their S3 objects
 *      deleted depends on whether the blobstore passes unmap down to the
 *      bs_dev when releasing clusters (lvols are created with
 *      LVOL_CLEAR_WITH_UNMAP here, which suggests it does), but that chain is
 *      **unverified**. Guessing wrong orphans the objects, and reclaiming
 *      orphans relies on s3_gc.c, which does not exist yet -- i.e. paying
 *      storage forever.
 *
 * Reconsider lifting the guard once GC has landed and a real machine confirms
 * unmap propagates.
 * ========================================================================== */

struct lvol_resize_ctx {
	struct spdk_lvol        *lvol;
	spdk_lvol_op_complete    cb_fn;
	void                    *cb_arg;
};

static void
s3lvol_lvol_resize_cb(void *cb_arg, int lvolerrno)
{
	struct lvol_resize_ctx *ctx = cb_arg;
	struct spdk_lvol *lvol = ctx->lvol;
	spdk_lvol_op_complete cb_fn = ctx->cb_fn;
	void *user_arg = ctx->cb_arg;
	uint64_t total_size;
	int rc = lvolerrno;

	free(ctx);

	if (rc != 0) {
		SPDK_ERRLOG("Failed to resize lvol '%s': %s\n",
			    lvol->name, spdk_strerror(-rc));
		goto out;
	}

	/* The blob is the source of truth, so the new bdev size is read back from
	 * it rather than taken from what the caller asked for: spdk_lvol_resize()
	 * rounds up to a whole number of clusters. */
	total_size = spdk_blob_get_num_clusters(lvol->blob) *
		     spdk_bs_get_cluster_size(lvol->lvol_store->blobstore);

	rc = spdk_bdev_notify_blockcnt_change(lvol->bdev,
					      total_size / lvol->bdev->blocklen);
	if (rc != 0) {
		/* The blob grew but the bdev did not, so the two now disagree and
		 * the extra space is unreachable. Reported rather than papered
		 * over; a re-attach rebuilds blockcnt from the blob and fixes it. */
		SPDK_ERRLOG("lvol '%s' was resized to %" PRIu64 " bytes but the bdev "
			    "could not be notified (%d); re-attach the lvstore to "
			    "resynchronise\n", lvol->name, total_size, rc);
		goto out;
	}

	SPDK_NOTICELOG("Resized lvol '%s' to %" PRIu64 " MiB (%" PRIu64 " blocks)\n",
		       lvol->name, total_size / (1024 * 1024),
		       lvol->bdev->blockcnt);

out:
	if (cb_fn) {
		cb_fn(user_arg, rc);
	}
}

static int
s3lvol_lvol_resize_admitted(struct spdk_lvol *lvol, uint64_t size_bytes,
		   spdk_lvol_op_complete cb_fn, void *cb_arg)
{
	struct lvol_resize_ctx *ctx;
	uint64_t cluster_sz, current_size;

	if (!lvol || !lvol->blob || size_bytes == 0) {
		return -EINVAL;
	}
	/* Upstream asserts on this. Here it is a real possibility rather than a
	 * programming error: a snapshot whose bdev registration failed is still
	 * in the lvstore's list and can be named in an RPC. */
	if (!lvol->bdev) {
		SPDK_ERRLOG("lvol '%s' has no bdev; nothing to resize\n", lvol->name);
		return -EINVAL;
	}
	if (spdk_blob_is_read_only(lvol->blob)) {
		/* Snapshots are read-only by construction. Growing one would make
		 * the clones that share its clusters disagree about where their
		 * backing device ends. */
		SPDK_ERRLOG("lvol '%s' is read-only (a snapshot?) and cannot be "
			    "resized\n", lvol->name);
		return -EPERM;
	}

	cluster_sz = spdk_bs_get_cluster_size(lvol->lvol_store->blobstore);
	current_size = spdk_blob_get_num_clusters(lvol->blob) * cluster_sz;

	/* Compared after rounding up, so asking for one byte more than the current
	 * size is a no-op rather than an error -- the request is already satisfied,
	 * which is what the caller wanted. */
	if (spdk_divide_round_up(size_bytes, cluster_sz) * cluster_sz < current_size) {
		SPDK_ERRLOG("lvol '%s' is %" PRIu64 " MiB; shrinking it to %" PRIu64
			    " MiB is not supported\n", lvol->name,
			    current_size / (1024 * 1024), size_bytes / (1024 * 1024));
		return -ENOTSUP;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		return -ENOMEM;
	}
	ctx->lvol   = lvol;
	ctx->cb_fn  = cb_fn;
	ctx->cb_arg = cb_arg;

	spdk_lvol_resize(lvol, size_bytes, s3lvol_lvol_resize_cb, ctx);
	return 0;
}

/* Admission wrappers: callbacks retain the store until after delivery. */

int
s3lvol_lvol_create(struct s3lvol_lvstore *lvs, const char *name,
		   uint64_t size_bytes, bool thin_provision,
		   s3lvol_lvol_op_cb cb_fn, void *cb_arg)
{
	struct source_control_ticket *ticket;
	struct s3lvol_lvstore *owner = lvs;
	int rc = s3lvol_control_enter(owner);
	if (rc) return rc;
	ticket = calloc(1, sizeof(*ticket));
	if (!ticket) { s3lvol_control_leave(owner, 0); return -ENOMEM; }
	ticket->lvs = owner; ticket->arg = cb_arg;
	ticket->lvol_cb = cb_fn;
	rc = s3lvol_lvol_create_admitted(lvs, name, size_bytes, thin_provision, source_control_lvol_done, ticket);
	if (rc) { s3lvol_control_leave(owner, 0); free(ticket); }
	return rc;
}

int
s3lvol_lvol_create_snapshot(struct s3lvol_lvstore *lvs, struct spdk_lvol *lvol,
			    const char *snapshot_name,
			    bool *out_cancelled_decouple,
			    s3lvol_lvol_op_cb cb_fn, void *cb_arg)
{
	if (s3lvol_lvol_source_held(lvol)) return -EBUSY;
	struct source_control_ticket *ticket;
	struct s3lvol_lvstore *owner = lvs;
	int rc = s3lvol_control_enter(owner);
	if (rc) return rc;
	ticket = calloc(1, sizeof(*ticket));
	if (!ticket) { s3lvol_control_leave(owner, 0); return -ENOMEM; }
	ticket->lvs = owner; ticket->arg = cb_arg;
	ticket->lvol_cb = cb_fn;
	rc = s3lvol_lvol_create_snapshot_admitted(lvs, lvol, snapshot_name, out_cancelled_decouple, source_control_lvol_done, ticket);
	if (rc) { s3lvol_control_leave(owner, 0); free(ticket); }
	return rc;
}

int
s3lvol_lvol_create_clone(struct s3lvol_lvstore *lvs, struct spdk_lvol *lvol,
			 const char *clone_name,
			 s3lvol_lvol_op_cb cb_fn, void *cb_arg)
{
	if (s3lvol_lvol_source_held(lvol)) return -EBUSY;
	struct source_control_ticket *ticket;
	struct s3lvol_lvstore *owner = lvs;
	int rc = s3lvol_control_enter(owner);
	if (rc) return rc;
	ticket = calloc(1, sizeof(*ticket));
	if (!ticket) { s3lvol_control_leave(owner, 0); return -ENOMEM; }
	ticket->lvs = owner; ticket->arg = cb_arg;
	ticket->lvol_cb = cb_fn;
	rc = s3lvol_lvol_create_clone_admitted(lvs, lvol, clone_name, source_control_lvol_done, ticket);
	if (rc) { s3lvol_control_leave(owner, 0); free(ticket); }
	return rc;
}

int
s3lvol_lvol_resize(struct spdk_lvol *lvol, uint64_t size_bytes,
		   spdk_lvol_op_complete cb_fn, void *cb_arg)
{
	if (s3lvol_lvol_source_held(lvol)) return -EBUSY;
	struct source_control_ticket *ticket;
	struct s3lvol_lvstore *owner = s3lvol_lvstore_of_lvol(lvol);
	int rc = s3lvol_control_enter(owner);
	if (rc) return rc;
	ticket = calloc(1, sizeof(*ticket));
	if (!ticket) { s3lvol_control_leave(owner, 0); return -ENOMEM; }
	ticket->lvs = owner; ticket->arg = cb_arg;
	ticket->cb = cb_fn;
	rc = s3lvol_lvol_resize_admitted(lvol, size_bytes, source_control_done, ticket);
	if (rc) { s3lvol_control_leave(owner, 0); free(ticket); }
	return rc;
}
