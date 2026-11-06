/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_recovery -- the versioned descriptor that makes a remote-only rebuild
 *   possible
 *
 *   The design rationale lives in include/s3lvol/s3_recovery.h. In short: the
 *   checkpoint object describes the chunk map but not the geometry, identity
 *   and journal layout a fresh local device needs, so a separate small object
 *   records those at a point where they are known and durable.
 *
 *   Three things to keep an eye on here:
 *
 *   1. *The CRC covers the whole struct with the crc field zeroed*, the same
 *      rule the super block and the checkpoint header use. Getting it wrong
 *      would make every descriptor fail validation, which is loud, or -- worse
 *      -- make a corrupted one pass, which is not.
 *   2. *Validation is the only gate.* Nothing else in this file decides
 *      whether a descriptor is usable, so every rejection has to be here and
 *      has to name a specific errno. The recovery path turns those errnos into
 *      operator-facing messages.
 *   3. *A missing descriptor is -ENOENT, not an error to paper over.* It means
 *      the lvstore predates this module, and the honest answer is "remote-only
 *      recovery is not possible for this one".
 *
 *   The local claim (s3_recovery_claim_*) exists so the blank proof and the
 *   format that follows it run on one descriptor with one exclusive claim held
 *   throughout. Its guarantees, and the threats it does not cover, are spelled
 *   out in the header; do not restate them more strongly here.
 */

#include "spdk/stdinc.h"
#include "spdk/bdev.h"
#include "spdk/bdev_module.h"
#include "spdk/crc32.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"
#include "spdk/uuid.h"

#include "s3lvol/s3_recovery.h"
#include "s3lvol/s3_checkpoint.h"
#include "s3lvol/s3_types.h"

/* Same prefix rule as the data objects, the owner marker and the checkpoint. */
#define RECOVERY_KEY_FMT   "%s/meta/recovery"
#define RECOVERY_KEY_MAX   256

struct recovery_ctx {
	struct s3_client        *client;
	char                     key[RECOVERY_KEY_MAX];

	/* The lvstore name, kept separately from `key`: validation compares the
	 * descriptor's name against the lvstore, not against the object key.
	 * Passing the key here was a real defect -- it made every honest
	 * descriptor fail with -EINVAL, because "dst" never equals
	 * "dst/meta/recovery". */
	char                     lvs_name[RECOVERY_KEY_MAX];

	/* The serialised object. Its lifetime must cover the whole PUT/GET, hence
	 * living here rather than on the stack. */
	void                    *buf;
	uint64_t                 buf_len;
	struct iovec             iov;

	/* Load only. */
	uint64_t                 obj_size;

	s3_op_cb                 cb;
	s3_recovery_load_cb      load_cb;
	void                    *cb_arg;
};

/* ==========================================================================
 * CRC
 * ========================================================================== */

static uint32_t
recovery_calc_crc(const struct s3_recovery_desc *desc)
{
	struct s3_recovery_desc tmp;

	/* Treat the crc field itself as zero, otherwise verification could never
	 * reproduce the same value. */
	memcpy(&tmp, desc, sizeof(tmp));
	tmp.crc = 0;

	return spdk_crc32c_update(&tmp, sizeof(tmp), 0);
}

/* ==========================================================================
 * Validation
 * ========================================================================== */

int
s3_recovery_desc_validate(const struct s3_recovery_desc *desc,
			  const char *expect_name)
{
	if (!desc || !expect_name) {
		return -EINVAL;
	}

	if (desc->magic != S3_RECOVERY_MAGIC) {
		SPDK_ERRLOG("Recovery descriptor for '%s' has a bad magic\n",
			    expect_name);
		return -EILSEQ;
	}
	if (desc->version != S3_RECOVERY_VERSION) {
		SPDK_ERRLOG("Recovery descriptor for '%s' is version %u, this "
			    "build understands %d\n", expect_name, desc->version,
			    S3_RECOVERY_VERSION);
		return -EPROTO;
	}
	if (recovery_calc_crc(desc) != desc->crc) {
		SPDK_ERRLOG("Recovery descriptor for '%s' has a CRC mismatch\n",
			    expect_name);
		return -EILSEQ;
	}

	/* The name is the key prefix. A descriptor under a different prefix
	 * describes a different lvstore, and applying it would point this one at
	 * another lvstore's objects. */
	if (desc->lvs_name[sizeof(desc->lvs_name) - 1] != '\0') {
		SPDK_ERRLOG("Recovery descriptor for '%s' has an unterminated "
			    "name\n", expect_name);
		return -EILSEQ;
	}
	if (strcmp(desc->lvs_name, expect_name) != 0) {
		SPDK_ERRLOG("Recovery descriptor at '%s' names lvstore '%s' -- "
			    "refusing to use it\n", expect_name, desc->lvs_name);
		return -EINVAL;
	}

	/* Geometry. A zero capacity cannot be handed to blobstore, and a chunk
	 * size that is not a power of two would make the LBA-to-chunk shift
	 * compute wrong indices. */
	if (desc->capacity_bytes == 0) {
		SPDK_ERRLOG("Recovery descriptor for '%s' records no capacity\n",
			    expect_name);
		return -EINVAL;
	}
	if (desc->capacity_bytes % S3LVOL_BLOCK_SIZE != 0) {
		SPDK_ERRLOG("Recovery descriptor for '%s' records capacity %" PRIu64
			    ", which is not a multiple of the %d-byte block\n",
			    expect_name, desc->capacity_bytes, S3LVOL_BLOCK_SIZE);
		return -EINVAL;
	}
	if (desc->chunk_size < S3LVOL_BLOCK_SIZE ||
	    (desc->chunk_size & (desc->chunk_size - 1)) != 0) {
		SPDK_ERRLOG("Recovery descriptor for '%s' records chunk_size %u, "
			    "which is not a power of two at least %d\n",
			    expect_name, desc->chunk_size, S3LVOL_BLOCK_SIZE);
		return -EINVAL;
	}
	/* The capacity has to divide into whole chunks, or the last chunk is
	 * partially outside the device and the derived chunk count would be
	 * wrong in a way the checkpoint preflight depends on. */
	if (desc->capacity_bytes < desc->chunk_size ||
	    desc->capacity_bytes % desc->chunk_size != 0) {
		SPDK_ERRLOG("Recovery descriptor for '%s' records capacity %"
			    PRIu64 ", which is not a whole number of %u-byte "
			    "chunks\n", expect_name, desc->capacity_bytes,
			    desc->chunk_size);
		return -EINVAL;
	}

	/* Identity. All-zero is refused here, unlike in the checkpoint header:
	 * the header tolerates it because a create that died early leaves it,
	 * but a descriptor is only written once the uuid is known, so zero means
	 * the descriptor is not trustworthy. */
	if (spdk_uuid_is_null(&desc->lvs_uuid)) {
		SPDK_ERRLOG("Recovery descriptor for '%s' records a null lvstore "
			    "uuid; it cannot be validated against the checkpoint\n",
			    expect_name);
		return -EINVAL;
	}

	/* The checkpoint it names. gen 0 means "no checkpoint has ever
	 * completed", which is not a state remote recovery can work from. */
	if (desc->checkpoint_gen == 0) {
		SPDK_ERRLOG("Recovery descriptor for '%s' names checkpoint gen 0; "
			    "there is nothing to restore\n", expect_name);
		return -EINVAL;
	}
	/* At minimum a header, and at most the same ceiling the checkpoint
	 * loader itself enforces. */
	if (desc->checkpoint_bytes < sizeof(struct s3_ckpt_header) ||
	    desc->checkpoint_bytes > (4ULL * 1024 * 1024 * 1024)) {
		SPDK_ERRLOG("Recovery descriptor for '%s' records an implausible "
			    "checkpoint size %" PRIu64 "\n", expect_name,
			    desc->checkpoint_bytes);
		return -EINVAL;
	}

	return 0;
}

/* ==========================================================================
 * publish
 * ========================================================================== */

static void
recovery_ctx_free(struct recovery_ctx *ctx)
{
	free(ctx->buf);
	free(ctx);
}

static void
recovery_put_done(void *cb_arg, int status)
{
	struct recovery_ctx *ctx = cb_arg;
	s3_op_cb cb_fn = ctx->cb;
	void *user_arg = ctx->cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("Failed to upload the recovery descriptor '%s': %d\n",
			    ctx->key, status);
	}

	recovery_ctx_free(ctx);

	if (cb_fn) {
		cb_fn(user_arg, status);
	}
}

void
s3_recovery_publish(struct s3_client *client, const char *lvs_name,
		    const struct spdk_uuid *lvs_uuid,
		    uint64_t capacity_bytes, uint32_t chunk_size,
		    uint64_t journal_size, uint64_t wal_size,
		    uint64_t checkpoint_gen, uint64_t checkpoint_lsn,
		    uint64_t checkpoint_bytes,
		    uint32_t checkpoint_entries_crc,
		    s3_op_cb cb_fn, void *cb_arg)
{
	struct recovery_ctx *ctx;
	struct s3_recovery_desc *desc;
	int rc;

	if (!client || !lvs_name || !lvs_uuid) {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}
	if (spdk_uuid_is_null(lvs_uuid)) {
		/* Refused rather than written: a descriptor without identity is
		 * exactly the thing validation would reject later, and writing it
		 * would turn a programming error into a recovery failure months
		 * afterwards. */
		SPDK_ERRLOG("Refusing to publish a recovery descriptor for '%s' "
			    "with a null lvstore uuid\n", lvs_name);
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}
	if (strlen(lvs_name) >= sizeof(desc->lvs_name)) {
		SPDK_ERRLOG("lvstore name '%s' does not fit in the recovery "
			    "descriptor\n", lvs_name);
		if (cb_fn) {
			cb_fn(cb_arg, -ENAMETOOLONG);
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
	ctx->client = client;
	ctx->cb     = cb_fn;
	ctx->cb_arg = cb_arg;

	rc = snprintf(ctx->key, sizeof(ctx->key), RECOVERY_KEY_FMT, lvs_name);
	if (rc < 0 || (size_t)rc >= sizeof(ctx->key)) {
		free(ctx);
		if (cb_fn) {
			cb_fn(cb_arg, -ENAMETOOLONG);
		}
		return;
	}

	ctx->buf = calloc(1, sizeof(*desc));
	if (!ctx->buf) {
		free(ctx);
		if (cb_fn) {
			cb_fn(cb_arg, -ENOMEM);
		}
		return;
	}
	ctx->buf_len = sizeof(*desc);

	desc = ctx->buf;
	desc->magic          = S3_RECOVERY_MAGIC;
	desc->version        = S3_RECOVERY_VERSION;
	desc->chunk_size     = chunk_size;
	desc->capacity_bytes = capacity_bytes;
	spdk_uuid_copy(&desc->lvs_uuid, lvs_uuid);
	desc->checkpoint_gen         = checkpoint_gen;
	desc->checkpoint_lsn         = checkpoint_lsn;
	desc->checkpoint_bytes       = checkpoint_bytes;
	desc->checkpoint_entries_crc = checkpoint_entries_crc;
	desc->journal_size           = journal_size;
	desc->wal_size               = wal_size;
	snprintf(desc->lvs_name, sizeof(desc->lvs_name), "%s", lvs_name);
	desc->crc = recovery_calc_crc(desc);

	ctx->iov.iov_base = ctx->buf;
	ctx->iov.iov_len  = ctx->buf_len;

	SPDK_NOTICELOG("Publishing recovery descriptor for '%s': gen=%" PRIu64
		       ", lsn=%" PRIu64 ", capacity=%" PRIu64 ", chunk=%u\n",
		       lvs_name, checkpoint_gen, checkpoint_lsn, capacity_bytes,
		       chunk_size);

	rc = s3_put(client, ctx->key, &ctx->iov, 1, false,
		    recovery_put_done, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit the recovery descriptor upload for "
			    "'%s': %d\n", lvs_name, rc);
		recovery_ctx_free(ctx);
		if (cb_fn) {
			cb_fn(cb_arg, rc);
		}
	}
}

/* ==========================================================================
 * load
 * ========================================================================== */

void
s3_recovery_desc_free(struct s3_recovery_desc *desc)
{
	free(desc);
}

static void
recovery_load_finish(struct recovery_ctx *ctx, struct s3_recovery_desc *desc,
		     int status)
{
	s3_recovery_load_cb cb_fn = ctx->load_cb;
	void *user_arg = ctx->cb_arg;

	recovery_ctx_free(ctx);

	if (cb_fn) {
		cb_fn(user_arg, desc, status);
	}
}

static void
recovery_get_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct recovery_ctx *ctx = cb_arg;
	struct s3_recovery_desc *desc;
	int rc;

	if (status != 0) {
		SPDK_ERRLOG("Failed to download the recovery descriptor '%s': "
			    "%d\n", ctx->key, status);
		recovery_load_finish(ctx, NULL, status);
		return;
	}

	if (bytes_read != sizeof(struct s3_recovery_desc)) {
		SPDK_ERRLOG("Recovery descriptor '%s' is %" PRIu64 " bytes, "
			    "expected %zu\n", ctx->key, bytes_read,
			    sizeof(struct s3_recovery_desc));
		recovery_load_finish(ctx, NULL, -EILSEQ);
		return;
	}

	/* Copied out rather than handed back in place: the caller keeps the
	 * descriptor past this callback, and ctx is freed here. */
	desc = calloc(1, sizeof(*desc));
	if (!desc) {
		recovery_load_finish(ctx, NULL, -ENOMEM);
		return;
	}
	memcpy(desc, ctx->buf, sizeof(*desc));

	rc = s3_recovery_desc_validate(desc, ctx->lvs_name);
	if (rc != 0) {
		/* validate() has already logged which field failed. */
		free(desc);
		recovery_load_finish(ctx, NULL, rc);
		return;
	}

	recovery_load_finish(ctx, desc, 0);
}

static void
recovery_head_done(void *cb_arg, int status)
{
	struct recovery_ctx *ctx = cb_arg;
	int rc;

	if (status == -ENOENT) {
		/* No descriptor: this lvstore predates the module, or has never
		 * been checkpointed by a build that has it. Reported distinctly
		 * so the caller can say so instead of guessing. */
		SPDK_NOTICELOG("No recovery descriptor at '%s'; remote-only "
			       "recovery is not possible for this lvstore\n",
			       ctx->key);
		recovery_load_finish(ctx, NULL, -ENOENT);
		return;
	}
	if (status != 0) {
		SPDK_ERRLOG("Failed to stat the recovery descriptor '%s': %d\n",
			    ctx->key, status);
		recovery_load_finish(ctx, NULL, status);
		return;
	}

	if (ctx->obj_size != sizeof(struct s3_recovery_desc)) {
		SPDK_ERRLOG("Recovery descriptor '%s' has size %" PRIu64
			    ", expected %zu\n", ctx->key, ctx->obj_size,
			    sizeof(struct s3_recovery_desc));
		recovery_load_finish(ctx, NULL, -EILSEQ);
		return;
	}

	ctx->buf = calloc(1, ctx->obj_size);
	if (!ctx->buf) {
		recovery_load_finish(ctx, NULL, -ENOMEM);
		return;
	}
	ctx->buf_len = ctx->obj_size;

	rc = s3_get_range(ctx->client, ctx->key, 0, ctx->obj_size, ctx->buf,
			  recovery_get_done, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit the recovery descriptor download "
			    "for '%s': %d\n", ctx->key, rc);
		recovery_load_finish(ctx, NULL, rc);
	}
}

void
s3_recovery_load(struct s3_client *client, const char *lvs_name,
		 s3_recovery_load_cb cb_fn, void *cb_arg)
{
	struct recovery_ctx *ctx;
	int rc;

	assert(cb_fn != NULL);

	if (!client || !lvs_name) {
		cb_fn(cb_arg, NULL, -EINVAL);
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		cb_fn(cb_arg, NULL, -ENOMEM);
		return;
	}
	ctx->client  = client;
	ctx->load_cb = cb_fn;
	ctx->cb_arg  = cb_arg;

	rc = snprintf(ctx->key, sizeof(ctx->key), RECOVERY_KEY_FMT, lvs_name);
	if (rc < 0 || (size_t)rc >= sizeof(ctx->key)) {
		free(ctx);
		cb_fn(cb_arg, NULL, -ENAMETOOLONG);
		return;
	}
	snprintf(ctx->lvs_name, sizeof(ctx->lvs_name), "%s", lvs_name);

	rc = s3_head(client, ctx->key, &ctx->obj_size, recovery_head_done, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit the recovery descriptor stat for "
			    "'%s': %d\n", lvs_name, rc);
		free(ctx);
		cb_fn(cb_arg, NULL, rc);
	}
}

/* ==========================================================================
 * delete
 * ========================================================================== */

static void
recovery_delete_done(void *cb_arg, int status)
{
	struct recovery_ctx *ctx = cb_arg;
	s3_op_cb cb_fn = ctx->cb;
	void *user_arg = ctx->cb_arg;

	if (status != 0 && status != -ENOENT) {
		SPDK_WARNLOG("Failed to delete the recovery descriptor '%s': "
			     "%d\n", ctx->key, status);
	}

	recovery_ctx_free(ctx);

	if (cb_fn) {
		cb_fn(user_arg, status == -ENOENT ? 0 : status);
	}
}

void
s3_recovery_delete(struct s3_client *client, const char *lvs_name,
		   s3_op_cb cb_fn, void *cb_arg)
{
	struct recovery_ctx *ctx;
	int rc;

	if (!client || !lvs_name) {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
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
	ctx->client = client;
	ctx->cb     = cb_fn;
	ctx->cb_arg = cb_arg;

	rc = snprintf(ctx->key, sizeof(ctx->key), RECOVERY_KEY_FMT, lvs_name);
	if (rc < 0 || (size_t)rc >= sizeof(ctx->key)) {
		free(ctx);
		if (cb_fn) {
			cb_fn(cb_arg, -ENAMETOOLONG);
		}
		return;
	}

	rc = s3_delete(client, ctx->key, recovery_delete_done, ctx);
	if (rc != 0) {
		free(ctx);
		if (cb_fn) {
			cb_fn(cb_arg, rc);
		}
	}
}

/* ==========================================================================
	* Positive blank-device proof
	*
	* The previous probe called s3_local_dev_open() and treated -EILSEQ as "no
	* layout". That errno is also returned when the super block magic is right but
	* the CRC is wrong, and when a *valid* super block from a newer build is
	* rejected -- so a corrupt or foreign device could be formatted. This scan
	* replaces it: blankness is proven by finding nothing but zeroes, and a parse
	* error is never consulted.
	* ========================================================================== */

int
s3_recovery_blank_check(const void *buf, size_t len)
{
	const uint8_t *p = buf;
	size_t i;

	if (!buf || len == 0) {
		return -EINVAL;
	}
	for (i = 0; i < len; i++) {
		if (p[i] != 0) {
			return -EILSEQ;
		}
	}
	return 0;
}

/* ==========================================================================
 * Local claim
 *
 * One object owns one descriptor and one claim. Everything the recovery chain
 * does to the device between the blank proof and the format goes through it, so
 * there is no moment at which the device is unclaimed and reopenable.
 *
 * The claim type is READ_MANY_WRITE_ONE rather than the older EXCL_WRITE
 * because it is the only pinned API that refuses to be granted while another
 * descriptor already has the bdev open for writing (claim_verify_rwo in
 * bdev.c). EXCL_WRITE only checks the claim word, so a writer that opened
 * before the claim would keep writing and the claim would still "succeed".
 * ========================================================================== */

struct blank_scan_ctx;

/* Defined with the scanner below; the claim forwards events to it. */
static void blank_scan_event(enum spdk_bdev_event_type type,
			     struct spdk_bdev *bdev, void *event_ctx);

struct s3_recovery_claim {
	struct spdk_bdev_desc   *desc;
	struct spdk_bdev        *bdev;
	struct spdk_bdev_module *module;
	bool                     claimed;

	/* First REMOVE / RESIZE seen while the claim is held, latched here and
	 * not only in the active scan.
	 *
	 * A scan latches an event for its own lifetime, but the window that
	 * matters for the recovery chain continues past the scan: the format
	 * writes through this same descriptor. Without a claim-level latch an
	 * event arriving after the scan callback and before the first format
	 * write would only be logged, and the format would write to a device
	 * that is gone or has changed geometry. The formatter consults this
	 * before submitting a write.
	 */
	int                      event_error;

	/* The scan currently reading through this claim, if any.
	 *
	 * SPDK delivers bdev events to the callback registered at open time, and
	 * a descriptor has exactly one. The claim owns the descriptor, so the
	 * claim must own the callback too, and forward events to whoever is
	 * using the device at that moment. Without this the claimed scan would
	 * be blind to REMOVE/RESIZE -- it would keep reading a device that is
	 * gone or has changed geometry, which is precisely the failure the
	 * unclaimed scanner was fixed to avoid. */
	struct blank_scan_ctx   *scan;
};

static void
recovery_claim_event(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		     void *event_ctx)
{
	struct s3_recovery_claim *claim = event_ctx;
	int status;

	if (!claim) {
		return;
	}

	/* Latch the first event on the claim itself, not only on the scan. The
	 * format writes through this descriptor after the scan has finished and
	 * detached, so an event in that window must be visible to the formatter
	 * before it submits a write. */
	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		status = -ENODEV;
		break;
	case SPDK_BDEV_EVENT_RESIZE:
		status = -ERANGE;
		break;
	default:
		status = 0;
		break;
	}
	if (status != 0 && claim->event_error == 0) {
		claim->event_error = status;
		SPDK_ERRLOG("bdev '%s' reported event %d while claimed for "
			    "recovery; the claim is poisoned\n",
			    bdev ? spdk_bdev_get_name(bdev) : "(gone)", type);
	}

	/* Forward to the active scan so it stops reading and unwinds. */
	if (claim->scan) {
		blank_scan_event(type, bdev, claim->scan);
	}
}

int
s3_recovery_claim_open(const char *bdev_name, struct spdk_bdev_module *module,
		       struct s3_recovery_claim **out_claim)
{
	struct s3_recovery_claim *claim;
	struct spdk_bdev_claim_opts opts;
	struct spdk_bdev_desc *desc = NULL;
	int rc;

	if (!bdev_name || !module || !out_claim) {
		return -EINVAL;
	}
	*out_claim = NULL;

	claim = calloc(1, sizeof(*claim));
	if (!claim) {
		return -ENOMEM;
	}
	claim->module = module;

	/* Read-only first: READ_MANY_WRITE_ONE refuses a descriptor that is
	 * already writable, and upgrades this one itself once the claim is
	 * granted. Opening read-only also means a failed claim leaves nothing
	 * writable behind. */
	rc = spdk_bdev_open_ext(bdev_name, false, recovery_claim_event, claim,
				&desc);
	if (rc != 0) {
		SPDK_ERRLOG("Cannot open '%s' to claim it for recovery: %s\n",
			    bdev_name, spdk_strerror(-rc));
		free(claim);
		return rc;
	}

	claim->desc = desc;
	claim->bdev = spdk_bdev_desc_get_bdev(desc);
	if (!claim->bdev) {
		SPDK_ERRLOG("'%s' has no bdev behind its descriptor\n", bdev_name);
		spdk_bdev_close(desc);
		free(claim);
		return -ENODEV;
	}

	spdk_bdev_claim_opts_init(&opts, sizeof(opts));
	snprintf(opts.name, sizeof(opts.name), "s3lvol-recovery");

	rc = spdk_bdev_module_claim_bdev_desc(desc,
					      SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE,
					      &opts, module);
	if (rc != 0) {
		/* -EPERM covers both "already claimed" and "another descriptor
		 * has it open for writing". Both mean the same thing here: this
		 * process cannot prove the device is exclusively ours, so it
		 * must not be formatted. */
		SPDK_ERRLOG("Cannot claim '%s' for recovery (%s); refusing to "
			    "treat it as exclusively ours\n", bdev_name,
			    spdk_strerror(-rc));
		spdk_bdev_close(desc);
		free(claim);
		return rc;
	}

	claim->claimed = true;
	*out_claim = claim;
	return 0;
}

struct spdk_bdev_desc *
s3_recovery_claim_desc(struct s3_recovery_claim *claim)
{
	return claim ? claim->desc : NULL;
}

struct spdk_bdev *
s3_recovery_claim_bdev(struct s3_recovery_claim *claim)
{
	return claim ? claim->bdev : NULL;
}

int
s3_recovery_claim_error(struct s3_recovery_claim *claim)
{
	return claim ? claim->event_error : -EINVAL;
}

void
s3_recovery_claim_release(struct s3_recovery_claim *claim)
{
	if (!claim) {
		return;
	}

	/* The claim is SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE, which is a v2
	 * claim. v2 claims are owned by the descriptor: bdev_close() ->
	 * bdev_desc_release_claims() -> claim_reset() drops them.
	 *
	 * Do NOT call spdk_bdev_module_release_bdev() here. That is the v1
	 * release path and it asserts claim_type == SPDK_BDEV_CLAIM_EXCL_WRITE
	 * (bdev.c:9455), so on a v2 claim it aborts in a debug build and
	 * corrupts the claim bookkeeping in a release build.
	 */
	if (claim->desc) {
		spdk_bdev_close(claim->desc);
		claim->desc = NULL;
	}
	claim->claimed = false;
	claim->bdev = NULL;
	free(claim);
}

/* One chunk at a time, so the scan's memory cost does not scale with the
	* device. 1 MiB matches the transfer size the rest of this code uses. */
#define S3_RECOVERY_SCAN_CHUNK  (1024 * 1024)

struct blank_scan_ctx {
	char                    *name;
	struct spdk_bdev_desc   *desc;
	struct spdk_io_channel  *ch;
	void                    *buf;

	/* Set when the descriptor belongs to a caller-held claim. The scan then
	 * must not close it: the claim outlives the scan on purpose, so the
	 * format that follows runs on the same descriptor. */
	bool                     borrowed_desc;

	/* The claim this scan runs through, when borrowed_desc is set. The claim
	 * forwards bdev events here, and must be told when the scan is over so a
	 * later event is not delivered to a freed context. */
	struct s3_recovery_claim *claim;

	uint64_t                 offset;
	uint64_t                 total;
	uint64_t                 this_len;
	uint32_t                 block_size;

	/* The scanner owns this context until every submission has completed.
	 * A bdev event only latches a failure; it never frees anything while
	 * io_pending is set, because the in-flight completion still carries
	 * this pointer. */
	bool                     io_pending;
	bool                     finished;
	int                      event_error;

	s3_recovery_blank_cb     cb;
	void                    *cb_arg;
};

static void
blank_scan_finish(struct blank_scan_ctx *ctx, int status)
{
	s3_recovery_blank_cb cb_fn;
	void *cb_arg;

	/* Release each resource and report once, even if a completion and an
	 * event arrive on the same context. */
	if (ctx->finished) {
		return;
	}
	ctx->finished = true;

	cb_fn = ctx->cb;
	cb_arg = ctx->cb_arg;

	if (ctx->ch) {
		spdk_put_io_channel(ctx->ch);
		ctx->ch = NULL;
	}
	/* Stop the claim forwarding events here before this context is freed:
	 * a REMOVE arriving after the callback would otherwise be delivered to
	 * freed memory. */
	if (ctx->claim) {
		ctx->claim->scan = NULL;
		ctx->claim = NULL;
	}
	/* A borrowed descriptor belongs to the claim, which the caller keeps
	 * across the format. Closing it here would reopen the window this whole
	 * path exists to close. */
	if (ctx->desc && !ctx->borrowed_desc) {
		spdk_bdev_close(ctx->desc);
		ctx->desc = NULL;
	}
	spdk_dma_free(ctx->buf);
	ctx->buf = NULL;
	free(ctx->name);
	ctx->name = NULL;
	free(ctx);

	if (cb_fn) {
		cb_fn(cb_arg, status);
	}
}

static void
blank_scan_event(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		 void *event_ctx)
{
	struct blank_scan_ctx *ctx = event_ctx;
	int status;

	if (!ctx || ctx->finished) {
		return;
	}

	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		status = -ENODEV;
		break;
	case SPDK_BDEV_EVENT_RESIZE:
		/* The total this scan is proving is no longer the device's
		 * geometry, so a zero-filled prefix says nothing about the
		 * replacement. Fail closed. */
		status = -ERANGE;
		break;
	default:
		SPDK_WARNLOG("bdev '%s' reported event %d during the blank "
			     "scan; ignoring it\n",
			     bdev ? spdk_bdev_get_name(bdev) : "(gone)", type);
		return;
	}

	if (ctx->event_error == 0) {
		ctx->event_error = status;
		SPDK_ERRLOG("bdev '%s' reported event %d during the blank scan; "
			    "the scan will fail once outstanding I/O completes\n",
			    ctx->name, type);
	}

	/* Finishing here while a read is outstanding would free ctx under the
	 * completion callback. The completion observes event_error and finishes
	 * instead. */
	if (!ctx->io_pending) {
		blank_scan_finish(ctx, ctx->event_error);
	}
}

static void blank_scan_next(struct blank_scan_ctx *ctx);

static void
blank_scan_io_done(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct blank_scan_ctx *ctx = cb_arg;
	int rc;

	ctx->io_pending = false;
	spdk_bdev_free_io(bdev_io);

	if (ctx->event_error != 0) {
		SPDK_ERRLOG("Blank scan on '%s' aborted by a bdev event at "
			    "offset %" PRIu64 ": %d\n", ctx->name, ctx->offset,
			    ctx->event_error);
		blank_scan_finish(ctx, ctx->event_error);
		return;
	}

	if (!success) {
		SPDK_ERRLOG("Blank scan read failed on '%s' at offset %" PRIu64
			    "\n", ctx->name, ctx->offset);
		blank_scan_finish(ctx, -EIO);
		return;
	}

	rc = s3_recovery_blank_check(ctx->buf, (size_t)ctx->this_len);
	if (rc != 0) {
		SPDK_ERRLOG("'%s' is not blank: non-zero byte in [%" PRIu64
			    ", %" PRIu64 ")\n", ctx->name, ctx->offset,
			    ctx->offset + ctx->this_len);
		blank_scan_finish(ctx, rc);
		return;
	}

	ctx->offset += ctx->this_len;
	blank_scan_next(ctx);
}

static void
blank_scan_next(struct blank_scan_ctx *ctx)
{
	uint64_t remain = ctx->total - ctx->offset;
	uint64_t len;
	int rc;

	/* A latched event means the device this scan describes is gone or has
	 * changed geometry. Never submit another read against it. */
	if (ctx->event_error != 0) {
		blank_scan_finish(ctx, ctx->event_error);
		return;
	}

	assert(!ctx->io_pending);

	if (remain == 0) {
		SPDK_NOTICELOG("'%s' is zero-filled across %" PRIu64 " bytes; "
			       "safe to treat as a blank replacement device\n",
			       ctx->name, ctx->total);
		blank_scan_finish(ctx, 0);
		return;
	}

	len = spdk_min(remain, (uint64_t)S3_RECOVERY_SCAN_CHUNK);
	len -= len % ctx->block_size;
	if (len == 0) {
		SPDK_ERRLOG("'%s' has %" PRIu64 " trailing bytes that are not a "
			    "whole %u-byte block\n", ctx->name, remain,
			    ctx->block_size);
		blank_scan_finish(ctx, -EINVAL);
		return;
	}

	ctx->this_len = len;
	/* Mark the read outstanding before submitting it. An event delivered
	 * after submission but before completion must not free this context. */
	ctx->io_pending = true;
	rc = spdk_bdev_read(ctx->desc, ctx->ch, ctx->buf, ctx->offset, len,
			    blank_scan_io_done, ctx);
	if (rc != 0) {
		/* The callback is not called when submission fails, so the
		 * context is no longer in flight. -ENOMEM means no queued buffer
		 * was available. Retrying would mean implementing the io-wait
		 * path; failing is the safe direction for a proof that gates a
		 * format. */
		ctx->io_pending = false;
		SPDK_ERRLOG("Could not submit a %" PRIu64 "-byte read on '%s' at "
			    "offset %" PRIu64 ": %d\n", len, ctx->name,
			    ctx->offset, rc);
		blank_scan_finish(ctx, rc);
	}
}

/* Shared body of both scan entry points.
 *
 * \c desc is either freshly opened here (unclaimed scan) or borrowed from a
 * claim (claimed scan). Everything after the descriptor is identical, which is
 * the point: the proof is the same proof, only its lifetime differs.
 */
static void
blank_scan_start(const char *bdev_name, struct spdk_bdev_desc *desc,
		 struct s3_recovery_claim *claim, s3_recovery_blank_cb cb_fn,
		 void *cb_arg)
{
	struct blank_scan_ctx *ctx;
	struct spdk_bdev *bdev;
	uint64_t block_size;
	uint64_t num_blocks;
	int rc;

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		cb_fn(cb_arg, -ENOMEM);
		return;
	}
	ctx->cb = cb_fn;
	ctx->cb_arg = cb_arg;
	ctx->borrowed_desc = claim != NULL;
	ctx->name = strdup(bdev_name);
	if (!ctx->name) {
		free(ctx);
		cb_fn(cb_arg, -ENOMEM);
		return;
	}

	if (desc) {
		ctx->desc = desc;
		/* Route bdev events to this scan for as long as it runs. The
		 * claim owns the descriptor, so it owns the event callback. */
		ctx->claim = claim;
		claim->scan = ctx;
		/* An event latched between the previous scan and this one (or
		 * before the claim was handed in) must fail this scan too: the
		 * geometry it is about to prove may already be stale. */
		ctx->event_error = claim->event_error;
	} else {
		/* Read-only open: the scan must not be able to change the device
		 * even if something below it misbehaves. */
		rc = spdk_bdev_open_ext(bdev_name, false, blank_scan_event, ctx,
					&ctx->desc);
		if (rc != 0) {
			SPDK_ERRLOG("Cannot open '%s' for the blank scan: %d\n",
				    bdev_name, rc);
			blank_scan_finish(ctx, rc);
			return;
		}
	}

	bdev = spdk_bdev_desc_get_bdev(ctx->desc);
	if (!bdev) {
		SPDK_ERRLOG("'%s' has no bdev behind its descriptor\n",
			    bdev_name);
		blank_scan_finish(ctx, -ENODEV);
		return;
	}

	block_size = spdk_bdev_get_block_size(bdev);
	num_blocks = spdk_bdev_get_num_blocks(bdev);

	/* Reject geometry the scan cannot represent instead of letting the
	 * block total wrap or the block size truncate. The chunk must hold at
	 * least one whole block, and block_size is stored as 32 bits. */
	if (block_size == 0 || num_blocks == 0 ||
	    block_size > (uint64_t)S3_RECOVERY_SCAN_CHUNK ||
	    block_size > UINT32_MAX ||
	    num_blocks > UINT64_MAX / block_size) {
		SPDK_ERRLOG("'%s' reports unusable geometry (%" PRIu64
			    " blocks of %" PRIu64 " bytes)\n", bdev_name,
			    num_blocks, block_size);
		blank_scan_finish(ctx, -EINVAL);
		return;
	}

	ctx->block_size = (uint32_t)block_size;
	ctx->total = num_blocks * block_size;

	ctx->ch = spdk_bdev_get_io_channel(ctx->desc);
	if (!ctx->ch) {
		SPDK_ERRLOG("No I/O channel for '%s'\n", bdev_name);
		blank_scan_finish(ctx, -ENOMEM);
		return;
	}

	ctx->buf = spdk_dma_zmalloc(S3_RECOVERY_SCAN_CHUNK, 0, NULL);
	if (!ctx->buf) {
		SPDK_ERRLOG("Cannot allocate the blank-scan buffer for '%s'\n",
			    bdev_name);
		blank_scan_finish(ctx, -ENOMEM);
		return;
	}

	SPDK_NOTICELOG("Scanning '%s' (%" PRIu64 " bytes) for a positive blank "
		       "proof%s\n", bdev_name, ctx->total,
		       claim ? " (claim retained)" : "");
	blank_scan_next(ctx);
}

void
s3_recovery_blank_scan(const char *bdev_name, s3_recovery_blank_cb cb_fn,
		       void *cb_arg)
{
	assert(cb_fn != NULL);

	if (!bdev_name) {
		cb_fn(cb_arg, -EINVAL);
		return;
	}

	blank_scan_start(bdev_name, NULL, NULL, cb_fn, cb_arg);
}

void
s3_recovery_blank_scan_claimed(struct s3_recovery_claim *claim,
			       s3_recovery_blank_cb cb_fn, void *cb_arg)
{
	struct spdk_bdev_desc *desc;
	struct spdk_bdev *bdev;

	assert(cb_fn != NULL);

	if (!claim) {
		cb_fn(cb_arg, -EINVAL);
		return;
	}

	desc = s3_recovery_claim_desc(claim);
	bdev = s3_recovery_claim_bdev(claim);
	if (!desc || !bdev) {
		cb_fn(cb_arg, -EINVAL);
		return;
	}

	blank_scan_start(spdk_bdev_get_name(bdev), desc, claim, cb_fn, cb_arg);
}

/* ==========================================================================
	* Descriptor-to-checkpoint preflight
	*
	* Runs before the owner marker is touched and before the local device is
	* formatted. The bytes validated here are the bytes the chain later applies, so
	* a checkpoint that changes between the preflight and the format cannot be the
	* one restored.
	* ========================================================================== */

int
s3_recovery_ckpt_validate_blob(const struct s3_recovery_desc *desc,
			       const void *buf, uint64_t len)
{
	struct s3_ckpt_expect expect = {0};
	struct s3_ckpt_view view;
	int rc;

	if (!desc || !buf) {
		return -EINVAL;
	}
	if (len != desc->checkpoint_bytes) {
		SPDK_ERRLOG("Checkpoint object is %" PRIu64 " bytes, the "
			    "descriptor names %" PRIu64 "\n", len,
			    desc->checkpoint_bytes);
		return -EILSEQ;
	}

	if (desc->capacity_bytes % desc->chunk_size != 0) {
		SPDK_ERRLOG("Descriptor capacity %" PRIu64 " is not a whole "
			    "number of %u-byte chunks\n", desc->capacity_bytes,
			    desc->chunk_size);
		return -EINVAL;
	}

	expect.chunk_size        = desc->chunk_size;
	expect.lvs_uuid          = &desc->lvs_uuid;
	expect.check_point       = true;
	expect.gen               = desc->checkpoint_gen;
	expect.lsn               = desc->checkpoint_lsn;
	expect.num_chunks_limit  = desc->capacity_bytes / desc->chunk_size;

	rc = s3_checkpoint_validate(buf, len, &expect, &view);
	if (rc != 0) {
		return rc;
	}

	if (view.hdr->entries_crc != desc->checkpoint_entries_crc) {
		SPDK_ERRLOG("Checkpoint entry CRC %08x does not match the "
			    "descriptor's %08x\n", view.hdr->entries_crc,
			    desc->checkpoint_entries_crc);
		return -EILSEQ;
	}

	return 0;
}

struct ckpt_preflight_ctx {
	struct s3_client         *client;
	char                      key[RECOVERY_KEY_MAX];

	/* Copied: the caller's descriptor does not outlive the download. */
	struct s3_recovery_desc   desc;

	void                     *buf;
	uint64_t                  obj_size;

	s3_recovery_ckpt_cb       cb;
	void                     *cb_arg;
};

static void
ckpt_preflight_finish(struct ckpt_preflight_ctx *ctx, int status)
{
	s3_recovery_ckpt_cb cb_fn = ctx->cb;
	void *cb_arg = ctx->cb_arg;
	void *ckpt = NULL;
	uint64_t len = 0;

	if (status == 0) {
		/* Ownership passes to the caller. */
		ckpt = ctx->buf;
		len = ctx->obj_size;
		ctx->buf = NULL;
	}

	free(ctx->buf);
	free(ctx);

	if (cb_fn) {
		cb_fn(cb_arg, ckpt, len, status);
	}
}

static void
ckpt_preflight_get_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct ckpt_preflight_ctx *ctx = cb_arg;
	int rc;

	if (status != 0) {
		SPDK_ERRLOG("Failed to download the checkpoint '%s': %d\n",
			    ctx->key, status);
		ckpt_preflight_finish(ctx, status);
		return;
	}
	if (bytes_read != ctx->obj_size) {
		SPDK_ERRLOG("Checkpoint '%s' is %" PRIu64 " bytes, HEAD said %"
			    PRIu64 "\n", ctx->key, bytes_read, ctx->obj_size);
		ckpt_preflight_finish(ctx, -EILSEQ);
		return;
	}

	rc = s3_recovery_ckpt_validate_blob(&ctx->desc, ctx->buf,
					    ctx->obj_size);
	if (rc != 0) {
		SPDK_ERRLOG("Checkpoint '%s' does not match the recovery "
			    "descriptor\n", ctx->key);
		ckpt_preflight_finish(ctx, rc);
		return;
	}

	ckpt_preflight_finish(ctx, 0);
}

static void
ckpt_preflight_head_done(void *cb_arg, int status)
{
	struct ckpt_preflight_ctx *ctx = cb_arg;
	int rc;

	if (status == -ENOENT) {
		/* The descriptor names a checkpoint that is not there. Guessing
		 * is not an option: there is nothing to restore. */
		SPDK_ERRLOG("The recovery descriptor for '%s' names a checkpoint "
			    "that is not in the bucket\n", ctx->key);
		ckpt_preflight_finish(ctx, -ENOENT);
		return;
	}
	if (status != 0) {
		SPDK_ERRLOG("Failed to stat the checkpoint '%s': %d\n",
			    ctx->key, status);
		ckpt_preflight_finish(ctx, status);
		return;
	}

	if (ctx->obj_size != ctx->desc.checkpoint_bytes) {
		SPDK_ERRLOG("Checkpoint '%s' is %" PRIu64 " bytes, the "
			    "descriptor names %" PRIu64 " -- the object was "
			    "replaced or truncated\n", ctx->key, ctx->obj_size,
			    ctx->desc.checkpoint_bytes);
		ckpt_preflight_finish(ctx, -EILSEQ);
		return;
	}
	if (ctx->obj_size < sizeof(struct s3_ckpt_header) ||
	    ctx->obj_size > S3_RECOVERY_CKPT_MAX) {
		SPDK_ERRLOG("Checkpoint '%s' has an implausible size %" PRIu64
			    "\n", ctx->key, ctx->obj_size);
		ckpt_preflight_finish(ctx, -EINVAL);
		return;
	}

	ctx->buf = calloc(1, ctx->obj_size);
	if (!ctx->buf) {
		ckpt_preflight_finish(ctx, -ENOMEM);
		return;
	}

	rc = s3_get_range(ctx->client, ctx->key, 0, ctx->obj_size, ctx->buf,
			  ckpt_preflight_get_done, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit the checkpoint download for '%s': "
			    "%d\n", ctx->key, rc);
		ckpt_preflight_finish(ctx, rc);
	}
}

void
s3_recovery_ckpt_preflight(struct s3_client *client, const char *lvs_name,
			   const struct s3_recovery_desc *desc,
			   s3_recovery_ckpt_cb cb_fn, void *cb_arg)
{
	struct ckpt_preflight_ctx *ctx;
	int rc;

	assert(cb_fn != NULL);

	if (!client || !lvs_name || !desc) {
		cb_fn(cb_arg, NULL, 0, -EINVAL);
		return;
	}

	/* Re-validate the descriptor rather than trusting the caller: this is
	 * the entry the recovery chain uses, and it must not be possible to
	 * skip validation by calling it directly. */
	rc = s3_recovery_desc_validate(desc, lvs_name);
	if (rc != 0) {
		cb_fn(cb_arg, NULL, 0, rc);
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		cb_fn(cb_arg, NULL, 0, -ENOMEM);
		return;
	}
	ctx->client  = client;
	ctx->desc    = *desc;
	ctx->cb      = cb_fn;
	ctx->cb_arg  = cb_arg;

	rc = snprintf(ctx->key, sizeof(ctx->key), S3_CKPT_KEY_FMT, lvs_name);
	if (rc < 0 || (size_t)rc >= sizeof(ctx->key)) {
		free(ctx);
		cb_fn(cb_arg, NULL, 0, -ENAMETOOLONG);
		return;
	}

	rc = s3_head(client, ctx->key, &ctx->obj_size,
		     ckpt_preflight_head_done, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit the checkpoint stat for '%s': %d\n",
			    lvs_name, rc);
		free(ctx);
		cb_fn(cb_arg, NULL, 0, rc);
	}
}

void
s3_recovery_ckpt_free(void *ckpt)
{
	free(ckpt);
}
