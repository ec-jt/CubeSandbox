/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   Template bases: identical template snapshots on every node
 *
 *   === What a template base is ===
 *
 *   A read-only snapshot holding a template's rootfs or memory image, with an
 *   identity that is the same on every node: the uuid of the export it was
 *   published as. The builder node publishes its snapshot once; every other
 *   node imports those exact bytes. Sandboxes clone the base, so a sandbox
 *   export only needs to carry what the sandbox changed (a template delta,
 *   manifest version 5), and the importing node supplies the rest from its own
 *   copy of the same base.
 *
 *   === Why an anchor and not an xattr on the snapshot ===
 *
 *   A snapshot's metadata is read-only (md_ro) once it is sealed, and blobstore
 *   refuses spdk_blob_set_xattr() on it. Lifting md_ro needs blobstore
 *   internals this module does not reach into. So the mark lives on a writable
 *   clone of the base that is never written: the anchor. The anchor carries
 *   rcow.template_base.v1, which names the base uuid and binds itself to both
 *   its own identity and its parent snapshot's identity. A base is valid only
 *   while:
 *
 *     - the anchor's parent snapshot is the snapshot the record names (uuid),
 *     - that snapshot is read-only,
 *     - the anchor owns no clusters (it is byte-identical to the base).
 *
 *   On an importing node the anchor is the import volume itself: it was
 *   decoupled (fully local) and then snapshotted, which leaves it an empty
 *   clone of the new snapshot. Its import origin is the proof of identity, so
 *   the mark is refused unless that origin names the same export.
 *
 *   On the builder node the anchor is a fresh clone of the template snapshot,
 *   and the proof is the export registry: an export of that very snapshot
 *   under the base uuid.
 *
 *   Several bases may live on one lvstore at once, one per (template build,
 *   role). Nothing here replaces an older base: a rebuild publishes a new uuid
 *   and the old base stays until it is deleted explicitly, which blobstore
 *   refuses while any clone (a sandbox, or the anchor) still reads it.
 */

#include "spdk/stdinc.h"
#include "spdk/blob.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/uuid.h"
#include "spdk_internal/lvolstore.h"

#include "vbdev_s3lvol.h"

SPDK_LOG_REGISTER_COMPONENT(s3lvol_tbase)

#define TEMPLATE_BASE_XATTR "rcow.template_base.v1"

/* Little-endian, fixed size, one metadata page. */
struct template_base_v1 {
	uint32_t version;
	uint32_t role;          /* S3LVOL_TBASE_ROLE_* */
	uint64_t anchor_blob_id;
	char     base_uuid[SPDK_UUID_STRING_LEN];
	char     anchor_uuid[SPDK_UUID_STRING_LEN];
	char     snapshot_uuid[SPDK_UUID_STRING_LEN];
	char     lvstore_uuid[SPDK_UUID_STRING_LEN];
};

static struct spdk_lvol *
tbase_lvol_by_blob_id(struct spdk_lvol_store *store, spdk_blob_id id)
{
	struct spdk_lvol *lvol;

	TAILQ_FOREACH(lvol, &store->lvols, link) {
		if (lvol->blob && lvol->blob_id == id) {
			return lvol;
		}
	}
	return NULL;
}

/* Read and verify the mark on \p anchor. On success *out_base is the base
 * snapshot. Every failure is "not a base" rather than an error worth
 * propagating: a mark that does not verify must never make a volume look like
 * something else's base. */
static int
tbase_read(struct spdk_lvol *anchor, struct template_base_v1 *out,
	   struct spdk_lvol **out_base)
{
	struct spdk_lvol_store *store;
	struct spdk_lvol *base;
	const void *value = NULL;
	size_t len = 0;
	spdk_blob_id pid;
	char lvs_uuid[SPDK_UUID_STRING_LEN];

	if (!anchor || !anchor->blob || !anchor->lvol_store) {
		return -EINVAL;
	}
	if (spdk_blob_get_xattr_value(anchor->blob, TEMPLATE_BASE_XATTR, &value, &len) != 0) {
		return -ENODATA;
	}
	if (len != sizeof(*out)) {
		return -EINVAL;
	}
	memcpy(out, value, sizeof(*out));
	if (!memchr(out->base_uuid, 0, sizeof(out->base_uuid)) ||
	    !memchr(out->anchor_uuid, 0, sizeof(out->anchor_uuid)) ||
	    !memchr(out->snapshot_uuid, 0, sizeof(out->snapshot_uuid)) ||
	    !memchr(out->lvstore_uuid, 0, sizeof(out->lvstore_uuid))) {
		return -EINVAL;
	}
	store = anchor->lvol_store;
	spdk_uuid_fmt_lower(lvs_uuid, sizeof(lvs_uuid), &store->uuid);
	if (le32toh(out->version) != 1 ||
	    strcmp(out->anchor_uuid, anchor->uuid_str) != 0 ||
	    strcmp(out->lvstore_uuid, lvs_uuid) != 0 ||
	    le64toh(out->anchor_blob_id) != spdk_blob_get_id(anchor->blob)) {
		/* Copied with a snapshot or clone of the anchor: not the anchor. */
		return -EINVAL;
	}
	if (spdk_blob_get_num_allocated_clusters(anchor->blob) != 0) {
		/* Someone wrote to it, so it no longer equals the base. */
		return -ESTALE;
	}
	pid = spdk_blob_get_parent_snapshot(store->blobstore, spdk_blob_get_id(anchor->blob));
	if (pid == SPDK_BLOBID_INVALID) {
		return -EINVAL;
	}
	base = tbase_lvol_by_blob_id(store, pid);
	if (!base || !spdk_blob_is_read_only(base->blob) ||
	    strcmp(base->uuid_str, out->snapshot_uuid) != 0) {
		return -EINVAL;
	}
	if (out_base) {
		*out_base = base;
	}
	return 0;
}

struct spdk_lvol *
s3lvol_template_base_find(struct s3lvol_lvstore *lvs, const char *base_uuid)
{
	struct spdk_lvol_store *store;
	struct spdk_lvol *lvol;

	if (!lvs || !base_uuid || base_uuid[0] == '\0') {
		return NULL;
	}
	store = s3lvol_lvstore_get_lvs(lvs);
	if (!store) {
		return NULL;
	}
	TAILQ_FOREACH(lvol, &store->lvols, link) {
		struct template_base_v1 rec;
		struct spdk_lvol *base = NULL;

		if (tbase_read(lvol, &rec, &base) != 0) {
			continue;
		}
		if (strcmp(rec.base_uuid, base_uuid) == 0) {
			return base;
		}
	}
	return NULL;
}

int
s3lvol_template_base_of(struct s3lvol_lvstore *lvs, struct spdk_blob *snapshot,
			char *out_uuid, size_t out_len)
{
	struct spdk_lvol_store *store;
	struct spdk_lvol *lvol;

	if (!lvs || !snapshot || !out_uuid || out_len < SPDK_UUID_STRING_LEN) {
		return -EINVAL;
	}
	store = s3lvol_lvstore_get_lvs(lvs);
	if (!store || !spdk_blob_is_read_only(snapshot)) {
		return -ENOENT;
	}
	TAILQ_FOREACH(lvol, &store->lvols, link) {
		struct template_base_v1 rec;
		struct spdk_lvol *base = NULL;

		if (tbase_read(lvol, &rec, &base) != 0 || !base) {
			continue;
		}
		if (base->blob == snapshot) {
			snprintf(out_uuid, out_len, "%s", rec.base_uuid);
			return 0;
		}
	}
	return -ENOENT;
}

int
s3lvol_template_base_below(struct s3lvol_lvstore *lvs, struct spdk_lvol *lvol,
			   char *out_uuid, size_t out_len)
{
	struct spdk_lvol_store *store;
	struct spdk_lvol *cur = lvol;
	uint32_t depth = 0;

	if (!lvs || !lvol || !out_uuid || out_len < SPDK_UUID_STRING_LEN) {
		return -EINVAL;
	}
	store = s3lvol_lvstore_get_lvs(lvs);
	if (!store) {
		return -ENODEV;
	}
	/* The lvol itself counts: exporting a base publishes that base. */
	while (cur && cur->blob && depth++ < S3LVOL_DEFAULT_MAX_CHAIN_DEPTH) {
		spdk_blob_id pid;

		if (s3lvol_template_base_of(lvs, cur->blob, out_uuid, out_len) == 0) {
			return 0;
		}
		if (spdk_blob_is_esnap_clone(cur->blob)) {
			break;
		}
		pid = spdk_blob_get_parent_snapshot(store->blobstore,
						    spdk_blob_get_id(cur->blob));
		if (pid == SPDK_BLOBID_INVALID) {
			break;
		}
		cur = tbase_lvol_by_blob_id(store, pid);
	}
	out_uuid[0] = '\0';
	return -ENOENT;
}

int
s3lvol_template_base_foreach(struct s3lvol_lvstore *lvs, s3lvol_template_base_cb cb,
			     void *cb_arg)
{
	struct spdk_lvol_store *store;
	struct spdk_lvol *lvol;

	if (!lvs || !cb) {
		return -EINVAL;
	}
	store = s3lvol_lvstore_get_lvs(lvs);
	if (!store) {
		return -ENODEV;
	}
	TAILQ_FOREACH(lvol, &store->lvols, link) {
		struct template_base_v1 rec;
		struct spdk_lvol *base = NULL;
		struct s3lvol_template_base_info info = {0};
		int rc = tbase_read(lvol, &rec, &base);

		if (rc == -ENODATA) {
			continue;
		}
		info.anchor = lvol;
		info.base = rc == 0 ? base : NULL;
		info.base_uuid = rc == -ENODATA ? "" : rec.base_uuid;
		info.role = le32toh(rec.role);
		info.valid = rc == 0;
		info.error = rc;
		cb(cb_arg, &info);
	}
	return 0;
}

const char *
s3lvol_template_base_role_name(uint32_t role)
{
	switch (role) {
	case S3LVOL_TBASE_ROLE_BUILDER: return "builder";
	case S3LVOL_TBASE_ROLE_IMPORTED: return "imported";
	default: return "unknown";
	}
}

/* ==========================================================================
 * Marking
 * ========================================================================== */

struct tbase_mark_ctx {
	struct s3lvol_lvstore *lvs;
	char                   snapshot_name[SPDK_LVOL_NAME_MAX];
	char                   anchor_name[SPDK_LVOL_NAME_MAX];
	char                   base_uuid[SPDK_UUID_STRING_LEN];
	uint32_t               role;
	struct spdk_lvol      *anchor;
	s3lvol_lvol_op_cb      cb_fn;
	void                  *cb_arg;
};

static void
tbase_mark_done(struct tbase_mark_ctx *ctx, int status)
{
	s3lvol_lvol_op_cb cb_fn = ctx->cb_fn;
	void *cb_arg = ctx->cb_arg;
	struct spdk_lvol *anchor = status == 0 ? ctx->anchor : NULL;

	if (status == 0) {
		SPDK_NOTICELOG("Template base %s (%s) is snapshot '%s', anchored by '%s'\n",
			       ctx->base_uuid, s3lvol_template_base_role_name(ctx->role),
			       ctx->snapshot_name, ctx->anchor_name);
	} else {
		SPDK_ERRLOG("Marking '%s' as template base %s failed: %s\n",
			    ctx->snapshot_name, ctx->base_uuid, spdk_strerror(-status));
	}
	free(ctx);
	if (cb_fn) {
		cb_fn(cb_arg, anchor, status);
	}
}

static void
tbase_mark_synced(void *cb_arg, int bserrno)
{
	struct tbase_mark_ctx *ctx = cb_arg;

	if (bserrno != 0) {
		/* Not left half-true in memory: an unsynced mark must not be found. */
		spdk_blob_remove_xattr(ctx->anchor->blob, TEMPLATE_BASE_XATTR);
	}
	tbase_mark_done(ctx, bserrno);
}

static void
tbase_mark_write(struct tbase_mark_ctx *ctx, struct spdk_lvol *anchor)
{
	struct template_base_v1 rec = {0};
	struct spdk_lvol *snapshot;
	int rc;

	ctx->anchor = anchor;
	snapshot = s3lvol_lvol_find(ctx->lvs, ctx->snapshot_name);
	if (!snapshot) {
		tbase_mark_done(ctx, -ENOENT);
		return;
	}
	rec.version = htole32(1);
	rec.role = htole32(ctx->role);
	rec.anchor_blob_id = htole64(spdk_blob_get_id(anchor->blob));
	snprintf(rec.base_uuid, sizeof(rec.base_uuid), "%s", ctx->base_uuid);
	snprintf(rec.anchor_uuid, sizeof(rec.anchor_uuid), "%s", anchor->uuid_str);
	snprintf(rec.snapshot_uuid, sizeof(rec.snapshot_uuid), "%s", snapshot->uuid_str);
	spdk_uuid_fmt_lower(rec.lvstore_uuid, sizeof(rec.lvstore_uuid),
			    &anchor->lvol_store->uuid);

	rc = spdk_blob_set_xattr(anchor->blob, TEMPLATE_BASE_XATTR, &rec, sizeof(rec));
	if (rc != 0) {
		tbase_mark_done(ctx, rc);
		return;
	}
	spdk_blob_sync_md(anchor->blob, tbase_mark_synced, ctx);
}

static void
tbase_anchor_created(void *cb_arg, struct spdk_lvol *lvol, int lvolerrno)
{
	struct tbase_mark_ctx *ctx = cb_arg;

	if (lvolerrno != 0) {
		tbase_mark_done(ctx, lvolerrno);
		return;
	}
	tbase_mark_write(ctx, lvol);
}

/* Is \p base_uuid provably the content of \p snapshot here? */
static int
tbase_prove_identity(struct s3lvol_lvstore *lvs, struct spdk_lvol *snapshot,
		     struct spdk_lvol *anchor, const char *base_uuid, uint32_t *out_role)
{
	struct s3lvol_export *exp;
	struct s3lvol_export_entry entry;
	struct spdk_lvol_store *store = s3lvol_lvstore_get_lvs(lvs);
	int rc;

	/* Builder: this lvstore published the snapshot under that uuid. */
	exp = s3lvol_export_find(lvs, base_uuid);
	if (exp) {
		s3lvol_export_get(exp, &entry);
		if (entry.publication_pending) {
			return -EBUSY;
		}
		if (!entry.snapshot_uuid || strcmp(entry.snapshot_uuid, snapshot->uuid_str) != 0) {
			SPDK_ERRLOG("export %s is not of snapshot '%s'\n", base_uuid,
				    snapshot->name);
			return -EXDEV;
		}
		*out_role = S3LVOL_TBASE_ROLE_BUILDER;
		return 0;
	}

	/* Importer: the anchor is the decoupled import of that export, and the
	 * snapshot was taken of it, so the anchor's own import origin names it. */
	if (!anchor) {
		return -ENOENT;
	}
	rc = s3lvol_import_origin_export_uuid_is(anchor, base_uuid);
	if (rc != 0) {
		return rc;
	}
	if (spdk_blob_is_esnap_clone(anchor->blob)) {
		/* Still reads through the export: the base is not local. */
		return -EAGAIN;
	}
	if (spdk_blob_get_parent_snapshot(store->blobstore, spdk_blob_get_id(anchor->blob)) !=
	    spdk_blob_get_id(snapshot->blob)) {
		return -EXDEV;
	}
	*out_role = S3LVOL_TBASE_ROLE_IMPORTED;
	return 0;
}

int
s3lvol_template_base_mark(struct s3lvol_lvstore *lvs, const char *snapshot_name,
			  const char *anchor_name, const char *base_uuid,
			  s3lvol_lvol_op_cb cb_fn, void *cb_arg)
{
	struct spdk_lvol *snapshot, *anchor, *existing;
	struct tbase_mark_ctx *ctx;
	struct spdk_uuid parsed;
	uint32_t role = 0;
	int rc;

	if (!lvs || !snapshot_name || !anchor_name || !base_uuid ||
	    spdk_uuid_parse(&parsed, base_uuid) != 0) {
		return -EINVAL;
	}
	snapshot = s3lvol_lvol_find(lvs, snapshot_name);
	if (!snapshot || !spdk_blob_is_read_only(snapshot->blob)) {
		SPDK_ERRLOG("template base needs an existing read-only snapshot, '%s' is %s\n",
			    snapshot_name, snapshot ? "writable" : "missing");
		return snapshot ? -EPERM : -ENOENT;
	}
	existing = s3lvol_template_base_find(lvs, base_uuid);
	if (existing) {
		/* Idempotent for the same snapshot, a conflict for another. */
		return existing == snapshot ? -EALREADY : -EEXIST;
	}
	anchor = s3lvol_lvol_find(lvs, anchor_name);
	rc = tbase_prove_identity(lvs, snapshot, anchor, base_uuid, &role);
	if (rc != 0) {
		SPDK_ERRLOG("cannot prove '%s' is template base %s: %s\n", snapshot_name,
			    base_uuid, spdk_strerror(-rc));
		return rc;
	}
	if (anchor && spdk_blob_get_num_allocated_clusters(anchor->blob) != 0) {
		return -ESTALE;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		return -ENOMEM;
	}
	ctx->lvs = lvs;
	ctx->role = role;
	ctx->cb_fn = cb_fn;
	ctx->cb_arg = cb_arg;
	snprintf(ctx->snapshot_name, sizeof(ctx->snapshot_name), "%s", snapshot_name);
	snprintf(ctx->anchor_name, sizeof(ctx->anchor_name), "%s", anchor_name);
	snprintf(ctx->base_uuid, sizeof(ctx->base_uuid), "%s", base_uuid);

	if (anchor) {
		tbase_mark_write(ctx, anchor);
		return 0;
	}
	rc = s3lvol_lvol_create_clone(lvs, snapshot, anchor_name, tbase_anchor_created, ctx);
	if (rc != 0) {
		free(ctx);
	}
	return rc;
}
