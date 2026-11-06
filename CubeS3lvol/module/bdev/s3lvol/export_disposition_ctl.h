/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Legacy / REF retention disposition: the pure classification.
 *
 * Header-only and dependency-free on purpose. The derivation must be testable
 * with synthetic fixtures and must not be able to reach module state, an S3
 * client, or the registry. Keeping it here makes that structural rather than a
 * convention: if a future change needs module state, it cannot be made without
 * moving the function, which is the review signal we want.
 *
 * Invariants (asserted by test/integration/export_disposition_test.c):
 *   - release_supported is true for exactly the dense-v4 releasable and
 *     releasing dispositions, and for nothing else.
 *   - migration_supported is false everywhere. There is no supported automated
 *     migration for REF or legacy dense exports.
 *   - Every disposition names a retention reason, an operator action, and the
 *     evidence that is missing. An empty string is a bug.
 *
 * No field here authorises a deletion. They describe; they do not permit.
 */
#ifndef EXPORT_DISPOSITION_CTL_H
#define EXPORT_DISPOSITION_CTL_H

#include <stdbool.h>
#include <string.h>

enum s3lvol_export_disposition {
	/* Not classified yet (entry still being published, or reaping). */
	S3LVOL_EXPORT_DISP_UNKNOWN = 0,
	/* Opt-in dense v4 with a durable OPEN admission: release is supported. */
	S3LVOL_EXPORT_DISP_DENSE_V4_RELEASABLE,
	/* Dense v4 whose publication never committed: reconcile may finish it. */
	S3LVOL_EXPORT_DISP_DENSE_V4_PENDING,
	/* Dense v4 whose admission is CLOSED/CLEANUP/DONE: release is resumable. */
	S3LVOL_EXPORT_DISP_DENSE_V4_RELEASING,
	/* Dense v4 whose admission is missing or malformed: no safe action. */
	S3LVOL_EXPORT_DISP_DENSE_V4_ADMISSION_UNUSABLE,
	/* Dense manifest older than v4 (v2/v3): no admission record exists. */
	S3LVOL_EXPORT_DISP_LEGACY_DENSE,
	/* Zero-copy REF export: its objects are the snapshot's live chunks. */
	S3LVOL_EXPORT_DISP_REF,
	/* REF that was materialised: dense bytes, sticky source obligation. */
	S3LVOL_EXPORT_DISP_REF_MATERIALISED,
};

/* Admission document state as observed for one export, or "absent". */
enum s3lvol_export_admission {
	S3LVOL_EXPORT_ADMISSION_ABSENT = 0,	/* no document (legacy export) */
	S3LVOL_EXPORT_ADMISSION_OPEN,
	S3LVOL_EXPORT_ADMISSION_CLOSED,
	S3LVOL_EXPORT_ADMISSION_CLEANUP,
	S3LVOL_EXPORT_ADMISSION_DONE,
	S3LVOL_EXPORT_ADMISSION_MALFORMED,	/* present but not a valid record */
};

struct s3lvol_export_disposition_info {
	enum s3lvol_export_disposition disposition;
	enum s3lvol_export_admission    admission;

	/* True only for DENSE_V4_RELEASABLE / DENSE_V4_RELEASING. */
	bool release_supported;
	/* True only for DENSE_V4_PENDING (explicit reconcile is the action). */
	bool reconcile_supported;
	/* Always false today. Kept explicit so a caller cannot infer it. */
	bool migration_supported;

	/* Stable machine-readable reason the export is still retained. */
	const char *retention_reason;
	/* What the operator may do, in one sentence. Never a deletion recipe. */
	const char *required_operator_action;
	/* Evidence that is missing before any stronger action could be proven. */
	const char *evidence_missing;
};

/* The facts the classification depends on, extracted from a registry entry.
 * A plain struct so a test can state them directly. */
struct s3lvol_export_facts {
	bool is_ref;			/* layout == REF */
	bool source_obligation;		/* sticky obligation retained */
	bool publication_pending;	/* registry PUT did not commit */
	bool reaping;			/* a release is already in flight */
};

static inline const char *
s3lvol_export_disposition_str(enum s3lvol_export_disposition d)
{
	switch (d) {
	case S3LVOL_EXPORT_DISP_DENSE_V4_RELEASABLE:
		return "dense_v4_releasable";
	case S3LVOL_EXPORT_DISP_DENSE_V4_PENDING:
		return "dense_v4_pending";
	case S3LVOL_EXPORT_DISP_DENSE_V4_RELEASING:
		return "dense_v4_releasing";
	case S3LVOL_EXPORT_DISP_DENSE_V4_ADMISSION_UNUSABLE:
		return "dense_v4_admission_unusable";
	case S3LVOL_EXPORT_DISP_LEGACY_DENSE:
		return "legacy_dense";
	case S3LVOL_EXPORT_DISP_REF:
		return "ref";
	case S3LVOL_EXPORT_DISP_REF_MATERIALISED:
		return "ref_materialised";
	case S3LVOL_EXPORT_DISP_UNKNOWN:
	default:
		return "unknown";
	}
}

static inline const char *
s3lvol_export_admission_str(enum s3lvol_export_admission a)
{
	switch (a) {
	case S3LVOL_EXPORT_ADMISSION_OPEN:
		return "open";
	case S3LVOL_EXPORT_ADMISSION_CLOSED:
		return "closed";
	case S3LVOL_EXPORT_ADMISSION_CLEANUP:
		return "cleanup";
	case S3LVOL_EXPORT_ADMISSION_DONE:
		return "done";
	case S3LVOL_EXPORT_ADMISSION_MALFORMED:
		return "malformed";
	case S3LVOL_EXPORT_ADMISSION_ABSENT:
	default:
		return "absent";
	}
}

/* Derive the retention disposition. Pure: reads \p f and \p adm, writes \p out.
 *
 * Why there is no migration action
 * --------------------------------
 * The tempting "migration" is to republish a REF export as a fresh dense v4
 * export under a new UUID. It is not implemented, and the reason is not
 * laziness:
 *
 *   1. A REF manifest names the snapshot's live chunk objects. Copying them is
 *      exactly what rcow_materialise_export already does, and materialisation
 *      deliberately does NOT discharge the source obligation: a derived
 *      manifest published from this export can cache the original source refs,
 *      so the source snapshot must stay pinned even after the top-level
 *      manifest holds its own bytes. A "migration" that released the source
 *      would break those derived readers.
 *   2. A fresh export under a new UUID is a different export identity. Existing
 *      importers hold the old UUID in their own registries and esnap metadata;
 *      they would keep reading the old manifest. Nothing here can rewrite
 *      another node's registry, and pretending otherwise would strand readers.
 *   3. Legacy v2/v3 dense exports have no admission document at all. There is
 *      no durable record of who read them, so no proof of reader independence
 *      can be constructed after the fact. Inventing one (a TTL, a lease, a
 *      local reader count) is precisely what this codebase refuses to do.
 *
 * So the honest contract is: these exports are retained, here is why, here is
 * what evidence is missing, and the only supported forward path is a NEW
 * export under a NEW UUID through the opt-in dense v4 path, performed by an
 * operator who accepts that it is a new identity and does not release the old
 * one. That is stated in required_operator_action, not performed automatically.
 */
static inline void
s3lvol_export_disposition(const struct s3lvol_export_facts *f,
			  enum s3lvol_export_admission adm,
			  struct s3lvol_export_disposition_info *out)
{
	if (!out) {
		return;
	}
	memset(out, 0, sizeof(*out));
	out->admission = adm;
	/* Never true. Set explicitly so a reader cannot mistake the zero value
	 * for "not computed". */
	out->migration_supported = false;

	if (!f) {
		out->disposition = S3LVOL_EXPORT_DISP_UNKNOWN;
		out->retention_reason = "no registry entry";
		out->required_operator_action =
			"none; the export is not in this node's registry";
		out->evidence_missing = "a registry entry";
		return;
	}

	/* A publication that never committed is not yet any of the below: the
	 * manifest may not exist, and the reservation is deliberately pinned. */
	if (f->publication_pending) {
		out->disposition = S3LVOL_EXPORT_DISP_DENSE_V4_PENDING;
		out->reconcile_supported = true;
		out->retention_reason =
			"publication reservation is pending; the registry PUT did not "
			"commit, so the export is conservatively pinned";
		out->required_operator_action =
			"rcow_reconcile_export re-reads the manifest and admission and "
			"commits the registry if both prove a first-generation dense v4 "
			"publication; it refuses otherwise and changes nothing";
		out->evidence_missing =
			"a committed registry entry; reconcile supplies it only when the "
			"manifest and an OPEN admission document agree";
		return;
	}

	if (f->reaping) {
		out->disposition = S3LVOL_EXPORT_DISP_UNKNOWN;
		out->retention_reason =
			"a release is already in flight for this entry";
		out->required_operator_action =
			"wait for the in-flight release to finish, then re-read status";
		out->evidence_missing = "a completed release";
		return;
	}

	/* REF first: layout is authoritative for whether the manifest names the
	 * snapshot's own live chunks. A materialised REF is dense on disk but
	 * keeps source_obligation, which is what distinguishes it. */
	if (f->is_ref) {
		out->disposition = S3LVOL_EXPORT_DISP_REF;
		out->retention_reason =
			"zero-copy REF export: its manifest names the snapshot's live "
			"chunk objects, so the snapshot must stay until every reader is "
			"durably detached";
		out->required_operator_action =
			"none supported. rcow_release_export refuses. To stop depending "
			"on this export, publish a NEW export of the snapshot through the "
			"opt-in dense v4 path (release_safe=true) under a NEW uuid and "
			"migrate importers to it; the old export stays retained";
		out->evidence_missing =
			"a durable detach/lineage protocol proving every importer and "
			"every derived manifest no longer references this export";
		return;
	}

	/* Dense from here on. */
	if (f->source_obligation) {
		/* Materialised from REF: dense bytes, sticky obligation. */
		out->disposition = S3LVOL_EXPORT_DISP_REF_MATERIALISED;
		out->retention_reason =
			"materialised REF export: the manifest now holds its own copies, "
			"but derived manifests can still cache the original source refs, "
			"so the source obligation is retained";
		out->required_operator_action =
			"none supported. rcow_release_export refuses. Materialising again "
			"is a no-op; the source snapshot stays pinned";
		out->evidence_missing =
			"proof that no derived manifest retains the original source refs";
		return;
	}

	/* Dense with no source obligation: either the opt-in v4 path (which has
	 * an admission document) or a legacy v2/v3 dense export (which does not). */
	switch (adm) {
	case S3LVOL_EXPORT_ADMISSION_OPEN:
		out->disposition = S3LVOL_EXPORT_DISP_DENSE_V4_RELEASABLE;
		out->release_supported = true;
		out->retention_reason =
			"opt-in dense v4 export with a durable OPEN admission document; "
			"it is retained only until its registered readers are detached";
		out->required_operator_action =
			"each importing store must decouple and then call "
			"rcow_ack_export_detach; once the reader set is empty, "
			"rcow_release_export closes admission and cleans up";
		out->evidence_missing =
			"an empty durable reader set (each reader must acknowledge detach)";
		return;
	case S3LVOL_EXPORT_ADMISSION_CLOSED:
	case S3LVOL_EXPORT_ADMISSION_CLEANUP:
	case S3LVOL_EXPORT_ADMISSION_DONE:
		out->disposition = S3LVOL_EXPORT_DISP_DENSE_V4_RELEASING;
		out->release_supported = true;
		out->retention_reason =
			"opt-in dense v4 export whose admission is already closed; "
			"release is resumable and will not reopen admission";
		out->required_operator_action =
			"call rcow_release_export again; it resumes CLOSED/CLEANUP/DONE "
			"from the durable tombstone";
		out->evidence_missing =
			"a completed cleanup (the tombstone records how far it got)";
		return;
	case S3LVOL_EXPORT_ADMISSION_MALFORMED:
		out->disposition = S3LVOL_EXPORT_DISP_DENSE_V4_ADMISSION_UNUSABLE;
		out->retention_reason =
			"dense export whose admission document exists but is not a valid "
			"record; release refuses rather than guess";
		out->required_operator_action =
			"none supported. Do not delete or rewrite the admission object; "
			"investigate the bucket writer that produced it";
		out->evidence_missing =
			"a valid admission document matching this export id and owner";
		return;
	case S3LVOL_EXPORT_ADMISSION_ABSENT:
	default:
		break;
	}

	/* Dense, no obligation, no admission document. Two possibilities, and the
	 * registry cannot tell them apart: a legacy v2/v3 export published before
	 * admission existed, or a v4 export whose admission object was lost. Both
	 * are retained, and neither may be released. */
	out->disposition = S3LVOL_EXPORT_DISP_LEGACY_DENSE;
	out->retention_reason =
		"dense export with no admission document: either a legacy v2/v3 "
		"export published before admission existed, or a v4 export whose "
		"admission object is missing. Neither can be proven releasable";
	out->required_operator_action =
		"none supported. rcow_release_export refuses. Publish a NEW export "
		"under a NEW uuid through the opt-in dense v4 path if a releasable "
		"copy is needed; the old export stays retained";
	out->evidence_missing =
		"an admission document recording the readers of this export; it "
		"cannot be reconstructed after the fact";
}

#endif /* EXPORT_DISPOSITION_CTL_H */
