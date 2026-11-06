/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Focused unit test for the legacy/REF retention disposition contract.
 *
 * SYNTHETIC FIXTURES ONLY. Every struct s3lvol_export_facts below is built by
 * hand in this file; nothing here touches S3, a blobstore, a registry, or any
 * module state. The point is to pin the classification rules and, above all,
 * the invariants:
 *
 *   - release_supported is true for exactly the dense-v4 releasable and
 *     releasing dispositions, and for nothing else;
 *   - migration_supported is never true;
 *   - every disposition explains itself (non-empty reason, action, evidence);
 *   - a non-releasable export never offers a release recipe.
 *
 * What this does NOT test: the release path itself (unchanged, covered by the
 * runtime harness), or that an admission document on S3 really says what the
 * cached state claims. It tests the pure derivation only.
 */
#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "export_disposition_ctl.h"

/* A synthetic registry entry's facts. Zero-initialised then overridden, which
 * is what a freshly calloc()'d entry looks like. */
static struct s3lvol_export_facts
facts(bool is_ref, bool source_obligation, bool publication_pending,
      bool reaping)
{
	struct s3lvol_export_facts f;

	memset(&f, 0, sizeof(f));
	f.is_ref = is_ref;
	f.source_obligation = source_obligation;
	f.publication_pending = publication_pending;
	f.reaping = reaping;
	return f;
}

static void
expect(const struct s3lvol_export_facts *f, enum s3lvol_export_admission adm,
       enum s3lvol_export_disposition want, bool release_supported,
       bool reconcile_supported, const char *label)
{
	struct s3lvol_export_disposition_info d;

	s3lvol_export_disposition(f, adm, &d);
	if (d.disposition != want) {
		fprintf(stderr, "%s: disposition %s, expected %s\n", label,
			s3lvol_export_disposition_str(d.disposition),
			s3lvol_export_disposition_str(want));
		assert(0);
	}
	if (d.release_supported != release_supported) {
		fprintf(stderr, "%s: release_supported %d, expected %d\n", label,
			d.release_supported, release_supported);
		assert(0);
	}
	if (d.reconcile_supported != reconcile_supported) {
		fprintf(stderr, "%s: reconcile_supported %d, expected %d\n", label,
			d.reconcile_supported, reconcile_supported);
		assert(0);
	}
	/* The invariant that matters most: no supported migration exists. */
	if (d.migration_supported) {
		fprintf(stderr, "%s: migration_supported must never be true\n",
			label);
		assert(0);
	}
	/* Every disposition must explain itself. An empty reason is a bug: the
	 * whole point of this contract is that an operator can read why. */
	if (!d.retention_reason || !d.retention_reason[0]) {
		fprintf(stderr, "%s: empty retention_reason\n", label);
		assert(0);
	}
	if (!d.required_operator_action || !d.required_operator_action[0]) {
		fprintf(stderr, "%s: empty required_operator_action\n", label);
		assert(0);
	}
	if (!d.evidence_missing || !d.evidence_missing[0]) {
		fprintf(stderr, "%s: empty evidence_missing\n", label);
		assert(0);
	}
	/* A releasable export must not claim no action is supported. */
	if (release_supported &&
	    strstr(d.required_operator_action, "none supported") != NULL) {
		fprintf(stderr, "%s: releasable export says no action is supported\n",
			label);
		assert(0);
	}
	/* A non-releasable export must not offer a release recipe. */
	if (!release_supported &&
	    strstr(d.required_operator_action, "rcow_release_export closes") != NULL) {
		fprintf(stderr, "%s: non-releasable export offers a release\n",
			label);
		assert(0);
	}
}

int
main(void)
{
	struct s3lvol_export_facts f;
	struct s3lvol_export_disposition_info d;

	/* ---- REF: retained regardless of admission state ------------------ */
	f = facts(true, true, false, false);
	expect(&f, S3LVOL_EXPORT_ADMISSION_ABSENT, S3LVOL_EXPORT_DISP_REF,
	       false, false, "ref/absent");
	expect(&f, S3LVOL_EXPORT_ADMISSION_OPEN, S3LVOL_EXPORT_DISP_REF,
	       false, false, "ref/open");
	expect(&f, S3LVOL_EXPORT_ADMISSION_DONE, S3LVOL_EXPORT_DISP_REF,
	       false, false, "ref/done");

	/* ---- materialised REF: dense bytes, sticky obligation ------------- */
	f = facts(false, true, false, false);
	expect(&f, S3LVOL_EXPORT_ADMISSION_ABSENT,
	       S3LVOL_EXPORT_DISP_REF_MATERIALISED, false, false,
	       "materialised/absent");
	expect(&f, S3LVOL_EXPORT_ADMISSION_OPEN,
	       S3LVOL_EXPORT_DISP_REF_MATERIALISED, false, false,
	       "materialised/open");

	/* ---- legacy dense: no obligation, no admission document ----------- */
	f = facts(false, false, false, false);
	expect(&f, S3LVOL_EXPORT_ADMISSION_ABSENT,
	       S3LVOL_EXPORT_DISP_LEGACY_DENSE, false, false, "legacy/absent");

	/* ---- dense v4 releasable: OPEN admission -------------------------- */
	expect(&f, S3LVOL_EXPORT_ADMISSION_OPEN,
	       S3LVOL_EXPORT_DISP_DENSE_V4_RELEASABLE, true, false,
	       "dense_v4/open");

	/* ---- dense v4 releasing: CLOSED/CLEANUP/DONE ---------------------- */
	expect(&f, S3LVOL_EXPORT_ADMISSION_CLOSED,
	       S3LVOL_EXPORT_DISP_DENSE_V4_RELEASING, true, false,
	       "dense_v4/closed");
	expect(&f, S3LVOL_EXPORT_ADMISSION_CLEANUP,
	       S3LVOL_EXPORT_DISP_DENSE_V4_RELEASING, true, false,
	       "dense_v4/cleanup");
	expect(&f, S3LVOL_EXPORT_ADMISSION_DONE,
	       S3LVOL_EXPORT_DISP_DENSE_V4_RELEASING, true, false,
	       "dense_v4/done");

	/* ---- dense v4 with malformed admission: no safe action ------------ */
	expect(&f, S3LVOL_EXPORT_ADMISSION_MALFORMED,
	       S3LVOL_EXPORT_DISP_DENSE_V4_ADMISSION_UNUSABLE, false, false,
	       "dense_v4/malformed");

	/* ---- pending publication: reconcile only -------------------------- */
	f = facts(false, false, true, false);
	expect(&f, S3LVOL_EXPORT_ADMISSION_ABSENT,
	       S3LVOL_EXPORT_DISP_DENSE_V4_PENDING, false, true,
	       "pending/absent");
	/* A pending REF reservation is still pending: the manifest may not exist
	 * yet, so nothing about its layout is proven. */
	f = facts(true, true, true, false);
	expect(&f, S3LVOL_EXPORT_ADMISSION_ABSENT,
	       S3LVOL_EXPORT_DISP_DENSE_V4_PENDING, false, true,
	       "pending/ref-layout");

	/* ---- reaping: release in flight, nothing to decide ---------------- */
	f = facts(false, false, false, true);
	expect(&f, S3LVOL_EXPORT_ADMISSION_OPEN, S3LVOL_EXPORT_DISP_UNKNOWN,
	       false, false, "reaping");

	/* ---- NULL facts: no registry row ---------------------------------- */
	s3lvol_export_disposition(NULL, S3LVOL_EXPORT_ADMISSION_ABSENT, &d);
	assert(d.disposition == S3LVOL_EXPORT_DISP_UNKNOWN);
	assert(!d.release_supported && !d.reconcile_supported &&
	       !d.migration_supported);
	assert(d.retention_reason && d.retention_reason[0]);

	/* ---- NULL out: must not crash ------------------------------------- */
	s3lvol_export_disposition(&f, S3LVOL_EXPORT_ADMISSION_OPEN, NULL);

	/* ---- string tables are total -------------------------------------- */
	assert(strcmp(s3lvol_export_disposition_str(S3LVOL_EXPORT_DISP_REF),
		      "ref") == 0);
	assert(strcmp(s3lvol_export_disposition_str(
		      S3LVOL_EXPORT_DISP_REF_MATERIALISED),
		      "ref_materialised") == 0);
	assert(strcmp(s3lvol_export_disposition_str(
		      S3LVOL_EXPORT_DISP_LEGACY_DENSE), "legacy_dense") == 0);
	assert(strcmp(s3lvol_export_disposition_str(
		      S3LVOL_EXPORT_DISP_DENSE_V4_RELEASABLE),
		      "dense_v4_releasable") == 0);
	assert(strcmp(s3lvol_export_disposition_str(
		      S3LVOL_EXPORT_DISP_DENSE_V4_PENDING),
		      "dense_v4_pending") == 0);
	assert(strcmp(s3lvol_export_disposition_str(
		      S3LVOL_EXPORT_DISP_DENSE_V4_RELEASING),
		      "dense_v4_releasing") == 0);
	assert(strcmp(s3lvol_export_disposition_str(
		      S3LVOL_EXPORT_DISP_DENSE_V4_ADMISSION_UNUSABLE),
		      "dense_v4_admission_unusable") == 0);
	assert(strcmp(s3lvol_export_disposition_str(
		      S3LVOL_EXPORT_DISP_UNKNOWN), "unknown") == 0);
	assert(strcmp(s3lvol_export_admission_str(
		      S3LVOL_EXPORT_ADMISSION_ABSENT), "absent") == 0);
	assert(strcmp(s3lvol_export_admission_str(
		      S3LVOL_EXPORT_ADMISSION_OPEN), "open") == 0);
	assert(strcmp(s3lvol_export_admission_str(
		      S3LVOL_EXPORT_ADMISSION_MALFORMED), "malformed") == 0);

	/* ---- exhaustive: only the two dense-v4 dispositions release ------- */
	{
		enum s3lvol_export_disposition all[] = {
			S3LVOL_EXPORT_DISP_UNKNOWN,
			S3LVOL_EXPORT_DISP_DENSE_V4_RELEASABLE,
			S3LVOL_EXPORT_DISP_DENSE_V4_PENDING,
			S3LVOL_EXPORT_DISP_DENSE_V4_RELEASING,
			S3LVOL_EXPORT_DISP_DENSE_V4_ADMISSION_UNUSABLE,
			S3LVOL_EXPORT_DISP_LEGACY_DENSE,
			S3LVOL_EXPORT_DISP_REF,
			S3LVOL_EXPORT_DISP_REF_MATERIALISED,
		};
		enum s3lvol_export_admission adms[] = {
			S3LVOL_EXPORT_ADMISSION_ABSENT,
			S3LVOL_EXPORT_ADMISSION_OPEN,
			S3LVOL_EXPORT_ADMISSION_CLOSED,
			S3LVOL_EXPORT_ADMISSION_CLEANUP,
			S3LVOL_EXPORT_ADMISSION_DONE,
			S3LVOL_EXPORT_ADMISSION_MALFORMED,
		};
		unsigned i, j, k;
		unsigned seen[8] = {0};

		for (i = 0; i < 2; i++) {
			for (j = 0; j < 2; j++) {
				for (k = 0; k < 2; k++) {
					unsigned a;

					f = facts(i, j, k, false);
					for (a = 0; a < sizeof(adms) / sizeof(adms[0]);
					     a++) {
						s3lvol_export_disposition(&f, adms[a],
									  &d);
						seen[d.disposition]++;
						assert(!d.migration_supported);
						if (d.release_supported) {
							assert(d.disposition ==
							       S3LVOL_EXPORT_DISP_DENSE_V4_RELEASABLE ||
							       d.disposition ==
							       S3LVOL_EXPORT_DISP_DENSE_V4_RELEASING);
						}
						if (d.reconcile_supported) {
							assert(d.disposition ==
							       S3LVOL_EXPORT_DISP_DENSE_V4_PENDING);
						}
					}
				}
			}
		}
		/* Every non-pending, non-reaping disposition must be reachable, so
		 * the table above is not silently missing a case. */
		for (i = 0; i < sizeof(all) / sizeof(all[0]); i++) {
			if (all[i] == S3LVOL_EXPORT_DISP_UNKNOWN) {
				continue;	/* only via reaping/NULL */
			}
			if (!seen[all[i]]) {
				fprintf(stderr, "unreachable disposition %s\n",
					s3lvol_export_disposition_str(all[i]));
				assert(0);
			}
		}
	}

	puts("legacy/REF disposition: classification, invariants, exhaustive "
	     "release/reconcile exclusivity and string tables: PASS");
	return 0;
}
