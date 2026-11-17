/* SPDX-License-Identifier: Apache-2.0 */
/* Offline test for the export per-chunk retry policy (export_chunk_retry.h). */
#include <stdio.h>
#include <stdlib.h>

#include "export_chunk_retry.h"

static int failures;

static void
expect(const char *what, int status, uint32_t attempts, bool export_failed, bool want)
{
	bool got = export_chunk_should_retry(status, attempts, export_failed);

	if (got != want) {
		fprintf(stderr, "FAIL: %s: status=%d attempts=%u failed=%d: got %d want %d\n",
			what, status, attempts, export_failed, got, want);
		failures++;
	}
}

int
main(void)
{
	/* Success is never retried. */
	expect("success", 0, 1, false, false);

	/* The live failure: the B16 deadline reports -ETIMEDOUT, a cancelled
	 * request -ECANCELED. Retried until the attempt budget runs out. */
	expect("timeout first", -ETIMEDOUT, 1, false, true);
	expect("timeout third", -ETIMEDOUT, EXPORT_CHUNK_MAX_ATTEMPTS - 1, false, true);
	expect("timeout budget", -ETIMEDOUT, EXPORT_CHUNK_MAX_ATTEMPTS, false, false);
	expect("timeout over", -ETIMEDOUT, EXPORT_CHUNK_MAX_ATTEMPTS + 3, false, false);
	expect("cancelled", -ECANCELED, 1, false, true);
	expect("reset", -ECONNRESET, 2, false, true);
	expect("eio", -EIO, 1, false, true);

	/* Another chunk already failed the export: no point retrying. */
	expect("export failed", -ETIMEDOUT, 1, true, false);

	/* Permanent errors fail at once. */
	expect("access", -EACCES, 1, false, false);
	expect("perm", -EPERM, 1, false, false);
	expect("nomem", -ENOMEM, 1, false, false);
	expect("noent", -ENOENT, 1, false, false);
	expect("inval", -EINVAL, 1, false, false);

	if (EXPORT_CHUNK_MAX_ATTEMPTS < 2) {
		fprintf(stderr, "FAIL: retry budget allows no retry\n");
		failures++;
	}

	if (failures) {
		fprintf(stderr, "%d failure(s)\n", failures);
		return 1;
	}
	printf("s3_export_chunk_retry_test: OK\n");
	return 0;
}
