/* SPDX-License-Identifier: Apache-2.0 */
/* Offline test for s3lvol_warm_template_name() (lvol_warm_select.h). */
#include <stdio.h>
#include <stdlib.h>

#include "lvol_warm_select.h"

static int failures;

static void
expect(const char *name, bool want)
{
	bool got = s3lvol_warm_template_name(name);

	if (got != want) {
		fprintf(stderr, "FAIL: %s: got %d want %d\n", name ? name : "(null)", got, want);
		failures++;
	}
}

int
main(void)
{
	expect("tpl-tpl-c5d449e33f734cdbb764de68-rootfs", true);
	expect("tpl-tpl-c5d449e33f734cdbb764de68-memory-snap", true);
	expect("tpl-tpl-x-b6d1f2a901b2c-rootfs-anchor", true);
	expect("s3-meta-tpl-4f8c35922d41485c9a9f2514", true);
	expect("s3-meta-tpl-4f8c35922d41485c9a9f2514-snap", true);

	expect("tpl-tpl-x-build-rootfs", false);
	expect("s3-meta-tpl-x-build-1", false);
	expect("tpl-snap-94cab90d795b475db67d7d0b-rootfs", false);
	expect("s3-meta-snap-94cab90d795b475db67d7d0b-snap", false);
	expect("s3-meta-5ac01d1ef0a64b999c4cbd3a4edd8398", false);
	expect("sb-5ac01d1ef0a64b999c4cbd3a4edd8398-rootfs-0", false);
	expect("cubelet-s3-metadata-base", false);
	expect("", false);
	expect(NULL, false);

	if (failures) {
		fprintf(stderr, "s3_warm_select_test: %d failure(s)\n", failures);
		return 1;
	}
	printf("s3_warm_select_test: OK\n");
	return 0;
}
