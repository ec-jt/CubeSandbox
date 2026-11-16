/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Which lvols the lvstore-attach warm loop pulls into the local s3_cache
 * (s3lvol_lvstore_warm_templates). Pure, so it is unit tested offline
 * (test/integration/s3_warm_select_test.c).
 *
 * Warmed: template data every restore or create reads.
 *   tpl-tpl-<template>-...        template rootfs / memory snapshots and the
 *                                 template-base anchors.
 *   s3-meta-tpl-<template>[-snap] the template package metadata disk. Every
 *                                 create from the template derives and mounts
 *                                 a metadata disk from it; a cold read here
 *                                 made that mount miss its 3 s budget.
 *
 * Not warmed:
 *   ...-build-...                 build scratch, never read by a sandbox.
 *   tpl-snap-*, s3-meta-snap-*    per-sandbox pause snapshots: there can be
 *                                 many and they would push shared template
 *                                 data out of the cache.
 *   s3-meta-<sandbox>, sb-*       per-sandbox writable volumes.
 */
#ifndef S3LVOL_LVOL_WARM_SELECT_H
#define S3LVOL_LVOL_WARM_SELECT_H

#include <stdbool.h>
#include <string.h>

static inline bool
s3lvol_warm_template_name(const char *name)
{
	if (name == NULL || strstr(name, "-build-") != NULL) {
		return false;
	}
	return strncmp(name, "tpl-tpl-", 8) == 0 ||
	       strncmp(name, "s3-meta-tpl-", 12) == 0;
}

#endif /* S3LVOL_LVOL_WARM_SELECT_H */
