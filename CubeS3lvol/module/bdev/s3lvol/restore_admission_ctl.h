/* SPDX-License-Identifier: Apache-2.0 */
#ifndef RESTORE_ADMISSION_CTL_H
#define RESTORE_ADMISSION_CTL_H
#include <stdbool.h>
#include "export_admission_ctl.h"

/* Separate wire version: never enlarge ea_doc or reinterpret a reader UUID.
 * A v1 writer rejects this object's length/magic before mutation. Upgrade is
 * one ETag CAS, preserving every v1 reader and its state. No downgrade exists.
 * Attempts are 256-bit identities bound to a 256-bit committed-record key.
 * There is deliberately no clock, TTL, process-liveness or force operation. */
#define ER_ATTEMPTS 64
struct er_hold { uint8_t attempt[32], commit[32]; };
struct er_doc {
	struct ea_doc base;
	uint8_t count;
	uint8_t reserved[7];
	struct er_hold holds[ER_ATTEMPTS];
};
static inline int er_validate(const struct er_doc *d, size_t len)
{
	struct ea_doc base = d->base;
	static const uint8_t zero[64];
	unsigned i, j;
	if (len == sizeof(base)) return ea_validate(&base, len);
	if (len != sizeof(*d) || memcmp(base.magic, "S3EA0002", 8) ||
	    d->count > ER_ATTEMPTS || memcmp(d->reserved, zero, 7) ||
	    (base.state >= EA_CLEANUP && d->count)) return -EINVAL;
	memcpy(base.magic, "S3EA0001", 8);
	if (ea_validate(&base, sizeof(base))) return -EINVAL;
	for (i = 0; i < ER_ATTEMPTS; i++) {
		if (i >= d->count) {
			if (memcmp(&d->holds[i], zero, sizeof(d->holds[i]))) return -EINVAL;
			continue;
		}
		if (!memcmp(d->holds[i].attempt, zero, 32) ||
		    !memcmp(d->holds[i].commit, zero, 32)) return -EINVAL;
		for (j = 0; j < i; j++)
			if (!memcmp(d->holds[i].attempt, d->holds[j].attempt, 32)) return -EINVAL;
	}
	return 0;
}
static inline int er_reader_change(struct er_doc *d, size_t len,
				 enum ea_action action, const uint8_t reader[16])
{
	struct ea_doc base;
	int rc = er_validate(d, len);
	if (rc) return rc;
	if (len == sizeof(struct ea_doc)) return ea_change(&d->base, action, reader);
	if ((action == EA_BEGIN_CLEANUP || action == EA_FINISH) && d->count) return -EBUSY;
	base = d->base;
	memcpy(base.magic, "S3EA0001", 8);
	rc = ea_change(&base, action, reader);
	if (!rc) { memcpy(base.magic, "S3EA0002", 8); d->base = base; }
	return rc;
}
static inline int er_attempt_change(struct er_doc *d, size_t len, bool acquire,
				  const uint8_t attempt[32], const uint8_t commit[32])
{
	static const uint8_t zero[32];
	unsigned i;
	int rc = er_validate(d, len);
	if (rc) return rc;
	if (!attempt || !commit || !memcmp(attempt, zero, 32) || !memcmp(commit, zero, 32)) return -EINVAL;
	if (acquire && d->base.state != EA_OPEN) return -EBUSY;
	if (len == sizeof(struct ea_doc)) {
		/* Removal cannot create a new format or pretend a legacy hold exists. */
		if (!acquire) return -ENOENT;
		memset((uint8_t *)d + sizeof(d->base), 0, sizeof(*d) - sizeof(d->base));
		memcpy(d->base.magic, "S3EA0002", 8);
	}
	for (i = 0; i < d->count; i++)
		if (!memcmp(d->holds[i].attempt, attempt, 32)) break;
	if (i < d->count && memcmp(d->holds[i].commit, commit, 32)) return -EPERM;
	if (acquire) {
		if (i < d->count) return 0;
		if (d->count == ER_ATTEMPTS) return -ENOSPC;
		memcpy(d->holds[d->count].attempt, attempt, 32);
		memcpy(d->holds[d->count++].commit, commit, 32);
	} else if (i < d->count) {
		memmove(&d->holds[i], &d->holds[i + 1], (d->count - i - 1) * sizeof(d->holds[0]));
		memset(&d->holds[--d->count], 0, sizeof(d->holds[0]));
	}
	return 0;
}
#endif
