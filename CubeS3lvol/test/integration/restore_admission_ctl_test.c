/* SPDX-License-Identifier: Apache-2.0 */
#include <assert.h>
#include <stdio.h>
#include "restore_admission_ctl.h"
int main(void)
{
	struct er_doc d = {0}, saved;
	uint8_t a[32] = {1}, b[32] = {2}, key[32] = {3}, bad[32] = {4};
	uint8_t reader[16] = {1};
	memcpy(d.base.magic, "S3EA0001", 8);
	d.base.nonce[0] = 1; d.base.export_id[0] = 2; d.base.owner_id[0] = 3;
	strcpy(d.base.prefix, "source"); d.base.chunks_be[7] = 1; d.base.state = EA_OPEN;
	assert(sizeof(struct ea_doc) == 1224);
	assert(!ea_change(&d.base, EA_ADD, reader));
	saved = d;
	assert(!er_attempt_change(&d, sizeof(d.base), true, a, key));
	assert(!er_validate(&d, sizeof(d)) && d.base.count == 1 && d.count == 1);
	/* Both old full-object and truncated old-reader validation fail closed. */
	assert(ea_validate(&d.base, sizeof(d)) == -EINVAL);
	assert(ea_validate(&d.base, sizeof(d.base)) == -EINVAL);
	assert(!er_reader_change(&d, sizeof(d), EA_DROP, reader));
	assert(d.base.count == 0 && d.count == 1); /* Same bytes, separate namespaces. */
	assert(!er_attempt_change(&d, sizeof(d), true, b, key));
	assert(er_attempt_change(&d, sizeof(d), false, a, bad) == -EPERM);
	assert(!er_attempt_change(&d, sizeof(d), false, b, key));
	assert(d.count == 1 && !memcmp(d.holds[0].attempt, a, 32));
	assert(!er_reader_change(&d, sizeof(d), EA_CLOSE, NULL));
	assert(er_reader_change(&d, sizeof(d), EA_BEGIN_CLEANUP, NULL) == -EBUSY);
	assert(er_attempt_change(&d, sizeof(d), true, a, key) == -EBUSY);
	assert(!er_attempt_change(&d, sizeof(d), false, a, key));
	assert(!er_reader_change(&d, sizeof(d), EA_BEGIN_CLEANUP, NULL));
	assert(er_attempt_change(&d, sizeof(d), true, a, key) == -EBUSY);
	assert(!er_reader_change(&d, sizeof(d), EA_FINISH, NULL));
	assert(er_attempt_change(&d, sizeof(d), true, a, key) == -EBUSY);
	/* Opposite CAS order: close wins before the v1 -> v2 upgrade. */
	assert(!ea_change(&saved.base, EA_CLOSE, NULL));
	assert(er_attempt_change(&saved, sizeof(saved.base), true, a, key) == -EBUSY);
	assert(!memcmp(saved.base.magic, "S3EA0001", 8) && saved.base.count == 1);
	puts("PASS: v1 preservation, old writer refusal, typed attempts, close ordering, cleanup exclusion");
	return 0;
}
