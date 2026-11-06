/* SPDX-License-Identifier: Apache-2.0 */
#ifndef EXPORT_ADMISSION_CTL_H
#define EXPORT_ADMISSION_CTL_H

/* Fixed-width admission document. No native integer encoding or padding on wire.
 * All mutations use one ETag CAS and a fresh nonce, including reader removal.
 * No lease, TTL, conditional DELETE, or list/read-count inference is involved.
 * A missing/invalid document is never reconstructed by an importer/releaser.
 */
#include <errno.h>
#include <stdint.h>
#include <string.h>

#define EA_READERS 64
#define EA_OPEN 1
#define EA_CLOSED 2
#define EA_CLEANUP 3
#define EA_DONE 4
struct ea_doc {
	uint8_t magic[8];
	uint8_t nonce[16];
	uint8_t export_id[16];
	uint8_t owner_id[16];
	char prefix[128];
	uint8_t chunks_be[8];
	uint8_t state;
	uint8_t count;
	uint8_t reserved[6];
	uint8_t readers[EA_READERS][16];
};

static inline uint64_t ea_chunks(const struct ea_doc *d)
{
	uint64_t n = 0;
	unsigned i;
	for (i = 0; i < 8; i++) n = (n << 8) | d->chunks_be[i];
	return n;
}

static inline int ea_validate(const struct ea_doc *d, size_t len)
{
	unsigned i, j;
	static const uint8_t zero[16];
	if (len != sizeof(*d) || memcmp(d->magic, "S3EA0001", 8) ||
	    d->state < EA_OPEN || d->state > EA_DONE || d->count > EA_READERS ||
	    !memchr(d->prefix, 0, sizeof(d->prefix)) || !d->prefix[0] ||
	    !ea_chunks(d) || !memcmp(d->nonce, zero, 16) ||
	    !memcmp(d->export_id, zero, 16) || !memcmp(d->owner_id, zero, 16) ||
	    memcmp(d->reserved, zero, sizeof(d->reserved)) ||
	    (d->state >= EA_CLEANUP && d->count)) return -EINVAL;
	for (i = 0; i < EA_READERS; i++) {
		if (i >= d->count) {
			if (memcmp(d->readers[i], zero, 16)) return -EINVAL;
			continue;
		}
		if (!memcmp(d->readers[i], zero, 16)) return -EINVAL;
		for (j = 0; j < i; j++)
			if (!memcmp(d->readers[i], d->readers[j], 16)) return -EINVAL;
	}
	return 0;
}

enum ea_action { EA_ADD, EA_DROP, EA_CLOSE, EA_BEGIN_CLEANUP, EA_FINISH };
static inline int ea_change(struct ea_doc *d, enum ea_action action,
			    const uint8_t reader[16])
{
	unsigned i;
	static const uint8_t zero[16];
	if (ea_validate(d, sizeof(*d))) return -EINVAL;
	if (action == EA_CLOSE) {
		if (d->state == EA_OPEN) d->state = EA_CLOSED;
		return 0;
	}
	if (action == EA_BEGIN_CLEANUP) {
		if (d->state == EA_OPEN || d->count) return -EBUSY;
		if (d->state == EA_CLOSED) d->state = EA_CLEANUP;
		return 0;
	}
	if (action == EA_FINISH) {
		if (d->state < EA_CLEANUP || d->count) return -EBUSY;
		d->state = EA_DONE;
		return 0;
	}
	if (!reader || !memcmp(reader, zero, 16)) return -EINVAL;
	for (i = 0; i < d->count; i++)
		if (!memcmp(d->readers[i], reader, 16)) break;
	if (action == EA_ADD) {
		/* Even an existing reader cannot admit a new clone after closure. */
		if (d->state != EA_OPEN) return -EBUSY;
		if (i < d->count) return 0;
		if (d->count == EA_READERS) return -ENOSPC;
		memcpy(d->readers[d->count++], reader, 16);
		return 0;
	}
	if (action != EA_DROP) return -EINVAL;
	if (i < d->count) {
		memmove(d->readers[i], d->readers[i + 1], (d->count - i - 1) * 16);
		memset(d->readers[--d->count], 0, 16);
	}
	return 0;
}
#endif
