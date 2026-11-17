/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Per-chunk retry policy for the export copy engine (s3_export.c).
 *
 * Before this, one failed chunk PUT failed the whole export. CRT already
 * retries inside a request, but the per-request deadline (B16) caps the total,
 * so a chunk that met a few stalled connections in a row ("failure to meet
 * throughput minimum" against Hetzner Object Storage) came back -ETIMEDOUT.
 * Live, 2026-11-17: a 592-cluster template memory export failed three times
 * in a row on 1-5 of its chunks, and every failure left a publication_pending
 * registry entry that an operator had to release by hand.
 *
 * A chunk key is written once per export uuid and never with other bytes (a
 * re-export is a new uuid), and the slot keeps its buffer until the chunk
 * completes, so sending the same PUT again is safe: it is the same reasoning
 * that already allows the PUT to be hedged.
 *
 * Only transient transport errors are retried. Anything else (a refused
 * credential, a missing bucket, ENOMEM) still fails the export at once.
 */
#ifndef S3LVOL_EXPORT_CHUNK_RETRY_H
#define S3LVOL_EXPORT_CHUNK_RETRY_H

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>

/* Total PUT submissions per chunk, the first one included. */
#define EXPORT_CHUNK_MAX_ATTEMPTS 4

static inline bool
export_chunk_status_retryable(int status)
{
	switch (status) {
	case -ETIMEDOUT:
	case -ECANCELED:
	case -ECONNRESET:
	case -ECONNABORTED:
	case -ECONNREFUSED:
	case -EPIPE:
	case -EAGAIN:
	case -EIO:
		return true;
	default:
		return false;
	}
}

/* attempts: PUTs already submitted for this chunk (>= 1 when called).
 * export_failed: true once another chunk has failed the export for good; a
 * retry is then pointless, the export ends anyway. */
static inline bool
export_chunk_should_retry(int status, uint32_t attempts, bool export_failed)
{
	if (status == 0 || export_failed) {
		return false;
	}
	if (attempts >= EXPORT_CHUNK_MAX_ATTEMPTS) {
		return false;
	}
	return export_chunk_status_retryable(status);
}

#endif /* S3LVOL_EXPORT_CHUNK_RETRY_H */
