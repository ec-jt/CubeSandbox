/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_throttle -- the write-rate curve behind S3LVOL_WAL_MAX_DIRTY_GB
 *
 *   With S3 degraded the flusher cannot keep up and the WAL grows: 20 GB on
 *   app2, which then had to be replayed and flushed before anything else. The
 *   WAL's own backpressure (85% of 32 GiB) is a wall, not a slope: writes stop
 *   dead and guests see I/O timeouts.
 *
 *   Below half the cap nothing changes. Between half the cap and the cap the
 *   admitted write rate falls linearly from S3_THROTTLE_RATE_HI to
 *   S3_THROTTLE_RATE_LO; past the cap it stays at S3_THROTTLE_RATE_FLOOR.
 *   Writers slow down, they are never refused, so an fsync takes longer
 *   instead of timing out, and the backlog grows at most at the floor rate
 *   while S3 makes no progress at all.
 *
 *   Pure; no SPDK. The token bucket that applies it lives in s3_bs_dev.c.
 */
#ifndef S3LVOL_THROTTLE_H
#define S3LVOL_THROTTLE_H

#include <stdint.h>

#define S3_THROTTLE_DEFAULT_CAP_GB  8ULL
#define S3_THROTTLE_RATE_HI   (256ULL << 20)   /* bytes/s at half the cap */
#define S3_THROTTLE_RATE_LO   (16ULL << 20)    /* bytes/s at the cap */
#define S3_THROTTLE_RATE_FLOOR (2ULL << 20)    /* bytes/s past the cap */
#define S3_THROTTLE_UNLIMITED UINT64_MAX

/* Admitted bytes/s with \p backlog bytes unflushed and a cap of \p cap bytes
 * (0 = throttle off). */
static inline uint64_t
s3_throttle_rate(uint64_t backlog, uint64_t cap)
{
	uint64_t half;

	if (cap == 0) {
		return S3_THROTTLE_UNLIMITED;
	}
	half = cap / 2;
	if (backlog < half) {
		return S3_THROTTLE_UNLIMITED;
	}
	if (backlog >= cap) {
		return S3_THROTTLE_RATE_FLOOR;
	}
	return S3_THROTTLE_RATE_HI -
	       (S3_THROTTLE_RATE_HI - S3_THROTTLE_RATE_LO) * (backlog - half) /
	       (cap - half);
}

#endif /* S3LVOL_THROTTLE_H */
