/* SPDX-License-Identifier: Apache-2.0 */
#ifndef S3_PRODUCER_GATE_H
#define S3_PRODUCER_GATE_H

#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>

/* One atomic word linearizes admission against irreversible closure. A writer
 * owns its reference until its LAST completion callback returns, not merely
 * until the lower device has acknowledged a WAL append. No reopen operation.
 * The containing store must outlive every reference and every barrier callback.
 */
struct s3_producer_gate {
	_Atomic uint64_t state;
	_Atomic int error;
};
#define S3_PRODUCER_CLOSED (UINT64_C(1) << 63)

static inline int
s3_producer_enter(struct s3_producer_gate *g)
{
	uint64_t old = atomic_load(&g->state);
	for (;;) {
		if (old & S3_PRODUCER_CLOSED) return -ESHUTDOWN;
		if (old == S3_PRODUCER_CLOSED - 1) return -EOVERFLOW;
		if (atomic_compare_exchange_weak(&g->state, &old, old + 1)) return 0;
	}
}

static inline void
s3_producer_leave(struct s3_producer_gate *g, int status)
{
	int zero = 0;
	if (status) atomic_compare_exchange_strong(&g->error, &zero, status);
	uint64_t old = atomic_fetch_sub(&g->state, 1);
	assert((old & ~S3_PRODUCER_CLOSED) != 0);
}

static inline void
s3_producer_close(struct s3_producer_gate *g)
{
	atomic_fetch_or(&g->state, S3_PRODUCER_CLOSED);
}

static inline bool
s3_producer_closed(const struct s3_producer_gate *g)
{
	return (atomic_load(&g->state) & S3_PRODUCER_CLOSED) != 0;
}

static inline uint64_t
s3_producer_count(const struct s3_producer_gate *g)
{
	return atomic_load(&g->state) & ~S3_PRODUCER_CLOSED;
}
#endif
