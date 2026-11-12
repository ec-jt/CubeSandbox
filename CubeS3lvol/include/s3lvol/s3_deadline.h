/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_deadline -- a total time limit per S3 request (B16)
 *
 *   The CRT client has a connect timeout, a throughput monitor and TCP keepalive,
 *   but no bound on how long one meta request may take in total. Its own retry
 *   loop (up to 5 retries, each allowed to stall for the full throughput window)
 *   let a single 1 MiB flush PUT run for minutes against a struggling endpoint,
 *   and everything waiting on it (a drain, an export) waited with it.
 *
 *   This module is the watchdog: every request registers an entry with a
 *   deadline, a single background thread notices entries that passed it and
 *   cancels their target. It knows nothing about CRT; the target is opaque and
 *   reached only through the three ops below, which is what lets the ownership
 *   rules be unit tested with a fake.
 *
 *   === Ownership ===
 *
 *   An entry has two holders from s3_deadline_new(): the submitter and the
 *   completion. The submitter gives its hold up in s3_deadline_arm() (or
 *   s3_deadline_abandon() if the submit failed), the completion in
 *   s3_deadline_finish(). The entry is freed when both have.
 *
 *   The target may only be referenced by the watchdog between arm() and
 *   finish(). arm() takes its own reference (ops->acquire) unless finish() has
 *   already run; finish() drops it. A cancel always happens on a reference the
 *   watchdog acquired under the lock and releases after the call, so the target
 *   cannot be freed underneath it, and cancel can never be issued after finish()
 *   returned.
 *
 *   The completion of the request is NOT driven from here: cancelling only asks
 *   the target to finish early, and its own finish path still runs exactly once.
 *   finish() reports whether the deadline fired, so that path can report
 *   -ETIMEDOUT instead of whatever "cancelled" maps to.
 */

#ifndef S3LVOL_DEADLINE_H
#define S3LVOL_DEADLINE_H

#include <stdbool.h>
#include <stdint.h>

struct s3_deadline;

struct s3_deadline_ops {
	void (*acquire)(void *target);
	void (*release)(void *target);
	void (*cancel)(void *target);
};

/**
 * Register a request with a deadline \p timeout_ms from now. 0 means no
 * deadline, in which case NULL is returned and every other call accepts NULL as
 * a no-op. NULL is also returned on allocation failure; a request without a
 * deadline is the pre-B16 behaviour, not an error.
 */
struct s3_deadline *s3_deadline_new(uint64_t timeout_ms);

/**
 * Hand the watchdog the target to cancel. Gives up the submitter's hold. If the
 * deadline already passed, the cancel is issued from here. If finish() already
 * ran, nothing is acquired.
 */
void s3_deadline_arm(struct s3_deadline *d, void *target,
		     const struct s3_deadline_ops *ops);

/** The submit failed: no completion will ever run. Drops both holds. */
void s3_deadline_abandon(struct s3_deadline *d);

/**
 * The request completed. Stops the watchdog from touching the target, drops the
 * completion's hold, and returns whether the deadline fired (and a cancel was
 * or will no longer be issued). Call it before the target can be freed.
 */
bool s3_deadline_finish(struct s3_deadline *d);

/** Entries currently registered. Diagnostics and tests. */
uint64_t s3_deadline_pending(void);

/** Total deadlines that fired since start. */
uint64_t s3_deadline_fired_total(void);

/**
 * Stop the watchdog thread. Entries still registered are not cancelled; it is
 * restarted by the next s3_deadline_new(). For process teardown and tests.
 */
void s3_deadline_shutdown(void);

#endif /* S3LVOL_DEADLINE_H */
