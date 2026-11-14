/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_hedge -- hedged S3 requests for immutable objects
 *
 *   === Why ===
 *
 *   Hetzner Object Storage (Ceph RGW) answers most requests in 0.1-1 s, but the
 *   slowest 1% take 3-57 s. A single stalled GET used to sink a whole cross-node
 *   restore: the CRT throughput monitor killed each attempt after about 11 s,
 *   CRT retried, often on an equally stuck path, and after five attempts the
 *   60 s request deadline cancelled it and the decouple failed (2026-11-14,
 *   chunk 157 of 626 of a memory volume).
 *
 *   A hedge is the standard answer to tail latency: when a request has not
 *   completed after hedge_ms, issue one duplicate of it. The first success
 *   wins; the loser is abandoned and its completion ignored. Duplicates are only
 *   ever issued for keys whose content cannot change -- the uuid-named chunk
 *   objects, which are create-once -- so either copy is equally good.
 *
 *   === The gate ===
 *
 *   A GET writes into the caller's buffer from CRT threads. Without a fence the
 *   loser could still be writing after the winner was delivered and the caller
 *   had reused the buffer. The gate is that fence:
 *
 *     - every body write of a gated request happens between
 *       s3_gate_write_begin() and s3_gate_write_end(), under the gate's lock;
 *     - s3_gate_abandon() takes the same lock, so once it returns that request
 *       never writes another byte. It also asks CRT to cancel the request, so a
 *       stalled loser does not hold a connection until its deadline.
 *
 *   The duplicate additionally reads into a private buffer, so the bytes in the
 *   caller's buffer only ever come from one request.
 *
 *   === Exactly once ===
 *
 *   Both attempts complete on the submitting SPDK thread (s3_client.h bounces
 *   every callback there), and the hedge timer is a poller on that same thread.
 *   Picking the winner is therefore single threaded: the first attempt to
 *   complete successfully delivers, and every later completion only cleans up.
 *   The context is freed when the last attempt has completed, never earlier,
 *   so a late loser callback never touches freed memory.
 *
 *   === Configuration (environment) ===
 *
 *     S3LVOL_S3_HEDGE               1     0 disables hedging entirely
 *     S3LVOL_S3_HEDGE_MIN_MS        1000  floor of the hedge delay
 *     S3LVOL_S3_HEDGE_MAX_INFLIGHT  16    duplicates in flight, process wide:
 *                                         25% of the default prefetch window
 *
 *   hedge_ms = max(S3LVOL_S3_HEDGE_MIN_MS, 3 x p50 of the last 64 successful
 *   requests of the same kind, GET or PUT). The cap is what stops a fully
 *   degraded endpoint from receiving double the load: once it is reached a slow
 *   request waits for its own completion, exactly as before hedging existed.
 *
 *   No aws-c-* types here (see s3_client.h).
 */

#ifndef S3LVOL_HEDGE_H
#define S3LVOL_HEDGE_H

#include "s3lvol/s3_client.h"
#include "s3lvol/s3_deadline.h"

/* The gate (struct s3_gate, s3_gate_*) is declared in s3_deadline.h. */

/* ==========================================================================
 * Gated requests (implemented in s3_client_aws.c)
 *
 * A gate is only legal when the request is submitted from an SPDK thread: its
 * owner frees the gate from the callback, and only a bounced callback is
 * guaranteed to run after the submit function has returned.
 * ========================================================================== */

/** s3_get_range() with an optional gate (NULL behaves exactly like it). */
int s3_get_range_gated(struct s3_client *client, const char *key,
		       uint64_t offset, uint64_t len, void *buf,
		       struct s3_gate *gate, s3_get_cb cb, void *cb_arg);

/** Unconditional s3_put() with an optional gate. Here the gate only cancels: a
 * PUT copies its body at submit, so there is no buffer to fence. */
int s3_put_gated(struct s3_client *client, const char *key,
		 struct iovec *iov, int iovcnt, struct s3_gate *gate,
		 s3_op_cb cb, void *cb_arg);

/* ==========================================================================
 * Hedged requests (implemented in s3_hedge.c)
 * ========================================================================== */

/* Per call-site counters, only touched on the submitting thread, and only
 * before the request's callback runs. */
struct s3_hedge_counters {
	uint64_t issued;   /* duplicates sent */
	uint64_t won;      /* duplicates that completed successfully first */
	uint64_t skipped;  /* a hedge was due but the in-flight cap was reached */
};

/**
 * Hedged read. Same contract as s3_get_range(); \p counters may be NULL. Only
 * for keys whose content never changes (uuid-named chunk objects).
 *
 * Falls back to a plain GET when hedging is disabled or the caller is not on
 * an SPDK thread (there is no poller to time the hedge).
 */
int s3_get_range_hedged(struct s3_client *client, const char *key,
			uint64_t offset, uint64_t len, void *buf,
			struct s3_hedge_counters *counters,
			s3_get_cb cb, void *cb_arg);

/**
 * Hedged unconditional PUT of immutable content. Same contract as s3_put()
 * with if_none_match=false, plus one: the bytes \p iov points at must stay
 * valid until \p cb runs, because the duplicate copies them when it is sent,
 * which is after this call returns. The iovec array itself is copied.
 *
 * Never use for a key that is ever written with different content (manifests,
 * registries, leases, owner markers): a delayed loser could land after a newer
 * write. See s3_hedge.c for the paths that are deliberately not hedged.
 */
int s3_put_hedged(struct s3_client *client, const char *key,
		  struct iovec *iov, int iovcnt,
		  struct s3_hedge_counters *counters,
		  s3_op_cb cb, void *cb_arg);

/** (Re)read the S3LVOL_S3_HEDGE* environment and clear the latency history.
 * Done lazily on first use; tests call it after setenv(). */
void s3_hedge_config_load(void);

/** The delay a hedge of this kind would wait right now. */
uint64_t s3_hedge_delay_ms(bool put);

struct s3_hedge_stats {
	bool     enabled;
	uint64_t get_issued;
	uint64_t get_won;
	uint64_t put_issued;
	uint64_t put_won;
	uint64_t skipped_cap;
	uint64_t inflight;
	uint64_t max_inflight;
	uint64_t get_delay_ms;
	uint64_t put_delay_ms;
};

void s3_hedge_get_stats(struct s3_hedge_stats *out);

#endif /* S3LVOL_HEDGE_H */
