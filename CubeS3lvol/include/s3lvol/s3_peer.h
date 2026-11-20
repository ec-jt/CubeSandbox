/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_peer -- peer pull of export packs over the private network
 *
 *   === Why ===
 *
 *   A cross-node resume reads every pack of the paused sandbox's exports from
 *   Hetzner Object Storage. About 1% of those GETs take 3-57 s or abort
 *   (crt=2073), so a resume app2 -> app1 stalled for up to 128 s (2026-11-18).
 *   In most cross-node resumes the source node is healthy and wrote those very
 *   bytes minutes earlier. Reading them from it over the 10 Gbit vSwitch
 *   (6.6 Gbit/s measured with iperf3, 4 streams) avoids the S3 tail entirely.
 *
 *   === What is served ===
 *
 *   Only immutable, published pack objects. When the pack writer has PUT a
 *   pack to S3 it also writes the same bytes to
 *     <S3LVOL_PEER_DIR>/<export uuid>/pack-<n>
 *   (atomically: temp file + rename). Nothing else on the node is reachable:
 *   not the lvstore, not the snapshot, not the chunk map. A file exists only
 *   for an export this node published, it never changes, and it is deleted when
 *   the export is released (or evicted under S3LVOL_PEER_CACHE_MB). A request
 *   for anything else is a 404 and the importer reads S3.
 *
 *   === Integrity ===
 *
 *   The pack writer records the SHA-256 of every pack in the manifest
 *   (`pack_sha256`, version 6, only written while S3LVOL_PEER_PULL is on, so a
 *   node that cannot parse it is never handed one during a rollout). The
 *   importer verifies every byte it gets from a peer against that digest on the
 *   peer worker thread, before the reactor sees it. A mismatch is logged and
 *   that pack is read from S3. A pack without a digest is never pulled from a
 *   peer.
 *
 *   === Transport and authentication ===
 *
 *   One TCP connection per pack, to an address bound on the private interface
 *   only (S3LVOL_PEER_LISTEN, an explicit IP; a wildcard is refused). The
 *   request is a fixed 128-byte record authenticated with HMAC-SHA256 under a
 *   shared secret (S3LVOL_PEER_SECRET_FILE) and a timestamp window of 300 s.
 *   The response is the raw pack bytes; their authenticity is the manifest
 *   digest, not the transport. Replaying a request returns the same immutable
 *   bytes, so no nonce state is kept.
 *
 *   === Configuration (environment) ===
 *
 *     S3LVOL_PEER_PULL         0      1 enables digests on publish and pulling
 *     S3LVOL_PEER_LISTEN       unset  <ip>:<port> to serve on (private IP)
 *     S3LVOL_PEER_SECRET_FILE  /etc/cube/s3lvol-peer.key
 *     S3LVOL_PEER_DIR          /data/s3lvol-peer
 *     S3LVOL_PEER_CACHE_MB     16384  local pack copies kept, oldest evicted
 *     S3LVOL_PEER_PORT         17310  port used when a peer has none
 *     S3LVOL_PEER_CONNECT_MS   3000   connect timeout (above the 1 s first SYN
 *                                     retransmit: a dropped first SYN after idle
 *                                     failed every connect of a batch at 1000)
 *     S3LVOL_PEER_IO_MS        10000  whole-request timeout after connect
 *     S3LVOL_PEER_HEDGE_MS     2000   S3 is started if the peer has not
 *                                     answered by then (the peer may still win)
 *     S3LVOL_PEER_THREADS      8      client worker threads
 */

#ifndef S3LVOL_PEER_H
#define S3LVOL_PEER_H

#include "spdk/stdinc.h"

#define S3_PEER_SHA256_LEN     32
#define S3_PEER_DEFAULT_PORT   17310
#define S3_PEER_ADDR_MAX       64

/* Response status on the wire, and the client's mapping of it. */
enum s3_peer_status {
	S3_PEER_OK        = 0,
	S3_PEER_NOT_FOUND = 1,   /* -> -ENOENT */
	S3_PEER_AUTH      = 2,   /* -> -EACCES */
	S3_PEER_BAD       = 3,   /* -> -EINVAL */
	S3_PEER_BUSY      = 4,   /* -> -EBUSY */
};

/** S3LVOL_PEER_PULL is on. Read once. */
bool s3_peer_pull_enabled(void);

/** S3LVOL_PEER_LISTEN is set: this node keeps local pack copies. Read once. */
bool s3_peer_serve_enabled(void);

/* ==========================================================================
 * Source side
 * ========================================================================== */

/**
 * Start the server if S3LVOL_PEER_LISTEN is set. No-op otherwise. Must run on
 * a thread whose affinity the server may inherit (the spawner).
 * \return 0, or a negative errno (the caller logs and continues without it).
 */
int s3_peer_server_start(void);

/** Stop accepting. Connections in progress finish on their own. */
void s3_peer_server_stop(void);

/** Port the server is bound to (tests bind port 0). 0 when not running. */
uint16_t s3_peer_server_port(void);

/**
 * Keep a local copy of one published pack. Blocking file I/O: never call on a
 * reactor. Writes a temp file and renames it into place, then evicts the
 * oldest copies above S3LVOL_PEER_CACHE_MB.
 */
int s3_peer_store_pack(const char *uuid_str, uint32_t pack, const void *buf, size_t len);

/** Delete the local copies of an export's packs (release). Blocking, small. */
void s3_peer_drop_export(const char *uuid_str);

/**
 * Pin an export's local pack copies (peer_ready): its directory gets a
 * `.pinned` marker and the cache eviction never deletes anything in it, so the
 * packs stay servable while they exist nowhere else. Creates the directory.
 * Blocking, small. \return 0 or a negative errno (also -ENOTSUP when this node
 * does not serve peers, which is the caller's cue not to rely on the peer).
 */
int s3_peer_pin_export(const char *uuid_str);

/** Drop the pin (every pack is durable in S3). The copies stay as ordinary,
 * evictable cache entries. Blocking, small. */
void s3_peer_unpin_export(const char *uuid_str);

/** Whether the export's directory is pinned. */
bool s3_peer_export_pinned(const char *uuid_str);

/**
 * Read one local pack copy into \p buf (\p len bytes exactly). Blocking file
 * I/O: never call on a reactor. \return 0, -ENOENT when there is no copy or it
 * has another length, or a negative errno.
 */
int s3_peer_load_pack(const char *uuid_str, uint32_t pack, void *buf, size_t len);

/** Size of one local pack copy. \return 0, or -ENOENT when there is none. */
int s3_peer_pack_size(const char *uuid_str, uint32_t pack, size_t *out);

/* ==========================================================================
 * Importer side
 * ========================================================================== */

struct s3_peer_req;

/** Completion, on the SPDK thread that submitted. status 0: buf holds len
 * bytes whose SHA-256 matched. */
typedef void (*s3_peer_cb)(void *cb_arg, int status);

/**
 * Fetch pack \p pack of export \p uuid_str (\p len bytes) from \p peer
 * ("ip" or "ip:port") into \p buf, verifying \p sha256. Asynchronous; runs on
 * a peer worker thread and completes on the calling SPDK thread.
 *
 * \p buf must stay valid until the callback.
 *
 * \return the request (for s3_peer_abandon), or NULL with errno set when it
 * could not be queued (the callback is then never called).
 */
struct s3_peer_req *s3_peer_get(const char *peer, const char *uuid_str, uint32_t pack,
				uint64_t len, const uint8_t sha256[S3_PEER_SHA256_LEN],
				void *buf, s3_peer_cb cb, void *cb_arg);

/** The result is no longer wanted: stop early if possible. The callback still
 * runs (with -ECANCELED unless it already finished), and \p buf must still
 * stay valid until it does. */
void s3_peer_abandon(struct s3_peer_req *req);

/** Hedge deadline in microseconds (S3LVOL_PEER_HEDGE_MS). */
uint64_t s3_peer_hedge_us(void);

/** Drop cached configuration (tests change the environment between cases). */
void s3_peer_config_reload(void);

/** SHA-256 helper used on both sides. */
void s3_peer_sha256(const void *buf, size_t len, uint8_t out[S3_PEER_SHA256_LEN]);

#endif /* S3LVOL_PEER_H */
