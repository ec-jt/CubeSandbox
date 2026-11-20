/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   peer_ready: upload an export's pinned local packs to S3 in the background
 *
 *   Kept apart from the pack writer so that a user of this alone (the peer
 *   tests) does not link the walk and everything it needs.
 */

#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/thread.h"

#include "s3lvol/s3_client.h"
#include "s3lvol/s3_export.h"
#include "s3lvol/s3_hedge.h"
#include "s3lvol/s3_peer.h"
#include "s3lvol/s3_spawner.h"

static uint64_t
pack_now_ms(void)
{
	return spdk_get_ticks() * 1000 / spdk_get_ticks_hz();
}

/* ==========================================================================
 * peer_ready: background upload of pinned local packs
 *
 * One pack at a time per export, a few exports at a time across the node (the
 * S3 slot budget is shared with the flusher). Each pack is read from the pinned
 * local copy and checked against the manifest digest on a plain thread, then
 * PUT hedged. A failed PUT is retried with exponential backoff capped at 30 s,
 * forever: the snapshot cannot be released until this finishes, and giving up
 * would leave an export readable only while the source node lives.
 * ========================================================================== */

struct packs_upload {
	struct s3_client   *client;
	char                prefix[S3_EXPORT_PREFIX_MAX];
	char                uuid[SPDK_UUID_STRING_LEN];
	uint32_t            count;
	uint32_t            next;
	uint8_t            *sha;
	const bool         *cancel;
	void              (*cb)(void *cb_arg, int status);
	void               *cb_arg;
	struct spdk_thread *thread;
	struct spdk_poller *retry_poller;
	uint8_t            *buf;
	size_t              len;
	struct iovec        iov;
	char                key[S3_EXPORT_KEY_MAX];
	int                 load_rc;
	uint32_t            attempts;
	uint64_t            retries;
	uint64_t            start_ms;
};

static void packs_upload_next(struct packs_upload *u);
static void packs_upload_loaded(void *arg);

static void
packs_upload_end(struct packs_upload *u, int status)
{
	if (status == 0) {
		SPDK_NOTICELOG("Export %s: %u pinned pack(s) uploaded to S3 in %" PRIu64
			       " ms (%" PRIu64 " PUT retr(ies))\n", u->uuid, u->count,
			       pack_now_ms() - u->start_ms, u->retries);
	} else {
		SPDK_WARNLOG("Export %s: background pack upload stopped at pack %u of "
			     "%u: %s\n", u->uuid, u->next, u->count,
			     spdk_strerror(-status));
	}
	u->cb(u->cb_arg, status);
	free(u->buf);
	free(u->sha);
	s3_client_put(u->client);
	free(u);
}

static uint64_t
packs_upload_backoff_us(uint32_t attempts)
{
	uint64_t ms = 500;

	while (attempts-- > 1 && ms < 30000) {
		ms *= 2;
	}
	return (ms > 30000 ? 30000 : ms) * 1000;
}

static int packs_upload_put(struct packs_upload *u);

static int
packs_upload_retry_fire(void *arg)
{
	struct packs_upload *u = arg;

	spdk_poller_unregister(&u->retry_poller);
	if (u->cancel && *u->cancel) {
		packs_upload_end(u, -ECANCELED);
		return SPDK_POLLER_BUSY;
	}
	if (packs_upload_put(u) != 0) {
		/* Could not even submit: try again later rather than give up. */
		u->retry_poller = SPDK_POLLER_REGISTER(packs_upload_retry_fire, u,
						       packs_upload_backoff_us(++u->attempts));
	}
	return SPDK_POLLER_BUSY;
}

static void
packs_upload_put_done(void *cb_arg, int status)
{
	struct packs_upload *u = cb_arg;

	if (status == 0) {
		free(u->buf);
		u->buf = NULL;
		u->next++;
		packs_upload_next(u);
		return;
	}
	u->retries++;
	if (u->attempts == 1 || u->attempts % 10 == 0) {
		SPDK_WARNLOG("Export %s: PUT of pinned pack %u failed (%s), attempt %u; "
			     "retrying\n", u->uuid, u->next, spdk_strerror(-status),
			     u->attempts);
	}
	if (u->cancel && *u->cancel) {
		packs_upload_end(u, -ECANCELED);
		return;
	}
	u->retry_poller = SPDK_POLLER_REGISTER(packs_upload_retry_fire, u,
					       packs_upload_backoff_us(++u->attempts));
	if (!u->retry_poller) {
		packs_upload_end(u, -ENOMEM);
	}
}

static int
packs_upload_put(struct packs_upload *u)
{
	u->iov.iov_base = u->buf;
	u->iov.iov_len = u->len;
	return s3_put_hedged(u->client, u->key, &u->iov, 1, NULL,
			     packs_upload_put_done, u);
}

static void
packs_upload_loaded(void *arg)
{
	struct packs_upload *u = arg;

	if (u->load_rc != 0) {
		SPDK_ERRLOG("Export %s: pinned pack %u is missing or does not match its "
			    "digest locally (%s); it cannot be uploaded\n", u->uuid,
			    u->next, spdk_strerror(-u->load_rc));
		packs_upload_end(u, u->load_rc);
		return;
	}
	u->attempts = 1;
	if (packs_upload_put(u) != 0) {
		u->retry_poller = SPDK_POLLER_REGISTER(packs_upload_retry_fire, u,
						       packs_upload_backoff_us(++u->attempts));
		if (!u->retry_poller) {
			packs_upload_end(u, -ENOMEM);
		}
	}
}

/* Read the pinned local copy of the next pack and check it against the
 * manifest digest. Blocking: runs on a plain thread when there is one. */
static void
packs_upload_load(struct packs_upload *u)
{
	uint8_t digest[32];
	size_t len = 0;

	u->load_rc = s3_peer_pack_size(u->uuid, u->next, &len);
	if (u->load_rc != 0) {
		return;
	}
	u->len = len;
	u->buf = malloc(len);
	if (!u->buf) {
		u->load_rc = -ENOMEM;
		return;
	}
	u->load_rc = s3_peer_load_pack(u->uuid, u->next, u->buf, len);
	if (u->load_rc == 0) {
		s3_peer_sha256(u->buf, len, digest);
		if (memcmp(digest, u->sha + (size_t)u->next * 32, 32) != 0) {
			u->load_rc = -EBADMSG;
		}
	}
}

static void *
packs_upload_load_thread(void *arg)
{
	struct packs_upload *u = arg;

	pthread_detach(pthread_self());
	packs_upload_load(u);
	if (spdk_thread_send_msg(u->thread, packs_upload_loaded, u) != 0) {
		SPDK_ERRLOG("Export %s: upload lost its thread\n", u->uuid);
	}
	return NULL;
}

static void
packs_upload_load_spawn_failed(void *ctx, int err)
{
	struct packs_upload *u = ctx;

	(void)err;
	packs_upload_load(u);
	packs_upload_loaded(u);
}

static void
packs_upload_next(struct packs_upload *u)
{
	if (u->cancel && *u->cancel) {
		packs_upload_end(u, -ECANCELED);
		return;
	}
	if (u->next >= u->count) {
		packs_upload_end(u, 0);
		return;
	}
	s3_export_pack_key(u->prefix, u->uuid, u->next, u->key, sizeof(u->key));
	if (s3_spawner_is_started() &&
	    s3_spawner_pthread_create_async(packs_upload_load_thread, u,
					    packs_upload_load_spawn_failed, u) == 0) {
		return;
	}
	/* Tests, or no spawner: load inline. Correct, only slower. */
	packs_upload_load(u);
	packs_upload_loaded(u);
}

int
s3_export_packs_upload(struct s3_client *client, const char *prefix,
		       const char *uuid_str, uint32_t count, const uint8_t *sha,
		       const bool *cancel, void (*cb)(void *cb_arg, int status),
		       void *cb_arg)
{
	struct packs_upload *u;

	if (!client || !prefix || !uuid_str || !sha || !cb || count == 0) {
		return -EINVAL;
	}
	u = calloc(1, sizeof(*u));
	if (!u) {
		return -ENOMEM;
	}
	u->sha = malloc((size_t)count * 32);
	if (!u->sha) {
		free(u);
		return -ENOMEM;
	}
	memcpy(u->sha, sha, (size_t)count * 32);
	s3_client_get(client);
	u->client = client;
	snprintf(u->prefix, sizeof(u->prefix), "%s", prefix);
	snprintf(u->uuid, sizeof(u->uuid), "%s", uuid_str);
	u->count = count;
	u->cancel = cancel;
	u->cb = cb;
	u->cb_arg = cb_arg;
	u->thread = spdk_get_thread();
	u->start_ms = pack_now_ms();
	SPDK_NOTICELOG("Export %s: uploading %u pinned pack(s) to S3 in the "
		       "background\n", uuid_str, count);
	packs_upload_next(u);
	return 0;
}
