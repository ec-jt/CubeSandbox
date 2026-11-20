/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   Zero-copy export: describe a snapshot without reading it
 *
 *   === What this is ===
 *
 *   The whole of a cross-node handoff on the sending side, once the volume has
 *   been snapshotted and drained. It walks the snapshot's clone chain, turns each
 *   allocated cluster into the object the chunk map already has it in, and uploads
 *   one manifest naming them. No data is read; nothing is copied.
 *
 *   That is the difference between a handoff measured in round trips and one
 *   measured in gigabytes. The dense engine next door (s3_export.c) reads the
 *   snapshot and re-uploads it, which is the only option across buckets or
 *   regions, and is also what a ref export degrades into when the source
 *   eventually wants the snapshot back (see materialise, module side).
 *
 *   === The three-step translation, and where each step comes from ===
 *
 *     blob offset -> device LBA      spdk_blob_get_io_unit_lba(), which is the
 *                                    patch in patches/. Nothing else in the
 *                                    public API answers it.
 *     device LBA  -> chunk index     arithmetic, s3_lba_to_chunk_index()
 *     chunk index -> object uuid     the chunk map
 *
 *   The first step is why this file needs a patched SPDK. Reading blobstore's
 *   private cluster table would work too, right up until a field is added to
 *   struct spdk_blob and every LBA comes out wrong -- naming, with no error
 *   anywhere, objects that belong to another volume.
 *
 *   === Why a chain and not a blob ===
 *
 *   spdk_blob_get_next_allocated_io_unit() and spdk_blob_get_io_unit_lba() both
 *   read blob->active.clusters and stop there; neither follows a parent. That is
 *   exactly what this needs, but it means one blob is never the whole picture:
 *   taking a second snapshot of a volume hands the new snapshot only the clusters
 *   written since the first, because blobstore moves the cluster map across and
 *   leaves the older data in the older snapshot. So `lvol -> snap1 -> snap2`, the
 *   ordinary way to use this, produces a snap2 that owns almost nothing.
 *
 *   Hence the layers are walked nearest first, and a chunk already named is never
 *   revisited. Every layer's clusters live in the same bs_dev address space and
 *   resolve through the same chunk map, so a parent's object is not somebody
 *   else's -- it is in the same <prefix>/data/ as everything else.
 *
 *   === What the caller has to have done ===
 *
 *   Snapshotted, so the clusters cannot move; and drained, so every one of them
 *   has a committed mapping. A cluster blobstore has allocated whose data is
 *   still in the WAL has no object yet, and this refuses to continue rather than
 *   quietly leaving it out of the bitmap -- a chunk left out reads as zeroes on
 *   the importing node, which is data loss that announces itself nowhere.
 *
 *   Allocated does not imply an object, though, and the difference is not an
 *   error. write_zeroes on this device drops the mapping and deletes the object,
 *   since an unmapped chunk already reads back as zeroes -- so a cluster that
 *   was written and then zeroed stays allocated in blobstore while having
 *   nothing in S3. Filesystems do this in bulk: ext4's lazy inode table init
 *   zeroes megabytes of a freshly made volume, and every one of those clusters
 *   arrives here allocated and unmapped. Refusing them, which is what this used
 *   to do, turned an ordinary sparse snapshot into a full copy.
 *
 *   is_zeroes() separates the two, because it consults the overlay as well as
 *   the chunk map. A hole is left out of the present bitmap -- which is how
 *   the importer spells "reads as zeroes" -- and marked resolved so inherit()
 *   and older layers cannot put a parent object back. Anything else still
 *   stops the walk.
 */

#include "spdk/stdinc.h"
#include "spdk/blob.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/uuid.h"

#include "spdk/env.h"

#include "s3lvol/s3_bs_dev.h"
#include "s3lvol/s3_chunk_map.h"
#include "s3lvol/s3_export.h"
#include "s3lvol/s3_hedge.h"
#include "spdk/thread.h"
#include "s3lvol/s3_peer.h"
#include "s3lvol/s3_spawner.h"
#include "export_chunk_retry.h"

/* ==========================================================================
 * Pack writer (S3LVOL_EXPORT_PACKS, version 6)
 *
 * A template delta names its own chunks as live 1 MiB objects, and an importer
 * fetches each of them with its own GET: on Hetzner that is a few hundred
 * requests per volume, each one exposed to the slow tail. The pack writer
 * copies the delta's present chunks, in ascending order, into pack objects of
 * pack_chunks slots each (16 MiB by default) and publishes the manifest as
 * version 6 with a table mapping every chunk to (pack, slot). The importer then
 * reads slots as ranged GETs of a few large objects.
 *
 * Order is the contract, as for the dense engine: the manifest is published only
 * once every pack is durable. If any pack fails for good the packs that landed
 * are deleted and the plain version 5 manifest is published instead -- it is
 * still correct, because the refs it names stay alive with the snapshot.
 *
 * Bytes come from reading the exported snapshot through blobstore, which is what
 * an importer of the version 5 manifest would have read anyway: each present
 * chunk is owned by a layer above the template base, so the read resolves to it.
 * A slot is always chunk_size bytes; a partially written chunk carries zeroes
 * past its valid end, and the reader asks only for the valid part.
 * ========================================================================== */

#define PACK_INFLIGHT_DEFAULT 4
#define PACK_INFLIGHT_MAX     16

struct pack_writer;

struct pack_job {
	struct pack_writer *w;
	uint32_t            pack;
	uint32_t            nslots;
	uint32_t            reads_pending;
	uint32_t            put_attempts;
	int                 read_status;
	uint8_t            *buf;
	struct iovec        iov;
	char                key[S3_EXPORT_KEY_MAX];
	/* Peer pull: the thread to come back to after hashing off-reactor. */
	struct spdk_thread *thread;
	char                uuid[SPDK_UUID_STRING_LEN];
	int                 store_rc;
};

struct pack_writer {
	struct s3_export_manifest *m;
	struct s3_client          *client;
	struct spdk_blob          *blob;
	struct spdk_io_channel    *channel;
	s3_export_cb               cb;
	void                      *cb_arg;
	uint64_t                  *chunks;      /* present chunks, ascending */
	uint64_t                   nchunks;
	uint32_t                   blocks_per_chunk;
	uint32_t                   next_pack;
	uint32_t                   inflight;
	uint32_t                   max_inflight;
	uint32_t                   done_packs;
	uint64_t                   put_retries;
	uint64_t                   start_ms;
	int                        status;
	bool                       pumping;
	/* peer_ready (s3_export_run_packonly): packs are written to the pinned
	 * local peer cache only, never PUT here, and the manifest is published
	 * with packs_pending. A failure fails the export: there are no live refs
	 * to fall back to. */
	bool                       peer_ready;
};

static uint64_t
pack_now_ms(void)
{
	return spdk_get_ticks() * 1000 / spdk_get_ticks_hz();
}

static uint32_t
pack_inflight_from_env(void)
{
	const char *v = getenv("S3LVOL_EXPORT_PACK_INFLIGHT");
	unsigned long n;
	char *end = NULL;

	if (!v || !*v) {
		return PACK_INFLIGHT_DEFAULT;
	}
	n = strtoul(v, &end, 10);
	if (!end || *end != '\0' || n == 0) {
		return PACK_INFLIGHT_DEFAULT;
	}
	return n > PACK_INFLIGHT_MAX ? PACK_INFLIGHT_MAX : (uint32_t)n;
}

static void pack_pump(struct pack_writer *w);

static void
pack_writer_free(struct pack_writer *w)
{
	free(w->chunks);
	free(w);
}

static void
pack_publish_fallback_deleted(void *cb_arg, int status)
{
	struct pack_writer *w = cb_arg;
	int rc;

	(void)status;
	/* The packs are gone (or their delete is logged); publish the plain delta,
	 * which the live refs keep valid. */
	s3_export_manifest_clear_packs(w->m);
	rc = s3_export_manifest_publish(w->client, w->m, w->cb, w->cb_arg);
	if (rc != 0) {
		w->cb(w->cb_arg, NULL, rc);
	}
	s3_export_manifest_unref(w->m);
	pack_writer_free(w);
}

static void
pack_finish(struct pack_writer *w)
{
	int rc;

	if (w->peer_ready) {
		if (w->status != 0) {
			SPDK_ERRLOG("Export %s: peer_ready packing failed (%s) after %u of "
				    "%u pack(s); the export fails\n", w->m->uuid_str,
				    spdk_strerror(-w->status), w->done_packs, w->m->pack_count);
			w->cb(w->cb_arg, NULL, w->status);
			s3_export_manifest_unref(w->m);
			pack_writer_free(w);
			return;
		}
		w->m->packs_pending = true;
		s3_export_manifest_seal(w->m);
		SPDK_NOTICELOG("Export %s: %" PRIu64 " own chunk(s) in %u pinned local "
			       "pack(s) in %" PRIu64 " ms; publishing as peer_ready\n",
			       w->m->uuid_str, w->nchunks, w->m->pack_count,
			       pack_now_ms() - w->start_ms);
		rc = s3_export_manifest_publish(w->client, w->m, w->cb, w->cb_arg);
		if (rc != 0) {
			w->cb(w->cb_arg, NULL, rc);
		}
		s3_export_manifest_unref(w->m);
		pack_writer_free(w);
		return;
	}
	if (w->status != 0) {
		SPDK_WARNLOG("Export %s: packing failed (%s) after %u of %u pack(s); "
			     "publishing the unpacked delta instead\n", w->m->uuid_str,
			     spdk_strerror(-w->status), w->done_packs, w->m->pack_count);
		rc = s3_export_packs_delete(w->client, w->m->src.prefix, w->m->uuid_str,
					    w->m->pack_count, pack_publish_fallback_deleted, w);
		if (rc != 0) {
			pack_publish_fallback_deleted(w, rc);
		}
		return;
	}
	SPDK_NOTICELOG("Export %s: %" PRIu64 " own chunk(s) packed into %u pack(s) of "
		       "%u slot(s) in %" PRIu64 " ms (%" PRIu64 " PUT retr(ies))\n",
		       w->m->uuid_str, w->nchunks, w->m->pack_count, w->m->pack_chunks,
		       pack_now_ms() - w->start_ms, w->put_retries);
	rc = s3_export_manifest_publish(w->client, w->m, w->cb, w->cb_arg);
	if (rc != 0) {
		w->cb(w->cb_arg, NULL, rc);
	}
	s3_export_manifest_unref(w->m);
	pack_writer_free(w);
}

static void
pack_job_end(struct pack_job *j, int status)
{
	struct pack_writer *w = j->w;

	if (status != 0 && w->status == 0) {
		w->status = status;
	}
	if (status == 0) {
		w->done_packs++;
	}
	spdk_free(j->buf);
	free(j);
	w->inflight--;
	pack_pump(w);
}

static void pack_job_uploaded(void *cb_arg, int status);

static int
pack_job_put(struct pack_job *j)
{
	j->put_attempts++;
	/* Hedged and retried: a pack key is written once per export uuid with
	 * fixed bytes, and the buffer lives until the job ends. */
	return s3_put_hedged(j->w->client, j->key, &j->iov, 1, NULL,
			     pack_job_uploaded, j);
}

static void
pack_job_uploaded(void *cb_arg, int status)
{
	struct pack_job *j = cb_arg;
	struct pack_writer *w = j->w;

	if (export_chunk_should_retry(status, j->put_attempts, w->status != 0)) {
		SPDK_WARNLOG("Export %s pack %u PUT attempt %u failed: %s; retrying\n",
			     w->m->uuid_str, j->pack, j->put_attempts,
			     spdk_strerror(-status));
		w->put_retries++;
		if (pack_job_put(j) == 0) {
			return;
		}
	}
	if (status != 0) {
		SPDK_ERRLOG("Export %s: pack %u ('%s') failed: %s\n", w->m->uuid_str,
			    j->pack, j->key, spdk_strerror(-status));
	}
	pack_job_end(j, status);
}

/* Back on the writer's thread after the off-reactor hash: PUT the pack. */
static void
pack_job_hashed(void *arg)
{
	struct pack_job *j = arg;
	int rc;

	if (j->w->peer_ready) {
		/* The pinned local copy is the only copy until the background
		 * upload finishes: failing to write it fails the export. */
		if (j->store_rc != 0) {
			SPDK_ERRLOG("Export %s: pack %u could not be kept locally: %s\n",
				    j->uuid, j->pack, spdk_strerror(-j->store_rc));
		}
		pack_job_end(j, j->store_rc);
		return;
	}
	if (j->store_rc != 0) {
		/* Only the peer copy failed: S3 is still the source of truth and
		 * the digest is still right, so an importer just misses the peer. */
		SPDK_WARNLOG("Export %s: no local peer copy of pack %u: %s\n",
			     j->uuid, j->pack, spdk_strerror(-j->store_rc));
	}
	rc = pack_job_put(j);
	if (rc != 0) {
		pack_job_end(j, rc);
	}
}

/* Plain thread: hash the pack into the manifest's digest table and keep the
 * local copy the peer server serves. Neither may run on a reactor (16 MiB of
 * SHA-256 and a 16 MiB file write). The digest slot is this job's alone. */
static void *
pack_job_hash_thread(void *arg)
{
	struct pack_job *j = arg;

	pthread_detach(pthread_self());
	s3_peer_sha256(j->buf, j->iov.iov_len, j->w->m->pack_sha + (size_t)j->pack * 32);
	j->store_rc = s3_peer_store_pack(j->uuid, j->pack, j->buf, j->iov.iov_len);
	if (spdk_thread_send_msg(j->thread, pack_job_hashed, j) != 0) {
		/* Cannot happen while the writer's thread is alive, and it is:
		 * it is waiting for this job. */
		SPDK_ERRLOG("Export %s: pack %u lost its thread\n", j->uuid, j->pack);
	}
	return NULL;
}

static void
pack_job_hash_spawn_failed(void *ctx, int err)
{
	struct pack_job *j = ctx;

	/* Hash inline instead: slower for the reactor, never wrong. */
	SPDK_WARNLOG("Export %s: pack %u hashed inline (no thread: %s)\n", j->uuid,
		     j->pack, spdk_strerror(err));
	s3_peer_sha256(j->buf, j->iov.iov_len, j->w->m->pack_sha + (size_t)j->pack * 32);
	j->store_rc = -err;
	if (spdk_thread_send_msg(j->thread, pack_job_hashed, j) != 0) {
		SPDK_ERRLOG("Export %s: pack %u lost its thread\n", j->uuid, j->pack);
	}
}

static void
pack_job_read_done(void *cb_arg, int bserrno)
{
	struct pack_job *j = cb_arg;
	int rc;

	if (bserrno != 0 && j->read_status == 0) {
		j->read_status = bserrno;
	}
	if (--j->reads_pending > 0) {
		return;
	}
	if (j->read_status != 0) {
		SPDK_ERRLOG("Export %s: reading pack %u failed: %s\n", j->w->m->uuid_str,
			    j->pack, spdk_strerror(-j->read_status));
		pack_job_end(j, j->read_status);
		return;
	}
	j->iov.iov_base = j->buf;
	j->iov.iov_len = (size_t)j->nslots * j->w->m->chunk_size;
	if (j->w->m->pack_sha) {
		j->thread = spdk_get_thread();
		snprintf(j->uuid, sizeof(j->uuid), "%s", j->w->m->uuid_str);
		rc = s3_spawner_is_started() ?
		     s3_spawner_pthread_create_async(pack_job_hash_thread, j,
						     pack_job_hash_spawn_failed, j) :
		     -ENOTSUP;
		if (rc == 0) {
			return;
		}
		/* No spawner (tests) or a failed submit: hash here. */
		s3_peer_sha256(j->buf, j->iov.iov_len,
			       j->w->m->pack_sha + (size_t)j->pack * 32);
		j->store_rc = s3_peer_store_pack(j->uuid, j->pack, j->buf, j->iov.iov_len);
		pack_job_hashed(j);
		return;
	}
	rc = pack_job_put(j);
	if (rc != 0) {
		pack_job_end(j, rc);
	}
}

static int
pack_job_start(struct pack_writer *w, uint32_t pack)
{
	struct pack_job *j;
	uint64_t first = (uint64_t)pack * w->m->pack_chunks;
	uint32_t s;

	j = calloc(1, sizeof(*j));
	if (!j) {
		return -ENOMEM;
	}
	j->w = w;
	j->pack = pack;
	j->nslots = (uint32_t)spdk_min((uint64_t)w->m->pack_chunks, w->nchunks - first);
	j->buf = spdk_zmalloc((size_t)j->nslots * w->m->chunk_size, S3LVOL_BLOCK_SIZE,
			      NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	if (!j->buf) {
		free(j);
		return -ENOMEM;
	}
	s3_export_pack_key(w->m->src.prefix, w->m->uuid_str, pack, j->key, sizeof(j->key));
	w->inflight++;
	/* One self reference so a read that completes inline cannot end the job
	 * before every read is submitted. */
	j->reads_pending = 1;
	for (s = 0; s < j->nslots; s++) {
		uint64_t c = w->chunks[first + s];

		j->reads_pending++;
		spdk_blob_io_read(w->blob, w->channel,
				  j->buf + (size_t)s * w->m->chunk_size,
				  c * w->blocks_per_chunk, w->blocks_per_chunk,
				  pack_job_read_done, j);
	}
	pack_job_read_done(j, 0);
	return 0;
}

static void
pack_pump(struct pack_writer *w)
{
	if (w->pumping) {
		return;
	}
	w->pumping = true;
	while (w->status == 0 && w->next_pack < w->m->pack_count &&
	       w->inflight < w->max_inflight) {
		int rc = pack_job_start(w, w->next_pack);

		if (rc != 0) {
			w->status = rc;
			break;
		}
		w->next_pack++;
	}
	w->pumping = false;
	if (w->inflight == 0 &&
	    (w->status != 0 || w->next_pack == w->m->pack_count)) {
		pack_finish(w);
	}
}

/* Takes its own reference on m. 0 means the callback will run. */
static int
pack_writer_run_mode(const struct s3_export_ref_opts *opts, struct s3_export_manifest *m,
		     s3_export_cb cb, void *cb_arg, bool peer_ready);

static int
pack_writer_run(const struct s3_export_ref_opts *opts, struct s3_export_manifest *m,
		s3_export_cb cb, void *cb_arg)
{
	return pack_writer_run_mode(opts, m, cb, cb_arg, false);
}

static int
pack_writer_run_mode(const struct s3_export_ref_opts *opts, struct s3_export_manifest *m,
		     s3_export_cb cb, void *cb_arg, bool peer_ready)
{
	struct pack_writer *w;
	uint64_t i, n = 0;
	int rc;

	w = calloc(1, sizeof(*w));
	if (!w) {
		return -ENOMEM;
	}
	w->chunks = calloc(m->present_chunks ? m->present_chunks : 1, sizeof(*w->chunks));
	if (!w->chunks) {
		free(w);
		return -ENOMEM;
	}
	for (i = 0; i < m->num_chunks; i++) {
		if (s3_export_manifest_is_present(m, i)) {
			w->chunks[n++] = i;
		}
	}
	w->nchunks = n;
	rc = s3_export_manifest_set_packs(m, opts->pack_chunks);
	if (rc != 0) {
		pack_writer_free(w);
		return rc;
	}
	if (peer_ready || s3_peer_pull_enabled()) {
		/* Filled by each pack job before its PUT; the manifest carrying
		 * it is published only after every pack is durable. */
		m->pack_sha = calloc(m->pack_count, 32);
		if (!m->pack_sha) {
			s3_export_manifest_clear_packs(m);
			pack_writer_free(w);
			return -ENOMEM;
		}
	}
	s3_export_manifest_seal(m);
	w->m = m;
	s3_export_manifest_ref(m);
	w->client = opts->client;
	w->blob = opts->pack_blob;
	w->channel = opts->pack_channel;
	w->cb = cb;
	w->cb_arg = cb_arg;
	w->blocks_per_chunk = m->chunk_size / S3LVOL_BLOCK_SIZE;
	w->max_inflight = pack_inflight_from_env();
	w->start_ms = pack_now_ms();
	w->peer_ready = peer_ready;
	SPDK_NOTICELOG("Export %s: packing %" PRIu64 " own chunk(s) into %u pack(s) of "
		       "%u MiB, %u in flight\n", m->uuid_str, n, m->pack_count,
		       (uint32_t)(((uint64_t)m->pack_chunks * m->chunk_size) >> 20),
		       w->max_inflight);
	pack_pump(w);
	return 0;
}

struct ref_walk {
	struct s3_export_manifest *m;
	struct s3_chunk_map       *map;

	/* Needed for is_zeroes(): a cluster with no mapping is either a hole or
	 * data that has not been flushed, and only the device can tell which. */
	struct spdk_bs_dev        *bs_dev;

	uint32_t                   chunk_shift;
	uint64_t                   io_units_per_chunk;

	/* Of the snapshot, i.e. of chain[0]. Ancestors are never larger -- a volume
	 * can only grow -- but they can be smaller, and a layer's own length is what
	 * bounds its walk. This bounds the manifest. */
	uint64_t                   num_io_units;

	uint64_t                   named;
	uint64_t                   bytes_named;

	/* Clusters blobstore has allocated that hold no data at all. Counted
	 * rather than recorded as present: the published bitmap still says
	 * "not present" for them, which is what the importer needs. They are
	 * marked resolved so inherit cannot restore a parent object. */
	uint64_t                   holes;
};

/* One layer. Returns the number of chunks it contributed, or negative on error.
 *
 * Chunks already named, or already settled as local zeroes, are skipped rather
 * than overwritten: the layers arrive nearest first, so whoever got there first
 * is the closest layer that has the cluster, which is the one blobstore would
 * reach by reading through the chain.
 */
static int64_t
walk_layer(struct ref_walk *w, struct spdk_blob *blob, const char *uuid_str,
	   uint32_t depth)
{
	uint64_t layer_io_units = spdk_blob_get_num_io_units(blob);
	uint64_t offset = 0;
	int64_t contributed = 0;
	int rc;

	/* A layer longer than the snapshot would be describing chunks the manifest
	 * has no room for. Cannot happen from a resize -- ancestors are the older,
	 * smaller versions -- but the loop below indexes the ref table with it. */
	if (layer_io_units > w->num_io_units) {
		layer_io_units = w->num_io_units;
	}

	while (offset < layer_io_units) {
		struct spdk_uuid uuid;
		uint32_t valid_bytes = 0;
		uint64_t lba;
		uint64_t device_chunk;
		uint64_t export_chunk;

		offset = spdk_blob_get_next_allocated_io_unit(blob, offset);
		if (offset == UINT64_MAX || offset >= layer_io_units) {
			break;
		}

		export_chunk = offset / w->io_units_per_chunk;

		if (s3_export_manifest_is_present(w->m, export_chunk) ||
		    s3_export_manifest_is_resolved(w->m, export_chunk)) {
			/* A nearer layer already owns this chunk, or settled it as
			 * zeroes. */
			offset += w->io_units_per_chunk;
			continue;
		}

		lba = spdk_blob_get_io_unit_lba(blob, offset);
		if (lba == UINT64_MAX) {
			/* Allocated a moment ago according to the query above. Only a
			 * concurrent modification explains this, which means the blob is
			 * not the read-only snapshot it was supposed to be. */
			SPDK_ERRLOG("Export %s: cluster at io unit %" PRIu64 " of layer "
				    "%u went away mid-walk. Is the blob really "
				    "read-only?\n", uuid_str, offset, depth);
			return -EAGAIN;
		}

		device_chunk = s3_lba_to_chunk_index(lba, w->chunk_shift);

		rc = s3_chunk_map_lookup(w->map, device_chunk, &uuid, &valid_bytes);
		if (rc != 0) {
			/* No object holds this cluster. Two very different things look
			 * identical here, and treating them alike is what made this walk
			 * refuse whole snapshots it could have described:
			 *
			 *   a hole -- blobstore allocated the cluster and something then
			 *   zeroed it. write_zeroes on this device drops the mapping and
			 *   deletes the object, because an unmapped chunk reads back as
			 *   zeroes, so "allocated" and "has an object" stop agreeing. A
			 *   filesystem does this in bulk: ext4's lazy inode table init
			 *   zeroes megabytes of a freshly made volume.
			 *
			 *   data that is not in S3 yet -- still in the WAL or the overlay.
			 *   Naming no object for it would hand the importer zeroes in
			 *   place of real data, silently, which is the one outcome that
			 *   must not happen.
			 *
			 * is_zeroes() is the device's own answer to exactly this question
			 * -- it checks the overlay as well as the chunk map -- so it is
			 * asked rather than reimplemented. A hole is left absent from
			 * the present bitmap, which is how the importer spells "reads
			 * as zeroes", and marked resolved so inherit() cannot restore
			 * a parent object over the zero.
			 */
			if (w->bs_dev->is_zeroes(w->bs_dev, lba,
						 w->io_units_per_chunk)) {
				s3_export_manifest_set_resolved(w->m, export_chunk);
				w->holes++;
				offset += w->io_units_per_chunk;
				continue;
			}

			/* Not a hole: the caller is expected to drain and walk again, and
			 * normally does. It is only fatal if it happens a second time,
			 * which the caller reports. */
			SPDK_NOTICELOG("Export %s: chunk %" PRIu64 " of layer %u (device "
				       "chunk %" PRIu64 ", LBA %" PRIu64 ") has no committed "
				       "mapping yet (%s); the lvstore needs a drain\n",
				       uuid_str, export_chunk, depth, device_chunk, lba,
				       spdk_strerror(-rc));
			return -EIO;
		}

		rc = s3_export_manifest_set_ref(w->m, export_chunk, &uuid, valid_bytes);
		if (rc != 0) {
			SPDK_ERRLOG("Export %s: chunk %" PRIu64 " could not be recorded: "
				    "%s\n", uuid_str, export_chunk, spdk_strerror(-rc));
			return rc;
		}

		w->named++;
		w->bytes_named += valid_bytes;
		contributed++;
		offset += w->io_units_per_chunk;
	}

	return contributed;
}

/* ==========================================================================
 * peer_ready: pack-only export without a drain
 *
 * The ref walk needs every cluster's object to be durable in S3, which is what
 * the drain before it waits for: on a busy node that is the whole lvstore's
 * WAL backlog, measured at 130 s (app1 2026-11-20). A template delta does not
 * need refs at all when it is packed -- an importer reads every own chunk from
 * a pack -- so this walk names chunks by allocation alone, reads their bytes
 * through blobstore (which serves the WAL and the overlay too), and writes the
 * packs to the pinned local peer cache. The manifest goes out with
 * packs_pending: readable from the source peer at once, from S3 once the
 * caller's background upload has PUT every pack and republished it.
 *
 * The refs carry no object (nil uuid, full chunk). They are never read: a
 * version 6 manifest is always read from its packs, and this one is never
 * downgraded to version 5.
 * ========================================================================== */

int
s3_export_run_packonly(const struct s3_export_ref_opts *opts, s3_export_cb cb, void *cb_arg)
{
	struct s3_export_manifest *m = NULL;
	struct spdk_uuid nil = {0};
	uint64_t num_io_units, io_units_per_chunk, size_bytes, n = 0;
	uint32_t chunk_size, i;
	int rc;

	if (!opts || !opts->chain || opts->chain_len == 0 || !opts->client ||
	    !opts->uuid_str || !opts->base_uuid || opts->base_uuid[0] == '\0' ||
	    !opts->pack_blob || !opts->pack_channel || opts->pack_chunks == 0 ||
	    opts->parent) {
		return -EINVAL;
	}
	for (i = 0; i < opts->chain_len; i++) {
		if (!opts->chain[i] || spdk_blob_is_esnap_clone(opts->chain[i])) {
			return -EINVAL;
		}
	}
	chunk_size = opts->cluster_size;
	if (chunk_size < S3LVOL_BLOCK_SIZE || (chunk_size & (chunk_size - 1)) != 0) {
		return -EINVAL;
	}
	io_units_per_chunk = chunk_size / S3LVOL_BLOCK_SIZE;
	num_io_units = spdk_blob_get_num_io_units(opts->chain[0]);
	size_bytes = num_io_units * S3LVOL_BLOCK_SIZE;

	rc = s3_export_manifest_create(opts->uuid_str, size_bytes, chunk_size,
				       S3_EXPORT_LAYOUT_REF, &m);
	if (rc != 0) {
		return rc;
	}
	m->src = opts->src;
	m->cluster_size = opts->cluster_size;
	m->created_at = (uint64_t)time(NULL);
	m->expires_at = opts->expires_at;
	m->generation = opts->generation;

	/* Nearest layer first is irrelevant here: a chunk is present when any
	 * layer above the base allocated it, and its bytes are read through the
	 * snapshot, which resolves the layers itself. */
	for (i = 0; i < opts->chain_len; i++) {
		uint64_t layer = spdk_blob_get_num_io_units(opts->chain[i]);
		uint64_t off = 0;

		if (layer > num_io_units) {
			layer = num_io_units;
		}
		while (off < layer) {
			uint64_t c;

			off = spdk_blob_get_next_allocated_io_unit(opts->chain[i], off);
			if (off == UINT64_MAX || off >= layer) {
				break;
			}
			c = off / io_units_per_chunk;
			if (!s3_export_manifest_is_present(m, c)) {
				rc = s3_export_manifest_set_ref(m, c, &nil, chunk_size);
				if (rc != 0) {
					goto err;
				}
				n++;
			}
			off = (c + 1) * io_units_per_chunk;
		}
	}
	if (n == 0) {
		/* Nothing of its own: an empty pack table does not parse. The
		 * caller takes the ordinary path. */
		rc = -ENOTSUP;
		goto err;
	}
	rc = s3_export_manifest_set_base(m, opts->base_uuid);
	if (rc != 0) {
		goto err;
	}
	s3_export_manifest_seal(m);
	SPDK_NOTICELOG("Export %s is a template delta over base %s: %" PRIu64 " own "
		       "chunk(s) by allocation, no drain (peer_ready)\n", opts->uuid_str,
		       opts->base_uuid, n);
	rc = pack_writer_run_mode(opts, m, cb, cb_arg, true);
	if (rc != 0) {
		goto err;
	}
	s3_export_manifest_unref(m);
	return 0;
err:
	s3_export_manifest_unref(m);
	return rc;
}

int
s3_export_run_ref(const struct s3_export_ref_opts *opts, s3_export_cb cb, void *cb_arg)
{
	struct s3_export_manifest *m = NULL;
	struct ref_walk w = {0};
	uint64_t size_bytes;
	uint64_t inherited = 0;
	uint32_t chunk_size;
	uint32_t i;
	int64_t contributed;
	int rc;

	if (!opts || !opts->bs_dev || !opts->chain || opts->chain_len == 0 ||
	    !opts->client || !opts->prefix || !opts->uuid_str) {
		return -EINVAL;
	}
	for (i = 0; i < opts->chain_len; i++) {
		if (!opts->chain[i]) {
			return -EINVAL;
		}
	}

	if (opts->base_uuid && opts->base_uuid[0] != '\0' && opts->parent) {
		/* A delta names one base and nothing else; inheriting another
		 * export's chunks on top would need two kinds of "absent". */
		SPDK_ERRLOG("Export %s: a template delta cannot also inherit from an "
			    "export\n", opts->uuid_str);
		return -EINVAL;
	}

	/* An esnap chain with no parent manifest would produce a manifest that is
	 * short exactly where the export it reads through holds data, and short in a
	 * manifest means "reads as zeroes". Refused here as well as at the caller,
	 * because the caller is the one place that knows and this is the one place
	 * that can prove it. */
	if (spdk_blob_is_esnap_clone(opts->chain[opts->chain_len - 1]) &&
	    !opts->parent) {
		SPDK_ERRLOG("Export %s: the chain ends in an external snapshot but no "
			    "parent manifest was given, so the chunks it inherits could "
			    "not be named\n", opts->uuid_str);
		return -EINVAL;
	}

	if (opts->parent) {
		rc = s3_export_manifest_inheritable(opts->parent, &opts->src,
						    opts->uuid_str);
		if (rc != 0) {
			return rc;
		}
	}

	w.map = s3_bs_dev_get_chunk_map(opts->bs_dev);
	if (!w.map) {
		SPDK_ERRLOG("The source device has no chunk map, so there is nothing to "
			    "reference\n");
		return -EINVAL;
	}

	/* Asked per unmapped cluster, to tell a hole from data that is still local.
	 * A device without it cannot be walked safely: every unmapped cluster would
	 * have to be assumed to hold data, which is the old behaviour and refuses
	 * snapshots that are merely sparse.
	 *
	 * Not an error, and deliberately not logged as one: -ENOTSUP routes the
	 * caller to the copying engine, which needs none of this and produces a
	 * correct export. s3_bs_dev always provides is_zeroes, so this is here for
	 * some future device rather than for anything that exists. */
	if (!opts->bs_dev->is_zeroes) {
		SPDK_NOTICELOG("The source device cannot report zeroed ranges, so a "
			       "hole cannot be told from unflushed data. Exporting by "
			       "copying instead.\n");
		return -ENOTSUP;
	}
	w.bs_dev = opts->bs_dev;

	chunk_size = s3_bs_dev_get_chunk_size(opts->bs_dev);
	if (chunk_size == 0) {
		return -EINVAL;
	}

	/* One blob cluster has to be exactly one chunk map entry, which holds when
	 * the two sizes agree: clusters are allocated at cluster granularity from
	 * the start of the device, so cluster N begins at N * cluster_size, which is
	 * chunk N. Without that equality a cluster would span several chunks or
	 * share one, and neither the bitmap nor the ref table can express it. The
	 * caller falls back to the copying engine, which references nothing and so
	 * does not care. */
	if (opts->cluster_size != chunk_size) {
		SPDK_ERRLOG("A zero-copy export needs cluster_size == chunk_size, but "
			    "the lvstore has %u and %u. Export by copying instead.\n",
			    opts->cluster_size, chunk_size);
		return -ENOTSUP;
	}

	/* Chunk indices are only comparable between two manifests of the same chunk
	 * size: index i means a different byte range under each. Inheriting across a
	 * change of chunk size would need the ranges re-cut, which is reading and
	 * rewriting the data -- exactly what the copy engine does. */
	if (opts->parent && opts->parent->chunk_size != chunk_size) {
		SPDK_NOTICELOG("Export %s: parent %s has a chunk size of %u against "
			       "this lvstore's %u, so their chunk indices do not line "
			       "up. Exporting by copying instead.\n", opts->uuid_str,
			       opts->parent->uuid_str, opts->parent->chunk_size,
			       chunk_size);
		return -ENOTSUP;
	}

	w.chunk_shift = spdk_u32log2(chunk_size);
	w.io_units_per_chunk = chunk_size / S3LVOL_BLOCK_SIZE;
	w.num_io_units = spdk_blob_get_num_io_units(opts->chain[0]);
	size_bytes = w.num_io_units * S3LVOL_BLOCK_SIZE;

	rc = s3_export_manifest_create(opts->uuid_str, size_bytes, chunk_size,
				       S3_EXPORT_LAYOUT_REF, &m);
	if (rc != 0) {
		return rc;
	}
	w.m = m;

	m->src = opts->src;
	m->cluster_size = opts->cluster_size;
	m->created_at = (uint64_t)time(NULL);
	m->expires_at = opts->expires_at;
	m->generation = opts->generation;

	/* The walk. Every step is memory: blobstore is asked which cluster is next,
	 * not to read it. */
	for (i = 0; i < opts->chain_len; i++) {
		contributed = walk_layer(&w, opts->chain[i], opts->uuid_str, i);
		if (contributed < 0) {
			rc = (int)contributed;
			goto err;
		}
		SPDK_DEBUGLOG(s3lvol_export, "Export %s: layer %u named %" PRId64
			      " chunk(s)\n", opts->uuid_str, i, contributed);
	}

	/* Then whatever the chain inherited from an export rather than from a blob.
	 * Last, so every local layer has already claimed what it owns -- a chunk
	 * rewritten since the import must resolve to the rewritten cluster, and a
	 * locally zeroed chunk must stay a hole. */
	if (opts->parent) {
		uint64_t bytes = 0;

		rc = s3_export_manifest_inherit(m, opts->parent, &inherited, &bytes);
		if (rc != 0) {
			/* Including -E2BIG, which leaves m holding the sources that did
			 * fit and the chunks recorded against them. Not repaired,
			 * because err drops the manifest -- publishing happens further
			 * down and only on the success path, so a half-filled one is
			 * never written. The caller copies instead. */
			goto err;
		}
		w.named += inherited;
		w.bytes_named += bytes;
		SPDK_DEBUGLOG(s3lvol_export, "Export %s: inherited %" PRIu64
			      " chunk(s) from export %s\n", opts->uuid_str, inherited,
			      opts->parent->uuid_str);
	}

	/* A template delta: everything the walk did not name reads from the base
	 * on the importing node, so the chunks it settled as zeroes have to be
	 * written down rather than left absent. */
	if (opts->base_uuid && opts->base_uuid[0] != '\0') {
		rc = s3_export_manifest_set_base(m, opts->base_uuid);
		if (rc != 0) {
			SPDK_ERRLOG("Export %s: cannot record template base %s: %s\n",
				    opts->uuid_str, opts->base_uuid, spdk_strerror(-rc));
			goto err;
		}
		SPDK_NOTICELOG("Export %s is a template delta over base %s: %" PRIu64
			       " own chunk(s), %" PRIu64 " explicit zero chunk(s)\n",
			       opts->uuid_str, opts->base_uuid, w.named, w.holes);
	}

	SPDK_NOTICELOG("Export %s references %" PRIu64 " object(s) holding %" PRIu64
		       " byte(s) of a %" PRIu64 "-byte snapshot across %u layer(s), "
		       "skipping %" PRIu64 " zeroed cluster(s); nothing was copied\n",
		       opts->uuid_str, w.named, w.bytes_named, size_bytes,
		       opts->chain_len, w.holes);

	/* Said separately because it is the part an operator cannot infer: these
	 * chunks are not under this lvstore's prefix, so this node's own snapshot
	 * being present is not enough to keep the export readable. */
	if (inherited) {
		SPDK_NOTICELOG("Export %s inherits %" PRIu64 " of those chunk(s) from "
			       "export %s, across %u prefix(es) in total\n",
			       opts->uuid_str, inherited, opts->parent->uuid_str,
			       m->num_srcs);
	}

	/* Packs: only for a delta with enough own chunks to be worth it, and only
	 * when the caller supplied a way to read the snapshot. The writer
	 * publishes when every pack is durable (or falls back to version 5). */
	if (opts->pack_chunks > 0 && opts->pack_blob && opts->pack_channel &&
	    s3_export_manifest_is_delta(m)) {
		s3_export_manifest_seal(m);
		if (m->present_chunks >= S3_EXPORT_PACK_MIN_CHUNKS) {
			rc = pack_writer_run(opts, m, cb, cb_arg);
			if (rc == 0) {
				s3_export_manifest_unref(m);
				return 0;
			}
			SPDK_WARNLOG("Export %s: could not start packing (%s); publishing "
				     "the unpacked delta\n", opts->uuid_str,
				     spdk_strerror(-rc));
			s3_export_manifest_clear_packs(m);
		} else {
			SPDK_NOTICELOG("Export %s: %" PRIu64 " own chunk(s) is below the "
				       "pack threshold of %u; publishing plain objects\n",
				       opts->uuid_str, m->present_chunks,
				       S3_EXPORT_PACK_MIN_CHUNKS);
		}
	}

	/* Hands its reference to the callback. */
	rc = s3_export_manifest_publish(opts->client, m, cb, cb_arg);
	if (rc != 0) {
		goto err;
	}

	s3_export_manifest_unref(m);
	return 0;
err:
	s3_export_manifest_unref(m);
	return rc;
}
