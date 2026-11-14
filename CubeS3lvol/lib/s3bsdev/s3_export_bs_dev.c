/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_export_bs_dev -- the read-only bs_dev behind an imported export 
 *
 *   This is what makes an lvol on node B a clone of a snapshot on node A. It is
 *   handed to blobstore as an esnap parent: blobstore reads through it for any
 *   cluster the clone has not written yet, and copies out of it on the first
 *   write to such a cluster.
 *
 *   === What it is not ===
 *
 *   No WAL, no overlay, no flusher, no chunk map, no journal, no local device.
 *   The data it serves is already durable and immutable in S3, so all of that
 *   machinery has nothing to do. Reads are ranged GETs against object keys
 *   derived from the manifest, and everything on the write side reports -EROFS.
 *
 *   === Threading ===
 *
 *   Unlike s3_bs_dev, nothing here is bounced to an owner thread. There is no
 *   mutable shared state to protect: the manifest is immutable once parsed, and
 *   an I/O's aggregation state is touched only by the thread that submitted it
 *   (S3 completions are delivered back to the submitting thread, see
 *   s3_client.h). That matters because blobstore creates a channel per thread
 *   that touches the clone -- with nvmf that is one per poll group.
 *
 *   === Lifetime ===
 *
 *   destroy() is called by blobstore, when it decides the back device is no
 *   longer referenced. That is why both the manifest and the S3 client are held
 *   by reference here: the import RPC that created this is long gone, and a
 *   release_export may have happened in between.
 *
 *   === Whole-chunk fetches and prefetch (B19) ===
 *
 *   Where an imported volume's reads go, in order of time:
 *
 *     1. Before decouple has materialised a cluster, blobstore reads it through
 *        this device. This used to be one ranged GET per guest I/O, sized to the
 *        I/O (often 4 KiB) and never cached. On Ceph RGW a single small GET took
 *        6-28 s under load, so mounting an imported metadata disk timed out.
 *     2. Once decouple has materialised a cluster on the GET path, blobstore
 *        reads the importing lvol's own cluster. That goes through the lvstore
 *        bs_dev: overlay/WAL first (the materialise write lands there), then,
 *        after the flusher has uploaded it, s3_cache (the flusher populates it
 *        with the whole chunk), and only on a cache miss a GET of the lvol's own
 *        object.
 *     3. The CopyObject ingest variant of decouple makes the lvol own objects
 *        under its own prefix but writes nothing locally, so after it every
 *        read is still a GET (of the new objects). That is why an import that
 *        asks for prefetch skips ingest and uses the GET path (see
 *        vbdev_s3lvol_xfer.c, decouple).
 *
 *   This device therefore never serves guest I/O with sub-chunk GETs any more.
 *   Every present chunk is fetched as the whole object (valid_bytes for a REF
 *   chunk, chunk_size for a dense one) into a slot of a small table owned by
 *   home_thread, and every read of that chunk -- the one that missed and any
 *   that arrive while the GET is in flight -- is copied out of the same buffer.
 *   Reads arriving on other threads are bounced to home_thread for the lookup
 *   and the answer is sent back to the thread that issued the read.
 *
 *   The table has two partitions, so memory is bounded:
 *
 *     - EXPORT_MISS_SLOTS slots for demand misses. Filled ones stay as an LRU
 *       cache. When every one is in flight a miss gets a transient slot that is
 *       freed as soon as its waiters are served (bounded by the guest's queue
 *       depth), still a whole-object GET.
 *     - S3_EXPORT_PREFETCH_MAX slots for the prefetcher. A prefetched chunk stays
 *       *pinned* until its consumer releases it, so the prefetcher's window is
 *       exactly the number of whole-chunk buffers it holds:
 *       window x chunk_size bytes.
 *
 *   The prefetcher (s3_export_prefetcher_*) walks a list of present chunks in
 *   order with at most `window` fetched-but-not-released chunks at a time. The
 *   decouple that consumes it materialises clusters in the same order, so each
 *   materialise finds its chunk already fetched (or in flight) and the copy into
 *   the lvol's own clusters is local. Once decouple has finished, the volume no
 *   longer reads this device at all.
 *
 *   Holes and the REF tail are unchanged: a chunk absent from the manifest, or a
 *   byte range past a REF chunk's valid_bytes, is zero-filled without any I/O.
 */

#include "spdk/stdinc.h"
#include "spdk/blob.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"

#include "s3lvol/s3_chunk_map.h"
#include "s3lvol/s3_export.h"

/* Demand-miss partition of the slot table. See the file comment. */
#define EXPORT_MISS_SLOTS 16

enum export_slot_state {
	EXPORT_SLOT_FREE = 0,
	EXPORT_SLOT_FETCHING,
	EXPORT_SLOT_FILLED,
};

struct s3_export_chunk_io;
struct export_pf_wait;

/* One whole chunk object, fetched once and copied out to every reader. Owned by
 * home_thread: every field is touched only there. */
struct export_slot {
	struct s3_export_dev       *dev;
	enum export_slot_state      state;
	bool                        transient;
	bool                        prefetch_part;
	uint64_t                    chunk_index;
	char                        key[S3_EXPORT_KEY_MAX];
	uint32_t                    object_len;   /* bytes the GET asked for */
	uint8_t                    *buf;          /* chunk_size bytes, kept for reuse */
	uint32_t                    pins;         /* prefetcher references */
	uint64_t                    gen;          /* bumped on every reuse */
	uint64_t                    lru;
	TAILQ_HEAD(, s3_export_chunk_io) waiters;
	TAILQ_HEAD(, export_pf_wait)     pf_waiters;
};

struct s3_export_dev {
	/* Must be first: blobstore only ever holds &dev->bs_dev, and the cast
	 * back relies on the addresses being the same. */
	struct spdk_bs_dev         bs_dev;

	struct s3_client          *client;
	struct s3_export_manifest *m;

	s3_export_bs_dev_on_swap_fn on_swap;
	void                      *on_swap_arg;

	uint32_t                   chunk_shift;
	uint32_t                   blocks_per_chunk;

	/* io_device name, which spdk_io_device_register() keeps a pointer to. */
	char                       name[SPDK_UUID_STRING_LEN + 16];

	/* Where the refetch machinery runs, and the only thread that touches the
	 * three fields below.
	 *
	 * Everything else here is read-only after construction, which is what lets
	 * reads run on any thread with no serialisation. A refetch is the one thing
	 * that is not: several threads can hit 404 at the same moment on the same
	 * export, and they must produce one refetch between them rather than one
	 * each. Rather than lock, the state lives on one thread and the others send
	 * it a message -- the same shape s3_bs_dev uses for its owner thread, and
	 * the reason this device could avoid it everywhere else.
	 *
	 * The thread that created the device, since that is the one that will still
	 * be there: blobstore's channels come and go with the poll groups. */
	struct spdk_thread        *home_thread;
	bool                       refetch_in_flight;
	TAILQ_HEAD(, s3_export_io) refetch_waiters;

	/* Incremented from nvmf I/O threads as well as the home thread. */
	uint64_t                   reads;
	uint64_t                   bytes_read;
	uint64_t                   zero_fills;
	uint64_t                   refetches;

	/* Whole-chunk slot table, home_thread only. */
	struct export_slot         slots[EXPORT_MISS_SLOTS + S3_EXPORT_PREFETCH_MAX];
	uint64_t                   lru_clock;
	uint32_t                   fetch_inflight;
	bool                       destroy_pending;
	struct s3_export_bs_dev_stats stats;
};

/* One bs_dev read, possibly spanning several chunks. */
struct s3_export_io {
	struct s3_export_dev       *dev;
	struct s3_export_manifest  *m;
	struct spdk_bs_dev_cb_args *cb_args;

	uint32_t                    num_pending;
	int                         status;

	/* Stops the first sub-read from completing the whole I/O while the split
	 * loop is still adding to it. */
	bool                        submit_done;

	/* Enough to run the read again after the manifest has been replaced.
	 *
	 * The payload belongs to blobstore and stays valid until cb_fn is called,
	 * which is exactly what a retry postpones -- so holding it across the
	 * refetch is allowed by the same contract that makes the ordinary
	 * asynchronous completion legal. */
	void                       *payload;
	uint64_t                    lba;
	uint32_t                    lba_count;

	/* The thread this I/O was submitted on. The retry has to go back to it:
	 * cb_args->cb_fn is blobstore's, and blobstore expects it on the thread
	 * that issued the read. */
	struct spdk_thread         *origin;

	/* Set when a sub-read returned 404. It means the objects this manifest
	 * names are gone, which is what the source materialising the export looks
	 * like from here -- so the I/O waits for a newer manifest instead of
	 * failing. Once retried it is cleared, and a second 404 is a real one. */
	bool                        saw_missing;
	bool                        retried;

	TAILQ_ENTRY(s3_export_io)   waiter_link;
};

struct s3_export_chunk_io {
	struct s3_export_io *io;
	uint64_t             chunk_index;

	/* How many bytes this sub-read asked for. Kept so the completion can tell a
	 * short read from a complete one: the buffer it was given is only filled as
	 * far as the object store went, and the rest keeps whatever was there. */
	uint32_t             expected;

	/* Where in the chunk the read starts, where its bytes go, and how long the
	 * whole object is (valid_bytes for REF, chunk_size for dense). The fetch
	 * is always [0, object_len); the read copies its part out. */
	uint32_t             offset_in_chunk;
	uint8_t             *dst;
	uint32_t             object_len;

	/* Result carried back to the origin thread. */
	int                  result_status;
	uint64_t             result_bytes;

	TAILQ_ENTRY(s3_export_chunk_io) slot_link;

	char                 key[S3_EXPORT_KEY_MAX];
};

/* A prefetcher waiting for a slot it pinned to be filled. */
struct export_pf_wait {
	struct s3_export_prefetcher *p;
	uint64_t                     chunk_index;
	TAILQ_ENTRY(export_pf_wait)  link;
};

/* ==========================================================================
 * Key derivation
 *
 * The one place the two layouts differ on the read side. Everything else --
 * splitting, holes, channels, refusing writes -- is the same for both.
 * ========================================================================== */

static void
export_chunk_key(const struct s3_export_manifest *m, uint64_t chunk_index,
		 char *out, size_t out_len)
{
	const struct s3_export_ref *ref;

	if (m->layout == S3_EXPORT_LAYOUT_REF) {
		const char *prefix;

		/* A live object under whichever lvstore's prefix owns it. Built with the
		 * chunk map's own key function rather than a copy of the format, because
		 * reading a *different* key than the source writes would not fail -- it
		 * would either 404 or, worse, find something else.
		 *
		 * The prefix is per chunk, not per manifest: a snapshot taken on an
		 * imported volume owns some chunks and inherited the rest, so they live
		 * under different prefixes. chunk_prefix() answers src.prefix for every
		 * chunk of a single-source manifest, which is every version 2 one, so
		 * there is nothing to branch on here.
		 *
		 * An empty answer means the manifest cannot say -- an index outside its
		 * source table, which parse refuses, so only a manifest built in memory
		 * can be in that state. Treated as no key at all rather than falling
		 * back to src.prefix: a wrong prefix finds another chunk's object and
		 * returns bytes that look perfectly good. */
		ref = s3_export_manifest_get_ref(m, chunk_index);
		prefix = s3_export_manifest_chunk_prefix(m, chunk_index);
		if (ref && prefix[0] != '\0') {
			s3_chunk_data_key(prefix, &ref->uuid, out, out_len);
			return;
		}
		out[0] = '\0';
		return;
	}

	s3_export_chunk_key(m->src.prefix, m->uuid_str, chunk_index,
			    out, out_len);
}

/* ==========================================================================
 * Completion
 * ========================================================================== */

static void export_io_start_refetch(struct s3_export_io *io);
static void export_read_internal(struct spdk_bs_dev *bs_dev,
				 struct spdk_io_channel *channel, void *payload,
				 uint64_t lba, uint32_t lba_count,
				 struct spdk_bs_dev_cb_args *cb_args, bool is_retry);

static void
export_io_drop(struct s3_export_io *io)
{
	if (io->m) {
		s3_export_manifest_unref(io->m);
	}
	free(io);
}

static void
export_io_complete(struct s3_export_io *io)
{
	struct spdk_bs_dev_cb_args *cb_args = io->cb_args;
	int status = io->status;

	/* A chunk was missing and nothing worse went wrong: hold the I/O and go
	 * looking for a newer manifest instead of failing it. Checked here rather
	 * than per sub-read so that every sub-read of this I/O has finished first --
	 * retrying with some of them still in flight would have two of them writing
	 * into the same buffer.
	 *
	 * status is honoured first because a real error is a worse answer than a
	 * stale manifest, and refetching would not fix it. */
	if (status == 0 && io->saw_missing && !io->retried) {
		export_io_start_refetch(io);
		return;
	}

	/* Retried and still missing. The manifest is current, so the objects really
	 * are gone -- the source deleted the snapshot rather than materialising it. */
	if (status == 0 && io->saw_missing) {
		SPDK_ERRLOG("export %s: chunks are still missing after refetching the "
			    "manifest; the source has deleted them\n",
			    io->m ? io->m->uuid_str : io->dev->m->uuid_str);
		status = -ENOENT;
	}

	export_io_drop(io);
	cb_args->cb_fn(cb_args->channel, cb_args->cb_arg, status);
}

static void
export_io_put(struct s3_export_io *io)
{
	assert(io->num_pending > 0);
	if (--io->num_pending == 0 && io->submit_done) {
		export_io_complete(io);
	}
}

static void export_chunk_read_done(void *cb_arg, uint64_t bytes_read, int status);
static void export_pf_slot_done(struct export_pf_wait *w, int status);
static void export_destroy_now(struct s3_export_dev *dev);

/* ==========================================================================
 * Whole-chunk slot table (home_thread only)
 * ========================================================================== */

static bool
export_on_home(const struct s3_export_dev *dev)
{
	return spdk_get_thread() == dev->home_thread;
}

static struct export_slot *
export_slot_find(struct s3_export_dev *dev, uint64_t chunk_index, const char *key)
{
	uint32_t i;

	for (i = 0; i < SPDK_COUNTOF(dev->slots); i++) {
		struct export_slot *s = &dev->slots[i];

		if (s->state != EXPORT_SLOT_FREE && s->chunk_index == chunk_index &&
		    strcmp(s->key, key) == 0) {
			return s;
		}
	}
	return NULL;
}

/* A slot of the given partition to (re)use: a free one, else the least recently
 * used filled one nobody pins. NULL when every slot is in flight or pinned. */
static struct export_slot *
export_slot_alloc(struct s3_export_dev *dev, bool prefetch_part, uint32_t window)
{
	struct export_slot *victim = NULL;
	uint32_t i, lo, hi;

	/* The prefetch partition is cut to the prefetcher's window, so the buffers
	 * it ever allocates -- and keeps for reuse -- are window x chunk_size. */
	lo = prefetch_part ? EXPORT_MISS_SLOTS : 0;
	hi = prefetch_part ? EXPORT_MISS_SLOTS + window : EXPORT_MISS_SLOTS;
	for (i = lo; i < hi; i++) {
		struct export_slot *s = &dev->slots[i];

		if (s->state == EXPORT_SLOT_FREE) {
			victim = s;
			break;
		}
		if (s->state == EXPORT_SLOT_FILLED && s->pins == 0 &&
		    (!victim || s->lru < victim->lru)) {
			victim = s;
		}
	}
	if (!victim) {
		return NULL;
	}
	if (victim->state == EXPORT_SLOT_FILLED) {
		dev->stats.evictions++;
	}
	victim->dev = dev;
	victim->state = EXPORT_SLOT_FREE;
	victim->transient = false;
	victim->prefetch_part = prefetch_part;
	victim->pins = 0;
	victim->gen++;
	TAILQ_INIT(&victim->waiters);
	TAILQ_INIT(&victim->pf_waiters);
	return victim;
}

static void
export_slot_release_transient(struct export_slot *s)
{
	free(s->buf);
	free(s);
}

/* Copy a filled slot's bytes into one reader's buffer and record the result. */
static void
export_slot_copy_out(struct export_slot *s, struct s3_export_chunk_io *cio)
{
	uint64_t avail = s->object_len > cio->offset_in_chunk ?
			 s->object_len - cio->offset_in_chunk : 0;
	uint32_t n = (uint32_t)spdk_min(avail, (uint64_t)cio->expected);

	if (n > 0) {
		memcpy(cio->dst, s->buf + cio->offset_in_chunk, n);
	}
	cio->result_status = 0;
	cio->result_bytes = n;
}

static void
export_cio_deliver_msg(void *arg)
{
	struct s3_export_chunk_io *cio = arg;

	export_chunk_read_done(cio, cio->result_bytes, cio->result_status);
}

/* Hand a finished sub-read back to the thread that owns its I/O. */
static void
export_cio_deliver(struct s3_export_chunk_io *cio)
{
	struct spdk_thread *origin = cio->io->origin;

	if (origin == NULL || origin == spdk_get_thread()) {
		export_cio_deliver_msg(cio);
		return;
	}
	if (spdk_thread_send_msg(origin, export_cio_deliver_msg, cio) != 0) {
		SPDK_ERRLOG("export: could not bounce a chunk read completion back to "
			    "its thread; completing it here\n");
		export_cio_deliver_msg(cio);
	}
}

static void
export_slot_fetch_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct export_slot *s = cb_arg;
	struct s3_export_dev *dev = s->dev;
	struct s3_export_chunk_io *cio, *ctmp;
	struct export_pf_wait *w, *wtmp;
	TAILQ_HEAD(, s3_export_chunk_io) waiters;
	TAILQ_HEAD(, export_pf_wait) pf_waiters;

	assert(dev->fetch_inflight > 0);
	dev->fetch_inflight--;

	if (status == 0 && bytes_read != s->object_len) {
		/* Same rule as a ranged read: a short answer is an error, never zeroes. */
		SPDK_ERRLOG("Short read of export chunk '%s': asked for %u byte(s), "
			    "got %" PRIu64 ". Refusing to treat the remainder as "
			    "zeroes.\n", s->key, s->object_len, bytes_read);
		status = -EIO;
	}
	if (status == 0) {
		s->state = EXPORT_SLOT_FILLED;
		s->lru = ++dev->lru_clock;
	} else {
		if (status != -ENOENT) {
			SPDK_ERRLOG("Failed to read export chunk '%s': %s\n", s->key,
				    spdk_strerror(-status));
		}
		dev->stats.fetch_errors++;
		s->state = EXPORT_SLOT_FREE;
		s->pins = 0;
	}

	/* Detach both lists first: a completion can submit another read of the same
	 * chunk (the retry after a refetch), which must not see this list. */
	TAILQ_INIT(&waiters);
	TAILQ_CONCAT(&waiters, &s->waiters, slot_link);
	TAILQ_INIT(&pf_waiters);
	TAILQ_CONCAT(&pf_waiters, &s->pf_waiters, link);

	/* Copy into every reader before delivering to any. A delivery can run the
	 * reader's completion inline, and that can submit a new read which reuses
	 * this very slot for another chunk. */
	TAILQ_FOREACH(cio, &waiters, slot_link) {
		if (status == 0) {
			export_slot_copy_out(s, cio);
		} else {
			cio->result_status = status;
			cio->result_bytes = 0;
		}
	}
	if (s->transient) {
		export_slot_release_transient(s);
		s = NULL;
	}
	TAILQ_FOREACH_SAFE(cio, &waiters, slot_link, ctmp) {
		TAILQ_REMOVE(&waiters, cio, slot_link);
		export_cio_deliver(cio);
	}
	TAILQ_FOREACH_SAFE(w, &pf_waiters, link, wtmp) {
		TAILQ_REMOVE(&pf_waiters, w, link);
		export_pf_slot_done(w, status);
	}

	if (dev->destroy_pending && dev->fetch_inflight == 0) {
		export_destroy_now(dev);
	}
}

/* Start the whole-object GET for a slot already set up with its key. */
static int
export_slot_start_fetch(struct s3_export_dev *dev, struct export_slot *s)
{
	int rc;

	if (!s->buf) {
		s->buf = malloc(dev->m->chunk_size);
		if (!s->buf) {
			return -ENOMEM;
		}
	}
	s->state = EXPORT_SLOT_FETCHING;
	dev->fetch_inflight++;
	if (dev->fetch_inflight > dev->stats.peak_fetch_inflight) {
		dev->stats.peak_fetch_inflight = dev->fetch_inflight;
	}
	dev->stats.whole_gets++;
	dev->stats.get_bytes += s->object_len;
	rc = s3_get_range(dev->client, s->key, 0, s->object_len, s->buf,
			  export_slot_fetch_done, s);
	if (rc != 0) {
		dev->fetch_inflight--;
		dev->stats.whole_gets--;
		dev->stats.get_bytes -= s->object_len;
		s->state = EXPORT_SLOT_FREE;
		return rc;
	}
	return 0;
}

static void
export_slot_setup(struct export_slot *s, uint64_t chunk_index, const char *key,
		  uint32_t object_len)
{
	s->chunk_index = chunk_index;
	snprintf(s->key, sizeof(s->key), "%s", key);
	s->object_len = object_len;
}

/* On home_thread: serve one sub-read from the slot table, fetching the whole
 * chunk if nobody has it. */
static void
export_fetch_on_home(void *arg)
{
	struct s3_export_chunk_io *cio = arg;
	struct s3_export_dev *dev = cio->io->dev;
	struct export_slot *s;
	int rc;

	s = export_slot_find(dev, cio->chunk_index, cio->key);
	if (s && s->state == EXPORT_SLOT_FILLED) {
		s->lru = ++dev->lru_clock;
		dev->stats.slot_hits++;
		export_slot_copy_out(s, cio);
		export_cio_deliver(cio);
		return;
	}
	if (s && s->state == EXPORT_SLOT_FETCHING) {
		dev->stats.slot_joins++;
		TAILQ_INSERT_TAIL(&s->waiters, cio, slot_link);
		return;
	}

	s = export_slot_alloc(dev, false, 0);
	if (!s) {
		/* Every miss slot is in flight. Still a whole-object GET, in a buffer
		 * that lives only as long as its readers. */
		s = calloc(1, sizeof(*s));
		if (!s) {
			cio->result_status = -ENOMEM;
			cio->result_bytes = 0;
			export_cio_deliver(cio);
			return;
		}
		s->dev = dev;
		s->transient = true;
		TAILQ_INIT(&s->waiters);
		TAILQ_INIT(&s->pf_waiters);
		dev->stats.transient_fetches++;
	}
	export_slot_setup(s, cio->chunk_index, cio->key, cio->object_len);
	TAILQ_INSERT_TAIL(&s->waiters, cio, slot_link);
	dev->stats.miss_fetches++;
	rc = export_slot_start_fetch(dev, s);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit a read of export chunk '%s': %s\n",
			    cio->key, spdk_strerror(-rc));
		TAILQ_REMOVE(&s->waiters, cio, slot_link);
		if (s->transient) {
			export_slot_release_transient(s);
		}
		cio->result_status = rc;
		cio->result_bytes = 0;
		export_cio_deliver(cio);
	}
}

/* Submit one present-chunk sub-read: always through home_thread. */
static void
export_fetch_submit(struct s3_export_chunk_io *cio)
{
	struct s3_export_dev *dev = cio->io->dev;

	if (export_on_home(dev)) {
		export_fetch_on_home(cio);
		return;
	}
	if (spdk_thread_send_msg(dev->home_thread, export_fetch_on_home, cio) != 0) {
		cio->result_status = -ENOMEM;
		cio->result_bytes = 0;
		export_cio_deliver(cio);
	}
}

static void
export_chunk_read_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct s3_export_chunk_io *cio = cb_arg;
	struct s3_export_io *io = cio->io;

	if (status == -ENOENT && !io->retried) {
		/* The object this manifest names is gone. On a ref export that is what
		 * the source materialising it looks like from here: it rewrote the
		 * manifest with its own copies and deleted the originals, and nothing
		 * tells an importer -- the 404 is the notification.
		 *
		 * So this is not recorded as the I/O's status. The I/O is held instead,
		 * the manifest refetched, and the read run again against whatever comes
		 * back. -EALREADY from that swap still retries: another refetch may
		 * already have installed this generation. If the objects are genuinely
		 * gone the retry 404s, which is the other thing a 404 can mean.
		 *
		 * Only once. A 404 after the retry is against the manifest that was
		 * just fetched, so refetching again would find the same thing. */
		io->saw_missing = true;
	} else if (status != 0) {
		/* The fetch logged the cause once for all of its readers. */
		if (io->status == 0) {
			io->status = status;
		}
	} else if (bytes_read != cio->expected) {
		/* A short read that reported success. The buffer past bytes_read still
		 * holds whatever it held before -- zeroes, for a freshly allocated
		 * materialisation buffer -- so letting this pass as success is how a
		 * chunk ends up silently readable as zeroes, and once a decouple has
		 * written that into a local cluster there is no way left to tell it
		 * from a hole the export really had.
		 *
		 * A range request is either satisfied or it is an error; a partial
		 * answer is neither, so it is turned into one here rather than
		 * retried. The caller retries whole I/Os, and blobstore fails the
		 * materialisation, which leaves the volume still reading through the
		 * export -- wrong but recoverable, instead of wrong and permanent.
		 */
		SPDK_ERRLOG("Short read of export chunk '%s': asked for %u byte(s), "
			    "got %" PRIu64 ". Refusing to treat the remainder as "
			    "zeroes.\n", cio->key, cio->expected, bytes_read);
		if (io->status == 0) {
			io->status = -EIO;
		}
	} else {
		__atomic_fetch_add(&io->dev->bytes_read, bytes_read, __ATOMIC_RELAXED);
	}

	free(cio);
	export_io_put(io);
}

/* ==========================================================================
 * Read path
 * ========================================================================== */

/* The read itself. \p is_retry marks an I/O that has already been through one
 * refetch, so a 404 against the manifest just fetched is reported rather than
 * sent round again. */
static void
export_read_internal(struct spdk_bs_dev *bs_dev, struct spdk_io_channel *channel,
		     void *payload, uint64_t lba, uint32_t lba_count,
		     struct spdk_bs_dev_cb_args *cb_args, bool is_retry)
{
	struct s3_export_dev *dev = (struct s3_export_dev *)bs_dev;
	struct s3_export_io *io;
	uint64_t offset_bytes = lba * S3LVOL_BLOCK_SIZE;
	uint64_t remaining = (uint64_t)lba_count * S3LVOL_BLOCK_SIZE;
	uint8_t *buf = payload;

	if (lba + lba_count > bs_dev->blockcnt) {
		SPDK_ERRLOG("export read out of range: lba=%" PRIu64 " count=%u "
			    "blockcnt=%" PRIu64 "\n", lba, lba_count, bs_dev->blockcnt);
		cb_args->cb_fn(cb_args->channel, cb_args->cb_arg, -EINVAL);
		return;
	}
	if (lba_count == 0) {
		cb_args->cb_fn(cb_args->channel, cb_args->cb_arg, 0);
		return;
	}

	io = calloc(1, sizeof(*io));
	if (!io) {
		cb_args->cb_fn(cb_args->channel, cb_args->cb_arg, -ENOMEM);
		return;
	}
	io->dev = dev;
	io->m = __atomic_load_n(&dev->m, __ATOMIC_ACQUIRE);
	s3_export_manifest_ref(io->m);
	io->cb_args = cb_args;
	/* Kept so the read can be run again after a refetch. */
	io->payload = payload;
	io->lba = lba;
	io->lba_count = lba_count;
	io->origin = spdk_get_thread();
	io->retried = is_retry;
	/* Self reference, so a sub-read that completes inline -- which a hole
	 * always does -- cannot finish the I/O before the split loop is done. */
	io->num_pending = 1;

	__atomic_fetch_add(&dev->reads, 1, __ATOMIC_RELAXED);

	while (remaining > 0 && io->status == 0) {
		uint32_t chunk_size = io->m->chunk_size;
		uint32_t chunk_shift = (uint32_t)spdk_u32log2(chunk_size);
		uint64_t chunk_index = offset_bytes >> chunk_shift;
		uint32_t offset_in_chunk = (uint32_t)(offset_bytes &
						(chunk_size - 1));
		uint32_t length = (uint32_t)spdk_min(remaining,
						     chunk_size - offset_in_chunk);
		struct s3_export_chunk_io *cio;
		uint32_t get_len;

		/* A chunk with no object reads as zeroes. Not an optimisation: it is how
		 * an export represents a hole, and it is the common case across the
		 * sparse regions of a snapshot. */
		if (!s3_export_manifest_is_present(io->m, chunk_index)) {
			memset(buf, 0, length);
			__atomic_fetch_add(&dev->zero_fills, 1, __ATOMIC_RELAXED);
			goto next;
		}

		get_len = length;

		if (io->m->layout == S3_EXPORT_LAYOUT_REF) {
			const struct s3_export_ref *ref;

			ref = s3_export_manifest_get_ref(io->m, chunk_index);
			if (!ref) {
				/* Present in the bitmap with no ref behind it. parse()
				 * rejects that, so getting here means the manifest was
				 * mutated in memory afterwards. */
				SPDK_ERRLOG("export %s: chunk %" PRIu64 " is present but "
					    "has no ref\n", io->m->uuid_str, chunk_index);
				io->status = -EIO;
				break;
			}

			/* Same class of failure, for the prefix rather than the uuid: an
			 * index outside the source table, which parse() also rejects.
			 * Checked here rather than left to export_chunk_key() because this
			 * is where a failure can be reported -- and because the alternative
			 * is worse than an error. Falling back to src.prefix would build a
			 * key that very likely exists and holds another chunk's data, so the
			 * read would succeed and return the wrong bytes. */
			if (s3_export_manifest_chunk_prefix(io->m, chunk_index)[0] == '\0') {
				SPDK_ERRLOG("export %s: chunk %" PRIu64 " names a source this "
					    "manifest does not have\n", io->m->uuid_str,
					    chunk_index);
				io->status = -EIO;
				break;
			}

			/* A ref export names the source's live object, and that object is
			 * only as long as the writes which produced it. So the tail of a
			 * partially written chunk is zeroes here, not a range request
			 * against bytes that were never uploaded -- which would come back
			 * as an error, or as whatever the object store decides to do with
			 * an unsatisfiable range. A dense export needs none of this
			 * because it uploads whole chunks. */
			if (offset_in_chunk >= ref->valid_bytes) {
				memset(buf, 0, length);
				__atomic_fetch_add(&dev->zero_fills, 1, __ATOMIC_RELAXED);
				goto next;
			}
			if (offset_in_chunk + length > ref->valid_bytes) {
				get_len = ref->valid_bytes - offset_in_chunk;
				memset(buf + get_len, 0, length - get_len);
			}
		}

		cio = calloc(1, sizeof(*cio));
		if (!cio) {
			io->status = -ENOMEM;
			break;
		}
		cio->io = io;
		cio->chunk_index = chunk_index;
		cio->expected = get_len;
		cio->offset_in_chunk = offset_in_chunk;
		cio->dst = buf;
		cio->object_len = chunk_size;
		if (io->m->layout == S3_EXPORT_LAYOUT_REF) {
			cio->object_len = s3_export_manifest_get_ref(io->m,
					  chunk_index)->valid_bytes;
		}
		export_chunk_key(io->m, chunk_index, cio->key, sizeof(cio->key));

		/* Never a ranged GET sized to the guest I/O: the whole object is
		 * fetched once into the slot table and this read copies its part. */
		io->num_pending++;
		export_fetch_submit(cio);

next:
		buf += length;
		offset_bytes += length;
		remaining -= length;
	}

	io->submit_done = true;
	export_io_put(io);
}

/* blobstore's entry point: always a first attempt. */
static void
export_read(struct spdk_bs_dev *bs_dev, struct spdk_io_channel *channel,
	    void *payload, uint64_t lba, uint32_t lba_count,
	    struct spdk_bs_dev_cb_args *cb_args)
{
	export_read_internal(bs_dev, channel, payload, lba, lba_count, cb_args,
			     false);
}

static void
export_readv(struct spdk_bs_dev *bs_dev, struct spdk_io_channel *channel,
	     struct iovec *iov, int iovcnt, uint64_t lba, uint32_t lba_count,
	     struct spdk_bs_dev_cb_args *cb_args)
{
	if (iovcnt == 1) {
		export_read(bs_dev, channel, iov[0].iov_base, lba, lba_count, cb_args);
		return;
	}

	/* Same limitation, and the same reasoning, as s3_bs_dev: reporting it is
	 * safe, quietly reading the wrong bytes into the wrong buffer is not. The
	 * bdev layer splits multi-segment I/O for us (max_num_segments = 1), and
	 * blobstore's own copy-on-write path uses a single buffer. */
	SPDK_ERRLOG("export readv with iovcnt=%d is not supported\n", iovcnt);
	cb_args->cb_fn(cb_args->channel, cb_args->cb_arg, -ENOTSUP);
}

static void
export_readv_ext(struct spdk_bs_dev *bs_dev, struct spdk_io_channel *channel,
		 struct iovec *iov, int iovcnt, uint64_t lba, uint32_t lba_count,
		 struct spdk_bs_dev_cb_args *cb_args,
		 struct spdk_blob_ext_io_opts *ext_io_opts)
{
	/* ext_io_opts carries a memory domain, i.e. a payload this thread cannot
	 * memcpy from. Nothing in this path can honour that, and there is no
	 * memory domain in play in this stack today. */
	if (ext_io_opts && ext_io_opts->memory_domain) {
		SPDK_ERRLOG("export read with a memory domain is not supported\n");
		cb_args->cb_fn(cb_args->channel, cb_args->cb_arg, -ENOTSUP);
		return;
	}
	export_readv(bs_dev, channel, iov, iovcnt, lba, lba_count, cb_args);
}

/* ==========================================================================
 * Write path: there isn't one
 * ========================================================================== */

static void
export_reject_write(struct spdk_bs_dev_cb_args *cb_args, const char *what)
{
	/* blobstore does not write to a back device, so reaching this is a bug
	 * somewhere above. Loud, and -EROFS rather than -ENOTSUP, because the
	 * device is not merely lacking the feature: the data it serves is a
	 * snapshot that another node may still be reading. */
	SPDK_ERRLOG("%s on an imported export is not allowed; it is read-only\n", what);
	cb_args->cb_fn(cb_args->channel, cb_args->cb_arg, -EROFS);
}

static void
export_write(struct spdk_bs_dev *dev, struct spdk_io_channel *channel, void *payload,
	     uint64_t lba, uint32_t lba_count, struct spdk_bs_dev_cb_args *cb_args)
{
	export_reject_write(cb_args, "write");
}

static void
export_writev(struct spdk_bs_dev *dev, struct spdk_io_channel *channel,
	      struct iovec *iov, int iovcnt, uint64_t lba, uint32_t lba_count,
	      struct spdk_bs_dev_cb_args *cb_args)
{
	export_reject_write(cb_args, "writev");
}

static void
export_writev_ext(struct spdk_bs_dev *dev, struct spdk_io_channel *channel,
		  struct iovec *iov, int iovcnt, uint64_t lba, uint32_t lba_count,
		  struct spdk_bs_dev_cb_args *cb_args,
		  struct spdk_blob_ext_io_opts *ext_io_opts)
{
	export_reject_write(cb_args, "writev");
}

static void
export_write_zeroes(struct spdk_bs_dev *dev, struct spdk_io_channel *channel,
		    uint64_t lba, uint64_t lba_count,
		    struct spdk_bs_dev_cb_args *cb_args)
{
	export_reject_write(cb_args, "write_zeroes");
}

static void
export_unmap(struct spdk_bs_dev *dev, struct spdk_io_channel *channel,
	     uint64_t lba, uint64_t lba_count, struct spdk_bs_dev_cb_args *cb_args)
{
	export_reject_write(cb_args, "unmap");
}

static void
export_flush(struct spdk_bs_dev *dev, struct spdk_io_channel *channel,
	     struct spdk_bs_dev_cb_args *cb_args)
{
	/* Nothing was ever dirtied here. Succeeding is the honest answer. */
	cb_args->cb_fn(cb_args->channel, cb_args->cb_arg, 0);
}

/* ==========================================================================
 * Queries
 * ========================================================================== */

static bool
export_is_zeroes(struct spdk_bs_dev *bs_dev, uint64_t lba, uint64_t lba_count)
{
	struct s3_export_dev *dev = (struct s3_export_dev *)bs_dev;
	struct s3_export_manifest *m;
	uint32_t chunk_shift;
	uint64_t first, last;

	if (lba_count == 0) {
		return true;
	}

	m = __atomic_load_n(&dev->m, __ATOMIC_ACQUIRE);
	chunk_shift = (uint32_t)spdk_u32log2(m->chunk_size);

	/* Whole chunks only. A range that starts or ends inside a present chunk is
	 * reported as non-zero even if those particular bytes are zero: the answer
	 * has to be conservative, since blobstore uses it to skip reading the
	 * parent entirely. */
	first = (lba * S3LVOL_BLOCK_SIZE) >> chunk_shift;
	last = ((lba + lba_count - 1) * S3LVOL_BLOCK_SIZE) >> chunk_shift;

	return s3_export_manifest_range_is_zeroes(m, first, last - first + 1);
}

static bool
export_is_range_valid(struct spdk_bs_dev *bs_dev, uint64_t lba, uint64_t lba_count)
{
	return lba + lba_count <= bs_dev->blockcnt;
}

static bool
export_translate_lba(struct spdk_bs_dev *bs_dev, uint64_t lba, uint64_t *base_lba)
{
	/* Identity: dest->copy during ingest treats src_lba as an LBA on this
	 * export device, not as a block on the destination disk. Returning false
	 * would force allocate_and_copy_cluster down the GET+write path even when
	 * the destination has installed copy(). */
	(void)bs_dev;
	*base_lba = lba;
	return true;
}

static bool
export_is_degraded(struct spdk_bs_dev *bs_dev)
{
	/* Reachability of the source bucket is not tracked. Claiming degraded
	 * would make blobstore refuse I/O that would probably succeed; a real
	 * outage surfaces as failed reads, which is what an operator sees anyway. */
	return false;
}

/* ==========================================================================
 * Channels and teardown
 * ========================================================================== */

static int
export_channel_create_cb(void *io_device, void *ctx_buf)
{
	return 0;
}

static void
export_channel_destroy_cb(void *io_device, void *ctx_buf)
{
}

static struct spdk_io_channel *
export_create_channel(struct spdk_bs_dev *bs_dev)
{
	struct s3_export_dev *dev = (struct s3_export_dev *)bs_dev;

	/* A real spdk_io_channel is required, not a stub: blobstore caches one of
	 * these per (thread, blob) and releases some of them with
	 * spdk_put_io_channel() rather than through destroy_channel()
	 * (blobstore.c, blob_esnap_destroy_bs_channel). Anything not obtained from
	 * spdk_get_io_channel() would blow up there. */
	return spdk_get_io_channel(dev);
}

static void
export_destroy_channel(struct spdk_bs_dev *bs_dev, struct spdk_io_channel *channel)
{
	spdk_put_io_channel(channel);
}

static void
export_io_device_unregistered(void *io_device)
{
	struct s3_export_dev *dev = io_device;
	uint32_t i;

	for (i = 0; i < SPDK_COUNTOF(dev->slots); i++) {
		free(dev->slots[i].buf);
	}
	s3_export_manifest_unref(dev->m);
	if (dev->client) {
		s3_client_put(dev->client);
	}
	free(dev);
}

static void
export_destroy_now(struct s3_export_dev *dev)
{
	/* Frees in the callback: unregistering is asynchronous, and any channel
	 * still open would otherwise be pointing at freed memory. */
	spdk_io_device_unregister(dev, export_io_device_unregistered);
}

static void
export_destroy_on_home(void *arg)
{
	struct s3_export_dev *dev = arg;

	/* A whole-chunk GET still in flight writes into a slot of this device from
	 * its completion. Normally there is none -- every fetch has a reader or a
	 * prefetcher blobstore or decouple is still waiting for -- but a GET whose
	 * last reader was satisfied elsewhere is not impossible, so the free waits
	 * for it rather than racing it. */
	if (dev->fetch_inflight > 0) {
		dev->destroy_pending = true;
		return;
	}
	export_destroy_now(dev);
}

static void
export_destroy(struct spdk_bs_dev *bs_dev)
{
	struct s3_export_dev *dev = (struct s3_export_dev *)bs_dev;

	SPDK_NOTICELOG("Releasing imported export %s: %" PRIu64 " read(s), "
		       "%" PRIu64 " bytes served, %" PRIu64 " served as zeroes, "
		       "%" PRIu64 " manifest refetch(es), %" PRIu64 " whole-chunk "
		       "GET(s) (%" PRIu64 " prefetched, %" PRIu64 " on demand), "
		       "%" PRIu64 " slot hit(s), %" PRIu64 " joined in flight\n",
		       dev->m->uuid_str, dev->reads, dev->bytes_read, dev->zero_fills,
		       dev->refetches, dev->stats.whole_gets,
		       dev->stats.prefetch_fetches, dev->stats.miss_fetches,
		       dev->stats.slot_hits, dev->stats.slot_joins);

	/* No wait for a refetch, deliberately.
	 *
	 * A refetch only exists while an I/O is waiting on it, and that I/O is one
	 * blobstore is waiting for a callback on -- so blobstore cannot have decided
	 * the device is unreferenced. The same reasoning already covers an ordinary
	 * chunk GET, which writes to dev from its completion.
	 *
	 * What that rests on is that every path out of a refetch releases its
	 * waiters, including each failure: a refetch that returned without doing so
	 * would leave blobstore waiting for ever, and the leak would look like a
	 * hung volume rather than like anything to do with this file. */

	if (export_on_home(dev)) {
	 export_destroy_on_home(dev);
	} else if (spdk_thread_send_msg(dev->home_thread, export_destroy_on_home,
	 			dev) != 0) {
	 export_destroy_now(dev);
	}
}

/* ==========================================================================
 * Replacing the manifest
 *
 * A ref export names the source's live objects. When the source materialises it
 * -- rewrites the manifest as dense, with its own copies -- those objects stop
 * existing, and an importer still holding the old manifest reads 404s. Its way
 * out is to fetch the manifest again and carry on against the new one, which is
 * what `generation` is for.
 *
 * The hard part is not fetching it. It is that the whole of this file is lock
 * free *because* the manifest never changes: blobstore makes a channel per
 * thread that touches the clone, and every one of them reads dev->m with no
 * serialisation at all. Swapping it is therefore the one operation that has to
 * think about the other threads.
 *
 * What makes it tractable is that no reader holds the manifest across an await.
 * Every dereference is synchronous inside export_read() or export_is_zeroes():
 * a chunk's key is copied into the sub-I/O before the GET is submitted, and the
 * completion never looks at the manifest again. So a reader's window is one
 * synchronous stretch on its own thread.
 *
 * Hence a grace period rather than a lock. The pointer is replaced first -- a
 * pointer store, so every reader sees one manifest or the other, never a torn
 * value, and both are valid to read against. The old one is then released only
 * after spdk_for_each_channel() has run on every thread that has a channel:
 * having got a turn on each of them means every reader that could have loaded
 * the old pointer has already finished with it, because it could only have been
 * doing so from the same event loop the iteration just ran in.
 * ========================================================================== */

struct export_swap_ctx {
	/* Deliberately no dev pointer. destroy() can run while this is in flight --
	 * spdk_io_device_unregister() defers behind for_each, but the completion
	 * still arrives -- and holding the device here would suggest it is safe to
	 * dereference on the way out. Nothing below needs it: the old manifest's
	 * reference was transferred into this struct, so releasing it does not
	 * involve the device at all. */
	struct s3_export_manifest *old;
	s3_export_bs_dev_swap_cb   cb;
	void                      *cb_arg;
};

static void
export_swap_channel(struct spdk_io_channel_iter *i)
{
	/* Nothing to do per channel: being called here is the point. It says this
	 * thread has reached the iteration, so it is not inside a read that loaded
	 * the old manifest. */
	spdk_for_each_channel_continue(i, 0);
}

static void
export_swap_done(struct spdk_io_channel_iter *i, int status)
{
	struct export_swap_ctx *ctx = spdk_io_channel_iter_get_ctx(i);

	/* Past the grace period: the old manifest cannot be reachable from any
	 * reader now, so this reference is the last thing holding it. */
	s3_export_manifest_unref(ctx->old);

	if (ctx->cb) {
		ctx->cb(ctx->cb_arg, status);
	}
	free(ctx);
}

int
s3_export_bs_dev_swap_manifest(struct spdk_bs_dev *bs_dev,
			       struct s3_export_manifest *m,
			       s3_export_bs_dev_swap_cb cb, void *cb_arg)
{
	struct s3_export_dev *dev = (struct s3_export_dev *)bs_dev;
	struct export_swap_ctx *ctx;

	if (!bs_dev || !m) {
		return -EINVAL;
	}

	/* A different export entirely. Nothing else here would catch it -- the
	 * geometry could match by coincidence -- and the result would be a clone
	 * silently reading another volume. */
	if (strcmp(m->uuid_str, dev->m->uuid_str) != 0) {
		SPDK_ERRLOG("export %s: refusing a manifest for %s\n",
			    dev->m->uuid_str, m->uuid_str);
		return -EINVAL;
	}

	/* blobstore sized the clone against the old manifest and has been serving
	 * that size since. A parent that changed length would put the end of the
	 * volume somewhere the clone does not agree with, so this is refused rather
	 * than adopted -- reading through a shorter parent is data loss, and a
	 * longer one is data blobstore will never ask for. */
	if (m->size_bytes != dev->m->size_bytes) {
		SPDK_ERRLOG("export %s: refusing a manifest of %" PRIu64 " byte(s) "
			    "against the %" PRIu64 " this device was built for\n",
			    dev->m->uuid_str, m->size_bytes, dev->m->size_bytes);
		return -EINVAL;
	}

	if (m->chunk_size != dev->m->chunk_size) {
		SPDK_ERRLOG("export %s: refusing a manifest with chunk_size %u "
			    "against the %u this device was built for\n",
			    dev->m->uuid_str, m->chunk_size, dev->m->chunk_size);
		return -EINVAL;
	}

	/* Older or identical: reported as -EALREADY rather than success so the
	 * caller can tell "already on this generation" from "now reading a new
	 * one". A refetch must not treat that as data loss -- another refetch
	 * may have swapped while this GET still used the pre-swap keys. */
	if (m->generation <= dev->m->generation) {
		return -EALREADY;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		return -ENOMEM;
	}
	ctx->old    = dev->m;
	ctx->cb     = cb;
	ctx->cb_arg = cb_arg;

	s3_export_manifest_ref(m);

	/* Publish one snapshot. Each I/O captures `m` at submit and uses that
	 * manifest's geometry until it completes, so these stores do not have to
	 * appear atomic with a concurrent read's split loop. Rejecting a chunk-size
	 * change above is what keeps is_zeroes and a in-flight read from disagreeing
	 * on which byte range a chunk index covers. */
	dev->chunk_shift      = (uint32_t)spdk_u32log2(m->chunk_size);
	dev->blocks_per_chunk = m->chunk_size / S3LVOL_BLOCK_SIZE;
	__atomic_store_n(&dev->m, m, __ATOMIC_RELEASE);

	if (dev->on_swap) {
		dev->on_swap(dev->on_swap_arg, m);
	}

	SPDK_NOTICELOG("export %s: now reading generation %u (%s), was %u (%s)\n",
		       m->uuid_str, m->generation,
		       m->layout == S3_EXPORT_LAYOUT_REF ? "ref" : "copied",
		       ctx->old->generation,
		       ctx->old->layout == S3_EXPORT_LAYOUT_REF ? "ref" : "copied");

	spdk_for_each_channel(dev, export_swap_channel, ctx, export_swap_done);
	return 0;
}

/* ==========================================================================
 * Refetch: what a 404 means, and what to do about it
 *
 * A ref export names the source's live objects. The source's way of taking its
 * snapshot back is to materialise the export -- copy the data under the export's
 * own prefix, rewrite the manifest as dense with `generation` bumped, and delete
 * the originals. Nothing tells the importer. The 404 *is* the notification.
 *
 * So a missing object is not failed straight back to blobstore. The I/O is held,
 * the manifest fetched again, and the read run once more against whatever came
 * back. A caller sees a slower read rather than an error, which matters because
 * materialising is an ordinary operation on the source and the importer cannot
 * anticipate it.
 *
 * It all runs on home_thread. Several threads can hit 404 on the same export at
 * the same moment -- one large read splits into many chunk GETs, and after a
 * materialisation every one of them misses -- so they have to produce one refetch
 * between them. The waiters queue there and each is sent back to its own thread
 * to be retried, because blobstore's completion belongs on the thread that
 * issued the read.
 * ========================================================================== */

/* Big enough for any manifest this can produce, and small enough that a wrong
 * answer from HEAD cannot turn into a huge allocation. Matches the import path. */
#define EXPORT_MANIFEST_MAX (64u * 1024 * 1024)

struct export_refetch_ctx {
	struct s3_export_dev *dev;
	char                  key[S3_EXPORT_KEY_MAX];
	uint64_t              size;
	char                 *body;
};

static void export_retry_msg(void *arg);

/* Release every waiter back to its own thread. Called on home_thread once the
 * refetch has settled, whatever the outcome: a waiter that is not retried still
 * has to be completed, or blobstore waits for a callback that never comes. */
static void
export_release_waiters(struct s3_export_dev *dev, bool retry)
{
	struct s3_export_io *io, *tmp;

	TAILQ_FOREACH_SAFE(io, &dev->refetch_waiters, waiter_link, tmp) {
		TAILQ_REMOVE(&dev->refetch_waiters, io, waiter_link);
		/* Set either way: it is what stops the retry from refetching again. */
		io->retried = true;
		/* Cleared on the retry path, kept on the give-up path. The pair is what
		 * export_retry_msg() reads to tell "run it again against the new
		 * manifest" from "there is no new manifest, report the 404". */
		io->saw_missing = !retry;
		spdk_thread_send_msg(io->origin, export_retry_msg, io);
	}
	dev->refetch_in_flight = false;
}

static void
export_refetch_free(struct export_refetch_ctx *ctx)
{
	free(ctx->body);
	free(ctx);
}

static void
export_refetch_swapped(void *cb_arg, int status)
{
	struct export_refetch_ctx *ctx = cb_arg;

	/* The swap is complete and the old manifest is gone, so the waiters can now
	 * be re-run against the new one. */
	export_release_waiters(ctx->dev, true);
	export_refetch_free(ctx);
}

static void
export_refetch_got_manifest(void *cb_arg, uint64_t bytes_read, int status)
{
	struct export_refetch_ctx *ctx = cb_arg;
	struct s3_export_dev *dev = ctx->dev;
	struct s3_export_manifest *m = NULL;
	int rc;

	if (status != 0) {
		SPDK_ERRLOG("export %s: could not read the manifest back: %s\n",
			    dev->m->uuid_str, spdk_strerror(-status));
		goto give_up;
	}

	rc = s3_export_manifest_parse(ctx->body, bytes_read, &m);
	if (rc != 0) {
		SPDK_ERRLOG("export %s: the refetched manifest does not parse: %s\n",
			    dev->m->uuid_str, spdk_strerror(-rc));
		goto give_up;
	}

	rc = s3_export_bs_dev_swap_manifest(&dev->bs_dev, m,
					    export_refetch_swapped, ctx);
	/* The device holds its own reference now, or refused it; either way this
	 * one is done with. */
	s3_export_manifest_unref(m);

	if (rc == 0) {
		__atomic_fetch_add(&dev->refetches, 1, __ATOMIC_RELAXED);
		return;
	}

	if (s3_export_bs_dev_refetch_already_current(rc)) {
		/* Already on this generation. Retry against the keys now in
		 * dev->m: a GET that left before a swap 404s after it, and a
		 * second refetch then hits -EALREADY even though the new
		 * objects are there. Genuine deletion still fails -- the retry
		 * 404s and io->retried blocks another refetch. */
		export_release_waiters(dev, true);
		export_refetch_free(ctx);
		return;
	}

	SPDK_ERRLOG("export %s: the refetched manifest was refused: %s\n",
		    dev->m->uuid_str, spdk_strerror(-rc));

give_up:
	export_release_waiters(dev, false);
	export_refetch_free(ctx);
}

static void
export_refetch_head_done(void *cb_arg, int status)
{
	struct export_refetch_ctx *ctx = cb_arg;
	struct s3_export_dev *dev = ctx->dev;
	int rc;

	if (status != 0) {
		/* A 404 here is worth saying out loud: the manifest itself is gone, so
		 * there is nothing to refetch and nothing will fix this export. */
		SPDK_ERRLOG("export %s: cannot stat the manifest '%s': %s\n",
			    dev->m->uuid_str, ctx->key, spdk_strerror(-status));
		goto give_up;
	}
	if (ctx->size == 0 || ctx->size > EXPORT_MANIFEST_MAX) {
		SPDK_ERRLOG("export %s: manifest '%s' has an implausible size of %"
			    PRIu64 " bytes\n", dev->m->uuid_str, ctx->key, ctx->size);
		goto give_up;
	}

	ctx->body = malloc(ctx->size);
	if (!ctx->body) {
		goto give_up;
	}

	rc = s3_get_range(dev->client, ctx->key, 0, ctx->size, ctx->body,
			  export_refetch_got_manifest, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("export %s: could not submit the manifest read: %s\n",
			    dev->m->uuid_str, spdk_strerror(-rc));
		goto give_up;
	}
	return;

give_up:
	export_release_waiters(dev, false);
	export_refetch_free(ctx);
}

/* On home_thread. Queues the I/O and starts a refetch if one is not already
 * under way. */
static void
export_refetch_msg(void *arg)
{
	struct s3_export_io *io = arg;
	struct s3_export_dev *dev = io->dev;
	struct export_refetch_ctx *ctx;
	int rc;

	TAILQ_INSERT_TAIL(&dev->refetch_waiters, io, waiter_link);

	/* One refetch for all of them. After a materialisation every chunk read
	 * misses at once, and one GET per miss would be a burst of identical
	 * requests -- and several swaps racing to replace the same manifest. */
	if (dev->refetch_in_flight) {
		return;
	}
	dev->refetch_in_flight = true;

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		export_release_waiters(dev, false);
		return;
	}
	ctx->dev = dev;
	s3_export_manifest_key(dev->m->uuid_str, ctx->key, sizeof(ctx->key));

	SPDK_NOTICELOG("export %s: a chunk is missing; refetching the manifest "
		       "(generation %u now)\n", dev->m->uuid_str, dev->m->generation);

	rc = s3_head(dev->client, ctx->key, &ctx->size, export_refetch_head_done, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("export %s: could not submit the manifest stat: %s\n",
			    dev->m->uuid_str, spdk_strerror(-rc));
		export_release_waiters(dev, false);
		export_refetch_free(ctx);
	}
}

static void
export_io_start_refetch(struct s3_export_io *io)
{
	spdk_thread_send_msg(io->dev->home_thread, export_refetch_msg, io);
}

/* Back on the I/O's own thread, with the manifest already replaced. */
static void
export_retry_msg(void *arg)
{
	struct s3_export_io *io = arg;
	struct spdk_bs_dev *bs_dev = &io->dev->bs_dev;
	struct spdk_bs_dev_cb_args *cb_args = io->cb_args;
	void *payload = io->payload;
	uint64_t lba = io->lba;
	uint32_t lba_count = io->lba_count;

	if (io->retried && io->saw_missing) {
		/* The refetch gave up. Completed rather than re-read, since reading
		 * again would produce the same 404. */
		export_io_complete(io);
		return;
	}

	/* A fresh read against the new manifest, so the whole splitting and hole
	 * logic is reused rather than a second, subtly different, copy of it.
	 *
	 * is_retry carries across. Without it the new I/O would start with a clean
	 * slate, and a 404 against the manifest that was *just* fetched would send
	 * it round again -- a materialisation the source is still part way through
	 * would loop for as long as it lasted. One refetch per I/O is the rule, and
	 * this is where it is kept.
	 *
	 * The old I/O is dropped here: export_read() builds its own, and this one's
	 * captured manifest is released with it. */
	export_io_drop(io);
	export_read_internal(bs_dev, cb_args->channel, payload, lba, lba_count,
			     cb_args, true);
}

/* ==========================================================================
 * Construction
 * ========================================================================== */

int
s3_export_bs_dev_create(struct s3_client *client, struct s3_export_manifest *m,
			struct spdk_bs_dev **out)
{
	struct s3_export_dev *dev;

	if (!client || !m || !out) {
		return -EINVAL;
	}
	if (m->size_bytes % S3LVOL_BLOCK_SIZE != 0) {
		return -EINVAL;
	}

	dev = calloc(1, sizeof(*dev));
	if (!dev) {
		return -ENOMEM;
	}

	dev->client           = client;
	dev->m                = m;
	dev->chunk_shift      = (uint32_t)spdk_u32log2(m->chunk_size);
	dev->blocks_per_chunk = m->chunk_size / S3LVOL_BLOCK_SIZE;
	snprintf(dev->name, sizeof(dev->name), "esnap:%s", m->uuid_str);

	/* The refetch machinery's thread. Taken here rather than from the first
	 * read, because it has to be a thread that stays: blobstore's channels come
	 * and go with the poll groups, and a refetch outliving the thread that
	 * started it would send a message to an exited thread, which aborts. */
	dev->home_thread = spdk_get_thread();
	TAILQ_INIT(&dev->refetch_waiters);

	s3_export_manifest_ref(m);

	spdk_io_device_register(dev, export_channel_create_cb, export_channel_destroy_cb,
				0, dev->name);

	dev->bs_dev.blockcnt       = m->size_bytes / S3LVOL_BLOCK_SIZE;
	dev->bs_dev.blocklen       = S3LVOL_BLOCK_SIZE;
	dev->bs_dev.phys_blocklen  = S3LVOL_BLOCK_SIZE;

	dev->bs_dev.create_channel  = export_create_channel;
	dev->bs_dev.destroy_channel = export_destroy_channel;
	dev->bs_dev.destroy         = export_destroy;
	dev->bs_dev.read            = export_read;
	dev->bs_dev.readv           = export_readv;
	dev->bs_dev.readv_ext       = export_readv_ext;
	dev->bs_dev.write           = export_write;
	dev->bs_dev.writev          = export_writev;
	dev->bs_dev.writev_ext      = export_writev_ext;
	dev->bs_dev.write_zeroes    = export_write_zeroes;
	dev->bs_dev.unmap           = export_unmap;
	dev->bs_dev.flush           = export_flush;
	dev->bs_dev.is_zeroes       = export_is_zeroes;
	dev->bs_dev.is_range_valid  = export_is_range_valid;
	dev->bs_dev.translate_lba   = export_translate_lba;
	dev->bs_dev.is_degraded     = export_is_degraded;
	/* copy stays NULL: the destination of a CoW copy is the lvstore bs_dev. */

	SPDK_NOTICELOG("Imported export %s as a read-only parent: %" PRIu64 " bytes "
		       "(%" PRIu64 " blocks), %" PRIu64 " of %" PRIu64 " chunk(s) "
		       "present, source %s/%s\n", m->uuid_str, m->size_bytes,
		       dev->bs_dev.blockcnt, m->present_chunks, m->num_chunks,
		       m->src.bucket, m->src.prefix);

	/* Named individually when there is more than one, because the line above
	 * would otherwise describe a derived import as depending on a single prefix.
	 * Which prefixes an import reads from is what decides whose lease has to be
	 * renewed and whose snapshot cannot be deleted, so an operator looking into
	 * either has to be able to see it. */
	if (m->num_srcs > 1) {
		uint32_t si;

		for (si = 1; si < m->num_srcs; si++) {
			SPDK_NOTICELOG("Export %s also reads prefix '%s' (export %s)\n",
				       m->uuid_str, m->srcs[si].prefix,
				       m->srcs[si].export_uuid[0] ? m->srcs[si].export_uuid
				       : "unnamed");
		}
	}

	*out = &dev->bs_dev;
	return 0;
}

void
s3_export_bs_dev_set_on_swap(struct spdk_bs_dev *bs_dev,
			      s3_export_bs_dev_on_swap_fn fn, void *arg)
{
	struct s3_export_dev *dev = (struct s3_export_dev *)bs_dev;

	if (!bs_dev) {
		return;
	}
	dev->on_swap = fn;
	dev->on_swap_arg = arg;
}

bool
s3_export_bs_dev_is_export(const struct spdk_bs_dev *bs_dev)
{
	return bs_dev != NULL && bs_dev->destroy == export_destroy;
}

int
s3_export_bs_dev_get_stats(struct spdk_bs_dev *bs_dev,
			   struct s3_export_bs_dev_stats *out)
{
	struct s3_export_dev *dev = (struct s3_export_dev *)bs_dev;

	if (!s3_export_bs_dev_is_export(bs_dev) || !out) {
		return -EINVAL;
	}
	*out = dev->stats;
	out->fetch_inflight = dev->fetch_inflight;
	return 0;
}

/* ==========================================================================
 * Prefetcher
 *
 * Walks a caller-supplied list of present chunks in order and keeps at most
 * `window` of them fetched-or-in-flight and pinned. A chunk stays pinned until
 * the consumer releases it, so the window is also the memory bound. All calls
 * on home_thread.
 * ========================================================================== */

struct s3_export_prefetcher {
	struct s3_export_dev      *dev;
	uint64_t                  *chunks;
	uint64_t                   n;
	uint64_t                   next;       /* next index into chunks to launch */
	uint64_t                   released;   /* chunks[0..released) are released */
	uint32_t                   window;
	uint32_t                   inflight;   /* launched, fetch not completed */
	uint64_t                   fetched;
	uint64_t                   skipped;
	int                        status;
	bool                       pumping;
	bool                       stopped;
	s3_export_prefetch_cb      cb;
	void                      *cb_arg;
	/* chunks[i] holds a pin on pin_slot[i] while released <= i < next and
	 * pin_slot[i] is set. pin_gen[i] is the slot's gen when pinned, so a slot
	 * that has since been freed and reused is never unpinned by mistake. */
	struct export_slot       **pin_slot;
	uint64_t                  *pin_gen;
};

static uint32_t
pf_held(const struct s3_export_prefetcher *p)
{
	return (uint32_t)(p->next - p->released);
}

static void s3_export_prefetcher_pump_internal(struct s3_export_prefetcher *p);

static void
pf_fetched(struct s3_export_prefetcher *p, uint64_t chunk_index, int status)
{
	assert(p->inflight > 0);
	p->inflight--;
	if (status == 0) {
		p->fetched++;
	} else if (status == -ENOENT) {
		/* The object went away: the source materialised the export. Not a
		 * prefetch failure -- the demand read that needs this chunk takes the
		 * 404, refetches the manifest and retries (export_io_complete). */
		p->skipped++;
	} else if (p->status == 0) {
		p->status = status;
	}
	if (p->cb) {
		p->cb(p->cb_arg, chunk_index, status);
	}
}

static void
export_pf_slot_done(struct export_pf_wait *w, int status)
{
	struct s3_export_prefetcher *p = w->p;
	uint64_t chunk_index = w->chunk_index;

	free(w);
	pf_fetched(p, chunk_index, status);
	if (p->status == 0) {
		s3_export_prefetcher_pump_internal(p);
	}
}

struct pf_ready_msg {
	struct s3_export_prefetcher *p;
	uint64_t                     chunk_index;
};

static void
pf_ready_msg(void *arg)
{
	struct pf_ready_msg *m = arg;
	struct s3_export_prefetcher *p = m->p;
	uint64_t chunk_index = m->chunk_index;

	free(m);
	pf_fetched(p, chunk_index, 0);
	if (p->status == 0) {
		s3_export_prefetcher_pump_internal(p);
	}
}

/* Pin chunk i of the list, fetching it if needed. 0, or an error that stops the
 * prefetcher. */
static int
pf_launch_one(struct s3_export_prefetcher *p, uint64_t i)
{
	struct s3_export_dev *dev = p->dev;
	struct s3_export_manifest *m = dev->m;
	uint64_t chunk_index = p->chunks[i];
	char key[S3_EXPORT_KEY_MAX];
	uint32_t object_len = m->chunk_size;
	struct export_slot *s;
	struct export_pf_wait *w;
	int rc;

	if (!s3_export_manifest_is_present(m, chunk_index)) {
		/* A hole needs no fetch; count it as done without a pin. */
		p->pin_slot[i] = NULL;
		return 0;
	}
	if (m->layout == S3_EXPORT_LAYOUT_REF) {
		const struct s3_export_ref *ref = s3_export_manifest_get_ref(m, chunk_index);

		if (!ref || s3_export_manifest_chunk_prefix(m, chunk_index)[0] == '\0') {
			return -EIO;
		}
		object_len = ref->valid_bytes;
	}
	export_chunk_key(m, chunk_index, key, sizeof(key));
	if (key[0] == '\0' || object_len == 0) {
		p->pin_slot[i] = NULL;
		return 0;
	}

	s = export_slot_find(dev, chunk_index, key);
	if (s) {
		s->pins++;
		p->pin_slot[i] = s;
		p->pin_gen[i] = s->gen;
		p->inflight++;
		if (s->state == EXPORT_SLOT_FILLED) {
			struct pf_ready_msg *msg = calloc(1, sizeof(*msg));

			dev->stats.slot_hits++;
			if (!msg) {
				p->inflight--;
				s->pins--;
				p->pin_slot[i] = NULL;
				return -ENOMEM;
			}
			msg->p = p;
			msg->chunk_index = chunk_index;
			/* Deferred so the callback never runs inside pump. */
			spdk_thread_send_msg(spdk_get_thread(), pf_ready_msg, msg);
			return 0;
		}
		w = calloc(1, sizeof(*w));
		if (!w) {
			p->inflight--;
			s->pins--;
			p->pin_slot[i] = NULL;
			return -ENOMEM;
		}
		w->p = p;
		w->chunk_index = chunk_index;
		dev->stats.slot_joins++;
		TAILQ_INSERT_TAIL(&s->pf_waiters, w, link);
		return 0;
	}

	s = export_slot_alloc(dev, true, p->window);
	if (!s) {
		/* The window is at most S3_EXPORT_PREFETCH_MAX, and every pinned slot is
		 * one of those, so this means the table is full of pins. */
		return -EAGAIN;
	}
	w = calloc(1, sizeof(*w));
	if (!w) {
		return -ENOMEM;
	}
	w->p = p;
	w->chunk_index = chunk_index;
	export_slot_setup(s, chunk_index, key, object_len);
	s->pins = 1;
	TAILQ_INSERT_TAIL(&s->pf_waiters, w, link);
	p->pin_slot[i] = s;
	p->pin_gen[i] = s->gen;
	p->inflight++;
	dev->stats.prefetch_fetches++;
	rc = export_slot_start_fetch(dev, s);
	if (rc != 0) {
		TAILQ_REMOVE(&s->pf_waiters, w, link);
		free(w);
		s->pins = 0;
		p->pin_slot[i] = NULL;
		p->inflight--;
		return rc;
	}
	return 0;
}

static void
s3_export_prefetcher_pump_internal(struct s3_export_prefetcher *p)
{
	if (p->pumping) {
		return;
	}
	p->pumping = true;
	while (p->status == 0 && !p->stopped && p->next < p->n &&
	       pf_held(p) < p->window) {
		uint64_t i = p->next++;
		int rc = pf_launch_one(p, i);

		if (rc == -EAGAIN) {
			p->next--;
			break;
		}
		if (rc != 0) {
			/* Recorded, not reported through cb: this runs inside pump,
			 * and a consumer is allowed to free p from its callback. The
			 * consumer reads it through get_progress(). */
			SPDK_ERRLOG("export prefetch: could not start chunk %" PRIu64
				    ": %s\n", p->chunks[i], spdk_strerror(-rc));
			p->status = rc;
			break;
		}
	}
	p->pumping = false;
}

static void
pf_unpin(struct s3_export_prefetcher *p, uint64_t i)
{
	struct export_slot *s = p->pin_slot[i];

	if (!s) {
		return;
	}
	p->pin_slot[i] = NULL;
	/* A failed fetch frees its slot (pins dropped to 0) and the slot may have
	 * been reused since; only the pin this prefetcher took is given back. */
	if (s->gen == p->pin_gen[i] && s->state != EXPORT_SLOT_FREE && s->pins > 0) {
		s->pins--;
	}
}

int
s3_export_prefetcher_create(struct spdk_bs_dev *bs_dev, const uint64_t *chunks,
			    uint64_t n, uint32_t window, s3_export_prefetch_cb cb,
			    void *cb_arg, struct s3_export_prefetcher **out)
{
	struct s3_export_dev *dev = (struct s3_export_dev *)bs_dev;
	struct s3_export_prefetcher *p;

	if (!s3_export_bs_dev_is_export(bs_dev) || !out || (n > 0 && !chunks)) {
		return -EINVAL;
	}
	if (!export_on_home(dev)) {
		/* The slot table belongs to home_thread. */
		return -EXDEV;
	}
	if (window == 0) {
		window = S3_EXPORT_PREFETCH_DEFAULT;
	}
	if (window > S3_EXPORT_PREFETCH_MAX) {
		window = S3_EXPORT_PREFETCH_MAX;
	}
	p = calloc(1, sizeof(*p));
	if (!p) {
		return -ENOMEM;
	}
	p->chunks = calloc(n ? n : 1, sizeof(*p->chunks));
	p->pin_slot = calloc(n ? n : 1, sizeof(*p->pin_slot));
	p->pin_gen = calloc(n ? n : 1, sizeof(*p->pin_gen));
	if (!p->chunks || !p->pin_slot || !p->pin_gen) {
		free(p->chunks);
		free(p->pin_slot);
		free(p->pin_gen);
		free(p);
		return -ENOMEM;
	}
	if (n > 0) {
		memcpy(p->chunks, chunks, n * sizeof(*chunks));
	}
	p->dev = dev;
	p->n = n;
	p->window = window;
	p->cb = cb;
	p->cb_arg = cb_arg;
	*out = p;
	return 0;
}

void
s3_export_prefetcher_pump(struct s3_export_prefetcher *p)
{
	if (p) {
		s3_export_prefetcher_pump_internal(p);
	}
}

void
s3_export_prefetcher_release_below(struct s3_export_prefetcher *p, uint64_t chunk_bound)
{
	if (!p) {
		return;
	}
	while (p->released < p->next && p->chunks[p->released] < chunk_bound) {
		pf_unpin(p, p->released);
		p->released++;
	}
	if (p->status == 0) {
		s3_export_prefetcher_pump_internal(p);
	}
}

void
s3_export_prefetcher_get_progress(const struct s3_export_prefetcher *p,
				  struct s3_export_prefetch_progress *out)
{
	if (!p || !out) {
		return;
	}
	out->total = p->n;
	out->launched = p->next;
	out->fetched = p->fetched;
	out->skipped = p->skipped;
	out->released = p->released;
	out->inflight = p->inflight;
	out->held = pf_held(p);
	out->status = p->status;
}

void
s3_export_prefetcher_stop(struct s3_export_prefetcher *p)
{
	if (p) {
		p->stopped = true;
	}
}

bool
s3_export_prefetcher_idle(const struct s3_export_prefetcher *p)
{
	return !p || p->inflight == 0;
}

void
s3_export_prefetcher_destroy(struct s3_export_prefetcher *p)
{
	uint64_t i;

	if (!p) {
		return;
	}
	/* The caller waited for idle: no slot still has a waiter naming p. */
	assert(p->inflight == 0);
	for (i = p->released; i < p->next; i++) {
		pf_unpin(p, i);
	}
	free(p->chunks);
	free(p->pin_slot);
	free(p->pin_gen);
	free(p);
}
