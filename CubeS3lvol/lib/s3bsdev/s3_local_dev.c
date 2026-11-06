/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_local_dev -- super block and region layout on the local bdev 
 *
 *   Design notes live in include/s3lvol/s3_local_dev.h.
 */

#include "spdk/stdinc.h"
#include "spdk/bdev.h"
#include "spdk/crc32.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/thread.h"

#include "s3lvol/s3_local_dev.h"
#include "s3lvol/s3_recovery.h"

struct s3_local_dev {
	/* WAL bdev: super + journal + WAL ring. In a single-bdev layout the
	 * cache lives here too. */
	struct spdk_bdev_desc   *wal_desc;
	struct spdk_io_channel  *wal_ch;

	/* True when wal_desc came from a claim rather than from an open here.
	 * Such a descriptor is closed by the claim, never by s3_local_dev_free.
	 * It is set even on the paths where the claim has not been adopted yet,
	 * so a failed format does not close a descriptor the caller still owns. */
	bool                     wal_desc_borrowed;

	/* Adopted at successful format completion; NULL until then and NULL on
	 * every failure path. Ownership then lasts until s3_local_dev_close()
	 * has put both channels. */
	struct s3_recovery_claim *wal_claim;

	/* Cache bdev in a dual-bdev layout; NULL when single. */
	struct spdk_bdev_desc   *cache_desc;
	struct spdk_io_channel  *cache_ch;
	bool                     cache_desc_borrowed;
	/* Adopted with wal_claim; a cache claim is only accepted together with
	 * a WAL claim for a distinct device. */
	struct s3_recovery_claim *cache_claim;

	bool                     dual_bdev;

	struct s3_region         regions[S3_REGION_COUNT];
	struct s3_super_block    super;

	/* True between s3_local_dev_format_staged() and a successful
	 * s3_local_dev_commit_staged(). While set, the on-disk super block is
	 * the staging one and the device is not attachable. It is also the
	 * guard that stops a commit from being issued on a device that was
	 * never staged, which would otherwise publish a normal block over a
	 * layout whose rebuild steps never ran. */
	bool                     staged;

	/* Set while a commit is in flight, cleared when its callback fires.
	 *
	 * A commit is a multi-step sequence (preflush, super write, postflush)
	 * that mutates dev->super and dev->staged. Two concurrent commits would
	 * interleave those steps and could publish a block whose CRC was
	 * computed from a half-updated struct, so a second caller is refused
	 * with -EBUSY rather than queued. The recovery chain is the only caller
	 * and it is serialised by its own state machine, but the guard is here
	 * because "only one caller" is an assumption, not a property of this
	 * object. */
	bool                     commit_in_flight;

	/* Thread that opened the bdevs. Channels are per-thread, so using them
	 * from another thread is a bug. */
	struct spdk_thread      *owner_thread;
};

/* ==========================================================================
 * Super block CRC
 * ========================================================================== */

static uint32_t
s3_super_calc_crc(const struct s3_super_block *sb)
{
	struct s3_super_block tmp;

	/* Treat the crc field itself as zero, otherwise verification could
	 * never reproduce the same value. */
	memcpy(&tmp, sb, sizeof(tmp));
	tmp.crc = 0;

	return spdk_crc32c_update(&tmp, sizeof(tmp), ~0u);
}

/* ==========================================================================
 * There are no synchronous I/O helpers here, and none should be added back
 *
 * This file used to have a sync_write()/sync_read() pair that submitted a bdev
 * I/O and then spun in place: `while (!done) spdk_thread_poll()`. Both are
 * gone.
 *
 * They were removed not because they were slow, but because *nesting
 * spdk_thread_poll() corrupts the spdk_thread state machine*. The full
 * analysis is in the header comment of include/s3lvol/s3_journal.h; in short:
 *
 *   - The outer reactor is already iterating active_pollers; the inner poll
 *     runs that same set again. A poller's state gets flipped to RUNNING by
 *     the inner pass, and when the outer pass resumes it hits
 *     `default: assert(false)` in thread_execute_poller(). Release builds are
 *     compiled with -DNDEBUG, so the assert vanishes and it degrades into
 *     silent state-machine corruption.
 *   - Completion on the local bdev itself depends on a poller (aio relies on
 *     bdev_aio_group_poll). If the nesting happens inside that poller's own
 *     call stack it is already RUNNING, so the inner pass cannot run it --
 *     the completion never arrives and the while loop spins forever.
 *
 * One might argue that format/open are only called from the control plane,
 * which is not a reactor callback, so it would be safe there. Three reasons
 * not to keep them anyway:
 *
 *   1. update_checkpoint() genuinely runs on the data plane (when a
 *      checkpoint completes) and shared the same helpers -- keeping them
 *      guarantees the data plane ends up using them.
 *   2. RPC handlers themselves run on a reactor (spdk_rpc_accept is a poller
 *      in app.c), so "control plane == not a reactor" does not hold in SPDK.
 *   3. With a synchronous variant within reach, someone will eventually call
 *      it from the data plane, and this class of bug does not show up in unit
 *      tests -- a unit test has a single thread and no other pollers, which
 *      happens to be the only situation where nested polling is safe.
 * ========================================================================== */


/* ==========================================================================
 * Opening and closing bdevs
 * ========================================================================== */

static void
s3_local_dev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		      void *event_ctx)
{
	(void)event_ctx;

	/* REMOVE means the underlying bdev was yanked. We cannot clean up in
	 * place here -- the journal or WAL may still have I/O in flight. Log
	 * it and let the upper layer finish through the normal detach path. */
	SPDK_WARNLOG("Local bdev '%s' event %d; s3lvol may lose its WAL/cache\n",
		     spdk_bdev_get_name(bdev), type);
}

static void
s3_local_dev_free(struct s3_local_dev *dev)
{
	if (!dev) {
		return;
	}

	/* Channels first, descriptors second. A claimed descriptor is closed by
	 * s3_recovery_claim_release(), so the order here is what guarantees the
	 * claim outlives every consumer of the channel: the journal, the WAL and
	 * the cache all hold pointers into this object and submit I/O through
	 * these channels. Releasing before the channels are put would close the
	 * descriptor under live I/O. */
	if (dev->cache_ch) {
		spdk_put_io_channel(dev->cache_ch);
		dev->cache_ch = NULL;
	}
	if (dev->wal_ch) {
		spdk_put_io_channel(dev->wal_ch);
		dev->wal_ch = NULL;
	}
	if (dev->cache_claim) {
		/* The claim owns the descriptor; do not close it separately. */
		dev->cache_desc = NULL;
		dev->cache_desc_borrowed = false;
		s3_recovery_claim_release(dev->cache_claim);
		dev->cache_claim = NULL;
	} else if (dev->cache_desc && !dev->cache_desc_borrowed) {
		spdk_bdev_close(dev->cache_desc);
		dev->cache_desc = NULL;
	}
	if (dev->wal_claim) {
		dev->wal_desc = NULL;
		dev->wal_desc_borrowed = false;
		s3_recovery_claim_release(dev->wal_claim);
		dev->wal_claim = NULL;
	} else if (dev->wal_desc && !dev->wal_desc_borrowed) {
		spdk_bdev_close(dev->wal_desc);
		dev->wal_desc = NULL;
	}
	free(dev);
}

bool
s3_local_dev_owns_claim(struct s3_local_dev *dev,
			struct s3_recovery_claim *claim)
{
	if (!dev || !claim) {
		return false;
	}
	return dev->wal_claim == claim || dev->cache_claim == claim;
}

/* Adopt, do not open, each device that comes in claimed. A claim owns its
 * descriptor and upgrades it to writable, so an adopted device is used exactly
 * as an opened one but is never closed here while the claim is still the
 * caller's. */
static void
s3_local_dev_adopt_desc(struct s3_local_dev *dev,
			struct s3_recovery_claim *claim, bool cache)
{
	struct spdk_bdev_desc *desc = s3_recovery_claim_desc(claim);

	if (cache) {
		dev->cache_desc = desc;
		dev->cache_desc_borrowed = true;
	} else {
		dev->wal_desc = desc;
		dev->wal_desc_borrowed = true;
	}
}

static int
s3_local_dev_open_bdevs(struct s3_local_dev *dev, const char *wal_name,
			const char *cache_name,
			struct s3_recovery_claim *wal_claim,
			struct s3_recovery_claim *cache_claim)
{
	int rc;

	if (wal_claim) {
		s3_local_dev_adopt_desc(dev, wal_claim, false);
	} else {
		rc = spdk_bdev_open_ext(wal_name, true, s3_local_dev_event_cb,
					dev, &dev->wal_desc);
		if (rc != 0) {
			SPDK_ERRLOG("Could not open WAL bdev '%s': %s\n",
				    wal_name, spdk_strerror(-rc));
			return rc;
		}
	}
	dev->wal_ch = spdk_bdev_get_io_channel(dev->wal_desc);
	if (!dev->wal_ch) {
		SPDK_ERRLOG("Could not get IO channel for '%s'\n", wal_name);
		return -ENOMEM;
	}

	/* The block size must divide 4 KiB evenly -- both the super block and
	 * the journal are organized in 4 KiB blocks (P4 fixes block_size). */
	struct spdk_bdev *bdev = spdk_bdev_desc_get_bdev(dev->wal_desc);
	uint32_t bs = spdk_bdev_get_block_size(bdev);

	if (S3_SUPER_SIZE % bs != 0) {
		SPDK_ERRLOG("WAL bdev '%s' block size %u does not divide "
			    "%d\n", wal_name, bs, S3_SUPER_SIZE);
		return -EINVAL;
	}

	if (cache_name && strcmp(cache_name, wal_name) != 0) {
		if (cache_claim) {
			/* The formatter must consume the retained descriptor.
			 * Opening the cache writable here would fail with
			 * -EPERM, because the claim the caller took on that
			 * device forbids exactly this open. */
			s3_local_dev_adopt_desc(dev, cache_claim, true);
		} else {
			rc = spdk_bdev_open_ext(cache_name, true,
						s3_local_dev_event_cb, dev,
						&dev->cache_desc);
			if (rc != 0) {
				SPDK_ERRLOG("Could not open cache bdev '%s': %s\n",
					    cache_name, spdk_strerror(-rc));
				return rc;
			}
		}
		dev->cache_ch = spdk_bdev_get_io_channel(dev->cache_desc);
		if (!dev->cache_ch) {
			return -ENOMEM;
		}
		dev->dual_bdev = true;
	}

	return 0;
}

/* ==========================================================================
 * Formatting
 * ========================================================================== */

static uint64_t
bdev_capacity(struct spdk_bdev_desc *desc)
{
	struct spdk_bdev *bdev = spdk_bdev_desc_get_bdev(desc);

	return spdk_bdev_get_num_blocks(bdev) * spdk_bdev_get_block_size(bdev);
}

/* ==========================================================================
 * Flush
 *
 * The staged protocol has to be able to say "this write is durable" before it
 * reports success, because the whole point is that a crash after the callback
 * cannot leave a state the callback said was complete. spdk_bdev_flush() is the
 * only way to ask for that.
 *
 * A bdev that does not implement flush returns -ENOTSUP. That is treated as
 * success: there is nothing to flush *to*, and failing the format would make
 * the protocol unusable on such a device while proving nothing. It is logged,
 * because "we could not flush" is exactly the kind of thing that must not be
 * silent when someone later asks whether the commit was durable.
 * ========================================================================== */

struct local_dev_flush_ctx {
	s3_local_dev_cb  cb_fn;
	void            *cb_arg;
	const char      *what;
};

static void
flush_done(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct local_dev_flush_ctx *ctx = cb_arg;
	s3_local_dev_cb cb_fn = ctx->cb_fn;
	void *user_arg = ctx->cb_arg;
	const char *what = ctx->what;

	spdk_bdev_free_io(bdev_io);
	free(ctx);

	if (!success) {
		SPDK_ERRLOG("Flush after %s failed\n", what);
	}
	if (cb_fn) {
		cb_fn(user_arg, success ? 0 : -EIO);
	}
}

/* Flush the WAL device (which is also the cache device in a single-bdev
 * layout, and the only device the super block ever lives on). */
static void
local_dev_flush(struct s3_local_dev *dev, const char *what,
		s3_local_dev_cb cb_fn, void *cb_arg)
{
	struct local_dev_flush_ctx *ctx;
	int rc;

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		if (cb_fn) {
			cb_fn(cb_arg, -ENOMEM);
		}
		return;
	}
	ctx->cb_fn  = cb_fn;
	ctx->cb_arg = cb_arg;
	ctx->what   = what;

	rc = spdk_bdev_flush(dev->wal_desc, dev->wal_ch, 0,
			     bdev_capacity(dev->wal_desc), flush_done, ctx);
	/* Staging and publication require a successful durability barrier.
	 * Unsupported flush is an error, not evidence of durable completion. */
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit flush after %s: %d\n", what, rc);
		free(ctx);
		if (cb_fn) {
			cb_fn(cb_arg, rc);
		}
	}
}

/* Context shared by format's two writes.
 *
 * The dev is fully built before the first write is submitted (bdevs opened,
 * layout computed, super block assembled), but it is *not handed to the caller
 * until the callback fires* -- on a mid-way failure this context frees it, so
 * the caller either gets a usable dev or NULL. */
struct local_dev_format_ctx {
	struct s3_local_dev     *dev;
	void                    *buf;
	s3_local_dev_open_cb     cb_fn;
	void                    *cb_arg;
	char                    *wal_name;
	char                    *cache_name;

	/* Caller-owned until format_ctx_finish(0) adopts them into dev. */
	struct s3_recovery_claim *wal_claim;
	struct s3_recovery_claim *cache_claim;

	/* True for s3_local_dev_format_staged(): write the staging super block
	 * and flush both writes before reporting success. False for the
	 * historical s3_local_dev_format(), whose behaviour is unchanged. */
	bool                     staged;
};

/* A REMOVE or RESIZE latched on either claim means the devices the blank proof
 * covered are no longer the devices a write would reach. Checked before every
 * write submission, not only once, because the format has two writes and an
 * event can arrive between them. */
static int
format_claim_event_error(struct local_dev_format_ctx *ctx)
{
	int rc = 0;

	if (ctx->wal_claim) {
		rc = s3_recovery_claim_error(ctx->wal_claim);
	}
	if (rc == 0 && ctx->cache_claim) {
		rc = s3_recovery_claim_error(ctx->cache_claim);
	}
	return rc;
}

static void
format_ctx_finish(struct local_dev_format_ctx *ctx, int status)
{
	struct s3_local_dev *dev = ctx->dev;
	s3_local_dev_open_cb cb_fn = ctx->cb_fn;
	void *cb_arg = ctx->cb_arg;

	if (status == 0) {
		status = format_claim_event_error(ctx);
	}
	/* Transfer before freeing the temporary context. A failed final write
	 * or a latched device event leaves the claims with the caller. */
	if (status == 0) {
		dev->wal_claim = ctx->wal_claim;
		dev->cache_claim = ctx->cache_claim;
		/* Only now is the device really staged: the staging block and the
		 * cleared journal head are on the device and flushed. Setting this
		 * earlier would let a caller believe a device it is about to lose
		 * is staged. */
		dev->staged = ctx->staged;
	}

	if (ctx->buf) {
		spdk_dma_free(ctx->buf);
	}
	free(ctx->wal_name);
	free(ctx->cache_name);
	free(ctx);

	if (status != 0) {
		/* Claims are still the caller's: dev did not adopt them, and
		 * s3_local_dev_free does not touch a claim (only the borrowed
		 * descriptors are left alone). */
		s3_local_dev_free(dev);
		cb_fn(cb_arg, NULL, status);
		return;
	}

	/* Safe completion: the layout is durable, the caller is about to get a
	 * live object, and neither claim will be handed back. Ownership moves
	 * here so the claims live exactly as long as the object whose journal,
	 * WAL and cache use those descriptors. Every later teardown, including
	 * the setup failure paths, releases them from s3_local_dev_close(). */
	cb_fn(cb_arg, dev, 0);
}

static void
format_log_layout(struct local_dev_format_ctx *ctx)
{
	struct s3_local_dev *dev = ctx->dev;

	SPDK_NOTICELOG("Formatted %s local layout on '%s'%s%s:\n",
		       ctx->staged ? "STAGED (not attachable)" : "",
		       ctx->wal_name,
		       dev->dual_bdev ? " + " : "",
		       dev->dual_bdev && ctx->cache_name ? ctx->cache_name : "");
	SPDK_NOTICELOG("  super   @ %" PRIu64 " (%" PRIu64 " B)\n",
		       dev->regions[S3_REGION_SUPER].offset,
		       dev->regions[S3_REGION_SUPER].size);
	SPDK_NOTICELOG("  journal @ %" PRIu64 " (%" PRIu64 " MiB)\n",
		       dev->regions[S3_REGION_JOURNAL].offset,
		       dev->regions[S3_REGION_JOURNAL].size / (1024 * 1024));
	SPDK_NOTICELOG("  wal     @ %" PRIu64 " (%" PRIu64 " MiB)\n",
		       dev->regions[S3_REGION_WAL].offset,
		       dev->regions[S3_REGION_WAL].size / (1024 * 1024));
	SPDK_NOTICELOG("  cache   @ %" PRIu64 " (%" PRIu64 " MiB)%s\n",
		       dev->regions[S3_REGION_CACHE].offset,
		       dev->regions[S3_REGION_CACHE].size / (1024 * 1024),
		       dev->dual_bdev ? " [separate bdev]" : "");
}

/* The journal head is clear. On the staged path this is the last write before
 * the caller is told the device is staged, so it is flushed here: the caller
 * must not be able to observe "staged" and then have the staging block or the
 * cleared journal head still sitting in a volatile cache. */
static void
format_staged_flushed(void *cb_arg, int status)
{
	struct local_dev_format_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("Failed to flush the staged layout: %d\n", status);
		format_ctx_finish(ctx, status);
		return;
	}

	format_log_layout(ctx);
	format_ctx_finish(ctx, 0);
}

static void
format_journal_cleared(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct local_dev_format_ctx *ctx = cb_arg;

	spdk_bdev_free_io(bdev_io);

	if (!success) {
		SPDK_ERRLOG("Failed to clear journal head\n");
		format_ctx_finish(ctx, -EIO);
		return;
	}

	if (ctx->staged) {
		local_dev_flush(ctx->dev, "the staged layout",
				format_staged_flushed, ctx);
		return;
	}

	format_log_layout(ctx);
	format_ctx_finish(ctx, 0);
}

static void
format_super_written(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct local_dev_format_ctx *ctx = cb_arg;
	struct s3_local_dev *dev = ctx->dev;
	int rc;

	spdk_bdev_free_io(bdev_io);

	if (!success) {
		SPDK_ERRLOG("Failed to write super block\n");
		format_ctx_finish(ctx, -EIO);
		return;
	}

	/* Check again before the second write. An event between the two writes
	 * must stop the format before it clears the journal head; only logging
	 * it would let the caller report a clean format on a device that is
	 * gone or resized. */
	rc = format_claim_event_error(ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Aborting format after the super block: a claimed "
			    "device reported an event (%d)\n", rc);
		format_ctx_finish(ctx, rc);
		return;
	}

	/* Zero the journal head so a scan can tell "there are no records here".
	 * Only the first block is cleared -- replay stops at the first invalid
	 * record, so there is no need to wipe the whole region (it can be
	 * hundreds of MiB, which would make formatting very slow). */
	memset(ctx->buf, 0, S3_SUPER_SIZE);

	rc = spdk_bdev_write(dev->wal_desc, dev->wal_ch, ctx->buf,
			     dev->regions[S3_REGION_JOURNAL].offset,
			     S3_SUPER_SIZE, format_journal_cleared, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit journal head clear: %d\n", rc);
		format_ctx_finish(ctx, rc);
	}
}

static void
local_dev_format_common(const struct s3_local_dev_format_opts *opts,
			bool staged, s3_local_dev_open_cb cb_fn, void *cb_arg)
{
	struct local_dev_format_ctx *ctx;
	struct s3_local_dev *dev;
	uint64_t wal_cap, offset;
	uint64_t journal_size, wal_size;
	int rc;

	assert(cb_fn != NULL);

	if (!opts || !opts->wal_bdev_name || !opts->lvs_name ||
	    opts->capacity_bytes == 0) {
		cb_fn(cb_arg, NULL, -EINVAL);
		return;
	}
	/* Truncating the name would produce a disk that fails its own attach
	 * check for no visible reason, so refuse it here. */
	if (strlen(opts->lvs_name) >= S3_LVS_NAME_MAX) {
		SPDK_ERRLOG("lvstore name '%s' does not fit in the super block "
			    "(max %d characters)\n", opts->lvs_name,
			    S3_LVS_NAME_MAX - 1);
		cb_fn(cb_arg, NULL, -ENAMETOOLONG);
		return;
	}

	journal_size = opts->journal_size ? opts->journal_size :
		       S3_JOURNAL_DEFAULT_SIZE;
	wal_size = opts->wal_size ? opts->wal_size : S3_WAL_DEFAULT_SIZE;

	/* Round both up to 4 KiB so every region offset below stays block
	 * aligned. */
	journal_size = SPDK_ALIGN_CEIL(journal_size, S3_SUPER_SIZE);
	wal_size     = SPDK_ALIGN_CEIL(wal_size, S3_SUPER_SIZE);

	dev = calloc(1, sizeof(*dev));
	if (!dev) {
		cb_fn(cb_arg, NULL, -ENOMEM);
		return;
	}
	dev->owner_thread = spdk_get_thread();

	/* Retained claims are only usable if they belong to the devices the
	 * caller named, and if the two of them describe one layout unambiguously.
	 * Every check below is fatal: formatting through the wrong descriptor
	 * would write a super block onto a device the blank proof never covered.
	 *
	 * Writability is deliberately not re-checked. SPDK exposes no public
	 * accessor for it, and the only way to obtain a retained descriptor is
	 * s3_recovery_claim_open(), which upgrades it as part of granting the
	 * claim. A read-only descriptor reaching here would fail the first
	 * spdk_bdev_write() with -EPERM, which is the same outcome, one step
	 * later. */
	if (opts->cache_claim && !opts->wal_claim) {
		SPDK_ERRLOG("A cache claim requires a WAL claim; refusing to "
			    "format a half-claimed layout\n");
		s3_local_dev_free(dev);
		cb_fn(cb_arg, NULL, -EINVAL);
		return;
	}
	if (opts->wal_claim) {
		struct spdk_bdev_desc *desc =
			s3_recovery_claim_desc(opts->wal_claim);
		struct spdk_bdev *bdev = desc ?
					 spdk_bdev_desc_get_bdev(desc) : NULL;

		if (!desc || !bdev ||
		    strcmp(spdk_bdev_get_name(bdev),
			   opts->wal_bdev_name) != 0) {
			SPDK_ERRLOG("The retained claim does not belong to "
				    "'%s'; refusing to format through it\n",
				    opts->wal_bdev_name);
			s3_local_dev_free(dev);
			cb_fn(cb_arg, NULL, -EINVAL);
			return;
		}
	}
	if (opts->cache_claim) {
		struct spdk_bdev_desc *desc =
			s3_recovery_claim_desc(opts->cache_claim);
		struct spdk_bdev *bdev = desc ?
					 spdk_bdev_desc_get_bdev(desc) : NULL;

		if (!opts->cache_bdev_name) {
			SPDK_ERRLOG("A cache claim was supplied but no cache "
				    "bdev was named\n");
			s3_local_dev_free(dev);
			cb_fn(cb_arg, NULL, -EINVAL);
			return;
		}
		if (!desc || !bdev ||
		    strcmp(spdk_bdev_get_name(bdev),
			   opts->cache_bdev_name) != 0) {
			SPDK_ERRLOG("The retained cache claim does not belong "
				    "to '%s'; refusing to format through it\n",
				    opts->cache_bdev_name);
			s3_local_dev_free(dev);
			cb_fn(cb_arg, NULL, -EINVAL);
			return;
		}
		/* Compare the devices, not the names. Two names can alias one
		 * bdev, and adopting the same claim twice would release it
		 * twice. A single-bdev layout that names the same device twice
		 * is likewise ambiguous with a cache claim. */
		if (s3_recovery_claim_bdev(opts->wal_claim) == bdev) {
			SPDK_ERRLOG("WAL and cache claims refer to the same "
				    "bdev; a cache claim must name a distinct "
				    "device\n");
			s3_local_dev_free(dev);
			cb_fn(cb_arg, NULL, -EINVAL);
			return;
		}
	}

	/* The claimed formatter must not reopen anything: a writable open on a
	 * claimed bdev fails -EPERM, so a dual-bdev recovery that reopened the
	 * cache would fail exactly where this path is supposed to work. */
	if (opts->wal_claim && opts->cache_bdev_name &&
	    strcmp(opts->cache_bdev_name, opts->wal_bdev_name) != 0 &&
	    !opts->cache_claim) {
		SPDK_ERRLOG("Dual-bdev format through a retained claim needs a "
			    "retained cache claim too; refusing to reopen '%s'\n",
			    opts->cache_bdev_name);
		s3_local_dev_free(dev);
		cb_fn(cb_arg, NULL, -EINVAL);
		return;
	}

	rc = s3_local_dev_open_bdevs(dev, opts->wal_bdev_name,
				     opts->cache_bdev_name, opts->wal_claim,
				     opts->cache_claim);
	if (rc != 0) {
		s3_local_dev_free(dev);
		cb_fn(cb_arg, NULL, rc);
		return;
	}

	wal_cap = bdev_capacity(dev->wal_desc);

	/* Layout: super -> journal -> WAL ring -> (single bdev only) cache */
	offset = 0;
	dev->regions[S3_REGION_SUPER].offset = offset;
	dev->regions[S3_REGION_SUPER].size   = S3_SUPER_SIZE;
	offset += S3_SUPER_SIZE;

	dev->regions[S3_REGION_JOURNAL].offset = offset;
	dev->regions[S3_REGION_JOURNAL].size   = journal_size;
	offset += journal_size;

	dev->regions[S3_REGION_WAL].offset = offset;
	dev->regions[S3_REGION_WAL].size   = wal_size;
	offset += wal_size;

	if (offset > wal_cap) {
		SPDK_ERRLOG("WAL bdev '%s' too small: need %" PRIu64 " bytes for "
			    "super+journal+WAL, have %" PRIu64 "\n",
			    opts->wal_bdev_name, offset, wal_cap);
		s3_local_dev_free(dev);
		cb_fn(cb_arg, NULL, -ENOSPC);
		return;
	}

	if (dev->dual_bdev) {
		dev->regions[S3_REGION_CACHE].offset = 0;
		dev->regions[S3_REGION_CACHE].size   = bdev_capacity(dev->cache_desc);
	} else {
		/* Single bdev: the cache takes whatever is left after the WAL
		 * ring. */
		dev->regions[S3_REGION_CACHE].offset = offset;
		dev->regions[S3_REGION_CACHE].size   = wal_cap - offset;
		if (dev->regions[S3_REGION_CACHE].size < opts->chunk_size) {
			SPDK_WARNLOG("No room left for chunk cache on '%s' "
				     "(%" PRIu64 " bytes); cache disabled\n",
				     opts->wal_bdev_name,
				     dev->regions[S3_REGION_CACHE].size);
			dev->regions[S3_REGION_CACHE].size = 0;
		}
	}

	for (int i = 0; i < S3_REGION_COUNT; i++) {
		dev->regions[i].valid = dev->regions[i].size > 0;
	}

	/* Assemble the super block.
	 *
	 * The staged variant differs in exactly two fields -- magic and version
	 * -- and in nothing else. That is deliberate: the layout, the geometry
	 * and the identity are identical, so the commit is a pure validity
	 * transition and cannot change what the device means. A reader that
	 * understands only the normal block sees a device it refuses; a reader
	 * that understands both sees the same layout either way. */
	memset(&dev->super, 0, sizeof(dev->super));
	dev->super.magic      = staged ? S3_SUPER_MAGIC_STAGING : S3_SUPER_MAGIC;
	dev->super.version    = staged ? S3_SUPER_VERSION_STAGING :
			       S3_SUPER_VERSION;
	dev->super.block_size = 4096;
	dev->super.chunk_size = opts->chunk_size;
	dev->super.dual_bdev  = dev->dual_bdev ? 1 : 0;
	dev->super.capacity_bytes = opts->capacity_bytes;
	snprintf(dev->super.lvs_name, sizeof(dev->super.lvs_name), "%s",
		 opts->lvs_name);
	/* lvs_uuid stays all-zeroes: blobstore generates it and does not accept
	 * one from outside, so it is written back once spdk_lvs_init() has run.
	 * Zero means "unknown" to the attach path, which is the honest state for
	 * a disk formatted but never paired. */
	for (int i = 0; i < S3_REGION_COUNT; i++) {
		dev->super.regions[i].offset = dev->regions[i].offset;
		dev->super.regions[i].size   = dev->regions[i].size;
	}
	/* A fresh disk has no checkpoint yet. LSN starts at 0 and the journal
	 * is written from the beginning. */
	dev->super.checkpoint_gen = 0;
	dev->super.checkpoint_lsn = 0;
	dev->super.crc = s3_super_calc_crc(&dev->super);

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		s3_local_dev_free(dev);
		cb_fn(cb_arg, NULL, -ENOMEM);
		return;
	}
	ctx->dev    = dev;
	ctx->cb_fn  = cb_fn;
	ctx->cb_arg = cb_arg;
	ctx->wal_claim = opts->wal_claim;
	ctx->cache_claim = opts->cache_claim;
	ctx->staged = staged;
	/* The bdev names are only used for the summary log. *Keep our own
	 * copies*: now that this is asynchronous the callback can run long
	 * after the caller returned, by which point the caller's strings (an
	 * RPC-decoded name, say) are already freed. */
	ctx->wal_name = strdup(opts->wal_bdev_name);
	if (opts->cache_bdev_name) {
		ctx->cache_name = strdup(opts->cache_bdev_name);
	}
	if (!ctx->wal_name || (opts->cache_bdev_name && !ctx->cache_name)) {
		format_ctx_finish(ctx, -ENOMEM);
		return;
	}

	ctx->buf = spdk_dma_zmalloc(S3_SUPER_SIZE, 4096, NULL);
	if (!ctx->buf) {
		format_ctx_finish(ctx, -ENOMEM);
		return;
	}
	memcpy(ctx->buf, &dev->super, sizeof(dev->super));

	/* Nothing is written if a claimed device was removed or resized after
	 * the blank proof. This is the check that makes the window between the
	 * scan and the format meaningful; without it the event would only have
	 * been logged. */
	rc = format_claim_event_error(ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Refusing to format '%s': a claimed device reported "
			    "an event after the blank proof (%d)\n",
			    opts->wal_bdev_name, rc);
		format_ctx_finish(ctx, rc);
		return;
	}

	rc = spdk_bdev_write(dev->wal_desc, dev->wal_ch, ctx->buf,
			     dev->regions[S3_REGION_SUPER].offset, S3_SUPER_SIZE,
			     format_super_written, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit super block write: %d\n", rc);
		format_ctx_finish(ctx, rc);
	}
}

void
s3_local_dev_format(const struct s3_local_dev_format_opts *opts,
		    s3_local_dev_open_cb cb_fn, void *cb_arg)
{
	local_dev_format_common(opts, false, cb_fn, cb_arg);
}

void
s3_local_dev_format_staged(const struct s3_local_dev_format_opts *opts,
			   s3_local_dev_open_cb cb_fn, void *cb_arg)
{
	local_dev_format_common(opts, true, cb_fn, cb_arg);
}

bool
s3_local_dev_is_staged(struct s3_local_dev *dev)
{
	return dev ? dev->staged : false;
}

/* ==========================================================================
	* Opening an existing layout
	* ========================================================================== */

struct local_dev_open_ctx {
	struct s3_local_dev     *dev;
	void                    *buf;
	s3_local_dev_open_cb     cb_fn;
	void                    *cb_arg;
	char                    *wal_name;
	bool                     want_dual;
};

static void
open_ctx_finish(struct local_dev_open_ctx *ctx, int status)
{
	struct s3_local_dev *dev = ctx->dev;
	s3_local_dev_open_cb cb_fn = ctx->cb_fn;
	void *cb_arg = ctx->cb_arg;

	if (ctx->buf) {
		spdk_dma_free(ctx->buf);
	}
	free(ctx->wal_name);
	free(ctx);

	if (status != 0) {
		s3_local_dev_free(dev);
		cb_fn(cb_arg, NULL, status);
		return;
	}

	cb_fn(cb_arg, dev, 0);
}

static void
open_super_read(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct local_dev_open_ctx *ctx = cb_arg;
	struct s3_local_dev *dev = ctx->dev;
	struct s3_super_block *sb;
	uint32_t crc;

	spdk_bdev_free_io(bdev_io);

	if (!success) {
		SPDK_ERRLOG("Failed to read super block from '%s'\n", ctx->wal_name);
		open_ctx_finish(ctx, -EIO);
		return;
	}

	sb = ctx->buf;

	/* A staging block is a *recognised* state, not corruption, and it is
	 * reported as such: -EAGAIN says "this device is mid-rebuild, retry or
	 * recover it", which is actionable, whereas -EILSEQ would say "this is
	 * not ours" and invite a reformat over a device that is being rebuilt.
	 *
	 * The check is on magic *and* version, and it comes before the CRC
	 * check on purpose. A staging block has a valid CRC over its own
	 * contents, so a CRC-first order would pass it through to the version
	 * check and report -EPROTO; that is still a refusal, but the specific
	 * "staged" answer is more useful and does not depend on the version
	 * field being intact. */
	if (sb->magic == S3_SUPER_MAGIC_STAGING ||
	    sb->version == S3_SUPER_VERSION_STAGING) {
		SPDK_ERRLOG("'%s' carries a STAGING super block (magic %" PRIx64
			    ", version %u): a recovery was started and did not "
			    "complete. The device is not attachable; re-run "
			    "recovery or reformat it deliberately\n",
			    ctx->wal_name, sb->magic, sb->version);
		open_ctx_finish(ctx, -EAGAIN);
		return;
	}

	if (sb->magic != S3_SUPER_MAGIC) {
		SPDK_ERRLOG("'%s' has no s3lvol super block (magic %" PRIx64", "
			    "expected %" PRIx64 ") - format it first\n",
			    ctx->wal_name, sb->magic, (uint64_t)S3_SUPER_MAGIC);
		open_ctx_finish(ctx, -EILSEQ);
		return;
	}

	crc = s3_super_calc_crc(sb);
	if (crc != sb->crc) {
		SPDK_ERRLOG("Super block CRC mismatch on '%s' (got %08x, "
			    "computed %08x) - the layout is corrupt\n",
			    ctx->wal_name, sb->crc, crc);
		open_ctx_finish(ctx, -EILSEQ);
		return;
	}

	if (sb->version != S3_SUPER_VERSION) {
		SPDK_ERRLOG("Unsupported super block version %u on '%s' "
			    "(this build understands %d)\n",
			    sb->version, ctx->wal_name, S3_SUPER_VERSION);
		open_ctx_finish(ctx, -EPROTO);
		return;
	}

	/* The on-disk layout says dual-bdev but the caller did not pass a cache
	 * bdev -- the cache region would point at a device that is not open.
	 * Fail loudly instead of blowing up at runtime. */
	if (sb->dual_bdev && !ctx->want_dual) {
		SPDK_ERRLOG("'%s' was formatted with a separate cache bdev; "
			    "pass it too\n", ctx->wal_name);
		open_ctx_finish(ctx, -EINVAL);
		return;
	}
	if (!sb->dual_bdev && ctx->want_dual) {
		SPDK_ERRLOG("'%s' was formatted as a single-bdev layout; "
			    "do not pass a cache bdev\n", ctx->wal_name);
		open_ctx_finish(ctx, -EINVAL);
		return;
	}

	memcpy(&dev->super, sb, sizeof(dev->super));
	/* A CRC-valid super block should already carry a terminator, but the name
	 * is fed to strcmp() and to log formatting, so make it impossible for a
	 * non-terminated one to read past the field. */
	dev->super.lvs_name[sizeof(dev->super.lvs_name) - 1] = '\0';
	for (int i = 0; i < S3_REGION_COUNT; i++) {
		dev->regions[i].offset = sb->regions[i].offset;
		dev->regions[i].size   = sb->regions[i].size;
		dev->regions[i].valid  = sb->regions[i].size > 0;
	}

	SPDK_NOTICELOG("Opened local layout on '%s': lvs='%s', capacity=%" PRIu64
		       " MiB, chunk=%u, ckpt_gen=%" PRIu64 ", ckpt_lsn=%" PRIu64
		       ", journal=%" PRIu64 " MiB\n",
		       ctx->wal_name, dev->super.lvs_name,
		       dev->super.capacity_bytes / (1024 * 1024),
		       dev->super.chunk_size,
		       dev->super.checkpoint_gen, dev->super.checkpoint_lsn,
		       dev->regions[S3_REGION_JOURNAL].size / (1024 * 1024));

	open_ctx_finish(ctx, 0);
}

void
s3_local_dev_open(const char *wal_bdev_name, const char *cache_bdev_name,
		  s3_local_dev_open_cb cb_fn, void *cb_arg)
{
	struct local_dev_open_ctx *ctx;
	struct s3_local_dev *dev;
	int rc;

	assert(cb_fn != NULL);

	if (!wal_bdev_name) {
		cb_fn(cb_arg, NULL, -EINVAL);
		return;
	}

	dev = calloc(1, sizeof(*dev));
	if (!dev) {
		cb_fn(cb_arg, NULL, -ENOMEM);
		return;
	}
	dev->owner_thread = spdk_get_thread();

	rc = s3_local_dev_open_bdevs(dev, wal_bdev_name, cache_bdev_name,
				     NULL, NULL);
	if (rc != 0) {
		s3_local_dev_free(dev);
		cb_fn(cb_arg, NULL, rc);
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		s3_local_dev_free(dev);
		cb_fn(cb_arg, NULL, -ENOMEM);
		return;
	}
	ctx->dev = dev;
	ctx->cb_fn = cb_fn;
	ctx->cb_arg = cb_arg;
	/* open_bdevs() already set dual_bdev based on whether the caller passed
	 * a cache bdev, but the validation below compares it against the
	 * on-disk value -- stash it here because the error paths free dev. */
	ctx->want_dual = dev->dual_bdev;
	/* Keep our own copy of the name: the callback may run long after the
	 * caller returned. */
	ctx->wal_name = strdup(wal_bdev_name);
	if (!ctx->wal_name) {
		open_ctx_finish(ctx, -ENOMEM);
		return;
	}

	ctx->buf = spdk_dma_zmalloc(S3_SUPER_SIZE, 4096, NULL);
	if (!ctx->buf) {
		open_ctx_finish(ctx, -ENOMEM);
		return;
	}

	rc = spdk_bdev_read(dev->wal_desc, dev->wal_ch, ctx->buf, 0,
			    S3_SUPER_SIZE, open_super_read, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit super block read: %d\n", rc);
		open_ctx_finish(ctx, rc);
	}
}

void
s3_local_dev_close(struct s3_local_dev *dev)
{
	s3_local_dev_free(dev);
}

/* ==========================================================================
 * Accessors
 * ========================================================================== */

const struct s3_region *
s3_local_dev_get_region(struct s3_local_dev *dev, enum s3_region_id id)
{
	if (!dev || id >= S3_REGION_COUNT) {
		return NULL;
	}
	return &dev->regions[id];
}

struct spdk_bdev_desc *
s3_local_dev_get_desc(struct s3_local_dev *dev, enum s3_region_id id)
{
	if (!dev) {
		return NULL;
	}
	if (id == S3_REGION_CACHE && dev->dual_bdev) {
		return dev->cache_desc;
	}
	return dev->wal_desc;
}

struct spdk_io_channel *
s3_local_dev_get_channel(struct s3_local_dev *dev, enum s3_region_id id)
{
	if (!dev) {
		return NULL;
	}
	if (id == S3_REGION_CACHE && dev->dual_bdev) {
		return dev->cache_ch;
	}
	return dev->wal_ch;
}

const struct s3_super_block *
s3_local_dev_get_super(struct s3_local_dev *dev)
{
	return dev ? &dev->super : NULL;
}

/* ==========================================================================
 * Rewriting the super block in place
 *
 * Two callers update fields after formatting -- the checkpoint pointer and the
 * lvstore uuid -- and both need the same "recompute the CRC, write the whole
 * 4 KiB block, report through a callback" sequence. Sharing it keeps the CRC
 * step from being forgotten in one of them, which would produce a disk that
 * fails its own validation on the next open.
 * ========================================================================== */

struct super_write_ctx {
	void            *buf;
	const char      *what;      /* string literal, for the error message */
	s3_local_dev_cb  cb_fn;
	void            *cb_arg;
};

static void
super_write_done(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct super_write_ctx *ctx = cb_arg;
	s3_local_dev_cb cb_fn = ctx->cb_fn;
	void *user_arg = ctx->cb_arg;
	const char *what = ctx->what;

	spdk_bdev_free_io(bdev_io);

	spdk_dma_free(ctx->buf);
	free(ctx);

	if (!success) {
		SPDK_ERRLOG("Failed to persist %s in the super block\n", what);
	}

	if (cb_fn) {
		cb_fn(user_arg, success ? 0 : -EIO);
	}
}

/* Persist dev->super as it currently stands. The caller has already modified the
 * in-memory copy; this recomputes the CRC over it and writes it out. */
static void
super_write(struct s3_local_dev *dev, const char *what,
	    s3_local_dev_cb cb_fn, void *cb_arg)
{
	struct super_write_ctx *ctx;
	int rc;

	/* A staged commit owns the super block from the instant it starts its
	 * preflush until it reports. A checkpoint (or a uuid write) submitted
	 * in that window could land *after* the normal block and replace it
	 * with a stale staging block, which would make a published device
	 * unattachable. Refusing here is a defence in depth: the callers below
	 * also check before mutating the in-memory copy, so a refused write
	 * does not leave dev->super describing something that was not
	 * persisted. */
	if (dev->commit_in_flight) {
		SPDK_ERRLOG("Refusing to write %s while a staged commit owns "
			    "the super block\n", what);
		if (cb_fn) {
			cb_fn(cb_arg, -EBUSY);
		}
		return;
	}

	dev->super.crc = s3_super_calc_crc(&dev->super);

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		if (cb_fn) {
			cb_fn(cb_arg, -ENOMEM);
		}
		return;
	}
	ctx->what   = what;
	ctx->cb_fn  = cb_fn;
	ctx->cb_arg = cb_arg;

	ctx->buf = spdk_dma_zmalloc(S3_SUPER_SIZE, 4096, NULL);
	if (!ctx->buf) {
		free(ctx);
		if (cb_fn) {
			cb_fn(cb_arg, -ENOMEM);
		}
		return;
	}
	memcpy(ctx->buf, &dev->super, sizeof(dev->super));

	rc = spdk_bdev_write(dev->wal_desc, dev->wal_ch, ctx->buf,
			     dev->regions[S3_REGION_SUPER].offset, S3_SUPER_SIZE,
			     super_write_done, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit the %s super block write: %d\n",
			    what, rc);
		spdk_dma_free(ctx->buf);
		free(ctx);
		if (cb_fn) {
			cb_fn(cb_arg, rc);
		}
	}
}

void
s3_local_dev_flush_durable(struct s3_local_dev *dev, const char *what,
			   s3_local_dev_cb cb_fn, void *cb_arg)
{
	if (!dev) {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}

	/* A durability barrier over the whole WAL device, which is also where
	 * the super block lives. Used by the recovery chain to establish that
	 * every prerequisite write it made is on stable storage *before* it
	 * changes the in-memory super block to the normal identity.
	 *
	 * Unsupported flush is an error here, not a no-op: a device that cannot
	 * flush cannot prove the prerequisites are durable, and the staged
	 * protocol's whole claim is that publication happens only after they
	 * are. See local_dev_flush(). */
	local_dev_flush(dev, what ? what : "the recovery prerequisites",
			cb_fn, cb_arg);
}

void
s3_local_dev_update_checkpoint(struct s3_local_dev *dev, uint64_t gen,
			       uint64_t lsn, s3_local_dev_cb cb_fn, void *cb_arg)
{
	if (!dev) {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}
	if (dev->commit_in_flight) {
		/* Do not mutate dev->super: the commit is about to publish the
		 * normal identity, and a checkpoint pointer that was updated in
		 * memory but refused on disk would be lost silently. */
		SPDK_ERRLOG("Refusing to record a checkpoint while a staged "
			    "commit owns the super block\n");
		if (cb_fn) {
			cb_fn(cb_arg, -EBUSY);
		}
		return;
	}

	dev->super.checkpoint_gen = gen;
	dev->super.checkpoint_lsn = lsn;

	/* Updating the super block is the last step of a checkpoint, and it
	 * doubles as the proof that "the journal may now be truncated". The
	 * caller must wait for this callback (i.e. for durability) before
	 * truncating -- otherwise a crash pairs an old checkpoint with an
	 * already-truncated journal, which loses metadata.
	 *
	 * This used to be a synchronous write. Going async costs the caller a
	 * continuation, and buys the ability to call this safely from the data
	 * plane, which is where checkpoint completion runs. */
	super_write(dev, "the checkpoint pointer", cb_fn, cb_arg);
}

void
s3_local_dev_set_lvs_uuid(struct s3_local_dev *dev,
			  const struct spdk_uuid *lvs_uuid,
			  s3_local_dev_cb cb_fn, void *cb_arg)
{
	if (!dev || !lvs_uuid) {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}
	if (dev->commit_in_flight) {
		SPDK_ERRLOG("Refusing to record the lvstore uuid while a staged "
			    "commit owns the super block\n");
		if (cb_fn) {
			cb_fn(cb_arg, -EBUSY);
		}
		return;
	}

	spdk_uuid_copy(&dev->super.lvs_uuid, lvs_uuid);
	super_write(dev, "the lvstore uuid", cb_fn, cb_arg);
}

/* ==========================================================================
	* Committing a staged device
	*
	* This is the only transition from "not attachable" to "attachable", and it is
	* deliberately the last thing recovery does. Everything it depends on -- the
	* journal, the WAL, the checkpoint pointer, the uuid -- has already been
	* written and flushed by the caller, and the caller has already validated the
	* remote state. This function cannot verify any of that, so it does not pretend
	* to; what it does guarantee is narrower and checkable:
	*
	*   - it refuses a device that was never staged (-EINVAL), so a normal create
	*     cannot accidentally route through it;
	*   - it refuses a second concurrent commit (-EBUSY) instead of
	*     interleaving two sequences that both mutate dev->super;
	*   - it flushes the device *before* it changes the in-memory super block,
	*     so the prerequisites the caller wrote are durable before the normal
	*     identity exists in memory at all;
	*   - it writes exactly one 4 KiB block, the super block, and nothing else;
	*   - it flushes that block before reporting success;
	*   - on any failure the in-memory super block is restored to the staging
	*     magic/version, so a caller that retries does not commit a device whose
	*     in-memory state already claims to be normal.
	*
	* The last point matters for retry semantics. If the write fails, the device
	* on disk may already carry the normal block. Only the in-memory retry state
	* is restored; this is not a rollback and cannot prove attach refusal.
	*
	* === Why the preflush is inside this function ===
	*
	* The caller is required to have written and flushed every prerequisite
	* (journal, WAL, checkpoint pointer, uuid) before calling. That requirement
	* is not checkable from here, and the previous version of this function
	* relied on it entirely. A preflush makes the *ordering* self-enforcing for
	* the one thing this function can observe: whatever is in the device's
	* volatile cache at the moment of publication is pushed out first. It does
	* not replace the caller's own flush of writes it made through other
	* objects, and it does not make the caller's ordering correct by itself.
	*
	* The preflush also runs *before* the in-memory magic is switched, so a
	* preflush failure leaves the device staged in memory and on disk, with no
	* normal block ever submitted.
	* ========================================================================== */

struct commit_ctx {
	struct s3_local_dev *dev;
	void               *buf;
	s3_local_dev_cb      cb_fn;
	void                *cb_arg;
	/* The staging values, kept so a failed write can restore them. */
	uint64_t             staging_magic;
	uint32_t             staging_version;
};

/* Restore the in-memory retry state after a failure. Not a rollback: the
	* on-disk outcome is whatever the device did. */
static void
commit_restore_staging(struct commit_ctx *ctx)
{
	struct s3_local_dev *dev = ctx->dev;

	dev->super.magic = ctx->staging_magic;
	dev->super.version = ctx->staging_version;
	dev->super.crc = s3_super_calc_crc(&dev->super);
	dev->staged = true;
}

static void
commit_finish(struct commit_ctx *ctx, int status)
{
	struct s3_local_dev *dev = ctx->dev;
	s3_local_dev_cb cb_fn = ctx->cb_fn;
	void *user_arg = ctx->cb_arg;

	dev->commit_in_flight = false;
	free(ctx);
	if (cb_fn) {
		cb_fn(user_arg, status);
	}
}

static void
commit_flushed(void *cb_arg, int status)
{
	struct commit_ctx *ctx = cb_arg;
	struct s3_local_dev *dev = ctx->dev;

	if (status != 0) {
		/* Restore only the in-memory retry state. The normal block may
		 * already be readable or durable; a failed flush does not undo
		 * the write. The on-disk outcome is indeterminate. */
		commit_restore_staging(ctx);
		SPDK_ERRLOG("Failed to flush the committed super block; the "
			    "on-disk publication outcome is indeterminate\n");
		commit_finish(ctx, status);
		return;
	}

	dev->staged = false;
	SPDK_NOTICELOG("Committed staged layout on '%s': the device is "
		       "now attachable\n",
		       spdk_bdev_get_name(
			       spdk_bdev_desc_get_bdev(dev->wal_desc)));
	commit_finish(ctx, 0);
}

static void
commit_written(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct commit_ctx *ctx = cb_arg;

	spdk_bdev_free_io(bdev_io);
	spdk_dma_free(ctx->buf);
	ctx->buf = NULL;

	if (!success) {
		/* Same restore as a failed flush: the in-memory copy must not
		 * claim to be normal when the write did not land. */
		commit_restore_staging(ctx);
		SPDK_ERRLOG("Failed to write the committed super block; the "
			    "on-disk publication outcome is indeterminate\n");
		commit_finish(ctx, -EIO);
		return;
	}

	local_dev_flush(ctx->dev, "the committed super block",
			commit_flushed, ctx);
}

/* The prerequisites are durable. Only now is the in-memory identity switched
	* and the normal block submitted. */
static void
commit_preflushed(void *cb_arg, int status)
{
	struct commit_ctx *ctx = cb_arg;
	struct s3_local_dev *dev = ctx->dev;
	void *buf;
	int rc;

	if (status != 0) {
		/* Nothing was published: the in-memory super block is still the
		 * staging one because it has not been touched yet. */
		SPDK_ERRLOG("Failed to flush the recovery prerequisites before "
			    "publication (%s); the device stays STAGED\n",
			    spdk_strerror(-status));
		commit_finish(ctx, status);
		return;
	}

	/* Switch the in-memory copy to the normal identity and recompute the
	 * CRC over it. The layout fields are untouched, so the committed block
	 * describes exactly the same device the staging block did. */
	dev->super.magic = S3_SUPER_MAGIC;
	dev->super.version = S3_SUPER_VERSION;
	dev->super.crc = s3_super_calc_crc(&dev->super);

	buf = spdk_dma_zmalloc(S3_SUPER_SIZE, 4096, NULL);
	if (!buf) {
		commit_restore_staging(ctx);
		commit_finish(ctx, -ENOMEM);
		return;
	}
	memcpy(buf, &dev->super, sizeof(dev->super));
	ctx->buf = buf;

	rc = spdk_bdev_write(dev->wal_desc, dev->wal_ch, buf,
			     dev->regions[S3_REGION_SUPER].offset, S3_SUPER_SIZE,
			     commit_written, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit the committed super block write: "
			    "%d\n", rc);
		spdk_dma_free(buf);
		ctx->buf = NULL;
		commit_restore_staging(ctx);
		commit_finish(ctx, rc);
	}
}

void
s3_local_dev_commit_staged(struct s3_local_dev *dev,
			   s3_local_dev_cb cb_fn, void *cb_arg)
{
	struct commit_ctx *ctx;

	if (!dev) {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}

	/* Refuse a device that was never staged. Committing a normally
	 * formatted device would be a no-op at best and would hide a caller
	 * that skipped the staged path at worst. */
	if (!dev->staged) {
		SPDK_ERRLOG("Refusing to commit a device that is not staged; "
			    "s3_local_dev_commit_staged() is only valid after "
			    "s3_local_dev_format_staged()\n");
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}

	/* One commit at a time. A second caller is told -EBUSY rather than
	 * queued: the sequence mutates dev->super and dev->staged across
	 * several asynchronous steps, and interleaving two of them could
	 * publish a block whose CRC was computed from a half-updated struct.
	 * The caller must not treat -EBUSY as success. */
	if (dev->commit_in_flight) {
		SPDK_ERRLOG("Refusing a second concurrent commit of the staged "
			    "layout; one is already in flight\n");
		if (cb_fn) {
			cb_fn(cb_arg, -EBUSY);
		}
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		if (cb_fn) {
			cb_fn(cb_arg, -ENOMEM);
		}
		return;
	}
	ctx->dev = dev;
	ctx->cb_fn = cb_fn;
	ctx->cb_arg = cb_arg;
	ctx->staging_magic = dev->super.magic;
	ctx->staging_version = dev->super.version;

	dev->commit_in_flight = true;

	/* Durability barrier *before* the in-memory identity changes. See the
	 * section comment: this is the ordering the staged protocol claims, and
	 * it is enforced here rather than only documented. */
	local_dev_flush(dev, "the recovery prerequisites", commit_preflushed, ctx);
}
