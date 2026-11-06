/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_local_dev -- super block and region layout on the local bdev 
 *
 *   === Layout ===
 *
 *   Single bdev (WAL and cache share one device):
 *
 *     offset      region
 *     0           super (4 KiB)
 *     4K          metadata journal
 *     +256MWAL ring
 *     +...        chunk cache (whatever capacity is left)
 *
 *   Dual bdev (caller explicitly passes --wal-bdev + --cache-bdev):
 *
 *     WAL bdev:    super | metadata journal | WAL ring
 *     Cache bdev:  chunk cache
 *
 *   The super block *always records each region's offset/size explicitly*
 *   rather than deriving them from compile-time constants. That way changing
 *   the layout or a region size does not silently invalidate existing disks --
 *   the old layout is still readable and can be recognized.
 *
 *   === Why this is a separate file ===
 *
 *   The journal (s3_journal.c), the WAL (s3_wal.c) and the cache
 *   (s3_chunk_cache.c) all read and write some region of the local bdev, and
 *   all of them need to know "where does my region start and how big is it".
 *   Keeping the layout and the super block I/O here means each of them just
 *   takes a region descriptor and never has to parse the super block itself.
 */

#ifndef S3LVOL_LOCAL_DEV_H
#define S3LVOL_LOCAL_DEV_H

#include "spdk/stdinc.h"
#include "spdk/bdev.h"
#include "spdk/assert.h"
#include "spdk/uuid.h"

/* Super block magic "S3LVOLSB", used to tell whether a disk already carries
 * our layout. */
#define S3_SUPER_MAGIC          0x53334C564F4C5342ULL

/* Staging magic "S3LVOLST" (the last byte is 'T' instead of 'B').
 *
 * A *different magic*, not a reserved flag inside the normal one, and that is
 * the whole point. A reader that predates this protocol checks `magic ==
 * S3_SUPER_MAGIC` and nothing else before trusting the layout, so a reserved
 * bit would be read as "valid layout" by exactly the readers that must not
 * accept a half-built device. A different magic makes every such reader fail
 * its first check with -EILSEQ, which is the correct answer: this device is
 * not attachable yet.
 *
 * The staging super block is written *first*, before any other local write, so
 * a device that carries it is by construction one whose rebuild did not
 * finish. It is never a state a completed device is left in. */
#define S3_SUPER_MAGIC_STAGING  0x53334C564F4C5354ULL

/* Version 2 added capacity_bytes and lvs_name, both of which the attach path
 * needs: the geometry it hands to blobstore has to be the geometry the lvstore
 * was created with, and the name is what proves this disk belongs to the
 * lvstore being attached. A v1 disk carries neither, so it is rejected with
 * -EPROTO rather than guessed at. */
#define S3_SUPER_VERSION        2

/* Version 3 is the staging super block. It is a *distinct version* as well as a
 * distinct magic, so a reader that only compares versions (rather than magic)
 * also refuses it with -EPROTO instead of reinterpreting the fields. Both
 * checks are redundant on purpose: either one alone is enough to reject, and a
 * future reader that relaxes one of them still cannot accept a staging block.
 *
 * A staging block is never opened by s3_local_dev_open(). It exists only to be
 * written by s3_local_dev_format_staged() and replaced by a normal v2 block by
 * s3_local_dev_commit_staged(). */
#define S3_SUPER_VERSION_STAGING 3

#define S3_SUPER_SIZE           4096

/* Room for the lvstore name in the super block. Matches SPDK_LVS_NAME_MAX, but
 * spelled out here because lib/ must not depend on the lvol library. */
#define S3_LVS_NAME_MAX         64

/* Default metadata journal size.
 *
 * NOTE: 256 MiB was chosen against the checkpoint threshold ("journal >= 128
 * MiB triggers a checkpoint"), leaving 2x headroom so the journal does not fill
 * up before a checkpoint completes. The threshold should probably become a
 * percentage of capacity instead of an absolute value, otherwise a smaller
 * journal configured by the caller wraps before the absolute threshold is ever
 * reached. */
#define S3_JOURNAL_DEFAULT_SIZE (256ULL * 1024 * 1024)

/* Default WAL ring size. The design suggests 1-4 GiB. */
#define S3_WAL_DEFAULT_SIZE     (1024ULL * 1024 * 1024)

enum s3_region_id {
	S3_REGION_SUPER = 0,
	S3_REGION_JOURNAL,
	S3_REGION_WAL,
	S3_REGION_CACHE,
	S3_REGION_COUNT,
};

/* Where a region lives on some bdev. All I/O is expressed relative to this,
 * so a component never needs to know what precedes it on the device. */
struct s3_region {
	uint64_t         offset;    /* byte offset within the bdev */
	uint64_t         size;      /* byte length */
	bool             valid;
};

/* On-disk super block. *Once this layout ships, field order must not change*;
 * only append at the tail and bump the version. */
struct s3_super_block {
	uint64_t         magic;
	uint32_t         version;
	uint32_t         block_size;        /* always 4096 (P4); recorded so it can be validated */

	/* uuid of the lvstore this disk belongs to.
	 *
	 * All zeroes until the lvstore has actually been created: blobstore
	 * generates the uuid itself and will not accept one from outside, so it
	 * is only known once spdk_lvs_init() has completed and is written back
	 * then (s3_local_dev_set_lvs_uuid). The attach path treats all-zeroes as
	 * "unknown, do not check" -- a create that crashed between formatting the
	 * disk and initialising the lvstore leaves exactly that state. */
	struct spdk_uuid lvs_uuid;

	uint32_t         chunk_size;
	uint32_t         reserved0;

	/* Explicit per-region layout, indexed by enum s3_region_id.
	 * In the dual-bdev case CACHE's offset/size refer to the cache bdev. */
	struct {
		uint64_t offset;
		uint64_t size;
	} regions[S3_REGION_COUNT];

	/* Generation of the most recent checkpoint and the journal LSN it
	 * covers. Recovery uses this to decide where replay starts. */
	uint64_t         checkpoint_gen;
	uint64_t         checkpoint_lsn;

	/* Logical provisioning capacity of the lvstore, in bytes.
	 *
	 * Recorded because the attach path has to hand blobstore the *same*
	 * geometry it was created with. blobstore rejects a device smaller than
	 * the size in its own super block (blobstore.c:1855), and a device that
	 * is larger silently invites a grow. Asking the operator to repeat the
	 * capacity on every attach would make a typo look like corruption. */
	uint64_t         capacity_bytes;

	/* Name of the lvstore, NUL terminated.
	 *
	 * This is the one identity check the attach path can make *before* it
	 * replays anything, which is why it exists: the S3 key prefix is derived
	 * from the name, so a mismatch means the local device belongs to a
	 * different lvstore and replaying its log would write foreign data into
	 * this one. */
	char             lvs_name[S3_LVS_NAME_MAX];

	/* Dual-bdev marker. When single, the cache shares the journal/WAL
	 * device. */
	uint8_t          dual_bdev;
	uint8_t          reserved1[7];

	/* CRC32C over every field above. Kept last, and treated as zero while
	 * computing itself. */
	uint32_t         crc;
} __attribute__((packed));

SPDK_STATIC_ASSERT(sizeof(struct s3_super_block) <= S3_SUPER_SIZE,
		   "super block must fit in one 4 KiB block");

struct s3_local_dev;

/* Owned by s3_recovery.c. Declared here rather than included so the local
 * device header does not pull the bdev-module surface into every consumer; see
 * the note in s3_recovery.h. */
struct s3_recovery_claim;

typedef void (*s3_local_dev_cb)(void *cb_arg, int status);

typedef void (*s3_local_dev_open_cb)(void *cb_arg, struct s3_local_dev *dev,
				     int status);

/**
 * What to record when formatting a fresh layout.
 *
 * A struct rather than a parameter list because most of these end up verbatim in
 * the super block, and a call with seven positional arguments of which four are
 * integers is one transposition away from a silently wrong disk.
 */
struct s3_local_dev_format_opts {
	/* bdev holding super + journal + WAL. Required. */
	const char *wal_bdev_name;

	/* bdev holding the chunk cache; NULL means it shares the WAL device
	 * (single-bdev layout). */
	const char *cache_bdev_name;

	/* Optional retained, already-claimed devices.
	 *
	 * Set by the recovery path so the format runs on the *same* descriptors
	 * the blank proof was taken on, with the claims still held. Neither
	 * device is reopened: reopening is exactly what the claim forbids
	 * (a writable open fails -EPERM while the claim is held), so a
	 * formatter that reopened would fail on any dual-bdev recovery.
	 *
	 * Ownership contract, which the implementation enforces:
	 *   - the caller owns both claims when s3_local_dev_format() is called;
	 *   - on SUCCESS (callback status 0) the returned dev has ADOPTED both
	 *     claims. It releases them from s3_local_dev_close(), after its I/O
	 *     channels are put, so they live exactly as long as the object that
	 *     uses the descriptors;
	 *   - on FAILURE the dev does not adopt them and the caller still owns
	 *     them. This is why a failed format leaves the claims held rather
	 *     than closing descriptors the setup context may still release.
	 *
	 * Each claim must hold a live descriptor and belong to the named device.
	 * A cache claim passed for a device that is not distinct from the WAL
	 * device is rejected: one claim cannot be adopted twice.
	 *
	 * Both NULL keeps the historical behaviour: open both bdevs here, close
	 * them on every exit path, and hand the resulting dev to the caller.
	 */
	struct s3_recovery_claim *wal_claim;
	struct s3_recovery_claim *cache_claim;

	/* Which lvstore this disk belongs to. Required: the attach path has
	 * nothing else to check before it starts replaying. */
	const char *lvs_name;

	/* Logical provisioning capacity of the lvstore. Required, because the
	 * attach path reads it back instead of asking the operator again. */
	uint64_t    capacity_bytes;

	uint32_t    chunk_size;

	uint64_t    journal_size;   /* 0 for the default */
	uint64_t    wal_size;       /* 0 for the default */
};

/**
 * Open the local bdev(s) and *format* a fresh layout (used when creating a
 * new lvstore).
 *
 * Writes the super block and zeroes the journal head. *This overwrites
 * whatever was on the disk.*
 *
 * lvs_uuid is left all-zeroes: blobstore assigns it and will not take one from
 * outside, so it is written back with s3_local_dev_set_lvs_uuid() once
 * spdk_lvs_init() has completed.
 *
 * Asynchronous: when the callback fires the super block is durable and
 * \c dev is usable (NULL on failure). The callback *may run before this
 * function returns* (argument validation or submission failure).
 */
void s3_local_dev_format(const struct s3_local_dev_format_opts *opts,
			 s3_local_dev_open_cb cb_fn, void *cb_arg);

/**
 * Open the local bdev(s) and *read* the existing layout (used when attaching
 * an existing lvstore).
 *
 * Asynchronous. status: -ENODEV bdev not found; -EILSEQ bad super block magic
 * or CRC; -EPROTO unsupported version.
 */
void s3_local_dev_open(const char *wal_bdev_name, const char *cache_bdev_name,
		       s3_local_dev_open_cb cb_fn, void *cb_arg);

/**
 * Close the local device: put its I/O channels, then release the claims it
 * adopted (or close the descriptors it opened), then free it.
 *
 * This is the only place a claimed descriptor is released once a format has
 * succeeded. Callers must not release those claims themselves.
 */
void s3_local_dev_close(struct s3_local_dev *dev);

/* ==========================================================================
 * Staged formatting -- the crash-safe publication protocol
 *
 * s3_local_dev_format() writes a normal, attachable super block as its *first*
 * write. That is correct for create, where the device is being made from
 * nothing and a crash leaves a device that was never meant to be attachable.
 * It is wrong for recovery, where the device is rebuilt from a remote
 * checkpoint over several steps: a crash after the first write leaves a
 * *normal* super block on a device whose journal, WAL, checkpoint and uuid have
 * not been rebuilt. A later attach would accept it and replay from a
 * checkpoint pointer that describes data that is not there.
 *
 * The staged protocol splits that into three explicit states:
 *
 *   STAGED    magic = S3_SUPER_MAGIC_STAGING, version = 3.
 *             Written first, before any other local write. Every reader that
 *             checks magic or version refuses it (-EILSEQ / -EPROTO), so a
 *             half-built device cannot be attached by this build or by an
 *             older one. This is the state a crash leaves behind.
 *
 *   (rebuild) journal, WAL, checkpoint and uuid are written and flushed while
 *             the device still carries the staging block.
 *
 *   COMMITTED magic = S3_SUPER_MAGIC, version = 2.
 *             Written last, after every prerequisite write has been flushed
 *             and the caller has validated the remote blobstore state. Only
 *             this write makes the device attachable.
 *
 * === What this does and does not guarantee ===
 *
 * Guaranteed, and tested:
 *   - a device left in STAGED is refused by s3_local_dev_open() with a
 *     distinguishable error, so it is never attached as if complete;
 *   - the commit is a single 4 KiB block write followed by a flush, and the
 *     callback that reports success runs only after that flush completed;
 *   - a torn or corrupt commit block fails its CRC and is refused, leaving the
 *     device in STAGED (the previous block is still the staging one, because
 *     the commit is one block overwrite);
 *   - any failure before the commit leaves STAGED in place and does not
 *     silently re-format or blank the device.
 *
 * NOT guaranteed:
 *   - atomicity of a 4 KiB overwrite against arbitrary power loss. A device
 *     that loses power mid-commit may hold a block that is neither the staging
 *     block nor a valid normal block. That is detected (magic and CRC both
 *     fail) and refused, but it is *detection*, not atomicity. No claim is made
 *     that the commit is atomic on any particular device.
 *   - durability of writes the caller has not flushed. The protocol flushes
 *     what it writes; it cannot flush what a caller wrote behind its back.
 * ========================================================================== */

/**
 * Format a *staging* layout: same geometry computation as
 * s3_local_dev_format(), but the super block written is the staging one and
 * the journal head is cleared, and the callback reports only after both writes
 * have been flushed.
 *
 * The returned dev is usable for the rebuild steps (journal, WAL, checkpoint,
 * uuid) but the device is NOT attachable: s3_local_dev_open() refuses it.
 *
 * opts is the same struct as s3_local_dev_format(); lvs_uuid is left zero and
 * is expected to be recorded with s3_local_dev_set_lvs_uuid() during the
 * rebuild, exactly as on the normal path.
 *
 * On failure nothing is adopted and the caller still owns the claims.
 */
void s3_local_dev_format_staged(const struct s3_local_dev_format_opts *opts,
				s3_local_dev_open_cb cb_fn, void *cb_arg);

/**
 * Flush the local device and report only when the flush completed.
 *
 * A durability barrier over the whole WAL device, which is also where the super
 * block lives. The recovery chain uses it to establish that every prerequisite
 * write it made is on stable storage before it publishes the normal super
 * block.
 *
 * Unsupported flush is an error (-ENOTSUP), not a silent success: a device that
 * cannot flush cannot prove durability, and the staged protocol's claim is that
 * publication happens only after the prerequisites are durable.
 */
void s3_local_dev_flush_durable(struct s3_local_dev *dev, const char *what,
				s3_local_dev_cb cb_fn, void *cb_arg);

/**
 * Publish a staged device: rewrite the super block as a normal, attachable one
 * and flush it.
 *
 * This is the *last* step of recovery. The caller must have completed and
 * flushed every prerequisite local write (journal, WAL, checkpoint pointer,
 * uuid) and validated the remote state before calling this; this function
 * cannot check that, and deliberately does not try to.
 *
 * What it does enforce, in this order:
 *
 *   1. a preflush of the device, *before* the in-memory super block is
 *      switched to the normal identity. A preflush failure leaves the device
 *      staged in memory and on disk, and no normal block is ever submitted;
 *   2. the in-memory magic/version switch and CRC recomputation;
 *   3. one 4 KiB super block write;
 *   4. a postflush, after which the callback reports success.
 *
 * A second concurrent commit is refused with -EBUSY rather than queued: the
 * sequence mutates the in-memory super block across several asynchronous steps.
 *
 * Unsupported flush is an error. A failed write or flush has an indeterminate
 * on-disk outcome: a complete normal block may already be visible. Restoring
 * the in-memory staging state is not rollback. The caller must establish all
 * durable prerequisites before this operation.
 *
 * Calling this on a device that was not formatted with
 * s3_local_dev_format_staged() is a programming error and is refused with
 * -EINVAL.
 */
void s3_local_dev_commit_staged(struct s3_local_dev *dev,
				s3_local_dev_cb cb_fn, void *cb_arg);

/**
 * True when \c dev was formatted with s3_local_dev_format_staged() and has not
 * yet been committed. Used by the recovery chain to assert its own ordering.
 */
bool s3_local_dev_is_staged(struct s3_local_dev *dev);

/**
 * True when \c dev adopted \c claim during a successful format.
 *
 * The setup context uses this to decide whether a claim is still its to
 * release. Ownership moves to the dev at format completion, so the context
 * must not close the descriptor even though it may still hold the pointer:
 * doing so would close it out from under the live journal and WAL consumers.
 */
bool s3_local_dev_owns_claim(struct s3_local_dev *dev,
			     struct s3_recovery_claim *claim);

/**
 * Look up where a region lives.
 */
const struct s3_region *s3_local_dev_get_region(struct s3_local_dev *dev,
						enum s3_region_id id);

/**
 * Get the bdev descriptor / channel backing a region.
 *
 * In a single-bdev layout every region returns the same one; in a dual-bdev
 * layout the CACHE region returns the cache bdev.
 *
 * Channels are per-thread, so this must be called on the thread that will use
 * them.
 */
struct spdk_bdev_desc *s3_local_dev_get_desc(struct s3_local_dev *dev,
					     enum s3_region_id id);
struct spdk_io_channel *s3_local_dev_get_channel(struct s3_local_dev *dev,
						 enum s3_region_id id);

const struct s3_super_block *s3_local_dev_get_super(struct s3_local_dev *dev);

/**
 * Record the lvstore uuid in the super block and persist it.
 *
 * Called once on the create path, right after spdk_lvs_init() has handed out the
 * uuid. Nothing on the write path depends on it; it exists so that a later
 * attach can refuse a local device that belongs to a *different incarnation* of
 * an lvstore with the same name -- the case where the lvstore was destroyed and
 * recreated while this disk was kept. Without it that pairing looks valid and
 * replay writes stale data into the new lvstore.
 *
 * A failure here is not fatal to the create: the lvstore works, only the attach
 * check degrades to name-only. The caller should log it and carry on rather than
 * unwind a working lvstore.
 */
void s3_local_dev_set_lvs_uuid(struct s3_local_dev *dev,
			       const struct spdk_uuid *lvs_uuid,
			       s3_local_dev_cb cb_fn, void *cb_arg);

/**
 * Update the checkpoint pointer in the super block and persist it.
 *
 * Called once a checkpoint completes. It doubles as the proof that "the
 * journal may be truncated up to lsn", so the caller must wait for this
 * callback (i.e. for durability) before truncating the journal. Otherwise a
 * crash would leave an old checkpoint paired with an already-truncated
 * journal, which loses metadata.
 */
void s3_local_dev_update_checkpoint(struct s3_local_dev *dev,
				    uint64_t gen, uint64_t lsn,
				    s3_local_dev_cb cb_fn, void *cb_arg);

#endif /* S3LVOL_LOCAL_DEV_H */
