/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */

/**
 * \file
 * s3_recovery -- the versioned descriptor that makes a remote-only rebuild
 * possible
 *
 * === The problem this exists to solve ===
 *
 * The chunk map lives in the local device's journal. Attaching an lvstore
 * therefore requires that device, and `s3lvol_lvstore_attach()` refuses to run
 * without it. If the local WAL/cache is lost (disk replaced, host rebuilt,
 * container volume gone) the data objects are still in the bucket but nothing
 * maps an LBA to them, so the supported API cannot bring the lvstore back.
 *
 * The object store *does* hold a full chunk-map snapshot at
 * `<prefix>/meta/checkpoint` (`s3_ckpt_header` + `s3_ckpt_entry[]`). What it
 * does **not** hold is the rest of what a fresh local device needs:
 *
 *   - the logical capacity (`capacity_bytes`), which the attach path reads from
 *     the super block and hands to blobstore;
 *   - the chunk size, which is in the checkpoint header but is only checked
 *     against a map that already exists;
 *   - the lvstore uuid, which the checkpoint header tolerates as all-zero;
 *   - the checkpoint generation, which the super block records and the
 *     checkpoint header also carries -- but nothing ties the two together;
 *   - the journal layout (region sizes), which is a property of the *disk*,
 *     not of the data.
 *
 * Guessing any of those from caller-supplied defaults is exactly the failure
 * mode this module is meant to prevent: a wrong capacity makes blobstore fail
 * to load (or silently invite a grow), and a wrong chunk size reinterprets
 * every `chunk_index` in the snapshot.
 *
 * === What is published ===
 *
 * `<prefix>/meta/recovery` holds a small, versioned, CRC-protected descriptor
 * written at a point where all of the above is known and durable: immediately
 * after a checkpoint has been uploaded *and* the super block has been updated
 * to name it. That ordering is the same one the checkpoint path already
 * relies on (PUT, then super block, then truncate), so the descriptor can
 * never name a checkpoint the super block does not also name.
 *
 * The descriptor is **additive**. It does not replace the checkpoint, the
 * super block or the owner marker, and nothing on the normal attach path reads
 * it. An lvstore that has never been checkpointed by a build with this module
 * has no descriptor, and remote-only recovery for it fails with a clear
 * `-ENOENT` rather than a guess.
 *
 * === What it deliberately does not do ===
 *
 *   - It does not carry data. The chunk objects are named by the checkpoint.
 *   - It does not carry the journal or WAL contents. Recovery restores the
 *     state as of the last checkpoint, which is the RPO: everything
 *     acknowledged after that checkpoint but not yet checkpointed is lost.
 *     That is stated in the recovery API's documentation and in the reply.
 *   - It does not grant ownership. The caller must already hold the owner
 *     marker; see `s3_owner.h` for why that is a sequential-misoperation guard
 *     and not distributed mutual exclusion.
 */

#ifndef S3LVOL_S3_RECOVERY_H
#define S3LVOL_S3_RECOVERY_H

#include "spdk/stdinc.h"
#include "spdk/assert.h"
#include "spdk/bdev.h"
#include "spdk/uuid.h"

#include "s3lvol/s3_client.h"

/* Forward declared rather than pulled in from spdk/bdev_module.h: the claim
 * API only needs the pointer, and including the module header here would drag
 * the whole bdev-module surface into every consumer of this header (including
 * the standalone test harnesses, which deliberately link no SPDK archive). */
struct spdk_bdev_module;

#ifdef __cplusplus
extern "C" {
#endif

/* "S3LVRECV" */
#define S3_RECOVERY_MAGIC        0x53334C5652454356ULL
#define S3_RECOVERY_VERSION      1

/* Upper bound on the descriptor object. The real content is about 200 bytes;
 * this is also the length of the range GET used to read it, so it caps how
 * much can be read back from a corrupt or hostile object. */
#define S3_RECOVERY_DOC_MAX      1024

/* On-disk (in-S3) descriptor. Field order must not change once shipped; append
 * at the tail and bump the version. */
struct s3_recovery_desc {
	uint64_t         magic;
	uint32_t         version;

	/* Geometry the lvstore was created with. Both are required: the attach
	 * path reads them from the super block, and a fresh device has no super
	 * block to read. */
	uint32_t         chunk_size;
	uint64_t         capacity_bytes;

	/* Identity. lvs_uuid is the blobstore uuid, which is what the attach path
	 * compares against the local device once blobstore is loaded. */
	struct spdk_uuid lvs_uuid;

	/* The checkpoint this descriptor describes. `checkpoint_gen` and
	 * `checkpoint_lsn` must match the checkpoint object's header exactly;
	 * `checkpoint_bytes` is its object size, so a truncated or replaced
	 * checkpoint is detected before it is applied. */
	uint64_t         checkpoint_gen;
	uint64_t         checkpoint_lsn;
	uint64_t         checkpoint_bytes;

	/* CRC32C over the checkpoint's entry array, copied from its header. Lets
	 * the recovery path prove the checkpoint it downloaded is the one this
	 * descriptor was written for, without trusting the header alone. */
	uint32_t         checkpoint_entries_crc;
	uint32_t         reserved0;

	/* Journal layout to format on the fresh device. Recorded because it is a
	 * property of the disk the lvstore was created with, and the recovery
	 * path must reproduce a layout the existing data can be replayed into.
	 * `journal_size` and `wal_size` are bytes; 0 means "the built-in
	 * default", which is what a create with no explicit sizes records. */
	uint64_t         journal_size;
	uint64_t         wal_size;

	/* Name of the lvstore, NUL terminated. The S3 key prefix is derived from
	 * it, so a descriptor found under a different prefix is not this
	 * lvstore's. */
	char             lvs_name[64];

	/* CRC32C over every field above, itself counted as zero. */
	uint32_t         crc;
} __attribute__((packed));

SPDK_STATIC_ASSERT(sizeof(struct s3_recovery_desc) <= S3_RECOVERY_DOC_MAX,
		   "recovery descriptor must fit in one bounded read");

/**
 * Validate a descriptor that was read from S3.
 *
 * Checks, in order: magic, version, header CRC, a non-zero capacity, a
 * power-of-two chunk size that is at least one block, a non-zero checkpoint
 * generation, a plausible checkpoint size, and a NUL-terminated name that
 * matches \c expect_name.
 *
 * \param expect_name  the lvstore name the caller is recovering. Required:
 *                     a descriptor is only meaningful under its own prefix.
 *
 * \return 0 when usable; -EILSEQ on corruption; -EPROTO on a version this
 *         build does not understand; -EINVAL on a value that is well-formed
 *         but unusable (zero capacity, bad chunk size, name mismatch).
 */
int s3_recovery_desc_validate(const struct s3_recovery_desc *desc,
			      const char *expect_name);

/**
 * Publish the descriptor for \c lvs_name.
 *
 * Called after a checkpoint has been uploaded and the super block updated.
 * The object is written with a plain PUT: it is a single small object, and a
 * crash between the checkpoint and this write leaves the previous descriptor
 * in place, which still names a checkpoint that exists. That is the safe
 * direction -- recovery may restore an older checkpoint, never a newer one
 * than the descriptor claims.
 *
 * \param local_dev  supplies capacity, chunk size, name and the journal/WAL
 *                   region sizes. Required.
 * \param lvs_uuid   the blobstore uuid. Required; all-zero is refused, because
 *                   a descriptor without identity cannot be validated against
 *                   the checkpoint it names.
 */
void s3_recovery_publish(struct s3_client *client, const char *lvs_name,
			 const struct spdk_uuid *lvs_uuid,
			 uint64_t capacity_bytes, uint32_t chunk_size,
			 uint64_t journal_size, uint64_t wal_size,
			 uint64_t checkpoint_gen, uint64_t checkpoint_lsn,
			 uint64_t checkpoint_bytes,
			 uint32_t checkpoint_entries_crc,
			 s3_op_cb cb_fn, void *cb_arg);

/**
 * Read and validate the descriptor for \c lvs_name.
 *
 * A missing object is reported as -ENOENT, which the recovery API turns into
 * "this lvstore predates the descriptor; remote-only recovery is not possible
 * for it" rather than into a guess.
 *
 * The callback receives a heap-allocated descriptor it must free with
 * s3_recovery_desc_free(). On failure \c out_desc is NULL.
 */
typedef void (*s3_recovery_load_cb)(void *cb_arg,
				    struct s3_recovery_desc *desc, int status);

void s3_recovery_load(struct s3_client *client, const char *lvs_name,
		      s3_recovery_load_cb cb_fn, void *cb_arg);

void s3_recovery_desc_free(struct s3_recovery_desc *desc);

/**
 * Delete the descriptor. For destroying an lvstore; not part of normal
 * operation.
 */
void s3_recovery_delete(struct s3_client *client, const char *lvs_name,
			s3_op_cb cb_fn, void *cb_arg);

/* ==========================================================================
 * Recovery preflight
 *
 * These two checks are the prerequisites for touching anything. They are
 * deliberately *positive* checks: the recovery path must be able to say "this
 * device is zero-filled" and "this checkpoint is the one the descriptor names",
 * not merely "opening the device failed" or "the header parsed".
 *
 * Neither check writes anything, and neither claims the owner marker. The
 * recovery chain in vbdev_s3lvol_lvstore.c runs descriptor -> checkpoint ->
 * claim -> blank proof -> format. The scan covers only the blank-proof step; it
 * does not establish ownership, and it does not make the later format
 * atomic. That ordering reduces the window in which the world can change
 * between the checks and the first write; it does **not** close it, because the
 * owner marker is not mutual exclusion (see s3_owner.h). The checks are
 * necessary, not sufficient.
 * ========================================================================== */

/* Same ceiling the checkpoint loader enforces on the object size. */
#define S3_RECOVERY_CKPT_MAX  (4ULL * 1024 * 1024 * 1024)

/**
 * Prove that a buffer is entirely zero.
 *
 * This is the only definition of "blank" the recovery path uses. It never
 * parses a super block, so a parse error (wrong magic, bad CRC, unknown
 * version) can never be mistaken for blankness -- that was the defect in the
 * previous probe, which treated -EILSEQ from s3_local_dev_open() as "no
 * layout".
 *
 * \return 0 when every byte is zero; -EILSEQ on the first non-zero byte;
 *         -EINVAL when \c buf is NULL or \c len is 0.
 */
int s3_recovery_blank_check(const void *buf, size_t len);

/* ==========================================================================
 * Local claim: one descriptor, one claim, from the blank proof to the format
 *
 * The scan and the format used to be two independent opens of the same bdev.
 * Between them the descriptor was closed, so the device was unclaimed and any
 * other in-process writer could open it, write, and leave the format running
 * against a device that was no longer blank. This section removes that window
 * by keeping the *same* descriptor and the *same* exclusive claim alive across
 * both steps.
 *
 * === What the claim does and does not guarantee ===
 *
 * `spdk_bdev_module_claim_bdev()` sets `SPDK_BDEV_CLAIM_EXCL_WRITE` on the
 * bdev. In the pinned SPDK tree that means:
 *
 *   - a later `spdk_bdev_open_ext(name, true, ...)` fails with -EPERM
 *     (bdev.c: `if (write && claim_type != NONE) return -EPERM`), so no new
 *     writer can appear in this process;
 *   - a second claim fails with -EPERM (bdev.c: `claim_type != NONE`), so a
 *     competing claim is refused;
 *   - `spdk_bdev_module_claim_bdev_desc(desc, SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE)`
 *     additionally walks `open_descs` and refuses when *any other descriptor is
 *     already open for writing* (bdev.c `claim_verify_rwo`). That is the only
 *     pinned API that inspects pre-existing writers, so it is the one used here.
 *
 * It does **not** mean "nobody else can write the device":
 *
 *   - a read-only descriptor opened before the claim stays open and stays
 *     readable (the claim is about writers);
 *   - the claim is process-local. Another process, or the host, can still write
 *     the backing file of an aio bdev. Nothing in SPDK fences that, and the
 *     owner marker in S3 is not mutual exclusion either (see s3_owner.h).
 *
 * So the guarantee is: *within this process*, from the moment the claim is
 * taken until it is released, no other writer can be opened and no competing
 * claim can be granted. That is the window the recovery chain needs, and it is
 * the only window this API claims.
 * ========================================================================== */

struct s3_recovery_claim;

/**
 * Open \c bdev_name and take an exclusive-write claim on it.
 *
 * The descriptor is opened read-only and then upgraded by the claim, which is
 * what `SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE` requires (it refuses a descriptor
 * that is already writable). The claim is verified against existing writers
 * before it is granted, so a device that another descriptor in this process
 * already has open for writing is refused rather than silently shared.
 *
 * \param module  the claiming bdev module. Required: SPDK records it in the
 *                claim and reports it in "already claimed" diagnostics.
 *
 * \return 0 and a claim in \c *out_claim on success. The claim owns the
 *         descriptor; the caller must not close it separately.
 *         -ENODEV when the bdev does not exist, -EPERM when it is already
 *         claimed or another descriptor has it open for writing, -ENOMEM on
 *         allocation failure, other negative errno from the open.
 *
 * Must be called from an SPDK thread. The claim is bound to that thread.
 */
int s3_recovery_claim_open(const char *bdev_name,
			   struct spdk_bdev_module *module,
			   struct s3_recovery_claim **out_claim);

/**
 * The descriptor the claim holds. Valid until s3_recovery_claim_release().
 *
 * This is the handle the formatter consumes: passing it on means the format
 * runs on the very descriptor the blank proof was taken on, with the claim
 * still held, instead of reopening the device.
 */
struct spdk_bdev_desc *s3_recovery_claim_desc(struct s3_recovery_claim *claim);

/** The bdev behind the claim. Valid until s3_recovery_claim_release(). */
struct spdk_bdev *s3_recovery_claim_bdev(struct s3_recovery_claim *claim);

/**
 * The first REMOVE / RESIZE latched while the claim has been held, or 0.
 *
 * The latch is claim-wide rather than scan-wide so the window between the
 * blank proof and the format is covered: the formatter checks this before it
 * submits a write, and refuses when it is non-zero. -ENODEV for REMOVE,
 * -ERANGE for RESIZE.
 */
int s3_recovery_claim_error(struct s3_recovery_claim *claim);

/**
 * Release the claim and close the descriptor. NULL-safe.
 *
 * The claim is SPDK_BDEV_CLAIM_READ_MANY_WRITE_ONE (a v2 claim), and v2 claims
 * are dropped by closing the descriptor: bdev_close() ->
 * bdev_desc_release_claims() -> claim_reset(). Calling
 * spdk_bdev_module_release_bdev() here would take the v1 path, which asserts
 * claim_type == SPDK_BDEV_CLAIM_EXCL_WRITE. This function therefore closes the
 * descriptor and never calls the v1 release.
 *
 * The claim must be released on the thread that took it (SPDK descriptors are
 * per-thread). Releasing is the only way the device becomes writable by
 * another opener in this process again. A claim transferred to a
 * struct s3_local_dev is released by s3_local_dev_close(), after that object
 * has put its I/O channels.
 */
void s3_recovery_claim_release(struct s3_recovery_claim *claim);

/**
 * Scan a whole bdev and prove it is zero-filled.
 *
 * Opens the bdev read-only, reads it sequentially in bounded chunks, and
 * reports 0 only if every byte read was zero. Any read error, short read, or
 * non-zero byte is a failure, and the caller must not format the device in that
 * case. A bdev that cannot be opened, whose size is not a whole number of
 * blocks, or whose block count times block size would overflow is a failure
 * too. A non-zero byte anywhere counts, including a valid or corrupt-looking
 * super block: this function never parses a layout.
 *
 * A REMOVE or RESIZE event received during the scan latches a failure. The
 * scan then stops submitting reads, waits for any outstanding read to complete,
 * and only then closes the descriptor and invokes the callback. The callback is
 * invoked exactly once, on the calling thread.
 *
 * The buffer is allocated once (S3_RECOVERY_SCAN_CHUNK) and reused, so the scan
 * costs one chunk of memory regardless of device size. It reads the *entire*
 * designated device, including any separate cache bdev, because format
 * overwrites regions whose exact extent is a property of the layout being
 * replaced. Scanning less would be a weaker claim than the one made here.
 *
 * This entry point opens its own descriptor and is therefore **not** suitable
 * for the recovery chain: it releases the device before the callback runs. Use
 * s3_recovery_blank_scan_claimed() there.
 */
typedef void (*s3_recovery_blank_cb)(void *cb_arg, int status);

void s3_recovery_blank_scan(const char *bdev_name,
			    s3_recovery_blank_cb cb_fn, void *cb_arg);

/**
 * Prove a device blank through a claim that stays held afterwards.
 *
 * Same proof as s3_recovery_blank_scan(), with two differences that matter:
 *
 *   1. It does not open or close anything. It reads through
 *      s3_recovery_claim_desc() and leaves the claim, the descriptor and the
 *      I/O channel in place when the callback fires. The caller can then format
 *      the same device through the same descriptor without an intervening
 *      release, which is the whole point: the device cannot be reopened for
 *      writing in between.
 *   2. It takes its own I/O channel and releases that channel before the
 *      callback, so the formatter is free to take its own. The claim itself is
 *      untouched.
 *
 * A REMOVE or RESIZE event latches a failure exactly as in the unclaimed scan,
 * and the callback still runs only after the outstanding read completes. On
 * failure the claim is left held: the caller releases it, because the caller
 * owns it.
 */
void s3_recovery_blank_scan_claimed(struct s3_recovery_claim *claim,
				    s3_recovery_blank_cb cb_fn, void *cb_arg);

/**
 * Validate a checkpoint blob against the descriptor that names it, in memory.
 *
 * Checks the checkpoint's own structure (magic, version, header CRC, exact
 * size, entry CRC) through s3_checkpoint_validate(), then requires that the
 * blob agrees with \c desc: same object size, chunk size, lvstore uuid,
 * generation, covered LSN, entry-array CRC, and chunk indices inside the
 * capacity the descriptor records.
 *
 * Exposed separately from the download so a caller can validate bytes it
 * already has, and so the negative cases are testable without S3.
 */
int s3_recovery_ckpt_validate_blob(const struct s3_recovery_desc *desc,
				   const void *buf, uint64_t len);

/**
 * Download and validate the checkpoint named by \c desc.
 *
 * The object size is learned with HEAD and must equal desc->checkpoint_bytes
 * exactly; a truncated, replaced or grown object is rejected. The GET is a
 * sized range request, so allocation is bounded by the descriptor (itself
 * validated to at most S3_RECOVERY_CKPT_MAX).
 *
 * On success the callback receives the validated bytes and owns them until
 * s3_recovery_ckpt_free(). Those exact bytes are what the recovery chain later
 * applies, so the state restored is the state that was validated. On failure
 * \c ckpt is NULL.
 */
typedef void (*s3_recovery_ckpt_cb)(void *cb_arg, void *ckpt,
				    uint64_t ckpt_len, int status);

void s3_recovery_ckpt_preflight(struct s3_client *client, const char *lvs_name,
				const struct s3_recovery_desc *desc,
				s3_recovery_ckpt_cb cb_fn, void *cb_arg);

void s3_recovery_ckpt_free(void *ckpt);

#ifdef __cplusplus
}
#endif

#endif /* S3LVOL_S3_RECOVERY_H */
