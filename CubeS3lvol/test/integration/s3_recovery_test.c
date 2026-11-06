/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   Recovery descriptor validation -- no S3, no credentials, no DPDK, no threads
 *
 *   === What this test proves ===
 *
 *   The descriptor is the only thing standing between "the local device is
 *   gone" and "format a device and point it at whatever is in the bucket". If
 *   validation lets a wrong descriptor through, the recovery path formats a
 *   fresh device with the wrong geometry or the wrong identity and then loads a
 *   checkpoint whose chunk indices mean something else -- which does not fail,
 *   it silently returns the wrong bytes.
 *
 *   So each rejection is asserted with its *exact* errno, and each corruption
 *   flips exactly one thing with the CRC recomputed where the field under test
 *   is not the CRC itself. That way the test says which check fired, not merely
 *   that something did.
 *
 *   The load path needs an s3_client, so this test supplies its own: s3_head and
 *   s3_get_range are stubbed to hand back an in-memory object synchronously (the
 *   same "callback runs in place on the submitting thread" contract the real
 *   client has for non-SPDK-thread callers). s3_put and s3_delete are stubbed
 *   too, so s3_client_aws.o stays out of the link entirely.
 *
 *   Usage:
 *     ./s3_recovery_test
 */

#include "spdk/stdinc.h"
#include "spdk/crc32.h"
#include "spdk/log.h"
#include "spdk/util.h"

#include "s3lvol/s3_checkpoint.h"
#include "s3lvol/s3_chunk_map.h"
#include "s3lvol/s3_recovery.h"

static int g_pass;
static int g_fail;

static void
check_int(const char *what, int got, int want)
{
	if (got == want) {
		printf("  [PASS] %-58s rc=%d\n", what, got);
		g_pass++;
	} else {
		printf("  [FAIL] %-58s rc=%d (want %d)\n", what, got, want);
		g_fail++;
	}
}

static void
check_true(const char *what, bool ok, const char *detail)
{
	if (ok) {
		printf("  [PASS] %-58s %s\n", what, detail ? detail : "");
		g_pass++;
	} else {
		printf("  [FAIL] %-58s %s\n", what, detail ? detail : "");
		g_fail++;
	}
}

/* ==========================================================================
 * Stub s3_client
 * ========================================================================== */

static const uint8_t *g_obj;
static size_t         g_obj_len;
static uint64_t       g_head_size;
static int            g_head_status;

int
s3_head(struct s3_client *client, const char *key, uint64_t *out_size,
	s3_op_cb cb, void *cb_arg)
{
	(void)client;
	(void)key;
	if (out_size) {
		*out_size = g_head_size;
	}
	cb(cb_arg, g_head_status);
	return 0;
}

int
s3_get_range(struct s3_client *client, const char *key, uint64_t offset,
	     uint64_t len, void *buf, s3_get_cb cb, void *cb_arg)
{
	uint64_t n;

	(void)client;
	(void)key;
	(void)offset;
	n = spdk_min(len, (uint64_t)g_obj_len);
	if (buf && n) {
		memcpy(buf, g_obj, n);
	}
	cb(cb_arg, n, 0);
	return 0;
}

int
s3_put(struct s3_client *client, const char *key, struct iovec *iov, int iovcnt,
       bool if_none_match, s3_op_cb cb, void *cb_arg)
{
	(void)client;
	(void)key;
	(void)iov;
	(void)iovcnt;
	(void)if_none_match;
	if (cb) {
		cb(cb_arg, 0);
	}
	return 0;
}

int
s3_delete(struct s3_client *client, const char *key, s3_op_cb cb, void *cb_arg)
{
	(void)client;
	(void)key;
	if (cb) {
		cb(cb_arg, 0);
	}
	return 0;
}

/* ==========================================================================
 * Building a well-formed descriptor
 * ========================================================================== */

#define TEST_NAME      "dst"
#define TEST_CAPACITY  (2ULL * 1024 * 1024 * 1024)
#define TEST_CHUNK     (1024 * 1024)

/* Reproduce the module's CRC: crc field zeroed, seed 0. */
static uint32_t
desc_crc(const struct s3_recovery_desc *d)
{
	struct s3_recovery_desc t;

	memcpy(&t, d, sizeof(t));
	t.crc = 0;
	return spdk_crc32c_update(&t, sizeof(t), 0);
}

static void
make_uuid(struct spdk_uuid *u, uint32_t tag)
{
	uint8_t *raw = (uint8_t *)u;

	memset(u, 0, sizeof(*u));
	raw[0] = 0x5A;
	raw[1] = (uint8_t)(tag & 0xff);
	raw[2] = (uint8_t)((tag >> 8) & 0xff);
	raw[15] = 0x9C;
}

static void
build_desc(struct s3_recovery_desc *d)
{
	memset(d, 0, sizeof(*d));
	d->magic          = S3_RECOVERY_MAGIC;
	d->version        = S3_RECOVERY_VERSION;
	d->chunk_size     = TEST_CHUNK;
	d->capacity_bytes = TEST_CAPACITY;
	make_uuid(&d->lvs_uuid, 7);
	d->checkpoint_gen         = 3;
	d->checkpoint_lsn         = 1234;
	d->checkpoint_bytes       = 64 + 8 * 40;
	d->checkpoint_entries_crc = 0xDEADBEEF;
	d->journal_size           = 64ULL * 1024 * 1024;
	d->wal_size               = 64ULL * 1024 * 1024;
	snprintf(d->lvs_name, sizeof(d->lvs_name), "%s", TEST_NAME);
	d->crc = desc_crc(d);
}

/* ==========================================================================
 * The load callback, filled synchronously by the stubs
 * ========================================================================== */

struct load_result {
	struct s3_recovery_desc *desc;
	int                      status;
};

static void
load_cb(void *cb_arg, struct s3_recovery_desc *desc, int status)
{
	struct load_result *r = cb_arg;

	r->desc   = desc;
	r->status = status;
}

static struct load_result
run_load(const struct s3_recovery_desc *d, size_t len)
{
	struct load_result r = {0};

	g_obj = (const uint8_t *)d;
	g_obj_len = len;
	g_head_size = len;
	g_head_status = 0;

	s3_recovery_load((struct s3_client *)0x1, TEST_NAME, load_cb, &r);
	return r;
}

/* ==========================================================================
 * A well-formed checkpoint blob, and the preflight that judges it
 * ========================================================================== */

#define CKPT_ENTRIES  8

static uint8_t g_ckpt[sizeof(struct s3_ckpt_header) +
		       CKPT_ENTRIES * sizeof(struct s3_ckpt_entry)];
static size_t  g_ckpt_len;

static uint32_t
ckpt_hdr_crc(const struct s3_ckpt_header *h)
{
	struct s3_ckpt_header t;

	memcpy(&t, h, sizeof(t));
	t.crc = 0;
	return spdk_crc32c_update(&t, sizeof(t), 0);
}

/* Build a checkpoint that matches `d` apart from the two fields the caller has
 * to derive from the blob itself (size and entry CRC). */
static void
build_ckpt(const struct s3_recovery_desc *d)
{
	struct s3_ckpt_header *h = (struct s3_ckpt_header *)g_ckpt;
	struct s3_ckpt_entry *e = (struct s3_ckpt_entry *)(h + 1);
	int i;

	memset(g_ckpt, 0, sizeof(g_ckpt));
	h->magic          = S3_CKPT_MAGIC;
	h->version        = S3_CKPT_VERSION;
	h->chunk_size     = d->chunk_size;
	spdk_uuid_copy(&h->lvs_uuid, &d->lvs_uuid);
	h->checkpoint_lsn = d->checkpoint_lsn;
	h->gen            = d->checkpoint_gen;
	h->num_entries    = CKPT_ENTRIES;

	for (i = 0; i < CKPT_ENTRIES; i++) {
		/* Strictly increasing and inside the capacity the descriptor
		 * records (2 GiB / 1 MiB = 2048 chunks). */
		e[i].chunk_index = (uint64_t)i * 7;
		make_uuid(&e[i].uuid, 100 + i);
		e[i].valid_bytes = d->chunk_size;
		e[i].flags       = 0;
		e[i].gen         = d->checkpoint_gen;
	}

	h->entries_crc = spdk_crc32c_update(
		e, CKPT_ENTRIES * sizeof(struct s3_ckpt_entry), 0);
	h->crc = ckpt_hdr_crc(h);

	g_ckpt_len = sizeof(*h) + CKPT_ENTRIES * sizeof(struct s3_ckpt_entry);
}

/* Fill in the descriptor fields that name this exact blob. */
static void
desc_name_ckpt(struct s3_recovery_desc *d)
{
	struct s3_ckpt_header *h = (struct s3_ckpt_header *)g_ckpt;

	d->checkpoint_bytes       = g_ckpt_len;
	d->checkpoint_entries_crc = h->entries_crc;
	d->crc                    = desc_crc(d);
}

struct ckpt_result {
	void    *ckpt;
	uint64_t len;
	int      status;
};

static void
ckpt_cb(void *cb_arg, void *ckpt, uint64_t len, int status)
{
	struct ckpt_result *r = cb_arg;

	r->ckpt   = ckpt;
	r->len    = len;
	r->status = status;
}

static struct ckpt_result
run_preflight(const struct s3_recovery_desc *d, const uint8_t *obj,
	      size_t obj_len, uint64_t head_size, int head_status)
{
	struct ckpt_result r = {0};

	g_obj = obj;
	g_obj_len = obj_len;
	g_head_size = head_size;
	g_head_status = head_status;

	s3_recovery_ckpt_preflight((struct s3_client *)0x1, TEST_NAME, d,
				   ckpt_cb, &r);
	return r;
}

int
main(void)
{
	struct s3_recovery_desc d;
	struct load_result r;

	spdk_log_set_print_level(SPDK_LOG_ERROR);
	spdk_log_open(NULL);

	printf("=== recovery descriptor validation check ===\n\n");

	build_desc(&d);

	/* ---------- [1] the honest round trip ---------- */
	printf("[1] a well-formed descriptor loads and validates\n");
	check_int("validate() accepts it",
		  s3_recovery_desc_validate(&d, TEST_NAME), 0);
	r = run_load(&d, sizeof(d));
	check_int("load() succeeds", r.status, 0);
	check_true("and hands back a descriptor", r.desc != NULL, NULL);
	if (r.desc) {
		check_true("with the same uuid",
			   spdk_uuid_compare(&r.desc->lvs_uuid, &d.lvs_uuid) == 0,
			   NULL);
		check_true("and the same checkpoint generation",
			   r.desc->checkpoint_gen == d.checkpoint_gen, NULL);
		s3_recovery_desc_free(r.desc);
	}

	/* ---------- [2] a missing descriptor is -ENOENT, not a guess ---------- */
	printf("\n[2] a missing descriptor means \"this lvstore predates it\"\n");
	g_head_status = -ENOENT;
	{
		struct load_result r2 = {0};

		s3_recovery_load((struct s3_client *)0x1, TEST_NAME, load_cb, &r2);
		check_int("status is -ENOENT", r2.status, -ENOENT);
		check_true("and no descriptor is returned", r2.desc == NULL, NULL);
	}
	g_head_status = 0;

	/* ---------- [3] the rejections, each with its exact errno ---------- */
	printf("\n[3] corrupting one thing at a time and expecting the refusal\n");
	{
		struct s3_recovery_desc b;

		/* (a) bad magic. */
		memcpy(&b, &d, sizeof(b));
		b.magic = 0xDEADBEEFDEADBEEFULL;
		check_int("a bad magic is -EILSEQ",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EILSEQ);

		/* (b) unsupported version, CRC recomputed so only this fires. */
		memcpy(&b, &d, sizeof(b));
		b.version = 99;
		b.crc = desc_crc(&b);
		check_int("an unsupported version is -EPROTO",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EPROTO);

		/* (c) CRC mismatch -- flip a field, do not touch the crc. */
		memcpy(&b, &d, sizeof(b));
		b.checkpoint_lsn = 1235;
		check_int("a CRC mismatch is -EILSEQ",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EILSEQ);

		/* (d) a different lvstore's name. */
		memcpy(&b, &d, sizeof(b));
		snprintf(b.lvs_name, sizeof(b.lvs_name), "other");
		b.crc = desc_crc(&b);
		check_int("a name mismatch is -EINVAL",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EINVAL);

		/* (e) an unterminated name. */
		memcpy(&b, &d, sizeof(b));
		memset(b.lvs_name, 'x', sizeof(b.lvs_name));
		b.crc = desc_crc(&b);
		check_int("an unterminated name is -EILSEQ",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EILSEQ);

		/* (f) zero capacity -- blobstore cannot be handed this. */
		memcpy(&b, &d, sizeof(b));
		b.capacity_bytes = 0;
		b.crc = desc_crc(&b);
		check_int("zero capacity is -EINVAL",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EINVAL);

		/* (g) capacity not a multiple of the block size. */
		memcpy(&b, &d, sizeof(b));
		b.capacity_bytes = TEST_CAPACITY + 1;
		b.crc = desc_crc(&b);
		check_int("a misaligned capacity is -EINVAL",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EINVAL);

		/* (h) chunk size not a power of two -- every chunk_index would
		 * mean something else. */
		memcpy(&b, &d, sizeof(b));
		b.chunk_size = 3 * 1024 * 1024;
		b.crc = desc_crc(&b);
		check_int("a non-power-of-two chunk_size is -EINVAL",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EINVAL);

		/* (i) chunk size below one block. */
		memcpy(&b, &d, sizeof(b));
		b.chunk_size = 512;
		b.crc = desc_crc(&b);
		check_int("a sub-block chunk_size is -EINVAL",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EINVAL);

		/* (j) a null uuid -- the descriptor is only written once the uuid
		 * is known, so zero means it is not trustworthy. */
		memcpy(&b, &d, sizeof(b));
		memset(&b.lvs_uuid, 0, sizeof(b.lvs_uuid));
		b.crc = desc_crc(&b);
		check_int("a null lvstore uuid is -EINVAL",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EINVAL);

		/* (k) generation 0 -- no checkpoint has ever completed. */
		memcpy(&b, &d, sizeof(b));
		b.checkpoint_gen = 0;
		b.crc = desc_crc(&b);
		check_int("checkpoint gen 0 is -EINVAL",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EINVAL);

		/* (l) an implausibly small checkpoint object. */
		memcpy(&b, &d, sizeof(b));
		b.checkpoint_bytes = 8;
		b.crc = desc_crc(&b);
		check_int("a too-small checkpoint size is -EINVAL",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EINVAL);

		/* (m) an implausibly large checkpoint object. */
		memcpy(&b, &d, sizeof(b));
		b.checkpoint_bytes = 8ULL * 1024 * 1024 * 1024;
		b.crc = desc_crc(&b);
		check_int("an implausible checkpoint size is -EINVAL",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EINVAL);
	}

	/* ---------- [4] the load path rejects a wrong-sized object ---------- */
	printf("\n[4] the load path refuses an object of the wrong size\n");
	{
		struct load_result r3 = {0};

		/* A truncated object: HEAD reports the full size but fewer bytes
		 * arrive. */
		g_obj = (const uint8_t *)&d;
		g_obj_len = sizeof(d) - 8;
		g_head_size = sizeof(d);
		g_head_status = 0;
		s3_recovery_load((struct s3_client *)0x1, TEST_NAME, load_cb, &r3);
		check_int("a short body is -EILSEQ", r3.status, -EILSEQ);
		check_true("and no descriptor is returned", r3.desc == NULL, NULL);

		/* A HEAD that reports the wrong size outright. */
		g_obj = (const uint8_t *)&d;
		g_obj_len = sizeof(d);
		g_head_size = sizeof(d) + 4;
		g_head_status = 0;
		s3_recovery_load((struct s3_client *)0x1, TEST_NAME, load_cb, &r3);
		check_int("a wrong HEAD size is -EILSEQ", r3.status, -EILSEQ);
	}

	/* ---------- [5] positive blank proof ---------- */
	printf("\n[5] blankness is proven by zeroes, never by a parse error\n");
	{
		uint8_t *z = calloc(1, 64 * 1024);

		check_int("a zero-filled buffer is blank",
			  s3_recovery_blank_check(z, 64 * 1024), 0);
		z[64 * 1024 - 1] = 1;
		check_int("a byte at the very end is not blank",
			  s3_recovery_blank_check(z, 64 * 1024), -EILSEQ);
		z[64 * 1024 - 1] = 0;
		z[0] = 1;
		check_int("a byte at the start is not blank",
			  s3_recovery_blank_check(z, 64 * 1024), -EILSEQ);
		z[0] = 0;
		z[4096] = 1;
		check_int("a byte beyond the super block is not blank",
			  s3_recovery_blank_check(z, 64 * 1024), -EILSEQ);
		check_int("NULL is -EINVAL",
			  s3_recovery_blank_check(NULL, 4096), -EINVAL);
		check_int("zero length is -EINVAL",
			  s3_recovery_blank_check(z, 0), -EINVAL);
		free(z);
	}

	/* ---------- [6] descriptor <-> checkpoint preflight ---------- */
	printf("\n[6] the checkpoint must be the one the descriptor names\n");
	{
		struct s3_recovery_desc d2;
		struct s3_recovery_desc b;
		struct ckpt_result cr;

		memcpy(&d2, &d, sizeof(d2));
		build_ckpt(&d2);
		desc_name_ckpt(&d2);

		check_int("validate_blob accepts the matching pair",
			  s3_recovery_ckpt_validate_blob(&d2, g_ckpt, g_ckpt_len),
			  0);
		check_int("the descriptor itself still validates",
			  s3_recovery_desc_validate(&d2, TEST_NAME), 0);

		cr = run_preflight(&d2, g_ckpt, g_ckpt_len, g_ckpt_len, 0);
		check_int("preflight succeeds", cr.status, 0);
		check_true("and hands back the validated bytes",
			   cr.ckpt != NULL && cr.len == g_ckpt_len, NULL);
		if (cr.status == 0) {
			struct s3_chunk_map *map = NULL;
			int rc = s3_chunk_map_create(
				TEST_CAPACITY / S3LVOL_BLOCK_SIZE,
				S3LVOL_BLOCK_SIZE, TEST_CHUNK, &map);

			check_int("the validated bytes apply to an empty map", rc,
				  0);
			if (rc == 0) {
				struct s3_ckpt_expect ex = {0};

				ex.chunk_size = d2.chunk_size;
				ex.lvs_uuid = &d2.lvs_uuid;
				ex.check_point = true;
				ex.gen = d2.checkpoint_gen;
				ex.lsn = d2.checkpoint_lsn;
				ex.num_chunks_limit = d2.capacity_bytes /
						      d2.chunk_size;

				check_int("apply() accepts them",
					  s3_checkpoint_apply(map, cr.ckpt,
							      cr.len, &ex, NULL,
							      NULL), 0);
				check_true("and stamps the applied LSN",
					   s3_chunk_map_get_applied_lsn(map) ==
					   d2.checkpoint_lsn, NULL);
				check_true("and restores every mapping",
					   s3_chunk_map_get_allocated(map) ==
					   CKPT_ENTRIES, NULL);
				s3_chunk_map_destroy(map);
			}
			s3_recovery_ckpt_free(cr.ckpt);
		}

		/* --- descriptor-side mismatches --- */
		memcpy(&b, &d2, sizeof(b));
		b.checkpoint_entries_crc ^= 0x1;
		b.crc = desc_crc(&b);
		check_int("a wrong entry CRC in the descriptor is -EILSEQ",
			  s3_recovery_ckpt_validate_blob(&b, g_ckpt, g_ckpt_len),
			  -EILSEQ);
		cr = run_preflight(&b, g_ckpt, g_ckpt_len, g_ckpt_len, 0);
		check_int("and the preflight refuses it", cr.status, -EILSEQ);
		check_true("with no bytes handed back", cr.ckpt == NULL, NULL);

		memcpy(&b, &d2, sizeof(b));
		b.checkpoint_bytes += 40;
		b.crc = desc_crc(&b);
		check_int("a wrong object size in the descriptor is -EILSEQ",
			  s3_recovery_ckpt_validate_blob(&b, g_ckpt, g_ckpt_len),
			  -EILSEQ);

		memcpy(&b, &d2, sizeof(b));
		make_uuid(&b.lvs_uuid, 999);
		b.crc = desc_crc(&b);
		check_int("a wrong lvstore uuid is -EINVAL",
			  s3_recovery_ckpt_validate_blob(&b, g_ckpt, g_ckpt_len),
			  -EINVAL);

		memcpy(&b, &d2, sizeof(b));
		b.checkpoint_gen += 1;
		b.crc = desc_crc(&b);
		check_int("a wrong generation is -EINVAL",
			  s3_recovery_ckpt_validate_blob(&b, g_ckpt, g_ckpt_len),
			  -EINVAL);

		memcpy(&b, &d2, sizeof(b));
		b.checkpoint_lsn += 1;
		b.crc = desc_crc(&b);
		check_int("a wrong covered LSN is -EINVAL",
			  s3_recovery_ckpt_validate_blob(&b, g_ckpt, g_ckpt_len),
			  -EINVAL);

		memcpy(&b, &d2, sizeof(b));
		b.chunk_size = TEST_CHUNK * 2;
		b.crc = desc_crc(&b);
		check_int("a wrong chunk size is -EINVAL",
			  s3_recovery_ckpt_validate_blob(&b, g_ckpt, g_ckpt_len),
			  -EINVAL);

		/* --- checkpoint-side corruption --- */
		{
			uint8_t save[sizeof(g_ckpt)];

			memcpy(save, g_ckpt, sizeof(save));

			g_ckpt[0] ^= 0xff;	/* magic */
			check_int("a corrupt header is -EILSEQ",
				  s3_recovery_ckpt_validate_blob(&d2, g_ckpt,
								 g_ckpt_len),
				  -EILSEQ);
			memcpy(g_ckpt, save, sizeof(save));

			{
				struct s3_ckpt_header *h = (void *)g_ckpt;

				h->version = 99;
				h->crc = ckpt_hdr_crc(h);
			}
			check_int("an unknown checkpoint version is -EPROTO",
				  s3_recovery_ckpt_validate_blob(&d2, g_ckpt,
								 g_ckpt_len),
				  -EPROTO);
			memcpy(g_ckpt, save, sizeof(save));

			/* Truncated: fewer bytes than the descriptor names. */
			check_int("a truncated checkpoint is -EILSEQ",
				  s3_recovery_ckpt_validate_blob(&d2, g_ckpt,
								 g_ckpt_len - 8),
				  -EILSEQ);
			/* Same length, changed payload. */
			g_ckpt[sizeof(struct s3_ckpt_header) + 8] ^= 0xff;
			check_int("changed entry bytes are -EILSEQ",
				  s3_recovery_ckpt_validate_blob(&d2, g_ckpt,
								 g_ckpt_len),
				  -EILSEQ);
			memcpy(g_ckpt, save, sizeof(save));

			/* Chunk index outside the capacity. */
			{
				struct s3_ckpt_header *h = (void *)g_ckpt;
				struct s3_ckpt_entry *e = (void *)(h + 1);

				e[0].chunk_index = d2.capacity_bytes /
						   d2.chunk_size;
				h->entries_crc = spdk_crc32c_update(
					e, CKPT_ENTRIES * sizeof(*e), 0);
				h->crc = ckpt_hdr_crc(h);
				desc_name_ckpt(&d2);
			}
			check_int("a chunk index beyond the capacity is -EINVAL",
				  s3_recovery_ckpt_validate_blob(&d2, g_ckpt,
								 g_ckpt_len),
				  -EINVAL);
			memcpy(g_ckpt, save, sizeof(save));
			desc_name_ckpt(&d2);

			/* Duplicate / non-increasing index. */
			{
				struct s3_ckpt_header *h = (void *)g_ckpt;
				struct s3_ckpt_entry *e = (void *)(h + 1);

				e[1].chunk_index = e[0].chunk_index;
				h->entries_crc = spdk_crc32c_update(
					e, CKPT_ENTRIES * sizeof(*e), 0);
				h->crc = ckpt_hdr_crc(h);
				desc_name_ckpt(&d2);
			}
			check_int("a duplicate chunk index is -EILSEQ",
				  s3_recovery_ckpt_validate_blob(&d2, g_ckpt,
								 g_ckpt_len),
				  -EILSEQ);
			memcpy(g_ckpt, save, sizeof(save));
			desc_name_ckpt(&d2);
		}

		/* --- transport failures --- */
		cr = run_preflight(&d2, g_ckpt, g_ckpt_len, g_ckpt_len, -EIO);
		check_int("a HEAD read error is -EIO", cr.status, -EIO);
		check_true("with no bytes handed back", cr.ckpt == NULL, NULL);

		cr = run_preflight(&d2, g_ckpt, g_ckpt_len, g_ckpt_len + 4, 0);
		check_int("a HEAD size mismatch is -EILSEQ", cr.status, -EILSEQ);

		cr = run_preflight(&d2, g_ckpt, g_ckpt_len - 8, g_ckpt_len, 0);
		check_int("a short body is -EILSEQ", cr.status, -EILSEQ);

		cr = run_preflight(&d2, g_ckpt, g_ckpt_len, g_ckpt_len, -ENOENT);
		check_int("a missing checkpoint is -ENOENT", cr.status, -ENOENT);
	}

	/* ---------- [7] capacity has to divide into whole chunks ---------- */
	printf("\n[7] capacity must be a whole number of chunks\n");
	{
		struct s3_recovery_desc b;

		memcpy(&b, &d, sizeof(b));
		b.capacity_bytes = TEST_CAPACITY + TEST_CHUNK / 2;
		b.crc = desc_crc(&b);
		check_int("a partial final chunk is -EINVAL",
			  s3_recovery_desc_validate(&b, TEST_NAME), -EINVAL);
	}

	spdk_log_close();

	printf("\n=== result: %d passed, %d failed ===\n", g_pass, g_fail);
	return g_fail == 0 ? 0 : 1;
}
