/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   Pack exports (version 6): manifest, reader and GC
 *
 *   A packed template delta copies the delta's own chunks into a few large
 *   pack objects and maps every present chunk to (pack, slot). This checks the
 *   parts that can go wrong quietly, against an in-memory S3:
 *
 *     [2] round trip: set_packs assigns ascending chunks to consecutive slots,
 *         serialize/parse keeps every location, and the crc covers them;
 *     [3] an older reader refuses it: version 6 is above the delta version, a
 *         version 7 manifest is refused here, and a version 6 manifest with
 *         its pack table stripped (or a location outside the table) is refused
 *         rather than read through the refs;
 *     [4] partial reads: a sub-chunk read of a packed chunk issues one ranged
 *         GET of the pack at the slot's offset, never a GET of the ref
 *         object, and returns the right bytes; a prefetch reads every slot;
 *     [5] a missing pack fails closed: the read errors, nothing falls back to
 *         the ref object, and nothing reads as zeroes;
 *     [6] GC: s3_export_packs_delete deletes exactly packs 0..n-1 of the export;
 *     [7] S3LVOL_EXPORT_PACK_MIB is clamped to 4..64 MiB.
 *
 *   No network, no credentials: the client calls are defined here.
 */

#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/thread.h"
#include "spdk/blob.h"
#include "spdk/util.h"

#include "s3lvol/s3_export.h"
#include "s3lvol/s3_chunk_map.h"
#include "s3lvol/s3_client.h"
#include "s3lvol/s3_hedge.h"

#define CHUNK_SIZE  (1024 * 1024)
#define NUM_CHUNKS  32
#define BLOCK       4096
#define TEST_UUID   "6a1f04e0-4f89-11d3-9a0c-0305e82c3306"
#define BASE_UUID   "7b2f04e0-4f89-11d3-9a0c-0305e82c3307"
#define SRC_PREFIX  "srclvs"
#define PACK_CHUNKS 4

static int g_pass, g_fail;

static void
check_true(const char *what, bool ok)
{
	if (ok) {
		g_pass++;
		printf("\t[PASS] %s\n", what);
	} else {
		g_fail++;
		printf("\t[FAIL] %s\n", what);
	}
}

static void
check_u64(const char *what, uint64_t got, uint64_t want)
{
	if (got == want) {
		g_pass++;
		printf("\t[PASS] %s (got %" PRIu64 ")\n", what, got);
	} else {
		g_fail++;
		printf("\t[FAIL] %s (got %" PRIu64 ", want %" PRIu64 ")\n", what, got, want);
	}
}

/* ==========================================================================
 * In-memory S3: pack objects only
 * ========================================================================== */

static bool     g_present[NUM_CHUNKS];
static struct spdk_uuid g_uuids[NUM_CHUNKS];
static struct s3_export_manifest *g_m;  /* where the fake looks up slots */
static char     g_missing_pack[S3_EXPORT_KEY_MAX];
/* Fail the next GET whose offset is this (UINT64_MAX: none), after writing
 * g_fail_prefix bytes of it -- a stalled range that delivered a prefix. */
static uint64_t g_fail_off = UINT64_MAX;
static uint64_t g_fail_prefix;
static uint32_t g_fail_left;
#define MAX_SEEN 64
static uint64_t g_seen_off[MAX_SEEN], g_seen_len[MAX_SEEN];
static uint32_t g_nseen;
static uint32_t g_gets, g_pack_gets, g_ref_gets;
static uint64_t g_last_off, g_last_len;
static char     g_deleted[64][S3_EXPORT_KEY_MAX];
static uint32_t g_ndeleted;

static uint8_t
obj_byte(int chunk, uint64_t off)
{
	return (uint8_t)(((chunk * 37) + (off / BLOCK) * 11 + (off % 251)) % 255 + 1);
}

/* Which chunk a (pack key, offset) holds, or -1. */
static int
pack_slot_chunk(const char *key, uint64_t offset)
{
	uint64_t i;

	for (i = 0; i < NUM_CHUNKS; i++) {
		char k[S3_EXPORT_KEY_MAX];
		uint64_t off;
		uint32_t len;

		if (!g_m || s3_export_manifest_pack_range(g_m, i, k, sizeof(k), &off,
							  &len) != 0) {
			continue;
		}
		if (strcmp(k, key) == 0 && offset >= off && offset < off + CHUNK_SIZE) {
			return (int)i;
		}
	}
	return -1;
}

struct fake_get {
	char        key[S3_EXPORT_KEY_MAX];
	uint64_t    offset;
	uint64_t    len;
	void       *buf;
	s3_get_cb   cb;
	void       *cb_arg;
	TAILQ_ENTRY(fake_get) link;
};
static TAILQ_HEAD(, fake_get) g_pending = TAILQ_HEAD_INITIALIZER(g_pending);

int
s3_get_range(struct s3_client *client, const char *key, uint64_t offset,
	     uint64_t len, void *buf, s3_get_cb cb, void *cb_arg)
{
	struct fake_get *g = calloc(1, sizeof(*g));

	(void)client;
	if (!g) {
		return -ENOMEM;
	}
	snprintf(g->key, sizeof(g->key), "%s", key);
	g->offset = offset;
	g->len = len;
	g->buf = buf;
	g->cb = cb;
	g->cb_arg = cb_arg;
	g_gets++;
	if (strstr(key, "/pack-")) {
		g_pack_gets++;
	} else {
		g_ref_gets++;
	}
	g_last_off = offset;
	g_last_len = len;
	if (g_nseen < MAX_SEEN) {
		g_seen_off[g_nseen] = offset;
		g_seen_len[g_nseen] = len;
		g_nseen++;
	}
	TAILQ_INSERT_TAIL(&g_pending, g, link);
	return 0;
}

static uint32_t
fake_complete_all(void)
{
	uint32_t done = 0;

	while (!TAILQ_EMPTY(&g_pending)) {
		struct fake_get *g = TAILQ_FIRST(&g_pending);
		int c = -1;
		int status = 0;
		uint64_t got = 0, i;

		TAILQ_REMOVE(&g_pending, g, link);
		if (strstr(g->key, "/pack-") && strcmp(g->key, g_missing_pack) != 0) {
			c = pack_slot_chunk(g->key, g->offset);
		}
		if (c < 0) {
			status = -ENOENT;
		} else {
			uint64_t n = g->len;

			if (g->offset == g_fail_off && g_fail_left > 0) {
				g_fail_left--;
				n = spdk_min(g_fail_prefix, g->len);
				status = -ETIMEDOUT;
			}
			/* A range may span several slots of the pack. */
			for (i = 0; i < n; i++) {
				uint64_t p = g->offset + i;
				int cc = pack_slot_chunk(g->key, p);

				((uint8_t *)g->buf)[i] = cc < 0 ? 0 : obj_byte(cc, p % CHUNK_SIZE);
			}
			got = n;
		}
		g->cb(g->cb_arg, got, status);
		free(g);
		done++;
	}
	return done;
}

int
s3_head(struct s3_client *client, const char *key, uint64_t *out_size,
	s3_op_cb cb, void *cb_arg)
{
	(void)client;
	(void)key;
	(void)out_size;
	cb(cb_arg, -ENOENT);
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
		cb(cb_arg, -ENOTSUP);
	}
	return 0;
}

int
s3_delete(struct s3_client *client, const char *key, s3_op_cb cb, void *cb_arg)
{
	(void)client;
	if (g_ndeleted < SPDK_COUNTOF(g_deleted)) {
		snprintf(g_deleted[g_ndeleted++], S3_EXPORT_KEY_MAX, "%s", key);
	}
	if (cb) {
		cb(cb_arg, 0);
	}
	return 0;
}

void
s3_client_put(struct s3_client *client)
{
	(void)client;
}

/* ==========================================================================
 * Helpers
 * ========================================================================== */

static struct spdk_thread *g_thread;

static void
poll_some(int n)
{
	while (n-- > 0) {
		spdk_thread_poll(g_thread, 0, 0);
	}
}

static struct s3_export_manifest *
make_delta(void)
{
	struct s3_export_manifest *m = NULL;
	int i;

	if (s3_export_manifest_create(TEST_UUID, (uint64_t)NUM_CHUNKS * CHUNK_SIZE,
				      CHUNK_SIZE, S3_EXPORT_LAYOUT_REF, &m) != 0) {
		return NULL;
	}
	snprintf(m->src.prefix, sizeof(m->src.prefix), "%s", SRC_PREFIX);
	snprintf(m->srcs[0].prefix, sizeof(m->srcs[0].prefix), "%s", SRC_PREFIX);
	for (i = 0; i < NUM_CHUNKS; i++) {
		uint8_t raw[16];

		/* 10 own chunks, scattered; the rest read from the base. */
		g_present[i] = (i % 3 == 0) && i != 0;
		if (!g_present[i]) {
			continue;
		}
		memset(raw, 0x40 + i, sizeof(raw));
		memcpy(&g_uuids[i], raw, sizeof(g_uuids[i]));
		s3_export_manifest_set_ref(m, i, &g_uuids[i], CHUNK_SIZE);
	}
	if (s3_export_manifest_set_base(m, BASE_UUID) != 0) {
		s3_export_manifest_unref(m);
		return NULL;
	}
	s3_export_manifest_seal(m);
	return m;
}

static char *
replace_once(const char *s, const char *from, const char *to)
{
	const char *p = strstr(s, from);
	size_t n;
	char *out;

	if (!p) {
		return NULL;
	}
	n = strlen(s) - strlen(from) + strlen(to) + 1;
	out = calloc(1, n);
	if (!out) {
		return NULL;
	}
	memcpy(out, s, (size_t)(p - s));
	strcat(out, to);
	strcat(out, p + strlen(from));
	return out;
}

/* Remove a JSON member "name":<value> including its leading comma. */
static char *
strip_member(const char *s, const char *name)
{
	char pat[64];
	const char *p, *e;
	char *out;

	snprintf(pat, sizeof(pat), ",\"%s\":", name);
	p = strstr(s, pat);
	if (!p) {
		return NULL;
	}
	e = p + strlen(pat);
	if (*e == '"') {
		e = strchr(e + 1, '"') + 1;
	} else {
		while (*e && *e != ',' && *e != '}') {
			e++;
		}
	}
	out = calloc(1, strlen(s) + 1);
	if (!out) {
		return NULL;
	}
	memcpy(out, s, (size_t)(p - s));
	strcat(out, e);
	return out;
}

struct rd {
	bool    done;
	int     status;
	uint8_t *buf;
	struct spdk_bs_dev_cb_args args;
};

static void
rd_done(struct spdk_io_channel *ch, void *arg, int status)
{
	struct rd *r = arg;

	(void)ch;
	r->done = true;
	r->status = status;
}

static void
rd_start(struct spdk_bs_dev *dev, struct spdk_io_channel *ch, struct rd *r,
	 uint64_t byte_off, uint32_t len)
{
	memset(r, 0, sizeof(*r));
	r->buf = malloc(len);
	memset(r->buf, 0xEE, len);
	r->args.cb_fn = rd_done;
	r->args.cb_arg = r;
	r->args.channel = ch;
	dev->read(dev, ch, r->buf, byte_off / BLOCK, len / BLOCK, &r->args);
}

static bool
rd_matches(const struct rd *r, int chunk, uint64_t off_in_chunk, uint32_t len)
{
	uint32_t i;

	for (i = 0; i < len; i++) {
		if (r->buf[i] != obj_byte(chunk, off_in_chunk + i)) {
			printf("\t  mismatch chunk %d off %" PRIu64 "\n", chunk,
			       off_in_chunk + i);
			return false;
		}
	}
	return true;
}

static void
pf_cb(void *arg, uint64_t chunk_index, int status)
{
	(void)arg;
	(void)chunk_index;
	(void)status;
}

int
main(void)
{
	struct spdk_env_opts opts;
	struct s3_export_manifest *m = NULL, *p = NULL;
	struct spdk_bs_dev *dev = NULL;
	struct spdk_io_channel *ch = NULL;
	struct s3_export_prefetcher *pf = NULL;
	struct s3_export_prefetch_progress pg;
	char *json = NULL, *bad = NULL;
	size_t len = 0;
	uint64_t present[NUM_CHUNKS], n_present = 0, i;
	char key[S3_EXPORT_KEY_MAX];
	uint64_t off;
	uint32_t rlen;
	struct rd r;
	bool ok;
	int rc;

	spdk_log_set_print_level(SPDK_LOG_ERROR);
	spdk_log_open(NULL);
	printf("=== s3lvol pack export test (version 6) ===\n");

	/* Exact GET counts: no hedges, no per-chunk retries, and (for [4]) one
	 * range per pack: 4 MiB packs here, 4 MiB ranges. */
	setenv("S3LVOL_S3_HEDGE", "0", 1);
	setenv("S3LVOL_IMPORT_CHUNK_RETRIES", "0", 1);
	setenv("S3LVOL_PACK_RANGE_MIB", "4", 1);
	s3_hedge_config_load();

	printf("\n[1] SPDK env and a thread\n");
	opts.opts_size = sizeof(opts);
	spdk_env_opts_init(&opts);
	opts.name = "s3_export_pack_test";
	opts.no_huge = true;
	opts.mem_size = 64;
	if (spdk_env_init(&opts) < 0) {
		fprintf(stderr, "spdk_env_init failed; skipping\n");
		spdk_log_close();
		return 77;
	}
	rc = spdk_thread_lib_init(NULL, 0);
	check_true("spdk_thread_lib_init", rc == 0);
	g_thread = spdk_thread_create("pack_test", NULL);
	check_true("spdk_thread_create", g_thread != NULL);
	if (!g_thread) {
		goto out;
	}
	spdk_set_thread(g_thread);

	printf("\n[2] set_packs and the round trip\n");
	m = make_delta();
	check_true("delta manifest built", m != NULL && s3_export_manifest_is_delta(m));
	if (!m) {
		goto out;
	}
	for (i = 0; i < NUM_CHUNKS; i++) {
		if (g_present[i]) {
			present[n_present++] = i;
		}
	}
	check_u64("own chunks", m->present_chunks, n_present);
	rc = s3_export_manifest_set_packs(m, PACK_CHUNKS);
	check_true("set_packs", rc == 0 && s3_export_manifest_is_packed(m));
	check_u64("version 6", m->version, S3_EXPORT_PACK_VERSION);
	check_u64("pack count", m->pack_count, (n_present + PACK_CHUNKS - 1) / PACK_CHUNKS);
	ok = true;
	for (i = 0; i < n_present; i++) {
		uint32_t loc = m->pack_loc[present[i]];

		ok = ok && (loc >> 8) == i / PACK_CHUNKS && (loc & 0xff) == i % PACK_CHUNKS;
	}
	check_true("ascending chunks fill consecutive slots", ok);
	rc = s3_export_manifest_serialize(m, &json, &len);
	check_true("serialize", rc == 0 && json != NULL);
	if (rc != 0) {
		goto out;
	}
	check_true("wire carries version 6", strstr(json, "\"version\":6") != NULL);
	rc = s3_export_manifest_parse(json, len, &p);
	check_true("parse", rc == 0 && p != NULL);
	if (rc != 0) {
		goto out;
	}
	check_true("parsed is packed", s3_export_manifest_is_packed(p));
	check_true("parsed is a delta over the same base",
		   s3_export_manifest_is_delta(p) && strcmp(p->base_uuid, BASE_UUID) == 0);
	ok = p->pack_chunks == m->pack_chunks && p->pack_count == m->pack_count;
	for (i = 0; i < NUM_CHUNKS && ok; i++) {
		if (g_present[i]) {
			ok = p->pack_loc[i] == m->pack_loc[i];
		}
	}
	check_true("every location survives the round trip", ok);
	rc = s3_export_manifest_pack_range(p, present[5], key, sizeof(key), &off, &rlen);
	check_true("pack_range of the 6th own chunk", rc == 0 &&
		   strstr(key, "/exports/" TEST_UUID "/pack-00000001") != NULL &&
		   off == (uint64_t)1 * CHUNK_SIZE && rlen == CHUNK_SIZE);
	check_true("pack_range of a base chunk is ENOENT",
		   s3_export_manifest_pack_range(p, 1, key, sizeof(key), &off, &rlen) == -ENOENT);

	printf("\n[3] older readers and malformed tables are refused\n");
	check_true("version 6 is above the delta version an older reader accepts",
		   S3_EXPORT_PACK_VERSION > S3_EXPORT_DELTA_VERSION);
	bad = replace_once(json, "\"version\":6", "\"version\":7");
	check_true("version 7 refused", bad && s3_export_manifest_parse(bad, strlen(bad), &m) != 0);
	free(bad);
	bad = strip_member(json, "pack_loc");
	check_true("version 6 without a pack table refused",
		   bad && s3_export_manifest_parse(bad, strlen(bad), &m) != 0);
	free(bad);
	bad = replace_once(json, "\"pack_count\":3", "\"pack_count\":2");
	check_true("a location past the last pack refused",
		   bad && s3_export_manifest_parse(bad, strlen(bad), &m) != 0);
	free(bad);
	bad = replace_once(json, "\"version\":6", "\"version\":5");
	check_true("a version 5 manifest carrying a pack table refused",
		   bad && s3_export_manifest_parse(bad, strlen(bad), &m) != 0);
	free(bad);
	bad = NULL;

	printf("\n[4] partial reads are served from one GET of the whole pack\n");
	g_m = p;
	rc = s3_export_bs_dev_create((struct s3_client *)0x1, p, &dev);
	check_true("export device", rc == 0 && dev != NULL);
	if (rc != 0) {
		goto out;
	}
	ch = dev->create_channel(dev);
	g_gets = g_pack_gets = g_ref_gets = 0;
	/* 16 KiB, 8 KiB into own chunk 9 (the 3rd own chunk: pack 0, slot 2). */
	rd_start(dev, ch, &r, (uint64_t)9 * CHUNK_SIZE + 2 * BLOCK, 4 * BLOCK);
	poll_some(10);
	check_u64("one GET", g_gets, 1);
	check_u64("of a pack, never the ref object", g_pack_gets, 1);
	check_u64("from the start of the pack", g_last_off, 0);
	check_u64("for the whole pack", g_last_len, (uint64_t)PACK_CHUNKS * CHUNK_SIZE);
	fake_complete_all();
	poll_some(10);
	check_true("read completes", r.done && r.status == 0);
	check_true("bytes are the chunk's", rd_matches(&r, 9, 2 * BLOCK, 4 * BLOCK));
	free(r.buf);
	/* Another chunk of the same pack (own chunk 12: pack 0, slot 3) needs
	 * no further request. */
	g_gets = 0;
	rd_start(dev, ch, &r, (uint64_t)12 * CHUNK_SIZE + BLOCK, 2 * BLOCK);
	poll_some(10);
	check_u64("same pack: no new GET", g_gets, 0);
	check_true("same pack: bytes are the chunk's",
		   r.done && r.status == 0 && rd_matches(&r, 12, BLOCK, 2 * BLOCK));
	free(r.buf);
	/* A base chunk is not this device's to serve: it reads as a hole here
	 * and costs no request (the delta import clones the base for those). */
	g_gets = 0;
	rd_start(dev, ch, &r, (uint64_t)1 * CHUNK_SIZE, BLOCK);
	poll_some(10);
	check_true("base chunk: no GET", g_gets == 0 && r.done && r.status == 0);
	free(r.buf);
	g_gets = g_pack_gets = g_ref_gets = 0;
	rc = s3_export_prefetcher_create(dev, present, n_present, 4, pf_cb, NULL, &pf);
	check_true("prefetcher", rc == 0);
	s3_export_prefetcher_pump(pf);
	for (i = 0; i < 40; i++) {
		poll_some(5);
		fake_complete_all();
		poll_some(5);
		s3_export_prefetcher_release_below(pf, UINT64_MAX);
	}
	s3_export_prefetcher_get_progress(pf, &pg);
	check_u64("prefetch fetched every own chunk", pg.fetched, n_present);
	check_u64("prefetch status", (uint64_t)(pg.status == 0), 1);
	check_u64("prefetch GETs are all pack ranges", g_ref_gets, 0);
	check_true("prefetch issues one GET per pack, not per chunk",
		   g_pack_gets <= p->pack_count);
	s3_export_prefetcher_destroy(pf);
	pf = NULL;

	printf("\n[5] a missing pack fails closed\n");
	/* A fresh device: the prefetch above left every chunk in the old one's
	 * slot cache, which would serve the read without asking S3 at all. */
	dev->destroy_channel(dev, ch);
	dev->destroy(dev);
	poll_some(50);
	ch = NULL;
	dev = NULL;
	rc = s3_export_bs_dev_create((struct s3_client *)0x1, p, &dev);
	check_true("fresh export device", rc == 0 && dev != NULL);
	if (rc != 0) {
		goto out;
	}
	ch = dev->create_channel(dev);
	s3_export_pack_key(SRC_PREFIX, TEST_UUID, 2, g_missing_pack, sizeof(g_missing_pack));
	g_gets = g_pack_gets = g_ref_gets = 0;
	/* Own chunk present[8] lives in pack 2. */
	rd_start(dev, ch, &r, present[8] * CHUNK_SIZE, BLOCK);
	for (i = 0; i < 10; i++) {
		poll_some(5);
		fake_complete_all();
	}
	poll_some(20);
	check_true("read fails", r.done && r.status != 0);
	check_u64("never falls back to the ref object", g_ref_gets, 0);
	check_true("buffer is not zero-filled", r.buf[0] == 0xEE);
	free(r.buf);
	g_missing_pack[0] = '\0';

	printf("\n[5b] ranged pack fetch: split, assembly, a failed range retried alone\n");
	check_u64("range count 16 MiB / 4 MiB", s3_export_range_count(16u << 20, 4u << 20), 4);
	check_u64("range count 1 MiB object is one", s3_export_range_count(1u << 20, 4u << 20), 1);
	check_u64("range count rounds up", s3_export_range_count((9u << 20), 4u << 20), 3);
	check_u64("range count with no size", s3_export_range_count(4u << 20, 0), 1);
	dev->destroy_channel(dev, ch);
	dev->destroy(dev);
	poll_some(50);
	ch = NULL;
	dev = NULL;
	/* 1 MiB ranges over 4 MiB packs: 4 ranges per pack, retries on. */
	setenv("S3LVOL_PACK_RANGE_MIB", "1", 1);
	setenv("S3LVOL_IMPORT_CHUNK_RETRIES", "3", 1);
	setenv("S3LVOL_IMPORT_RETRY_BASE_MS", "1", 1);
	setenv("S3LVOL_IMPORT_RETRY_CAP_MS", "1", 1);
	rc = s3_export_bs_dev_create((struct s3_client *)0x1, p, &dev);
	check_true("ranged export device", rc == 0 && dev != NULL);
	if (rc != 0) {
		goto out;
	}
	ch = dev->create_channel(dev);
	g_gets = g_pack_gets = g_ref_gets = 0;
	g_nseen = 0;
	/* The range at pack offset 2 MiB stalls once after 300 KiB. */
	g_fail_off = 2u * CHUNK_SIZE;
	g_fail_prefix = 300 * 1024;
	g_fail_left = 1;
	/* Read own chunk 9 (pack 0, slot 2): its range is the one that fails. */
	rd_start(dev, ch, &r, (uint64_t)9 * CHUNK_SIZE + BLOCK, 8 * BLOCK);
	for (i = 0; i < 40 && !r.done; i++) {
		poll_some(5);
		fake_complete_all();
		usleep(2000);
		poll_some(5);
	}
	check_true("read completes after the retry", r.done && r.status == 0);
	check_true("bytes assembled from the ranges are the chunk's",
		   r.done && rd_matches(&r, 9, BLOCK, 8 * BLOCK));
	check_u64("4 range GETs + 1 retry", g_gets, 5);
	{
		uint32_t k, at_fail = 0, others = 0;
		bool sized = true;

		for (k = 0; k < g_nseen; k++) {
			if (g_seen_off[k] == 2u * CHUNK_SIZE) {
				at_fail++;
			} else {
				others++;
			}
			sized = sized && g_seen_len[k] == CHUNK_SIZE;
		}
		check_true("every GET is exactly one 1 MiB range", sized);
		/* This fake does not model the gate's prefix, so the retry asks
		 * for the whole range again; s3_hedge_test [8c] covers the
		 * ranged resume of a single range. */
		check_u64("only the failed range was sent again", at_fail, 2);
		check_u64("the other 3 ranges were sent once", others, 3);
	}
	free(r.buf);
	/* Every chunk of pack 0 is in the slot now: another read costs nothing. */
	g_gets = 0;
	rd_start(dev, ch, &r, (uint64_t)3 * CHUNK_SIZE, 2 * BLOCK);
	poll_some(10);
	check_true("assembled slot serves the other chunks of the pack",
		   g_gets == 0 && r.done && r.status == 0 && rd_matches(&r, 3, 0, 2 * BLOCK));
	free(r.buf);
	{
		struct s3_export_bs_dev_stats st;

		s3_export_bs_dev_get_stats(dev, &st);
		check_u64("ranged_fetches counted", st.ranged_fetches, 1);
		check_u64("one range retry", st.range_retries, 1);
	}
	/* A range that fails for good fails the read, after its own retries,
	 * and nothing reads as zeroes. */
	dev->destroy_channel(dev, ch);
	dev->destroy(dev);
	poll_some(50);
	rc = s3_export_bs_dev_create((struct s3_client *)0x1, p, &dev);
	ch = dev->create_channel(dev);
	g_fail_off = 1u * CHUNK_SIZE;
	g_fail_prefix = 0;
	g_fail_left = 100;
	g_gets = 0;
	rd_start(dev, ch, &r, (uint64_t)6 * CHUNK_SIZE, BLOCK);
	for (i = 0; i < 60 && !r.done; i++) {
		poll_some(5);
		fake_complete_all();
		usleep(2000);
		poll_some(5);
	}
	check_true("a range out of retries fails the read", r.done && r.status != 0);
	check_true("failed read is not zero-filled", r.buf[0] == 0xEE);
	check_u64("that range: 1 + 3 retries; the others once", g_gets, 4 + 3);
	free(r.buf);
	g_fail_off = UINT64_MAX;
	g_fail_left = 0;
	unsetenv("S3LVOL_IMPORT_RETRY_BASE_MS");
	unsetenv("S3LVOL_IMPORT_RETRY_CAP_MS");
	setenv("S3LVOL_IMPORT_CHUNK_RETRIES", "0", 1);
	setenv("S3LVOL_PACK_RANGE_MIB", "4", 1);

	printf("\n[6] pack GC\n");
	g_ndeleted = 0;
	rc = s3_export_packs_delete((struct s3_client *)0x1, SRC_PREFIX, TEST_UUID,
				    p->pack_count, NULL, NULL);
	check_true("packs_delete", rc == 0);
	check_u64("deleted exactly pack_count objects", g_ndeleted, p->pack_count);
	ok = true;
	for (i = 0; i < g_ndeleted; i++) {
		char want[S3_EXPORT_KEY_MAX];

		s3_export_pack_key(SRC_PREFIX, TEST_UUID, (uint32_t)i, want, sizeof(want));
		ok = ok && strcmp(want, g_deleted[i]) == 0;
	}
	check_true("and only this export's packs", ok);

	printf("\n[7] S3LVOL_EXPORT_PACK_MIB\n");
	unsetenv("S3LVOL_EXPORT_PACK_MIB");
	check_u64("default 16 MiB", s3_export_pack_chunks_from_env(CHUNK_SIZE), 16);
	setenv("S3LVOL_EXPORT_PACK_MIB", "32", 1);
	check_u64("32 MiB", s3_export_pack_chunks_from_env(CHUNK_SIZE), 32);
	setenv("S3LVOL_EXPORT_PACK_MIB", "1", 1);
	check_u64("clamped up to 4", s3_export_pack_chunks_from_env(CHUNK_SIZE), 4);
	setenv("S3LVOL_EXPORT_PACK_MIB", "512", 1);
	check_u64("clamped down to 64", s3_export_pack_chunks_from_env(CHUNK_SIZE), 64);
	unsetenv("S3LVOL_EXPORT_PACK_MIB");
	setenv("S3LVOL_EXPORT_PACKS", "0", 1);
	check_true("flag off", !s3_export_packs_enabled());
	setenv("S3LVOL_EXPORT_PACKS", "1", 1);
	check_true("flag on", s3_export_packs_enabled());

out:
	if (ch) {
		dev->destroy_channel(dev, ch);
	}
	if (dev) {
		dev->destroy(dev);
		poll_some(50);
	}
	s3_export_manifest_unref(p);
	s3_export_manifest_unref(m);
	free(json);
	if (g_thread) {
		spdk_thread_exit(g_thread);
		while (!spdk_thread_is_exited(g_thread)) {
			spdk_thread_poll(g_thread, 0, 0);
		}
		spdk_thread_destroy(g_thread);
	}
	spdk_thread_lib_fini();
	printf("\n=== %d passed, %d failed ===\n", g_pass, g_fail);
	spdk_log_close();
	return g_fail ? 1 : 0;
}
