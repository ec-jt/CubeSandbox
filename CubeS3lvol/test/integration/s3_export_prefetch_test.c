/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   Whole-chunk reads and prefetch of an imported export (B19)
 *
 *   === What this is for ===
 *
 *   A cross-node restore mounted an imported metadata disk whose every guest
 *   read went to S3 as a ranged GET sized to the I/O (4 KiB). On Ceph RGW one
 *   such GET took 6-28 s and the mount timed out. The fix is that the export
 *   device never issues a GET smaller than the object: a present chunk is
 *   fetched whole, once, and every reader of it is served from that buffer; and
 *   an import can prefetch every present chunk, whole and in parallel, before
 *   the guest needs it.
 *
 *   This checks exactly those properties, against an in-memory S3 whose GETs
 *   complete only when the test says so, so request shape, count, and how many
 *   are in flight at once are all observable:
 *
 *     [2] a prefetch of N present chunks issues exactly N GETs, each for the
 *         whole object (offset 0, length = valid_bytes), never more than the
 *         window in flight, and never more than the window held;
 *     [3] after the prefetch, reads of every present chunk do no S3 I/O and
 *         return the object's bytes; holes and the REF tail read as zeroes;
 *     [4] a miss with no prefetch does one whole-object GET, and concurrent
 *         readers of the same chunk wait on it rather than issuing their own;
 *     [5] a prefetch GET that fails is reported in the progress status.
 *
 *   No network, no credentials: s3_get_range and friends are defined here,
 *   which keeps s3_client_aws.o out of the link.
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

#define CHUNK_SIZE  (1024 * 1024)
#define NUM_CHUNKS  16
#define BLOCK       4096
#define TEST_UUID   "3f2504e0-4f89-11d3-9a0c-0305e82c3301"
#define SRC_PREFIX  "srclvs"
#define TAIL_CHUNK  5          /* REF chunk with a short object */
#define TAIL_VALID  (CHUNK_SIZE / 2 + 3 * BLOCK)

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
 * In-memory S3
 * ========================================================================== */

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
static uint32_t g_gets;           /* every GET issued */
static uint32_t g_small_gets;     /* GETs that were not whole objects */
static uint32_t g_inflight, g_peak_inflight;
static char     g_fail_key[S3_EXPORT_KEY_MAX];

static struct spdk_uuid g_uuids[NUM_CHUNKS];
static uint32_t         g_valid[NUM_CHUNKS];
static bool             g_present[NUM_CHUNKS];

static int
key_to_chunk(const char *key)
{
	char k[S3_EXPORT_KEY_MAX];
	int i;

	for (i = 0; i < NUM_CHUNKS; i++) {
		if (!g_present[i]) {
			continue;
		}
		s3_chunk_data_key(SRC_PREFIX, &g_uuids[i], k, sizeof(k));
		if (strcmp(k, key) == 0) {
			return i;
		}
	}
	return -1;
}

/* The byte an object holds at a given offset: anything that tells chunks and
 * offsets apart and is never zero, so a zero-fill is distinguishable. */
static uint8_t
obj_byte(int chunk, uint64_t off)
{
	return (uint8_t)(((chunk * 37) + (off / BLOCK) * 11 + (off % 251)) % 255 + 1);
}

int
s3_get_range(struct s3_client *client, const char *key, uint64_t offset,
	     uint64_t len, void *buf, s3_get_cb cb, void *cb_arg)
{
	struct fake_get *g = calloc(1, sizeof(*g));
	int c = key_to_chunk(key);

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
	if (c < 0 || offset != 0 || len != g_valid[c]) {
		g_small_gets++;
	}
	g_inflight++;
	if (g_inflight > g_peak_inflight) {
		g_peak_inflight = g_inflight;
	}
	TAILQ_INSERT_TAIL(&g_pending, g, link);
	return 0;
}

/* Complete up to n pending GETs, oldest first. */
static uint32_t
fake_complete(uint32_t n)
{
	uint32_t done = 0;

	while (done < n && !TAILQ_EMPTY(&g_pending)) {
		struct fake_get *g = TAILQ_FIRST(&g_pending);
		int c = key_to_chunk(g->key);
		uint64_t i, got = 0;
		int status = 0;

		TAILQ_REMOVE(&g_pending, g, link);
		g_inflight--;
		if (c < 0) {
			status = -ENOENT;
		} else if (g_fail_key[0] && strcmp(g_fail_key, g->key) == 0) {
			status = -EIO;
		} else {
			uint64_t end = spdk_min(g->offset + g->len, (uint64_t)g_valid[c]);

			for (i = g->offset; i < end; i++) {
				((uint8_t *)g->buf)[i - g->offset] = obj_byte(c, i);
			}
			got = end > g->offset ? end - g->offset : 0;
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
	(void)key;
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
make_manifest(void)
{
	struct s3_export_manifest *m = NULL;
	int i;

	if (s3_export_manifest_create(TEST_UUID, (uint64_t)NUM_CHUNKS * CHUNK_SIZE,
				      CHUNK_SIZE, S3_EXPORT_LAYOUT_REF, &m) != 0) {
		return NULL;
	}
	m->generation = 1;
	snprintf(m->src.prefix, sizeof(m->src.prefix), "%s", SRC_PREFIX);
	/* The read path takes each chunk's prefix from the source table. */
	snprintf(m->srcs[0].prefix, sizeof(m->srcs[0].prefix), "%s", SRC_PREFIX);
	for (i = 0; i < NUM_CHUNKS; i++) {
		uint8_t raw[16];

		/* chunks 1, 3, 5, 6, 7, 9, 10, 12, 14: nine present, the rest holes */
		g_present[i] = (i == 1 || i == 3 || i == TAIL_CHUNK || i == 6 ||
				i == 7 || i == 9 || i == 10 || i == 12 || i == 14);
		if (!g_present[i]) {
			continue;
		}
		memset(raw, 0x20 + i, sizeof(raw));
		memcpy(&g_uuids[i], raw, sizeof(g_uuids[i]));
		g_valid[i] = (i == TAIL_CHUNK) ? TAIL_VALID : CHUNK_SIZE;
		s3_export_manifest_set_ref(m, i, &g_uuids[i], g_valid[i]);
	}
	s3_export_manifest_seal(m);
	return m;
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

/* Data check: the object's bytes up to valid, zeroes past it. */
static bool
rd_matches(const struct rd *r, int chunk, uint64_t off_in_chunk, uint32_t len)
{
	uint32_t i;

	for (i = 0; i < len; i++) {
		uint64_t o = off_in_chunk + i;
		uint8_t want = (g_present[chunk] && o < g_valid[chunk]) ? obj_byte(chunk, o) : 0;

		if (r->buf[i] != want) {
			printf("\t  mismatch chunk %d off %" PRIu64 ": got %u want %u\n",
			       chunk, o, r->buf[i], want);
			return false;
		}
	}
	return true;
}

struct pf_seen {
	uint32_t ok;
	uint32_t failed;
};

static void
pf_cb(void *arg, uint64_t chunk_index, int status)
{
	struct pf_seen *s = arg;

	(void)chunk_index;
	if (status == 0) {
		s->ok++;
	} else {
		s->failed++;
	}
}

static void
reset_counters(void)
{
	g_gets = 0;
	g_small_gets = 0;
	g_peak_inflight = 0;
	g_fail_key[0] = '\0';
}

/* ==========================================================================
 * main
 * ========================================================================== */

int
main(void)
{
	struct spdk_env_opts opts;
	struct s3_export_manifest *m = NULL;
	struct spdk_bs_dev *dev = NULL, *dev2 = NULL, *dev3 = NULL;
	struct spdk_io_channel *ch = NULL;
	struct s3_export_prefetcher *pf = NULL;
	struct s3_export_prefetch_progress pg;
	struct s3_export_bs_dev_stats st;
	struct pf_seen seen = {0};
	uint64_t present[NUM_CHUNKS];
	uint32_t n_present = 0, window = 3, held_max = 0, i;
	struct rd r[4];
	bool all_ok;
	int rc;

	spdk_log_set_print_level(SPDK_LOG_ERROR);
	spdk_log_open(NULL);
	printf("=== s3lvol export whole-chunk + prefetch test (B19) ===\n");

	printf("\n[1] SPDK env, a thread, and a REF export with holes and a short tail\n");
	opts.opts_size = sizeof(opts);
	spdk_env_opts_init(&opts);
	opts.name = "s3_export_prefetch_test";
	opts.no_huge = true;
	opts.mem_size = 64;
	if (spdk_env_init(&opts) < 0) {
		fprintf(stderr, "spdk_env_init failed; skipping\n");
		spdk_log_close();
		return 77;
	}
	rc = spdk_thread_lib_init(NULL, 0);
	check_true("spdk_thread_lib_init", rc == 0);
	g_thread = spdk_thread_create("prefetch_test", NULL);
	check_true("spdk_thread_create", g_thread != NULL);
	if (!g_thread) {
		goto out;
	}
	spdk_set_thread(g_thread);

	m = make_manifest();
	check_true("manifest built", m != NULL);
	if (!m) {
		goto out;
	}
	for (i = 0; i < NUM_CHUNKS; i++) {
		if (g_present[i]) {
			present[n_present++] = i;
		}
	}
	check_u64("present chunks", m->present_chunks, n_present);

	rc = s3_export_bs_dev_create((struct s3_client *)0x1, m, &dev);
	check_true("export device created", rc == 0 && dev != NULL);
	if (rc != 0) {
		goto out;
	}
	check_true("recognised as an export device", s3_export_bs_dev_is_export(dev));
	ch = dev->create_channel(dev);

	printf("\n[2] prefetch: N whole-object GETs, window respected\n");
	reset_counters();
	rc = s3_export_prefetcher_create(dev, present, n_present, window, pf_cb, &seen, &pf);
	check_true("prefetcher created", rc == 0);
	s3_export_prefetcher_pump(pf);
	check_u64("pump launches exactly the window", g_inflight, window);
	/* The consumer releases nothing yet: completing GETs must not launch more,
	 * because the window bounds what is *held*, not just what is in flight. */
	fake_complete(window);
	poll_some(10);
	s3_export_prefetcher_get_progress(pf, &pg);
	check_u64("held chunks stay at the window until released", pg.held, window);
	check_u64("nothing new launched while the window is full", g_inflight, 0);
	/* Now consume in order, the way decouple does, and drain everything. */
	for (i = 0; i < n_present + 4; i++) {
		s3_export_prefetcher_get_progress(pf, &pg);
		if (pg.held > held_max) {
			held_max = pg.held;
		}
		fake_complete(1);
		poll_some(4);
		s3_export_prefetcher_get_progress(pf, &pg);
		if (pg.fetched > pg.released && pg.released < n_present) {
			/* release the oldest fetched chunk */
			s3_export_prefetcher_release_below(pf, present[pg.released] + 1);
		}
	}
	/* Release the rest only after all GETs; pins keep the data local. */
	s3_export_prefetcher_get_progress(pf, &pg);
	check_u64("every present chunk fetched", pg.fetched, n_present);
	check_u64("exactly N GETs", g_gets, n_present);
	check_u64("no GET below the whole object", g_small_gets, 0);
	check_true("never more than the window in flight", g_peak_inflight <= window);
	check_true("never more than the window held", held_max <= window);
	check_u64("prefetch status is clean", (uint64_t)(pg.status != 0), 0);
	check_u64("callback saw every chunk", seen.ok, n_present);

	printf("\n[3] after prefetch: reads are local; holes and the tail are zeroes\n");
	/* A fresh prefetcher over the whole list with a window big enough to hold
	 * everything, so every present chunk is in the slot table at once. */
	s3_export_prefetcher_destroy(pf);
	pf = NULL;
	reset_counters();
	rc = s3_export_prefetcher_create(dev, present, n_present, n_present, pf_cb, &seen, &pf);
	check_true("second prefetcher created", rc == 0);
	s3_export_prefetcher_pump(pf);
	while (fake_complete(64) > 0) {
		poll_some(4);
	}
	poll_some(10);
	s3_export_prefetcher_get_progress(pf, &pg);
	check_u64("all present chunks held", pg.fetched, n_present);
	reset_counters();
	all_ok = true;
	for (i = 0; i < n_present; i++) {
		int c = (int)present[i];
		uint64_t off = (c == TAIL_CHUNK) ? (TAIL_VALID / BLOCK) * BLOCK : 2 * BLOCK;

		rd_start(dev, ch, &r[0], (uint64_t)c * CHUNK_SIZE + off, 2 * BLOCK);
		poll_some(10);
		all_ok = all_ok && r[0].done && r[0].status == 0 &&
			 rd_matches(&r[0], c, off, 2 * BLOCK);
		free(r[0].buf);
	}
	check_true("every present chunk reads correctly", all_ok);
	check_u64("and none of those reads touched S3", g_gets, 0);
	/* A hole: chunk 0. */
	rd_start(dev, ch, &r[0], 0, 4 * BLOCK);
	poll_some(4);
	check_true("a hole reads as zeroes without S3",
		   r[0].done && r[0].status == 0 && rd_matches(&r[0], 0, 0, 4 * BLOCK) &&
		   g_gets == 0);
	free(r[0].buf);
	/* Entirely past the REF tail. */
	rd_start(dev, ch, &r[0], (uint64_t)TAIL_CHUNK * CHUNK_SIZE + CHUNK_SIZE - 2 * BLOCK,
		 2 * BLOCK);
	poll_some(4);
	check_true("past the REF tail reads as zeroes without S3",
		   r[0].done && r[0].status == 0 &&
		   rd_matches(&r[0], TAIL_CHUNK, CHUNK_SIZE - 2 * BLOCK, 2 * BLOCK) &&
		   g_gets == 0);
	free(r[0].buf);
	while (!s3_export_prefetcher_idle(pf)) {
		poll_some(1);
	}
	s3_export_prefetcher_destroy(pf);
	pf = NULL;

	printf("\n[4] a miss: one whole-object GET serves concurrent readers\n");
	/* A new device has an empty slot table. */
	rc = s3_export_bs_dev_create((struct s3_client *)0x1, m, &dev2);
	check_true("second device created", rc == 0);
	{
		struct spdk_io_channel *ch2 = dev2->create_channel(dev2);
		int c = 9;
		uint64_t base = (uint64_t)c * CHUNK_SIZE;

		reset_counters();
		rd_start(dev2, ch2, &r[0], base + 0, BLOCK);
		rd_start(dev2, ch2, &r[1], base + 64 * BLOCK, BLOCK);
		rd_start(dev2, ch2, &r[2], base + 255 * BLOCK, BLOCK);
		poll_some(10);
		check_u64("three readers, one GET", g_gets, 1);
		check_u64("and it is the whole object", g_small_gets, 0);
		check_true("nobody completes before the object arrives",
			   !r[0].done && !r[1].done && !r[2].done);
		fake_complete(1);
		poll_some(10);
		check_true("all three complete from it",
			   r[0].done && r[1].done && r[2].done &&
			   r[0].status == 0 && r[1].status == 0 && r[2].status == 0);
		check_true("with the right bytes",
			   rd_matches(&r[0], c, 0, BLOCK) &&
			   rd_matches(&r[1], c, 64 * BLOCK, BLOCK) &&
			   rd_matches(&r[2], c, 255 * BLOCK, BLOCK));
		free(r[0].buf);
		free(r[1].buf);
		free(r[2].buf);
		/* A later read of the same chunk is a slot hit. */
		reset_counters();
		rd_start(dev2, ch2, &r[3], base + 7 * BLOCK, BLOCK);
		poll_some(4);
		check_true("a later read of it is local",
			   r[3].done && r[3].status == 0 && g_gets == 0 &&
			   rd_matches(&r[3], c, 7 * BLOCK, BLOCK));
		free(r[3].buf);
		/* The short tail chunk on demand: one GET of exactly valid_bytes, and
		 * a read straddling valid_bytes gets bytes then zeroes. */
		reset_counters();
		rd_start(dev2, ch2, &r[0], (uint64_t)TAIL_CHUNK * CHUNK_SIZE +
			 (TAIL_VALID / BLOCK) * BLOCK - BLOCK, 4 * BLOCK);
		poll_some(4);
		fake_complete(1);
		poll_some(4);
		check_true("a read straddling the REF tail is bytes then zeroes",
			   r[0].done && r[0].status == 0 && g_gets == 1 && g_small_gets == 0 &&
			   rd_matches(&r[0], TAIL_CHUNK, (TAIL_VALID / BLOCK) * BLOCK - BLOCK,
				      4 * BLOCK));
		free(r[0].buf);
		if (s3_export_bs_dev_get_stats(dev2, &st) == 0) {
			check_u64("stats: joins counted", st.slot_joins, 2);
		}
		dev2->destroy_channel(dev2, ch2);
	}

	printf("\n[5] a failed prefetch GET is reported\n");
	rc = s3_export_bs_dev_create((struct s3_client *)0x1, m, &dev3);
	check_true("third device created", rc == 0);
	reset_counters();
	s3_chunk_data_key(SRC_PREFIX, &g_uuids[present[2]], g_fail_key, sizeof(g_fail_key));
	memset(&seen, 0, sizeof(seen));
	rc = s3_export_prefetcher_create(dev3, present, n_present, 4, pf_cb, &seen, &pf);
	check_true("prefetcher created", rc == 0);
	s3_export_prefetcher_pump(pf);
	while (fake_complete(64) > 0) {
		poll_some(4);
	}
	s3_export_prefetcher_get_progress(pf, &pg);
	check_true("status carries the error", pg.status == -EIO);
	check_u64("the failure reached the callback", seen.failed, 1);
	check_true("no further GETs launched after it",
		   g_gets <= 4 && pg.launched <= 4);
	check_true("idle once its GETs are done", s3_export_prefetcher_idle(pf));
	s3_export_prefetcher_destroy(pf);
	pf = NULL;

	dev3->destroy(dev3);
	dev2->destroy(dev2);
	dev->destroy_channel(dev, ch);
	dev->destroy(dev);
	poll_some(100);

out:
	if (m) {
		s3_export_manifest_unref(m);
	}
	if (g_thread) {
		spdk_thread_exit(g_thread);
		while (!spdk_thread_is_exited(g_thread)) {
			spdk_thread_poll(g_thread, 0, 0);
		}
		spdk_thread_destroy(g_thread);
		spdk_set_thread(NULL);
	}
	spdk_thread_lib_fini();
	printf("\n=== %d passed, %d failed ===\n", g_pass, g_fail);
	spdk_log_close();
	return g_fail == 0 ? 0 : 1;
}
