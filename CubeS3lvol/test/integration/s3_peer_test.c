/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   Peer pull: an importer reads packs from the source node before S3
 *
 *   Runs the real peer server on 127.0.0.1 and the real export device against
 *   an in-memory S3 that can be told to answer late or fail. Checks:
 *
 *     [2] manifest: pack digests round-trip, are covered by the crc, and are
 *         refused on a version that cannot carry them;
 *     [3] peer hit: every pack comes from the peer, S3 sees no GET, the bytes
 *         are right;
 *     [4] peer down (nothing listening): every pack falls back to S3, the read
 *         succeeds, the breaker stops asking after 3 failures;
 *     [5] digest mismatch (the peer copy is corrupt): S3 serves it, counted
 *         as a mismatch, the corrupt bytes never reach the reader;
 *     [6] peer slow (the server is stalled): the hedge starts S3, S3 wins;
 *     [7] auth failure (client and server keys differ): refused, S3 serves;
 *     [8] peer 404 (no local copy): S3 serves;
 *     [9] rescue: the peer is slow and S3 then fails for good, the peer's late
 *         verified copy is used;
 *    [10] the server refuses a wildcard bind and starts without a secret no
 *         longer;
 *    [12] peer_ready manifest: packs_pending round-trips, is covered by the
 *         crc, and is refused without pack digests;
 *    [13] pin: a pinned export is stored past the cap and never evicted;
 *         unpinned, it is an ordinary cache entry again;
 *    [14] peer-only read (packs_pending): every pack from the peer, S3 never
 *         asked; with the peer down or a pack missing the read fails cleanly,
 *         still without a single S3 GET for a pack;
 *    [15] background upload: every pinned pack is PUT after its digest is
 *         checked; a corrupt local copy stops it with -EBADMSG; a cancel stops
 *         it.
 *
 *   No network beyond loopback, no credentials.
 */

#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/thread.h"
#include "spdk/blob.h"
#include "spdk/util.h"

#include "s3lvol/s3_export.h"
#include "s3lvol/s3_client.h"
#include "s3lvol/s3_hedge.h"
#include "s3lvol/s3_peer.h"

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
	check_true(what, got == want);
	if (got != want) {
		printf("\t       got %" PRIu64 ", want %" PRIu64 "\n", got, want);
	}
}

/* ==========================================================================
 * Content and the in-memory S3
 * ========================================================================== */

static bool g_present[NUM_CHUNKS];
static struct s3_export_manifest *g_m;
static uint32_t g_s3_gets;
static bool g_s3_fail;          /* every S3 GET fails (-EIO) */
static uint32_t g_s3_delay_polls;

static uint8_t
obj_byte(int chunk, uint64_t off)
{
	return (uint8_t)(((chunk * 37) + (off / BLOCK) * 11 + (off % 251)) % 255 + 1);
}

static int
pack_slot_chunk(uint32_t pack, uint64_t offset)
{
	uint64_t i;

	for (i = 0; i < NUM_CHUNKS; i++) {
		uint32_t loc;

		if (!g_present[i]) {
			continue;
		}
		loc = g_m->pack_loc[i];
		if ((loc >> 8) == pack && offset / CHUNK_SIZE == (loc & 0xff)) {
			return (int)i;
		}
	}
	return -1;
}

static void
fill_pack(uint32_t pack, uint8_t *buf, uint64_t off, uint64_t len)
{
	uint64_t i;

	for (i = 0; i < len; i++) {
		int c = pack_slot_chunk(pack, off + i);

		buf[i] = c < 0 ? 0 : obj_byte(c, (off + i) % CHUNK_SIZE);
	}
}

static uint32_t
pack_len(uint32_t pack)
{
	uint64_t first = (uint64_t)pack * g_m->pack_chunks;

	return (uint32_t)spdk_min((uint64_t)g_m->pack_chunks,
				  g_m->present_chunks - first) * CHUNK_SIZE;
}

struct fake_get {
	char      key[S3_EXPORT_KEY_MAX];
	uint64_t  offset, len;
	void     *buf;
	s3_get_cb cb;
	void     *cb_arg;
	uint32_t  due;
	TAILQ_ENTRY(fake_get) link;
};
static TAILQ_HEAD(, fake_get) g_pending = TAILQ_HEAD_INITIALIZER(g_pending);

int
s3_get_range(struct s3_client *client, const char *key, uint64_t offset,
	     uint64_t len, void *buf, s3_get_cb cb, void *cb_arg)
{
	struct fake_get *g = calloc(1, sizeof(*g));

	(void)client;
	snprintf(g->key, sizeof(g->key), "%s", key);
	g->offset = offset;
	g->len = len;
	g->buf = buf;
	g->cb = cb;
	g->cb_arg = cb_arg;
	g->due = g_s3_delay_polls;
	g_s3_gets++;
	TAILQ_INSERT_TAIL(&g_pending, g, link);
	return 0;
}

static void
fake_complete(void)
{
	struct fake_get *g, *tmp;

	TAILQ_FOREACH_SAFE(g, &g_pending, link, tmp) {
		unsigned int pack = 0;
		const char *p;

		if (g->due > 0) {
			g->due--;
			continue;
		}
		TAILQ_REMOVE(&g_pending, g, link);
		p = strstr(g->key, "/pack-");
		if (g_s3_fail || !p || sscanf(p, "/pack-%x", &pack) != 1) {
			g->cb(g->cb_arg, 0, g_s3_fail ? -EIO : -ENOENT);
		} else {
			fill_pack(pack, g->buf, g->offset, g->len);
			g->cb(g->cb_arg, g->len, 0);
		}
		free(g);
	}
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

static bool g_put_ok;            /* PUTs succeed and are recorded */
static uint32_t g_puts, g_put_fail_first;
static char g_put_keys[16][S3_EXPORT_KEY_MAX];

int
s3_put(struct s3_client *client, const char *key, struct iovec *iov, int iovcnt,
       bool if_none_match, s3_op_cb cb, void *cb_arg)
{
	(void)client;
	(void)iov;
	(void)iovcnt;
	(void)if_none_match;
	if (!g_put_ok) {
		if (cb) {
			cb(cb_arg, -ENOTSUP);
		}
		return 0;
	}
	if (g_put_fail_first > 0) {
		g_put_fail_first--;
		if (cb) {
			cb(cb_arg, -EIO);
		}
		return 0;
	}
	if (g_puts < 16) {
		snprintf(g_put_keys[g_puts], sizeof(g_put_keys[0]), "%s", key);
	}
	g_puts++;
	if (cb) {
		cb(cb_arg, 0);
	}
	return 0;
}

void
s3_client_get(struct s3_client *client)
{
	(void)client;
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
static char g_dir[128];
static char g_key_file[160];
static char g_bad_key_file[160];

static void
poll_ms(uint32_t ms)
{
	uint32_t i;

	for (i = 0; i < ms; i++) {
		spdk_thread_poll(g_thread, 0, 0);
		fake_complete();
		usleep(1000);
	}
}

static struct s3_export_manifest *
make_packed(void)
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
		struct spdk_uuid u;

		g_present[i] = (i % 3 == 0) && i != 0;
		if (!g_present[i]) {
			continue;
		}
		memset(&u, 0x40 + i, sizeof(u));
		s3_export_manifest_set_ref(m, i, &u, CHUNK_SIZE);
	}
	s3_export_manifest_set_base(m, BASE_UUID);
	s3_export_manifest_seal(m);
	s3_export_manifest_set_packs(m, PACK_CHUNKS);
	m->pack_sha = calloc(m->pack_count, 32);
	return m;
}

/* What the pack writer does: digest every pack and keep the local copy. */
static void
publish_packs(struct s3_export_manifest *m)
{
	uint32_t p;

	for (p = 0; p < m->pack_count; p++) {
		uint32_t len = pack_len(p);
		uint8_t *buf = malloc(len);

		fill_pack(p, buf, 0, len);
		s3_peer_sha256(buf, len, m->pack_sha + (size_t)p * 32);
		s3_peer_store_pack(m->uuid_str, p, buf, len);
		free(buf);
	}
	s3_export_manifest_seal(m);
}

static void
corrupt_local_pack(uint32_t pack)
{
	char path[512];
	FILE *f;

	snprintf(path, sizeof(path), "%s/%s/pack-%08x", g_dir, TEST_UUID, pack);
	f = fopen(path, "r+");
	if (f) {
		fseek(f, 4097, SEEK_SET);
		fputc(0xA5, f);
		fclose(f);
	}
}

static void
remove_local_pack(uint32_t pack)
{
	char path[512];

	snprintf(path, sizeof(path), "%s/%s/pack-%08x", g_dir, TEST_UUID, pack);
	unlink(path);
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

/* Read every own chunk through a fresh device with \p peer, polling up to
 * \p ms. Returns the device stats and whether every byte matched. */
static bool
read_all(const char *peer, uint32_t ms, struct s3_export_bs_dev_stats *st,
	 uint64_t *elapsed_ms)
{
	struct spdk_bs_dev *dev = NULL;
	struct spdk_io_channel *ch;
	struct rd r[NUM_CHUNKS];
	struct timespec t0, t1;
	bool ok = true;
	uint32_t i, waited = 0;

	g_s3_gets = 0;
	if (s3_export_bs_dev_create((struct s3_client *)0x1, g_m, &dev) != 0) {
		return false;
	}
	s3_export_bs_dev_set_peer(dev, peer);
	ch = dev->create_channel(dev);
	clock_gettime(CLOCK_MONOTONIC, &t0);
	memset(r, 0, sizeof(r));
	for (i = 0; i < NUM_CHUNKS; i++) {
		if (!g_present[i]) {
			r[i].done = true;
			continue;
		}
		r[i].buf = malloc(CHUNK_SIZE);
		memset(r[i].buf, 0xEE, CHUNK_SIZE);
		r[i].args.cb_fn = rd_done;
		r[i].args.cb_arg = &r[i];
		r[i].args.channel = ch;
		dev->read(dev, ch, r[i].buf, (uint64_t)i * (CHUNK_SIZE / BLOCK),
			  CHUNK_SIZE / BLOCK, &r[i].args);
	}
	for (;;) {
		bool all = true;

		for (i = 0; i < NUM_CHUNKS; i++) {
			all = all && r[i].done;
		}
		if (all || waited >= ms) {
			break;
		}
		poll_ms(5);
		waited += 5;
	}
	clock_gettime(CLOCK_MONOTONIC, &t1);
	*elapsed_ms = (uint64_t)(((int64_t)(t1.tv_sec - t0.tv_sec) * 1000000000LL +
				  (int64_t)(t1.tv_nsec - t0.tv_nsec)) / 1000000);
	for (i = 0; i < NUM_CHUNKS; i++) {
		uint32_t k;

		if (!g_present[i]) {
			continue;
		}
		if (!r[i].done || r[i].status != 0) {
			ok = false;
		} else {
			for (k = 0; k < CHUNK_SIZE; k += 997) {
				if (r[i].buf[k] != obj_byte((int)i, k)) {
					ok = false;
					break;
				}
			}
		}
		free(r[i].buf);
	}
	s3_export_bs_dev_get_stats(dev, st);
	dev->destroy_channel(dev, ch);
	dev->destroy(dev);
	/* Late peer answers must land before the device can go. */
	poll_ms(300);
	return ok;
}

static void
pu_done(void *arg, int status)
{
	struct { bool done; int status; } *pu = arg;

	pu->done = true;
	pu->status = status;
}

static void
set_env(const char *dir, const char *key)
{
	char port[16];

	setenv("S3LVOL_PEER_PULL", "1", 1);
	setenv("S3LVOL_PEER_LISTEN", "127.0.0.1:0", 1);
	setenv("S3LVOL_PEER_DIR", dir, 1);
	setenv("S3LVOL_PEER_SECRET_FILE", key, 1);
	setenv("S3LVOL_PEER_CONNECT_MS", "300", 1);
	setenv("S3LVOL_PEER_IO_MS", "3000", 1);
	setenv("S3LVOL_PEER_HEDGE_MS", "400", 1);
	snprintf(port, sizeof(port), "%u", 1);
	(void)port;
	s3_peer_config_reload();
}

static void
write_key(const char *path, const char *k)
{
	FILE *f = fopen(path, "w");

	fputs(k, f);
	fclose(f);
}

int
main(void)
{
	struct spdk_env_opts opts;
	struct s3_export_manifest *p = NULL;
	struct s3_export_bs_dev_stats st;
	char *json = NULL, *bad;
	char peer[64];
	size_t len = 0;
	uint64_t el;
	bool ok;
	int rc;

	spdk_log_set_print_level(SPDK_LOG_ERROR);
	spdk_log_open(NULL);
	printf("=== s3lvol peer pull test ===\n");
	setenv("S3LVOL_S3_HEDGE", "0", 1);
	setenv("S3LVOL_IMPORT_CHUNK_RETRIES", "0", 1);
	setenv("S3LVOL_PACK_RANGE_MIB", "16", 1);
	s3_hedge_config_load();

	snprintf(g_dir, sizeof(g_dir), "/tmp/s3lvol-peer-test.%d", (int)getpid());
	mkdir(g_dir, 0700);
	snprintf(g_key_file, sizeof(g_key_file), "%s.key", g_dir);
	snprintf(g_bad_key_file, sizeof(g_bad_key_file), "%s.badkey", g_dir);
	write_key(g_key_file, "0123456789abcdef0123456789abcdef\n");
	write_key(g_bad_key_file, "ffffffffffffffffffffffffffffffff\n");
	set_env(g_dir, g_key_file);

	printf("\n[1] SPDK env, a thread, the server\n");
	opts.opts_size = sizeof(opts);
	spdk_env_opts_init(&opts);
	opts.name = "s3_peer_test";
	opts.no_huge = true;
	opts.mem_size = 256;
	if (spdk_env_init(&opts) < 0) {
		fprintf(stderr, "spdk_env_init failed; skipping\n");
		return 77;
	}
	spdk_thread_lib_init(NULL, 0);
	g_thread = spdk_thread_create("peer_test", NULL);
	spdk_set_thread(g_thread);
	rc = s3_peer_server_start();
	check_true("server starts on 127.0.0.1", rc == 0 && s3_peer_server_port() != 0);
	snprintf(peer, sizeof(peer), "127.0.0.1:%u", s3_peer_server_port());

	printf("\n[2] manifest carries the pack digests\n");
	g_m = make_packed();
	publish_packs(g_m);
	rc = s3_export_manifest_serialize(g_m, &json, &len);
	check_true("serialize", rc == 0 && strstr(json, "\"pack_sha256\":") != NULL);
	rc = s3_export_manifest_parse(json, len, &p);
	check_true("parse", rc == 0 && p && p->pack_sha &&
		   memcmp(p->pack_sha, g_m->pack_sha, (size_t)g_m->pack_count * 32) == 0);
	s3_export_manifest_unref(p);
	p = NULL;
	{
		char *d = strstr(json, "\"pack_sha256\":\"");

		bad = strdup(json);
		d = bad + (d - json) + strlen("\"pack_sha256\":\"");
		*d = *d == 'A' ? 'B' : 'A';
		check_true("a flipped digest fails the crc",
			   s3_export_manifest_parse(bad, strlen(bad), &p) != 0);
		free(bad);
	}

	printf("\n[3] peer hit\n");
	ok = read_all(peer, 5000, &st, &el);
	check_true("every own chunk reads right", ok);
	check_u64("every pack from the peer", st.peer_hits, g_m->pack_count);
	check_u64("peer bytes", st.peer_bytes, g_m->present_chunks * CHUNK_SIZE);
	check_u64("no S3 GET", g_s3_gets, 0);

	printf("\n[4] peer down: S3 serves, the breaker opens\n");
	ok = read_all("127.0.0.1:1", 5000, &st, &el);
	check_true("every own chunk reads right", ok);
	check_u64("no peer hit", st.peer_hits, 0);
	check_true("peer asked at most 3 times (breaker)", st.peer_gets <= 3 && st.peer_gets >= 1);
	check_u64("S3 served every pack", g_s3_gets, g_m->pack_count);

	printf("\n[5] digest mismatch: S3 serves, the corrupt copy never reaches the reader\n");
	corrupt_local_pack(1);
	ok = read_all(peer, 5000, &st, &el);
	check_true("every own chunk reads right", ok);
	check_u64("one mismatch", st.peer_mismatches, 1);
	check_u64("the other packs from the peer", st.peer_hits, g_m->pack_count - 1);
	check_u64("S3 served the corrupt one", g_s3_gets, 1);
	publish_packs(g_m);

	printf("\n[6] peer slow: the hedge starts S3, S3 wins\n");
	s3_peer_server_stop();
	{
		/* A listener that accepts and never answers. */
		int fd = socket(AF_INET, SOCK_STREAM, 0), one = 1;
		struct sockaddr_in sa = { .sin_family = AF_INET };
		socklen_t sl = sizeof(sa);
		char slow[64];

		sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
		bind(fd, (struct sockaddr *)&sa, sizeof(sa));
		listen(fd, 64);
		getsockname(fd, (struct sockaddr *)&sa, &sl);
		snprintf(slow, sizeof(slow), "127.0.0.1:%u", ntohs(sa.sin_port));
		ok = read_all(slow, 8000, &st, &el);
		check_true("every own chunk reads right", ok);
		check_true("hedged (S3 raced the silent peer)", st.peer_hedged >= 1);
		check_u64("S3 served every pack", g_s3_gets, g_m->pack_count);
		printf("\t       elapsed %" PRIu64 " ms, hedged %" PRIu64 ", gets %" PRIu64 "\n", el, st.peer_hedged, st.peer_gets);
		check_true("well before the peer I/O timeout", el < 2500);
		close(fd);
	}
	rc = s3_peer_server_start();
	snprintf(peer, sizeof(peer), "127.0.0.1:%u", s3_peer_server_port());

	printf("\n[7] auth failure: wrong key on the client, S3 serves\n");
	set_env(g_dir, g_bad_key_file);
	ok = read_all(peer, 5000, &st, &el);
	check_true("every own chunk reads right", ok);
	check_u64("no peer hit", st.peer_hits, 0);
	check_true("refused", st.peer_misses >= 1);
	set_env(g_dir, g_key_file);

	printf("\n[8] peer 404: no local copy, S3 serves\n");
	remove_local_pack(0);
	ok = read_all(peer, 5000, &st, &el);
	check_true("every own chunk reads right", ok);
	check_u64("one miss", st.peer_misses, 1);
	check_u64("S3 served that pack", g_s3_gets, 1);
	publish_packs(g_m);

	printf("\n[9] rescue: peer late, S3 fails, the peer's verified copy is used\n");
	setenv("S3LVOL_PEER_HEDGE_MS", "1", 1);
	s3_peer_config_reload();
	g_s3_fail = true;
	g_s3_delay_polls = 60;
	ok = read_all(peer, 5000, &st, &el);
	check_true("every own chunk reads right", ok);
	/* The peer answers (a hit) while S3 is already running; S3 then fails
	 * and the verified copy kept aside is used (a rescue). */
	/* A pack whose peer answer beats even the 1 ms hedge wins outright;
	 * every other one is a rescue. None may come from the failing S3. */
	check_true("at least one rescue from the peer copy", st.peer_rescues >= 1);
	check_u64("each rescue was a verified peer answer", st.peer_hits, g_m->pack_count);
	g_s3_fail = false;
	g_s3_delay_polls = 0;
	set_env(g_dir, g_key_file);

	printf("\n[12] peer_ready manifest: packs_pending\n");
	{
		struct s3_export_manifest *q = NULL;
		char *j2 = NULL, *b2;
		size_t l2 = 0;

		g_m->packs_pending = true;
		s3_export_manifest_seal(g_m);
		rc = s3_export_manifest_serialize(g_m, &j2, &l2);
		check_true("serialized with packs_pending",
			   rc == 0 && strstr(j2, "\"packs_pending\":true") != NULL);
		rc = s3_export_manifest_parse(j2, l2, &q);
		check_true("parses back pending", rc == 0 && q && q->packs_pending);
		s3_export_manifest_unref(q);
		q = NULL;
		b2 = strdup(j2);
		{
			char *at = strstr(b2, "\"packs_pending\":true");

			/* Same length: true -> fals(e) breaks the json; drop the member
			 * instead by renaming it, which keeps the crc of the original. */
			memcpy(at + 1, "packs_pendinX", 13);
		}
		check_true("dropping the flag fails the crc",
			   s3_export_manifest_parse(b2, strlen(b2), &q) != 0);
		free(b2);
		free(j2);
		j2 = NULL;
		g_m->packs_pending = false;
		s3_export_manifest_seal(g_m);
		rc = s3_export_manifest_serialize(g_m, &j2, &l2);
		check_true("not written when false",
			   rc == 0 && strstr(j2, "packs_pending") == NULL);
		free(j2);
		{
			/* Without digests nothing could read the packs at all. */
			uint8_t *sha = g_m->pack_sha;

			g_m->pack_sha = NULL;
			g_m->packs_pending = true;
			s3_export_manifest_seal(g_m);
			j2 = NULL;
			rc = s3_export_manifest_serialize(g_m, &j2, &l2);
			check_true("refused on parse without pack digests", rc == 0 &&
				   s3_export_manifest_parse(j2, l2, &q) != 0);
			free(j2);
			g_m->pack_sha = sha;
			g_m->packs_pending = false;
			s3_export_manifest_seal(g_m);
		}
	}

	printf("\n[13] pin: never evicted while pinned\n");
	{
		char d[256], pth[512];
		struct stat sb;
		uint32_t pk;
		bool all = true, gone = false;
		uint8_t small[4096];

		setenv("S3LVOL_PEER_CACHE_MB", "4", 1);
		s3_peer_config_reload();
		check_true("pin", s3_peer_pin_export(TEST_UUID) == 0 &&
			   s3_peer_export_pinned(TEST_UUID));
		publish_packs(g_m);   /* 10 MiB of packs against a 4 MiB cap */
		memset(small, 7, sizeof(small));
		/* An unpinned store runs the eviction. */
		s3_peer_store_pack("8c3f04e0-4f89-11d3-9a0c-0305e82c3308", 0, small,
				   sizeof(small));
		for (pk = 0; pk < g_m->pack_count; pk++) {
			snprintf(pth, sizeof(pth), "%s/%s/pack-%08x", g_dir, TEST_UUID, pk);
			all = all && stat(pth, &sb) == 0;
		}
		check_true("every pinned pack kept past the cap", all);
		snprintf(d, sizeof(d), "%s/%s/.pinned", g_dir, TEST_UUID);
		check_true("marker on disk", stat(d, &sb) == 0);
		s3_peer_unpin_export(TEST_UUID);
		check_true("unpinned", !s3_peer_export_pinned(TEST_UUID));
		for (pk = 0; pk < g_m->pack_count; pk++) {
			snprintf(pth, sizeof(pth), "%s/%s/pack-%08x", g_dir, TEST_UUID, pk);
			gone = gone || stat(pth, &sb) != 0;
		}
		check_true("unpinned copies are evictable again", gone);
		s3_peer_drop_export("8c3f04e0-4f89-11d3-9a0c-0305e82c3308");
		setenv("S3LVOL_PEER_CACHE_MB", "16384", 1);
		s3_peer_config_reload();
		publish_packs(g_m);
	}

	printf("\n[14] peer-only read (packs_pending): never S3\n");
	g_m->packs_pending = true;
	s3_export_manifest_seal(g_m);
	ok = read_all(peer, 5000, &st, &el);
	check_true("every own chunk reads right", ok);
	check_u64("every pack from the peer", st.peer_hits, g_m->pack_count);
	check_u64("no S3 GET", g_s3_gets, 0);
	remove_local_pack(1);
	ok = read_all(peer, 5000, &st, &el);
	check_true("a missing pack fails the read", !ok);
	check_u64("still no S3 GET for a pack", g_s3_gets, 0);
	ok = read_all("127.0.0.1:1", 5000, &st, &el);
	check_true("peer down fails the read", !ok);
	check_u64("no S3 GET with the peer down either", g_s3_gets, 0);
	corrupt_local_pack(0);
	publish_packs(g_m);   /* restores pack 1, re-digests everything */
	corrupt_local_pack(0);
	ok = read_all(peer, 5000, &st, &el);
	check_true("a pack that fails its digest fails the read", !ok);
	check_true("counted as mismatches", st.peer_mismatches >= 1);
	check_u64("never served from S3", g_s3_gets, 0);
	g_m->packs_pending = false;
	publish_packs(g_m);

	printf("\n[15] background upload of pinned packs\n");
	{
		struct pu { bool done; int status; } pu = {0};
		bool cancel = false;
		uint32_t pk;
		bool keys = true;

		g_put_ok = true;
		g_puts = 0;
		g_put_fail_first = 2;   /* retried, not fatal */
		rc = s3_export_packs_upload((struct s3_client *)0x1, SRC_PREFIX, TEST_UUID,
					    g_m->pack_count, g_m->pack_sha, &cancel,
					    pu_done, &pu);
		poll_ms(8000);
		check_true("uploaded", rc == 0 && pu.done && pu.status == 0);
		check_u64("one PUT per pack", g_puts, g_m->pack_count);
		for (pk = 0; pk < g_m->pack_count && pk < 16; pk++) {
			char want[S3_EXPORT_KEY_MAX];

			s3_export_pack_key(SRC_PREFIX, TEST_UUID, pk, want, sizeof(want));
			keys = keys && strcmp(g_put_keys[pk], want) == 0;
		}
		check_true("to the pack keys, in order", keys);

		corrupt_local_pack(1);
		memset(&pu, 0, sizeof(pu));
		g_puts = 0;
		rc = s3_export_packs_upload((struct s3_client *)0x1, SRC_PREFIX, TEST_UUID,
					    g_m->pack_count, g_m->pack_sha, &cancel,
					    pu_done, &pu);
		poll_ms(200);
		check_true("a corrupt local copy stops it", pu.done && pu.status == -EBADMSG);
		check_u64("nothing past the corrupt pack was PUT", g_puts, 1);
		publish_packs(g_m);

		memset(&pu, 0, sizeof(pu));
		cancel = true;
		rc = s3_export_packs_upload((struct s3_client *)0x1, SRC_PREFIX, TEST_UUID,
					    g_m->pack_count, g_m->pack_sha, &cancel,
					    pu_done, &pu);
		poll_ms(100);
		check_true("cancelled", pu.done && pu.status == -ECANCELED);
		g_put_ok = false;
	}

	printf("\n[10] the server refuses a wildcard bind\n");
	s3_peer_server_stop();
	setenv("S3LVOL_PEER_LISTEN", "0.0.0.0:0", 1);
	s3_peer_config_reload();
	check_true("0.0.0.0 refused", s3_peer_server_start() == -EINVAL);
	setenv("S3LVOL_PEER_LISTEN", "127.0.0.1:0", 1);
	setenv("S3LVOL_PEER_SECRET_FILE", "/nonexistent", 1);
	s3_peer_config_reload();
	check_true("no secret: not serving", s3_peer_server_start() == -EACCES);

	printf("\n[11] release drops the local copies\n");
	set_env(g_dir, g_key_file);
	s3_peer_drop_export(TEST_UUID);
	{
		char d[256];
		struct stat sb;

		snprintf(d, sizeof(d), "%s/%s", g_dir, TEST_UUID);
		check_true("export dir gone", stat(d, &sb) != 0);
	}

	free(json);
	s3_export_manifest_unref(g_m);
	unlink(g_key_file);
	unlink(g_bad_key_file);
	rmdir(g_dir);
	spdk_thread_exit(g_thread);
	while (!spdk_thread_is_exited(g_thread)) {
		spdk_thread_poll(g_thread, 0, 0);
	}
	spdk_thread_destroy(g_thread);
	spdk_thread_lib_fini();
	printf("\n=== %d passed, %d failed ===\n", g_pass, g_fail);
	spdk_log_close();
	return g_fail ? 1 : 0;
}
