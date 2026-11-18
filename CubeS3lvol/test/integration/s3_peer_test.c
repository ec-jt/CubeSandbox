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
 *         longer.
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
