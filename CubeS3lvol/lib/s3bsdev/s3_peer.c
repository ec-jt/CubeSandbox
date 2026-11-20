/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_peer -- peer pull of export packs over the private network
 *
 *   See include/s3lvol/s3_peer.h for the design. This file holds three parts:
 *
 *     1. configuration and the shared secret;
 *     2. the source side: local pack copies and a small TCP server bound to
 *        the private address, one request per connection, served by sendfile;
 *     3. the importer side: a few worker threads that fetch one pack per
 *        request, verify its SHA-256 and hand the result back to the SPDK
 *        thread that asked.
 *
 *   Nothing here runs on a reactor except the submit and the completion
 *   message: every socket, file and hash operation is on a plain thread.
 *
 *   === Wire format (version 1, little endian) ===
 *
 *   Request, 128 bytes:
 *     0   magic "S3PR"
 *     4   u16 version (1), u16 reserved (0)
 *     8   u64 unix seconds
 *     16  u32 pack, u32 reserved (0)
 *     24  u64 length the client expects
 *     32  char[40] export uuid, NUL padded
 *     72  24 bytes zero
 *     96  HMAC-SHA256(secret, bytes 0..95)
 *
 *   Response header, 16 bytes: "S3PA", u32 status (enum s3_peer_status),
 *   u64 length; then exactly that many bytes when the status is OK.
 */

#include "spdk/stdinc.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"

#include <dirent.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/sendfile.h>

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include "s3lvol/s3_peer.h"
#include "s3lvol/s3_spawner.h"

#define PEER_REQ_LEN      128
#define PEER_REQ_SIGNED   96
#define PEER_RSP_LEN      16
#define PEER_VERSION      1
#define PEER_SKEW_S       300
#define PEER_SECRET_MAX   256
#define PEER_SECRET_MIN   16
#define PEER_SERVER_CONNS 16
#define PEER_UUID_LEN     36

/* ==========================================================================
 * Configuration
 * ========================================================================== */

struct peer_config {
	bool     loaded;
	bool     pull;
	bool     serve;
	char     listen[S3_PEER_ADDR_MAX];
	char     dir[256];
	char     secret_file[256];
	uint8_t  secret[PEER_SECRET_MAX];
	size_t   secret_len;
	bool     secret_ok;
	uint64_t cache_bytes;
	uint16_t default_port;
	uint32_t connect_ms;
	uint32_t io_ms;
	uint32_t hedge_ms;
	uint32_t threads;
};

static pthread_mutex_t g_cfg_lock = PTHREAD_MUTEX_INITIALIZER;
static struct peer_config g_cfg;

static uint64_t
env_u64(const char *name, uint64_t dflt, uint64_t lo, uint64_t hi)
{
	const char *v = getenv(name);
	char *end = NULL;
	unsigned long long n;

	if (!v || !*v) {
		return dflt;
	}
	errno = 0;
	n = strtoull(v, &end, 10);
	if (errno || !end || *end != '\0' || n < lo || n > hi) {
		SPDK_WARNLOG("ignoring %s='%s'; using %" PRIu64 "\n", name, v, dflt);
		return dflt;
	}
	return n;
}

static bool
env_flag(const char *name)
{
	const char *v = getenv(name);

	return v && (strcmp(v, "1") == 0 || strcasecmp(v, "true") == 0 ||
		     strcasecmp(v, "yes") == 0 || strcasecmp(v, "on") == 0);
}

static void
cfg_load_secret(struct peer_config *c)
{
	FILE *f;
	size_t n;

	c->secret_ok = false;
	c->secret_len = 0;
	f = fopen(c->secret_file, "re");
	if (!f) {
		return;
	}
	n = fread(c->secret, 1, sizeof(c->secret), f);
	fclose(f);
	while (n > 0 && (c->secret[n - 1] == '\n' || c->secret[n - 1] == '\r' ||
			 c->secret[n - 1] == ' ')) {
		n--;
	}
	if (n >= PEER_SECRET_MIN) {
		c->secret_len = n;
		c->secret_ok = true;
	}
}

static const struct peer_config *
cfg(void)
{
	const char *v;

	pthread_mutex_lock(&g_cfg_lock);
	if (!g_cfg.loaded) {
		memset(&g_cfg, 0, sizeof(g_cfg));
		g_cfg.pull = env_flag("S3LVOL_PEER_PULL");
		v = getenv("S3LVOL_PEER_LISTEN");
		if (v && *v) {
			snprintf(g_cfg.listen, sizeof(g_cfg.listen), "%s", v);
			g_cfg.serve = true;
		}
		v = getenv("S3LVOL_PEER_DIR");
		snprintf(g_cfg.dir, sizeof(g_cfg.dir), "%s",
			 v && *v ? v : "/data/s3lvol-peer");
		v = getenv("S3LVOL_PEER_SECRET_FILE");
		snprintf(g_cfg.secret_file, sizeof(g_cfg.secret_file), "%s",
			 v && *v ? v : "/etc/cube/s3lvol-peer.key");
		g_cfg.cache_bytes = env_u64("S3LVOL_PEER_CACHE_MB", 16384, 0, 1u << 24) << 20;
		g_cfg.default_port = (uint16_t)env_u64("S3LVOL_PEER_PORT",
						       S3_PEER_DEFAULT_PORT, 1, 65535);
		g_cfg.connect_ms = (uint32_t)env_u64("S3LVOL_PEER_CONNECT_MS", 3000, 10, 60000);
		g_cfg.io_ms = (uint32_t)env_u64("S3LVOL_PEER_IO_MS", 10000, 100, 600000);
		g_cfg.hedge_ms = (uint32_t)env_u64("S3LVOL_PEER_HEDGE_MS", 2000, 1, 600000);
		g_cfg.threads = (uint32_t)env_u64("S3LVOL_PEER_THREADS", 8, 1, 64);
		cfg_load_secret(&g_cfg);
		g_cfg.loaded = true;
	}
	pthread_mutex_unlock(&g_cfg_lock);
	return &g_cfg;
}

void
s3_peer_config_reload(void)
{
	pthread_mutex_lock(&g_cfg_lock);
	g_cfg.loaded = false;
	pthread_mutex_unlock(&g_cfg_lock);
}

bool
s3_peer_pull_enabled(void)
{
	return cfg()->pull;
}

bool
s3_peer_serve_enabled(void)
{
	return cfg()->serve;
}

uint64_t
s3_peer_hedge_us(void)
{
	return (uint64_t)cfg()->hedge_ms * 1000;
}

void
s3_peer_sha256(const void *buf, size_t len, uint8_t out[S3_PEER_SHA256_LEN])
{
	SHA256(buf, len, out);
}

static void
put_u16(uint8_t *p, uint16_t v)
{
	p[0] = (uint8_t)v;
	p[1] = (uint8_t)(v >> 8);
}

static void
put_u32(uint8_t *p, uint32_t v)
{
	int i;

	for (i = 0; i < 4; i++) {
		p[i] = (uint8_t)(v >> (8 * i));
	}
}

static void
put_u64(uint8_t *p, uint64_t v)
{
	int i;

	for (i = 0; i < 8; i++) {
		p[i] = (uint8_t)(v >> (8 * i));
	}
}

static uint32_t
get_u32(const uint8_t *p)
{
	return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 |
	       (uint32_t)p[3] << 24;
}

static uint64_t
get_u64(const uint8_t *p)
{
	uint64_t v = 0;
	int i;

	for (i = 7; i >= 0; i--) {
		v = v << 8 | p[i];
	}
	return v;
}

static void
req_mac(const struct peer_config *c, const uint8_t *req, uint8_t out[32])
{
	unsigned int n = 32;

	HMAC(EVP_sha256(), c->secret, (int)c->secret_len, req, PEER_REQ_SIGNED, out, &n);
}

static bool
uuid_ok(const char *s)
{
	size_t i;

	if (strnlen(s, PEER_UUID_LEN + 1) != PEER_UUID_LEN) {
		return false;
	}
	for (i = 0; i < PEER_UUID_LEN; i++) {
		char ch = s[i];

		if (i == 8 || i == 13 || i == 18 || i == 23) {
			if (ch != '-') {
				return false;
			}
		} else if (!isxdigit((unsigned char)ch)) {
			return false;
		}
	}
	return true;
}

/* Parse "ip" or "ip:port" into a sockaddr. IPv4 only: the vSwitch is IPv4. */
static int
parse_addr(const char *s, uint16_t dflt_port, struct sockaddr_in *out)
{
	char host[S3_PEER_ADDR_MAX];
	const char *colon;
	unsigned long port = dflt_port;
	char *end = NULL;

	if (!s || !*s || strlen(s) >= sizeof(host)) {
		return -EINVAL;
	}
	colon = strrchr(s, ':');
	if (colon) {
		memcpy(host, s, (size_t)(colon - s));
		host[colon - s] = '\0';
		errno = 0;
		port = strtoul(colon + 1, &end, 10);
		if (errno || !end || *end != '\0' || port > 65535) {
			return -EINVAL;
		}
	} else {
		snprintf(host, sizeof(host), "%s", s);
	}
	memset(out, 0, sizeof(*out));
	out->sin_family = AF_INET;
	out->sin_port = htons((uint16_t)port);
	if (inet_pton(AF_INET, host, &out->sin_addr) != 1) {
		return -EINVAL;
	}
	return 0;
}

/* ==========================================================================
 * Local pack copies
 * ========================================================================== */

static pthread_mutex_t g_store_lock = PTHREAD_MUTEX_INITIALIZER;

static int
pack_path(const char *uuid_str, uint32_t pack, char *out, size_t out_len)
{
	int n = snprintf(out, out_len, "%s/%s/pack-%08" PRIx32, cfg()->dir, uuid_str, pack);

	return n > 0 && (size_t)n < out_len ? 0 : -ENAMETOOLONG;
}

struct cache_ent {
	char     path[PATH_MAX];
	time_t   mtime;
	uint64_t size;
};

static int
cache_ent_cmp(const void *a, const void *b)
{
	const struct cache_ent *x = a, *y = b;

	return x->mtime < y->mtime ? -1 : x->mtime > y->mtime ? 1 : 0;
}

#define PEER_PIN_MARKER ".pinned"

static bool
dir_pinned(const char *dir)
{
	char p[PATH_MAX];
	struct stat st;

	return snprintf(p, sizeof(p), "%s/" PEER_PIN_MARKER, dir) < (int)sizeof(p) &&
	       stat(p, &st) == 0;
}

/* Delete the oldest copies until the total is within the cap. Called after a
 * store, under g_store_lock. A directory left empty is removed too.
 *
 * A pinned export (peer_ready: its packs are not in S3 yet) is never touched
 * and its bytes do not count against the cap: deleting one of its packs would
 * make the export unreadable from anywhere. */
static void
cache_evict(void)
{
	const struct peer_config *c = cfg();
	struct cache_ent *ents = NULL;
	size_t n = 0, cap = 0, i;
	uint64_t total = 0;
	DIR *top, *sub;
	struct dirent *de, *fe;

	top = opendir(c->dir);
	if (!top) {
		return;
	}
	while ((de = readdir(top)) != NULL) {
		char d[512];

		if (!uuid_ok(de->d_name) ||
		    snprintf(d, sizeof(d), "%s/%s", c->dir, de->d_name) >= (int)sizeof(d)) {
			continue;
		}
		if (dir_pinned(d)) {
			continue;
		}
		sub = opendir(d);
		if (!sub) {
			continue;
		}
		while ((fe = readdir(sub)) != NULL) {
			struct stat st;
			char p[1024];

			if (strncmp(fe->d_name, "pack-", 5) != 0) {
				continue;
			}
			if (snprintf(p, sizeof(p), "%s/%s", d, fe->d_name) >= (int)sizeof(p) ||
			    stat(p, &st) != 0) {
				continue;
			}
			if (n == cap) {
				struct cache_ent *ne;

				cap = cap ? cap * 2 : 256;
				ne = realloc(ents, cap * sizeof(*ents));
				if (!ne) {
					break;
				}
				ents = ne;
			}
			snprintf(ents[n].path, sizeof(ents[n].path), "%s", p);
			ents[n].mtime = st.st_mtime;
			ents[n].size = (uint64_t)st.st_size;
			total += ents[n].size;
			n++;
		}
		closedir(sub);
	}
	closedir(top);
	if (total > c->cache_bytes && n > 0) {
		qsort(ents, n, sizeof(*ents), cache_ent_cmp);
		for (i = 0; i < n && total > c->cache_bytes; i++) {
			if (unlink(ents[i].path) == 0) {
				char *slash;

				total -= ents[i].size;
				slash = strrchr(ents[i].path, '/');
				if (slash) {
					*slash = '\0';
					(void)rmdir(ents[i].path);
				}
			}
		}
	}
	free(ents);
}

int
s3_peer_store_pack(const char *uuid_str, uint32_t pack, const void *buf, size_t len)
{
	const struct peer_config *c = cfg();
	char dir[PATH_MAX], path[PATH_MAX], tmp[PATH_MAX];
	const uint8_t *p = buf;
	size_t done = 0;
	int fd, rc = 0;
	bool pinned;

	if (!c->serve) {
		return 0;
	}
	if (!uuid_ok(uuid_str)) {
		return -EINVAL;
	}
	if (snprintf(dir, sizeof(dir), "%s/%s", c->dir, uuid_str) >= (int)sizeof(dir) ||
	    pack_path(uuid_str, pack, path, sizeof(path)) != 0 ||
	    snprintf(tmp, sizeof(tmp), "%s.tmp.%d.%u", path, (int)getpid(), pack) >=
	    (int)sizeof(tmp)) {
		return -ENAMETOOLONG;
	}
	/* A pinned export's packs exist nowhere else: they are stored whatever
	 * the cap says. An ordinary copy is only an optimisation. */
	pinned = dir_pinned(dir);
	if (!pinned && (c->cache_bytes == 0 || len > c->cache_bytes)) {
		return 0;
	}
	pthread_mutex_lock(&g_store_lock);
	if (mkdir(c->dir, 0700) != 0 && errno != EEXIST) {
		rc = -errno;
		goto out;
	}
	if (mkdir(dir, 0700) != 0 && errno != EEXIST) {
		rc = -errno;
		goto out;
	}
	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
	if (fd < 0) {
		rc = -errno;
		goto out;
	}
	while (done < len) {
		ssize_t w = write(fd, p + done, len - done);

		if (w < 0) {
			if (errno == EINTR) {
				continue;
			}
			rc = -errno;
			break;
		}
		done += (size_t)w;
	}
	/* An ordinary copy is not fsynced: losing it in a crash is only a peer
	 * miss. A pinned one is the only copy until the S3 upload finishes, so it
	 * is made durable before anyone is told the export is peer_ready. The
	 * importer verifies every byte against the manifest digest either way. */
	if (rc == 0 && pinned && fsync(fd) != 0) {
		rc = -errno;
	}
	if (close(fd) != 0 && rc == 0) {
		rc = -errno;
	}
	if (rc == 0 && rename(tmp, path) != 0) {
		rc = -errno;
	}
	if (rc != 0) {
		(void)unlink(tmp);
		goto out;
	}
	cache_evict();
out:
	pthread_mutex_unlock(&g_store_lock);
	return rc;
}

void
s3_peer_drop_export(const char *uuid_str)
{
	const struct peer_config *c = cfg();
	char dir[PATH_MAX];
	DIR *d;
	struct dirent *de;

	if (!c->serve || !uuid_ok(uuid_str) ||
	    snprintf(dir, sizeof(dir), "%s/%s", c->dir, uuid_str) >= (int)sizeof(dir)) {
		return;
	}
	pthread_mutex_lock(&g_store_lock);
	d = opendir(dir);
	if (d) {
		while ((de = readdir(d)) != NULL) {
			char p[PATH_MAX];

			if (de->d_name[0] == '.') {
				continue;
			}
			if (snprintf(p, sizeof(p), "%s/%s", dir, de->d_name) < (int)sizeof(p)) {
				(void)unlink(p);
			}
		}
		closedir(d);
		(void)rmdir(dir);
	}
	pthread_mutex_unlock(&g_store_lock);
}

static int
export_dir(const char *uuid_str, char *dir, size_t len)
{
	const struct peer_config *c = cfg();

	if (!uuid_ok(uuid_str)) {
		return -EINVAL;
	}
	return snprintf(dir, len, "%s/%s", c->dir, uuid_str) < (int)len ? 0 : -ENAMETOOLONG;
}

int
s3_peer_pin_export(const char *uuid_str)
{
	const struct peer_config *c = cfg();
	char dir[PATH_MAX], marker[PATH_MAX];
	int fd, rc = 0;

	if (!c->serve) {
		return -ENOTSUP;
	}
	rc = export_dir(uuid_str, dir, sizeof(dir));
	if (rc != 0) {
		return rc;
	}
	if (snprintf(marker, sizeof(marker), "%s/" PEER_PIN_MARKER, dir) >= (int)sizeof(marker)) {
		return -ENAMETOOLONG;
	}
	pthread_mutex_lock(&g_store_lock);
	if (mkdir(c->dir, 0700) != 0 && errno != EEXIST) {
		rc = -errno;
		goto out;
	}
	if (mkdir(dir, 0700) != 0 && errno != EEXIST) {
		rc = -errno;
		goto out;
	}
	fd = open(marker, O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
	if (fd < 0) {
		rc = -errno;
		goto out;
	}
	if (fsync(fd) != 0) {
		rc = -errno;
	}
	(void)close(fd);
out:
	pthread_mutex_unlock(&g_store_lock);
	return rc;
}

void
s3_peer_unpin_export(const char *uuid_str)
{
	char dir[PATH_MAX], marker[PATH_MAX];

	if (export_dir(uuid_str, dir, sizeof(dir)) != 0 ||
		   snprintf(marker, sizeof(marker), "%s/" PEER_PIN_MARKER, dir) >= (int)sizeof(marker)) {
		return;
	}
	pthread_mutex_lock(&g_store_lock);
	(void)unlink(marker);
	/* The copies are ordinary cache entries now; bring the total back
		* under the cap. */
	cache_evict();
	pthread_mutex_unlock(&g_store_lock);
}

bool
s3_peer_export_pinned(const char *uuid_str)
{
	char dir[PATH_MAX];

	return export_dir(uuid_str, dir, sizeof(dir)) == 0 && dir_pinned(dir);
}

int
s3_peer_pack_size(const char *uuid_str, uint32_t pack, size_t *out)
{
	char path[PATH_MAX];
	struct stat st;

	if (!uuid_ok(uuid_str) || !out) {
		return -EINVAL;
	}
	if (pack_path(uuid_str, pack, path, sizeof(path)) != 0) {
		return -ENAMETOOLONG;
	}
	if (stat(path, &st) != 0 || st.st_size <= 0) {
		return -ENOENT;
	}
	*out = (size_t)st.st_size;
	return 0;
}

int
s3_peer_load_pack(const char *uuid_str, uint32_t pack, void *buf, size_t len)
{
	char path[PATH_MAX];
	struct stat st;
	uint8_t *p = buf;
	size_t done = 0;
	int fd, rc = 0;

	if (!uuid_ok(uuid_str)) {
		return -EINVAL;
	}
	if (pack_path(uuid_str, pack, path, sizeof(path)) != 0) {
		return -ENAMETOOLONG;
	}
	fd = open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0) {
		return errno == ENOENT ? -ENOENT : -errno;
	}
	if (fstat(fd, &st) != 0 || (uint64_t)st.st_size != len) {
		(void)close(fd);
		return -ENOENT;
	}
	while (done < len) {
		ssize_t r = read(fd, p + done, len - done);

		if (r < 0) {
			if (errno == EINTR) {
				continue;
			}
			rc = -errno;
			break;
		}
		if (r == 0) {
			rc = -EIO;
			break;
		}
		done += (size_t)r;
	}
	(void)close(fd);
	return rc;
}

/* ==========================================================================
 * Server
 * ========================================================================== */

/* The server's own copy of the secret, taken at start: a key rotation takes
 * effect on restart, and the client side may reload its configuration
 * independently (tests do). */
static uint8_t g_srv_secret[PEER_SECRET_MAX];
static size_t g_srv_secret_len;
static int g_listen_fd = -1;
static int g_stop_pipe[2] = {-1, -1};
static pthread_t g_accept_thread;
static bool g_server_running;
static uint16_t g_server_port;
static uint32_t g_server_conns;       /* atomic */

static int
write_all(int fd, const void *buf, size_t len)
{
	const uint8_t *p = buf;

	while (len > 0) {
		ssize_t w = send(fd, p, len, MSG_NOSIGNAL);

		if (w < 0) {
			if (errno == EINTR) {
				continue;
			}
			return -errno;
		}
		p += w;
		len -= (size_t)w;
	}
	return 0;
}

static int
read_all(int fd, void *buf, size_t len)
{
	uint8_t *p = buf;

	while (len > 0) {
		ssize_t r = recv(fd, p, len, 0);

		if (r == 0) {
			return -ECONNRESET;
		}
		if (r < 0) {
			if (errno == EINTR) {
				continue;
			}
			return -errno;
		}
		p += r;
		len -= (size_t)r;
	}
	return 0;
}

static void
server_reply(int fd, enum s3_peer_status st, uint64_t len)
{
	uint8_t h[PEER_RSP_LEN];

	memcpy(h, "S3PA", 4);
	put_u32(h + 4, (uint32_t)st);
	put_u64(h + 8, len);
	(void)write_all(fd, h, sizeof(h));
}

static void *
server_conn(void *arg)
{
	int fd = (int)(intptr_t)arg;
	uint8_t req[PEER_REQ_LEN], mac[32];
	char uuid[PEER_UUID_LEN + 4], path[PATH_MAX];
	struct timeval tv = { .tv_sec = 5 };
	uint64_t ts, len, now;
	uint32_t pack;
	struct stat st;
	int file = -1;
	off_t off = 0;

	pthread_detach(pthread_self());
	(void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
	tv.tv_sec = 30;
	(void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
	if (read_all(fd, req, sizeof(req)) != 0) {
		goto out;
	}
	if (memcmp(req, "S3PR", 4) != 0 || req[4] != PEER_VERSION || req[5] != 0) {
		server_reply(fd, S3_PEER_BAD, 0);
		goto out;
	}
	{
		unsigned int ml = sizeof(mac);

		HMAC(EVP_sha256(), g_srv_secret, (int)g_srv_secret_len, req, PEER_REQ_SIGNED,
		     mac, &ml);
	}
	if (g_srv_secret_len == 0 || CRYPTO_memcmp(mac, req + PEER_REQ_SIGNED, 32) != 0) {
		SPDK_WARNLOG("peer: request with a bad signature refused\n");
		server_reply(fd, S3_PEER_AUTH, 0);
		goto out;
	}
	ts = get_u64(req + 8);
	now = (uint64_t)time(NULL);
	if (ts + PEER_SKEW_S < now || ts > now + PEER_SKEW_S) {
		SPDK_WARNLOG("peer: request outside the %d s clock window refused\n",
			     PEER_SKEW_S);
		server_reply(fd, S3_PEER_AUTH, 0);
		goto out;
	}
	pack = get_u32(req + 16);
	len = get_u64(req + 24);
	memcpy(uuid, req + 32, PEER_UUID_LEN);
	uuid[PEER_UUID_LEN] = '\0';
	if (!uuid_ok(uuid) || pack_path(uuid, pack, path, sizeof(path)) != 0) {
		server_reply(fd, S3_PEER_BAD, 0);
		goto out;
	}
	file = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
	if (file < 0 || fstat(file, &st) != 0 || !S_ISREG(st.st_mode) ||
	    (uint64_t)st.st_size != len) {
		server_reply(fd, S3_PEER_NOT_FOUND, 0);
		goto out;
	}
	server_reply(fd, S3_PEER_OK, len);
	while ((uint64_t)off < len) {
		ssize_t w = sendfile(fd, file, &off, (size_t)spdk_min(len - (uint64_t)off,
				     (uint64_t)(8u << 20)));

		if (w <= 0) {
			if (w < 0 && errno == EINTR) {
				continue;
			}
			break;
		}
	}
out:
	if (file >= 0) {
		close(file);
	}
	close(fd);
	__atomic_fetch_sub(&g_server_conns, 1, __ATOMIC_RELAXED);
	return NULL;
}

static void *
server_accept_loop(void *arg)
{
	(void)arg;
	for (;;) {
		struct pollfd pfd[2] = {
			{ .fd = g_listen_fd, .events = POLLIN },
			{ .fd = g_stop_pipe[0], .events = POLLIN },
		};
		pthread_t t;
		int fd, one = 1;

		if (poll(pfd, 2, -1) < 0) {
			if (errno == EINTR) {
				continue;
			}
			break;
		}
		if (pfd[1].revents) {
			break;
		}
		if (!(pfd[0].revents & POLLIN)) {
			continue;
		}
		fd = accept4(g_listen_fd, NULL, NULL, SOCK_CLOEXEC);
		if (fd < 0) {
			continue;
		}
		(void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
		if (__atomic_fetch_add(&g_server_conns, 1, __ATOMIC_RELAXED) >=
		    PEER_SERVER_CONNS) {
			__atomic_fetch_sub(&g_server_conns, 1, __ATOMIC_RELAXED);
			server_reply(fd, S3_PEER_BUSY, 0);
			close(fd);
			continue;
		}
		if (pthread_create(&t, NULL, server_conn, (void *)(intptr_t)fd) != 0) {
			__atomic_fetch_sub(&g_server_conns, 1, __ATOMIC_RELAXED);
			close(fd);
		}
	}
	return NULL;
}

int
s3_peer_server_start(void)
{
	const struct peer_config *c = cfg();
	struct sockaddr_in sa;
	socklen_t sl = sizeof(sa);
	int fd, one = 1, rc;

	if (!c->serve || g_server_running) {
		return 0;
	}
	if (!c->secret_ok) {
		SPDK_ERRLOG("peer: S3LVOL_PEER_LISTEN is set but %s does not hold a "
			    "secret of at least %d bytes; not serving\n", c->secret_file,
			    PEER_SECRET_MIN);
		return -EACCES;
	}
	if (parse_addr(c->listen, c->default_port, &sa) != 0) {
		SPDK_ERRLOG("peer: S3LVOL_PEER_LISTEN=%s is not <ipv4>[:port]\n", c->listen);
		return -EINVAL;
	}
	/* Never all interfaces: the endpoint exists for the private network. */
	if (sa.sin_addr.s_addr == htonl(INADDR_ANY)) {
		SPDK_ERRLOG("peer: refusing to listen on 0.0.0.0; give the private "
			    "address\n");
		return -EINVAL;
	}
	fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (fd < 0) {
		return -errno;
	}
	(void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
	if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0 || listen(fd, 64) != 0 ||
	    getsockname(fd, (struct sockaddr *)&sa, &sl) != 0) {
		rc = -errno;
		close(fd);
		SPDK_ERRLOG("peer: cannot listen on %s: %s\n", c->listen, spdk_strerror(-rc));
		return rc;
	}
	if (pipe2(g_stop_pipe, O_CLOEXEC) != 0) {
		rc = -errno;
		close(fd);
		return rc;
	}
	g_listen_fd = fd;
	g_server_port = ntohs(sa.sin_port);
	memcpy(g_srv_secret, c->secret, c->secret_len);
	g_srv_secret_len = c->secret_len;
	rc = s3_spawner_is_started() ?
	     s3_spawner_pthread_create(&g_accept_thread, server_accept_loop, NULL) :
	     pthread_create(&g_accept_thread, NULL, server_accept_loop, NULL);
	if (rc != 0) {
		close(fd);
		close(g_stop_pipe[0]);
		close(g_stop_pipe[1]);
		g_listen_fd = g_stop_pipe[0] = g_stop_pipe[1] = -1;
		return -rc;
	}
	g_server_running = true;
	SPDK_NOTICELOG("peer: serving published packs from %s on %s (port %u)\n",
		       c->dir, c->listen, g_server_port);
	return 0;
}

void
s3_peer_server_stop(void)
{
	if (!g_server_running) {
		return;
	}
	if (write(g_stop_pipe[1], "x", 1) != 1) {
		SPDK_WARNLOG("peer: could not wake the accept thread\n");
	}
	pthread_join(g_accept_thread, NULL);
	close(g_listen_fd);
	close(g_stop_pipe[0]);
	close(g_stop_pipe[1]);
	g_listen_fd = g_stop_pipe[0] = g_stop_pipe[1] = -1;
	g_server_running = false;
	g_server_port = 0;
	OPENSSL_cleanse(g_srv_secret, sizeof(g_srv_secret));
	g_srv_secret_len = 0;
}

uint16_t
s3_peer_server_port(void)
{
	return g_server_running ? g_server_port : 0;
}

/* ==========================================================================
 * Client
 * ========================================================================== */

struct s3_peer_req {
	char                 peer[S3_PEER_ADDR_MAX];
	char                 uuid[PEER_UUID_LEN + 1];
	uint32_t             pack;
	uint64_t             len;
	uint8_t              sha[S3_PEER_SHA256_LEN];
	uint8_t             *buf;
	s3_peer_cb           cb;
	void                *cb_arg;
	struct spdk_thread  *origin;
	int                  status;
	bool                 abandoned;   /* atomic */
	TAILQ_ENTRY(s3_peer_req) link;
};

static pthread_mutex_t g_q_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_q_cond = PTHREAD_COND_INITIALIZER;
static TAILQ_HEAD(, s3_peer_req) g_queue = TAILQ_HEAD_INITIALIZER(g_queue);
static uint32_t g_workers;

static uint64_t
mono_ms(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static bool
req_abandoned(struct s3_peer_req *r)
{
	return __atomic_load_n(&r->abandoned, __ATOMIC_ACQUIRE);
}

/* Wait until fd is ready for \p events, in short slices so an abandon is seen
 * quickly. -ETIMEDOUT past \p deadline, -ECANCELED once abandoned. */
static int
wait_fd(struct s3_peer_req *r, int fd, short events, uint64_t deadline)
{
	for (;;) {
		struct pollfd pfd = { .fd = fd, .events = events };
		uint64_t now = mono_ms();
		int n;

		if (req_abandoned(r)) {
			return -ECANCELED;
		}
		if (now >= deadline) {
			return -ETIMEDOUT;
		}
		n = poll(&pfd, 1, (int)spdk_min(deadline - now, (uint64_t)100));
		if (n > 0) {
			return 0;
		}
		if (n < 0 && errno != EINTR) {
			return -errno;
		}
	}
}

static int
client_recv(struct s3_peer_req *r, int fd, void *buf, size_t len, uint64_t deadline)
{
	uint8_t *p = buf;
	int rc;

	while (len > 0) {
		ssize_t n;

		rc = wait_fd(r, fd, POLLIN, deadline);
		if (rc != 0) {
			return rc;
		}
		n = recv(fd, p, len, MSG_DONTWAIT);
		if (n == 0) {
			return -ECONNRESET;
		}
		if (n < 0) {
			if (errno == EINTR || errno == EAGAIN) {
				continue;
			}
			return -errno;
		}
		p += n;
		len -= (size_t)n;
	}
	return 0;
}

static int
client_fetch(struct s3_peer_req *r)
{
	const struct peer_config *c = cfg();
	struct sockaddr_in sa;
	uint8_t req[PEER_REQ_LEN] = {0}, rsp[PEER_RSP_LEN], got[S3_PEER_SHA256_LEN];
	uint64_t deadline;
	uint32_t st;
	socklen_t sl;
	int fd, rc, err = 0, one = 1;

	if (!c->secret_ok) {
		return -EACCES;
	}
	if (parse_addr(r->peer, c->default_port, &sa) != 0) {
		return -EINVAL;
	}
	fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
	if (fd < 0) {
		return -errno;
	}
	(void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	deadline = mono_ms() + c->connect_ms;
	if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0) {
		if (errno != EINPROGRESS) {
			rc = -errno;
			goto out;
		}
		rc = wait_fd(r, fd, POLLOUT, deadline);
		if (rc != 0) {
			goto out;
		}
		sl = sizeof(err);
		if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &sl) != 0 || err != 0) {
			rc = err ? -err : -errno;
			goto out;
		}
	}
	deadline = mono_ms() + c->io_ms;
	memcpy(req, "S3PR", 4);
	put_u16(req + 4, PEER_VERSION);
	put_u64(req + 8, (uint64_t)time(NULL));
	put_u32(req + 16, r->pack);
	put_u64(req + 24, r->len);
	memcpy(req + 32, r->uuid, PEER_UUID_LEN);
	req_mac(c, req, req + PEER_REQ_SIGNED);
	/* 128 bytes fit in any socket buffer: one non-blocking send is enough. */
	if (send(fd, req, sizeof(req), MSG_NOSIGNAL) != (ssize_t)sizeof(req)) {
		rc = -EIO;
		goto out;
	}
	rc = client_recv(r, fd, rsp, sizeof(rsp), deadline);
	if (rc != 0) {
		goto out;
	}
	if (memcmp(rsp, "S3PA", 4) != 0) {
		rc = -EPROTO;
		goto out;
	}
	st = get_u32(rsp + 4);
	switch (st) {
	case S3_PEER_OK:
		break;
	case S3_PEER_NOT_FOUND:
		rc = -ENOENT;
		goto out;
	case S3_PEER_AUTH:
		rc = -EACCES;
		goto out;
	case S3_PEER_BUSY:
		rc = -EBUSY;
		goto out;
	default:
		rc = -EINVAL;
		goto out;
	}
	if (get_u64(rsp + 8) != r->len) {
		rc = -EPROTO;
		goto out;
	}
	rc = client_recv(r, fd, r->buf, r->len, deadline);
	if (rc != 0) {
		goto out;
	}
	s3_peer_sha256(r->buf, r->len, got);
	if (CRYPTO_memcmp(got, r->sha, sizeof(got)) != 0) {
		rc = -EBADMSG;
	}
out:
	close(fd);
	return rc;
}

static void
client_deliver(void *arg)
{
	struct s3_peer_req *r = arg;

	r->cb(r->cb_arg, req_abandoned(r) ? -ECANCELED : r->status);
	free(r);
}

static void *
client_worker(void *arg)
{
	(void)arg;
	pthread_detach(pthread_self());
	for (;;) {
		struct s3_peer_req *r;

		pthread_mutex_lock(&g_q_lock);
		while (TAILQ_EMPTY(&g_queue)) {
			pthread_cond_wait(&g_q_cond, &g_q_lock);
		}
		r = TAILQ_FIRST(&g_queue);
		TAILQ_REMOVE(&g_queue, r, link);
		pthread_mutex_unlock(&g_q_lock);

		r->status = req_abandoned(r) ? -ECANCELED : client_fetch(r);
		if (spdk_thread_send_msg(r->origin, client_deliver, r) != 0) {
			/* The thread is gone: nobody can take the answer. */
			free(r);
		}
	}
	return NULL;
}

static int
client_workers_start(void)
{
	uint32_t want = cfg()->threads;

	while (g_workers < want) {
		pthread_t t;
		int rc = s3_spawner_is_started() ?
			 s3_spawner_pthread_create(&t, client_worker, NULL) :
			 pthread_create(&t, NULL, client_worker, NULL);

		if (rc != 0) {
			return g_workers ? 0 : -rc;
		}
		g_workers++;
	}
	return 0;
}

struct s3_peer_req *
s3_peer_get(const char *peer, const char *uuid_str, uint32_t pack, uint64_t len,
	    const uint8_t sha256[S3_PEER_SHA256_LEN], void *buf, s3_peer_cb cb,
	    void *cb_arg)
{
	struct s3_peer_req *r;
	int rc;

	if (!peer || !*peer || !uuid_ok(uuid_str) || !sha256 || !buf || !cb ||
	    len == 0 || !spdk_get_thread()) {
		errno = EINVAL;
		return NULL;
	}
	r = calloc(1, sizeof(*r));
	if (!r) {
		errno = ENOMEM;
		return NULL;
	}
	snprintf(r->peer, sizeof(r->peer), "%s", peer);
	snprintf(r->uuid, sizeof(r->uuid), "%s", uuid_str);
	r->pack = pack;
	r->len = len;
	memcpy(r->sha, sha256, sizeof(r->sha));
	r->buf = buf;
	r->cb = cb;
	r->cb_arg = cb_arg;
	r->origin = spdk_get_thread();
	pthread_mutex_lock(&g_q_lock);
	rc = client_workers_start();
	if (rc != 0) {
		pthread_mutex_unlock(&g_q_lock);
		free(r);
		errno = rc;
		return NULL;
	}
	TAILQ_INSERT_TAIL(&g_queue, r, link);
	pthread_cond_signal(&g_q_cond);
	pthread_mutex_unlock(&g_q_lock);
	return r;
}

void
s3_peer_abandon(struct s3_peer_req *req)
{
	if (req) {
		__atomic_store_n(&req->abandoned, true, __ATOMIC_RELEASE);
	}
}
