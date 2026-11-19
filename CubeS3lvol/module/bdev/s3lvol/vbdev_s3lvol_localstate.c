/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   Node-local copies of the small S3 metadata objects an attach reads.
 *
 *   === Why ===
 *
 *   A keep-connected restart (rcow_stop.sh --keep-connected, then a new
 *   target) attaches the same lvstore on the same device. Every object the
 *   attach used to GET from S3 -- the owner marker, the imports and exports
 *   registries -- was written by this node, so the node already knows its
 *   content. With S3 degraded (live, app2: 70% packet loss) those few GETs
 *   and the strict ownership probe could not complete, the attach failed with
 *   -ETIMEDOUT and the guests had no disks for ~9 minutes, although
 *   everything they needed was on the local device.
 *
 *   These files make the attach S3-independent:
 *
 *     owner.<lvs>.claim    the strict claim this node holds (written once the
 *                          lvstore is up, removed when the claim is released)
 *     imports.<lvs>.json   the imports registry, written before every S3 save
 *     exports.<lvs>.json   the exports registry, same
 *
 *   (The checkpoint object has its own local copy, kept by s3_bs_dev; the
 *   blobstore metadata pages by the metadata cache index.)
 *
 *   Each file carries a header with a magic, the lvstore uuid and a CRC32C of
 *   the body, so a torn, foreign or stale file (an lvstore recreated under
 *   the same name) is refused and the attach falls back to S3. Written with
 *   tmp + fsync + rename + directory fsync.
 *
 *   Never a source of authority beyond this node: a registry file is always
 *   at least as new as the S3 object (written before the PUT), and the claim
 *   file is only adopted under the conditions in lvs_attach_adopt_local_claim.
 */

#include "spdk/stdinc.h"
#include "spdk/crc32.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/uuid.h"

#include "vbdev_s3lvol.h"

#define LOCAL_MAGIC   "S3LVLOC1"
#define LOCAL_MAX     (64u * 1024 * 1024)

struct local_hdr {
	char             magic[8];
	uint32_t         version;
	uint32_t         body_crc;
	uint64_t         body_len;
	struct spdk_uuid lvs_uuid;
	uint32_t         hdr_crc;
	uint32_t         reserved;
};

static const char *
local_kind_name(enum s3lvol_local_kind kind)
{
	switch (kind) {
	case S3LVOL_LOCAL_CLAIM:
		return "owner";
	case S3LVOL_LOCAL_IMPORTS:
		return "imports";
	case S3LVOL_LOCAL_EXPORTS:
		return "exports";
	default:
		return NULL;
	}
}

const char *
s3lvol_local_dir(void)
{
	const char *d = getenv("S3LVOL_MDCACHE_DIR");

	return (d && *d) ? d : "/data/cubelet/rcow";
}

int
s3lvol_local_path(const char *lvs_name, enum s3lvol_local_kind kind,
		  char *out, size_t out_len)
{
	const char *k = local_kind_name(kind);
	int rc;

	if (!lvs_name || !k) {
		return -EINVAL;
	}
	rc = snprintf(out, out_len, "%s/%s.%s.%s", s3lvol_local_dir(), k, lvs_name,
		      kind == S3LVOL_LOCAL_CLAIM ? "claim" : "json");
	return (rc < 0 || (size_t)rc >= out_len) ? -ENAMETOOLONG : 0;
}

static uint32_t
local_hdr_crc(const struct local_hdr *h)
{
	struct local_hdr tmp = *h;

	tmp.hdr_crc = 0;
	return spdk_crc32c_update(&tmp, sizeof(tmp), 0);
}

static int
write_all(int fd, const void *buf, size_t len)
{
	const char *p = buf;

	while (len > 0) {
		ssize_t n = write(fd, p, len);

		if (n < 0) {
			if (errno == EINTR) {
				continue;
			}
			return -errno;
		}
		p += n;
		len -= (size_t)n;
	}
	return 0;
}

int
s3lvol_local_write(const char *lvs_name, enum s3lvol_local_kind kind,
		   const struct spdk_uuid *lvs_uuid, const void *body, size_t len)
{
	char path[PATH_MAX], tmp[PATH_MAX + 8];
	struct local_hdr h;
	int fd, dfd, rc;

	if (!lvs_uuid || spdk_uuid_is_null(lvs_uuid) || len > LOCAL_MAX ||
	    (len && !body)) {
		return -EINVAL;
	}
	rc = s3lvol_local_path(lvs_name, kind, path, sizeof(path));
	if (rc != 0) {
		return rc;
	}
	snprintf(tmp, sizeof(tmp), "%s.tmp", path);

	memset(&h, 0, sizeof(h));
	memcpy(h.magic, LOCAL_MAGIC, sizeof(h.magic));
	h.version = 1;
	h.body_len = len;
	h.body_crc = len ? spdk_crc32c_update(body, len, 0) : 0;
	spdk_uuid_copy(&h.lvs_uuid, lvs_uuid);
	h.hdr_crc = local_hdr_crc(&h);

	fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
	if (fd < 0) {
		return -errno;
	}
	rc = write_all(fd, &h, sizeof(h));
	if (rc == 0 && len) {
		rc = write_all(fd, body, len);
	}
	if (rc == 0 && fsync(fd) != 0) {
		rc = -errno;
	}
	close(fd);
	if (rc == 0 && rename(tmp, path) != 0) {
		rc = -errno;
	}
	if (rc != 0) {
		unlink(tmp);
		SPDK_WARNLOG("local state %s: write failed (%s); the next attach "
			     "reads S3 instead\n", path, spdk_strerror(-rc));
		return rc;
	}
	dfd = open(s3lvol_local_dir(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (dfd >= 0) {
		(void)fsync(dfd);
		close(dfd);
	}
	return 0;
}

int
s3lvol_local_read(const char *lvs_name, enum s3lvol_local_kind kind,
		  const struct spdk_uuid *lvs_uuid, void **out, size_t *out_len)
{
	char path[PATH_MAX];
	struct local_hdr h;
	struct stat st;
	void *body = NULL;
	ssize_t n;
	int fd, rc;

	*out = NULL;
	*out_len = 0;
	rc = s3lvol_local_path(lvs_name, kind, path, sizeof(path));
	if (rc != 0) {
		return rc;
	}
	fd = open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0) {
		return -errno;
	}
	rc = -EILSEQ;
	if (fstat(fd, &st) != 0 || (size_t)st.st_size < sizeof(h)) {
		goto out;
	}
	n = pread(fd, &h, sizeof(h), 0);
	if (n != (ssize_t)sizeof(h) || memcmp(h.magic, LOCAL_MAGIC, sizeof(h.magic)) != 0 ||
	    h.version != 1 || local_hdr_crc(&h) != h.hdr_crc || h.body_len > LOCAL_MAX ||
	    (uint64_t)st.st_size != sizeof(h) + h.body_len) {
		goto out;
	}
	if (!lvs_uuid || spdk_uuid_compare(&h.lvs_uuid, lvs_uuid) != 0) {
		rc = -ESTALE;
		goto out;
	}
	/* One spare byte, so a JSON body can be handed on NUL-terminated. */
	body = calloc(1, h.body_len + 1);
	if (!body) {
		rc = -ENOMEM;
		goto out;
	}
	if (h.body_len &&
	    (pread(fd, body, h.body_len, sizeof(h)) != (ssize_t)h.body_len ||
	     spdk_crc32c_update(body, h.body_len, 0) != h.body_crc)) {
		free(body);
		body = NULL;
		goto out;
	}
	*out = body;
	*out_len = h.body_len;
	rc = 0;
out:
	close(fd);
	if (rc != 0 && rc != -ENOENT) {
		SPDK_WARNLOG("local state %s refused (%s); reading S3 instead\n",
			     path, spdk_strerror(-rc));
	}
	return rc;
}

void
s3lvol_local_remove(const char *lvs_name, enum s3lvol_local_kind kind)
{
	char path[PATH_MAX];

	if (s3lvol_local_path(lvs_name, kind, path, sizeof(path)) == 0) {
		(void)unlink(path);
	}
}
