/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   vbdev_s3lvol_nvmf -- attach and detach a bdev as an NVMf namespace
 *
 *   Wraps the pause / mutate / resume dance that spdk_nvmf_subsystem_add_ns_ext()
 *   and spdk_nvmf_subsystem_remove_ns() require. Both must be called while the
 *   subsystem is paused, and both pause and resume are asynchronous, so a single
 *   logical operation is a three-callback chain.
 *
 *   === Why the failure paths get so much attention ===
 *
 *   A subsystem left paused is not a failed operation, it is 64 unusable
 *   volumes: pausing freezes the admin queue for the whole subsystem, and the
 *   other namespaces on it are collateral. So every path that pauses must reach
 *   a resume, including the ones that failed, and the resume itself has to be
 *   handled when it fails.
 *
 *   The structure is lifted from SPDK's own rpc_nvmf_subsystem_add_ns (see
 *   lib/nvmf/nvmf_rpc.c), including its failback: if the namespace was added but
 *   the resume then failed, the namespace is removed again and a second resume
 *   attempted, because reporting success for a namespace nobody can reach is
 *   worse than reporting the failure.
 */

#include "spdk/stdinc.h"
#include "spdk/bdev.h"
#include "spdk/uuid.h"
#include "spdk/log.h"
#include "spdk/nvmf.h"
#include "spdk/string.h"
#include "spdk/thread.h"

#include "s3lvol/s3_deadline.h"

#include <sys/sysmacros.h>

#include "vbdev_s3lvol.h"

SPDK_LOG_REGISTER_COMPONENT(s3lvol_nvmf)

struct nvmf_op_ctx {
	struct spdk_nvmf_subsystem *subsystem;

	/* Copied: the caller's strings need not outlive the first callback. */
	char      bdev_name[SPDK_LVOL_NAME_MAX + SPDK_LVS_NAME_MAX + 2];
	uint32_t  nsid;
	bool      removing;

	/* Result of the mutation, reported after the resume completes. */
	int       status;

	s3lvol_nvmf_op_cb cb_fn;
	void             *cb_arg;
};

void
s3lvol_nvmf_subsys_nqn(uint32_t index, char *out, size_t out_len)
{
	snprintf(out, out_len, RCOW_NQN_PREFIX "%02" PRIu32, index);
}

/* --------------------------------------------------------------------------
 * Completion
 * -------------------------------------------------------------------------- */

static void
nvmf_op_done(struct nvmf_op_ctx *ctx, int status)
{
	s3lvol_nvmf_op_cb cb_fn = ctx->cb_fn;
	void *cb_arg = ctx->cb_arg;
	uint32_t nsid = ctx->nsid;

	free(ctx);

	if (cb_fn) {
		cb_fn(cb_arg, nsid, status);
	}
}

/* The subsystem is running again. Report whatever the mutation did. */
static void
nvmf_op_resumed(struct spdk_nvmf_subsystem *subsystem, void *cb_arg, int status)
{
	struct nvmf_op_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("subsystem '%s' failed to resume: %s. It is left paused; "
			    "every namespace on it is now unreachable\n",
			    spdk_nvmf_subsystem_get_nqn(subsystem),
			    spdk_strerror(-status));
		nvmf_op_done(ctx, status);
		return;
	}

	nvmf_op_done(ctx, ctx->status);
}

/* The namespace was added but the subsystem could not be resumed, so the
 * addition has been undone. This is the second resume attempt; there is nothing
 * further to try after it. */
static void
nvmf_op_failback_resumed(struct spdk_nvmf_subsystem *subsystem, void *cb_arg,
			 int status)
{
	struct nvmf_op_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("subsystem '%s' could not be resumed even after rolling "
			    "back nsid %" PRIu32 "; it stays paused\n",
			    spdk_nvmf_subsystem_get_nqn(subsystem), ctx->nsid);
	}

	/* Report the resume failure, not the add: the namespace is gone. */
	nvmf_op_done(ctx, ctx->status != 0 ? ctx->status : -EIO);
}

static void
nvmf_op_add_resumed(struct spdk_nvmf_subsystem *subsystem, void *cb_arg,
		    int status)
{
	struct nvmf_op_ctx *ctx = cb_arg;
	int rc;

	if (status == 0) {
		nvmf_op_done(ctx, ctx->status);
		return;
	}

	/* Resume failed after a successful add. Take the namespace back out, so
	 * that a retry is not blocked by a half-applied nsid, then try once more
	 * to get the subsystem running. */
	if (ctx->status == 0 && ctx->nsid != 0) {
		SPDK_ERRLOG("subsystem '%s' failed to resume after adding nsid "
			    "%" PRIu32 "; rolling the namespace back\n",
			    spdk_nvmf_subsystem_get_nqn(subsystem), ctx->nsid);

		if (spdk_nvmf_subsystem_remove_ns(subsystem, ctx->nsid) != 0) {
			SPDK_ERRLOG("rollback of nsid %" PRIu32 " failed too\n",
				    ctx->nsid);
		}
		ctx->status = status;

		rc = spdk_nvmf_subsystem_resume(subsystem, nvmf_op_failback_resumed,
						ctx);
		if (rc != 0) {
			SPDK_ERRLOG("subsystem '%s' stays paused: %s\n",
				    spdk_nvmf_subsystem_get_nqn(subsystem),
				    spdk_strerror(-rc));
			nvmf_op_done(ctx, status);
		}
		return;
	}

	nvmf_op_done(ctx, status);
}

/* --------------------------------------------------------------------------
 * Paused-state mutations
 * -------------------------------------------------------------------------- */

static void
nvmf_op_paused(struct spdk_nvmf_subsystem *subsystem, void *cb_arg, int status)
{
	struct nvmf_op_ctx *ctx = cb_arg;
	spdk_nvmf_subsystem_state_change_done resume_cb;
	int rc;

	if (status != 0) {
		/* Never entered the paused state, so no resume is owed. */
		SPDK_ERRLOG("subsystem '%s' failed to pause: %s\n",
			    spdk_nvmf_subsystem_get_nqn(subsystem),
			    spdk_strerror(-status));
		nvmf_op_done(ctx, status);
		return;
	}

	if (ctx->removing) {
		ctx->status = spdk_nvmf_subsystem_remove_ns(subsystem, ctx->nsid);
		if (ctx->status != 0) {
			SPDK_ERRLOG("subsystem '%s': cannot remove nsid %" PRIu32
				    ": %s\n", spdk_nvmf_subsystem_get_nqn(subsystem),
				    ctx->nsid, spdk_strerror(-ctx->status));
		}
		/* Nothing to roll back on the remove path: a removal that failed
		 * changed nothing. */
		resume_cb = nvmf_op_resumed;
	} else {
		struct spdk_nvmf_ns_opts ns_opts;

		spdk_nvmf_ns_opts_get_defaults(&ns_opts, sizeof(ns_opts));

		/* The requested nsid, not a suggestion: recovery has to reproduce the
		 * exact layout, and add_ns_ext() returns 0 rather than relocating if
		 * the id is taken or out of range. Note that a subsystem created
		 * without max_namespaces >= this value rejects it with "NSID greater
		 * than maximum not allowed", which only shows up in the target log. */
		ns_opts.nsid = ctx->nsid;

		/* uuid is left at its default so that add_ns_ext() takes it from the
		 * bdev. That is what makes the host expose the lvol uuid, which is the
		 * only stable way back from a volume to its device path. */
		ctx->nsid = spdk_nvmf_subsystem_add_ns_ext(subsystem, ctx->bdev_name,
							&ns_opts, sizeof(ns_opts),
							   NULL);
		if (ctx->nsid == 0) {
			SPDK_ERRLOG("subsystem '%s': cannot add bdev '%s' as nsid "
				    "%" PRIu32 " (taken, out of range, or bdev "
				    "missing)\n",
				    spdk_nvmf_subsystem_get_nqn(subsystem),
				    ctx->bdev_name, ns_opts.nsid);
			ctx->status = -EINVAL;
		}
		resume_cb = nvmf_op_add_resumed;
	}

	rc = spdk_nvmf_subsystem_resume(subsystem, resume_cb, ctx);
	if (rc != 0) {
		/* The resume could not even be submitted. The subsystem stays
		 * paused -- this is the one outcome with no recovery from here. */
		SPDK_ERRLOG("subsystem '%s' stays paused, resume could not be "
			    "submitted: %s\n",
			    spdk_nvmf_subsystem_get_nqn(subsystem),
			    spdk_strerror(-rc));
		nvmf_op_done(ctx, rc);
	}
}

/* --------------------------------------------------------------------------
 * Entry points
 * -------------------------------------------------------------------------- */

static struct spdk_nvmf_subsystem *
nvmf_find_subsystem(const char *nqn)
{
	struct spdk_nvmf_tgt *tgt;

	/* The first target is the one an SPDK app creates for itself; this module
	 * does not create targets of its own. */
	tgt = spdk_nvmf_get_first_tgt();
	if (!tgt) {
		SPDK_ERRLOG("no NVMf target exists yet\n");
		return NULL;
	}

	return spdk_nvmf_tgt_find_subsystem(tgt, nqn);
}

static int
nvmf_op_start(const char *nqn, const char *bdev_name, uint32_t nsid,
	      bool removing, s3lvol_nvmf_op_cb cb_fn, void *cb_arg)
{
	struct spdk_nvmf_subsystem *subsystem;
	struct nvmf_op_ctx *ctx;
	int rc;

	subsystem = nvmf_find_subsystem(nqn);
	if (!subsystem) {
		SPDK_ERRLOG("no such NVMf subsystem: %s\n", nqn);
		return -ENODEV;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		return -ENOMEM;
	}

	ctx->subsystem = subsystem;
	ctx->nsid= nsid;
	ctx->removing  = removing;
	ctx->cb_fn     = cb_fn;
	ctx->cb_arg    = cb_arg;
	if (bdev_name) {
		snprintf(ctx->bdev_name, sizeof(ctx->bdev_name), "%s", bdev_name);
	}

	/* Pause the namespace being operated on, not "the subsystem".
	 *
	 * nsid is not a hint here, and 0 is not a wildcard. From nvmf.h:
	 *
	 *   In a paused state, all admin queues are frozen across the whole
	 *   subsystem. If a namespace ID is provided, all commands to that
	 *   namespace are quiesced [...]
	 *   \param nsid The namespace to pause. If 0, pause no namespaces.
	 *
	 * This used to pass 0, on the assumption that it meant "all of them". It
	 * means the opposite, and the consequence was not subtle: nvmf.c:2026 only
	 * marks a namespace PAUSING when `nsid - 1 < num_ns`, which is false for 0,
	 * so nothing was quiesced, the drain loop had no namespace to wait on, and
	 * the pause completed immediately while the host kept reading. remove_ns
	 * then ran and the resume released the namespace's bdev channel with I/O
	 * still on it, which is an assert in the bdev layer:
	 *
	 *   bdev_channel_destroy_resource: Assertion `TAILQ_EMPTY(&ch->io_submitted)'
	 *
	 * It took a host read in flight to show up, so it survived every test that
	 * activated and deactivated an idle volume, and it was the udev probe of a
	 * freshly appeared device that finally raced it.
	 *
	 * Per-namespace is also the granularity we want rather than a compromise:
	 * adding volume 37 to a subsystem must not stall the 36 already serving I/O
	 * from it. SPDK's own add_ns and remove_ns RPCs pass the namespace id for
	 * the same reason (nvmf_rpc.c:1293 and :1434); only the listener and ANA
	 * RPCs, which touch no namespace I/O, pass 0.
	 *
	 * On the add path the namespace does not exist yet, so there is nothing to
	 * drain -- but ns_info[nsid - 1] does exist (the array is sized by the
	 * subsystem's max_nsid), so the call is well defined and costs a state flag.
	 */
	rc = spdk_nvmf_subsystem_pause(subsystem, nsid, nvmf_op_paused, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("subsystem '%s': pause could not be submitted: %s\n",
			    nqn, spdk_strerror(-rc));
		free(ctx);
		return rc;
	}

	return 0;
}

int
s3lvol_nvmf_add_ns(const char *nqn, const char *bdev_name, uint32_t nsid,
		   s3lvol_nvmf_op_cb cb_fn, void *cb_arg)
{
	if (!nqn || !bdev_name || nsid == 0) {
		return -EINVAL;
	}
	return nvmf_op_start(nqn, bdev_name, nsid, false, cb_fn, cb_arg);
}

int
s3lvol_nvmf_remove_ns(const char *nqn, uint32_t nsid, s3lvol_nvmf_op_cb cb_fn,
		      void *cb_arg)
{
	if (!nqn || nsid == 0) {
		return -EINVAL;
	}
	return nvmf_op_start(nqn, NULL, nsid, true, cb_fn, cb_arg);
}

int
s3lvol_nvmf_ns_state(const char *nqn, uint32_t nsid, const char *uuid_str)
{
	struct spdk_nvmf_subsystem *subsystem;
	struct spdk_nvmf_ns *ns;
	struct spdk_bdev *bdev;
	char have[SPDK_UUID_STRING_LEN];

	if (!nqn || nsid == 0 || !uuid_str) {
		return -EINVAL;
	}
	subsystem = nvmf_find_subsystem(nqn);
	if (!subsystem) {
		return -ENODEV;
	}
	ns = spdk_nvmf_subsystem_get_ns(subsystem, nsid);
	if (!ns) {
		return S3LVOL_NS_ABSENT;
	}
	bdev = spdk_nvmf_ns_get_bdev(ns);
	if (!bdev) {
		return S3LVOL_NS_OTHER;
	}
	spdk_uuid_fmt_lower(have, sizeof(have), spdk_bdev_get_uuid(bdev));
	return strcasecmp(have, uuid_str) == 0 ? S3LVOL_NS_PRESENT : S3LVOL_NS_OTHER;
}

bool
s3lvol_nvmf_subsys_exists(uint32_t index)
{
	char nqn[SPDK_NVMF_NQN_MAX_LEN + 1];

	s3lvol_nvmf_subsys_nqn(index, nqn, sizeof(nqn));
	return nvmf_find_subsystem(nqn) != NULL;
}

/* --------------------------------------------------------------------------
 * Host-side device lookup
 *
 * This is the one part of the module that reaches across to the host: NQN and
 * nsid are target-side names, /dev/nvmeXnY is allocated by the host kernel, and
 * nothing on the target knows the mapping. It works here only because the target
 * and the initiator are the same machine.
 *
 * The lookup is by uuid rather than by nsid, and that is not a stylistic choice.
 * Measured on a live target: nsid 7 appeared as /dev/nvme0n1 and nsid 42 as
 * /dev/nvme0n2 -- the host numbers namespaces in discovery order. Reactivate
 * them in the opposite order and the two paths trade places, so any arithmetic
 * from nsid to a device name is wrong as soon as anything is detached and
 * reattached. The uuid, on the other hand, is the lvol's own and travels
 * unchanged: vbdev_s3lvol_lvol.c assigns bdev->uuid = lvol->uuid, add_ns takes
 * the namespace uuid from the bdev, and the host publishes it in sysfs.
 * -------------------------------------------------------------------------- */

/* Read a one-line sysfs attribute, trimming the newline. */
static bool
sysfs_read_line(const char *dir, const char *attr, char *out, size_t out_len)
{
	char path[PATH_MAX];
	FILE *fp;
	size_t len;

	if (snprintf(path, sizeof(path), "%s/%s", dir, attr) >= (int)sizeof(path)) {
		return false;
	}

	fp = fopen(path, "r");
	if (!fp) {
		return false;
	}
	if (!fgets(out, (int)out_len, fp)) {
		fclose(fp);
		return false;
	}
	fclose(fp);

	len = strlen(out);
	while (len > 0 && (out[len - 1] == '\n' || out[len - 1] == '\r')) {
		out[--len] = '\0';
	}
	return len > 0;
}

int
s3lvol_nvmf_resolve_device(const char *uuid_str, char *out, size_t out_len)
{
	DIR *d;
	struct dirent *ent;
	int rc = -ENOENT;

	if (!uuid_str || !uuid_str[0] || !out || out_len == 0) {
		return -EINVAL;
	}

	d = opendir("/sys/block");
	if (!d) {
		SPDK_ERRLOG("cannot read /sys/block: %s\n", strerror(errno));
		return -errno;
	}

	while ((ent = readdir(d)) != NULL) {
		char dir[PATH_MAX];
		char value[SPDK_UUID_STRING_LEN + 16];

		if (strncmp(ent->d_name, "nvme", 4) != 0) {
			continue;
		}

		/* Skip the per-controller sibling. A connected subsystem shows up
		 * twice -- as nvme0n1 and as nvme0c0n1 -- carrying the same uuid,
		 * and the c-form is the hidden path underneath the multipath device,
		 * not something to hand out for mounting. Without this filter the
		 * scan returns whichever readdir happens to yield first. */
		if (strchr(ent->d_name + 4, 'c') != NULL) {
			continue;
		}

		if (snprintf(dir, sizeof(dir), "/sys/block/%s", ent->d_name) >=
		    (int)sizeof(dir)) {
			continue;
		}

		/* uuid is the direct match. wwid is the fallback for kernels that do
		 * not publish uuid separately; it reads "uuid.<the uuid>". */
		if (sysfs_read_line(dir, "uuid", value, sizeof(value)) &&
		    strcasecmp(value, uuid_str) == 0) {
			snprintf(out, out_len, "/dev/%s", ent->d_name);
			rc = 0;
			break;
		}
		if (sysfs_read_line(dir, "wwid", value, sizeof(value)) &&
		    strcasestr(value, uuid_str) != NULL) {
			snprintf(out, out_len, "/dev/%s", ent->d_name);
			rc = 0;
			break;
		}
	}

	closedir(d);
	return rc;
}

bool
s3lvol_nvmf_device_is_ready(const char *dev, const char *uuid_str)
{
	struct stat st, st2;
	char sysdir[PATH_MAX];
	char id[SPDK_UUID_STRING_LEN + 16];
	char devnum[64];
	const char *leaf;
	unsigned maj, min;
	int n;

	if (!dev || !dev[0] || !uuid_str || !uuid_str[0]) {
		return false;
	}
	if (stat(dev, &st) != 0 || !S_ISBLK(st.st_mode)) {
		return false;
	}
	leaf = strrchr(dev, '/');
	if (leaf == NULL || leaf[1] == '\0') {
		return false;
	}
	n = snprintf(sysdir, sizeof(sysdir), "/sys/block/%s", leaf + 1);
	if (n < 0 || n >= (int)sizeof(sysdir)) {
		return false;
	}

	/* The uuid (or wwid) must be this lvol. After a same-nsid reactivate,
	 * /sys/block/<name>/uuid can already be the new namespace while /dev
	 * still holds the previous occupant's node; matching only major:minor
	 * would hand that node out. */
	if (sysfs_read_line(sysdir, "uuid", id, sizeof(id))) {
		if (strcasecmp(id, uuid_str) != 0) {
			return false;
		}
	} else if (sysfs_read_line(sysdir, "wwid", id, sizeof(id))) {
		if (strcasestr(id, uuid_str) == NULL) {
			return false;
		}
	} else {
		return false;
	}

	if (!sysfs_read_line(sysdir, "dev", devnum, sizeof(devnum))) {
		return false;
	}
	if (sscanf(devnum, "%u:%u", &maj, &min) != 2) {
		return false;
	}
	if (st.st_rdev != makedev(maj, min)) {
		return false;
	}

	/* udev may unlink the node between the first stat and the sysfs reads. */
	if (stat(dev, &st2) != 0 || !S_ISBLK(st2.st_mode) ||
	    st2.st_rdev != st.st_rdev) {
		return false;
	}
	return true;
}

bool
s3lvol_nvmf_udev_settled(void)
{
	/* udev keeps /run/udev/queue in place exactly while it still has events
	 * to apply -- this is the file udevadm settle waits on. Its absence is
	 * what makes a node that passes s3lvol_nvmf_device_is_ready() stable:
	 * nobody is left to unlink it. When udev is not running (containers
	 * with a static /dev) the file never appears, and nothing moves /dev
	 * behind us either, so treating that as settled is right. */
	return access("/run/udev/queue", F_OK) != 0;
}

uint32_t
s3lvol_nvmf_readahead_kb(void)
{
	static uint32_t cached;
	static bool resolved;
	const char *env;

	if (resolved) {
		return cached;
	}

	cached = RCOW_DEFAULT_READ_AHEAD_KB;
	env = getenv(S3LVOL_READ_AHEAD_ENV);
	if (env && env[0]) {
		char *end = NULL;
		unsigned long v = strtoul(env, &end, 10);

		if (end && *end == '\0' && v <= 32768) {
			/* 0 is meaningful: leave every device alone. */
			cached = (uint32_t)v;
		} else {
			SPDK_WARNLOG("%s='%s' is not a readahead in KiB; using %d\n",
				     S3LVOL_READ_AHEAD_ENV, env,
				     RCOW_DEFAULT_READ_AHEAD_KB);
		}
	}

	resolved = true;
	return cached;
}

bool
s3lvol_nvmf_set_readahead(const char *leaf, uint32_t kb)
{
	char path[PATH_MAX];
	char cur[32];
	uint32_t cur_kb = 0;
	bool have_cur = false;
	FILE *fp;

	if (!leaf || !leaf[0] || kb == 0) {
		return false;
	}

	/* Reject anything that could climb out of the directory. leaf comes from
	 * readdir on /sys/block in the only caller today, but this is a public
	 * function taking a string that ends up in a path, and the whole point of
	 * it is to write to that path. */
	if (strchr(leaf, '/') != NULL || strcmp(leaf, ".") == 0 ||
	    strcmp(leaf, "..") == 0) {
		return false;
	}

	if (snprintf(path, sizeof(path),
		     "/sys/block/%s/queue/read_ahead_kb", leaf) >=
	    (int)sizeof(path)) {
		return false;
	}

	fp = fopen(path, "r");
	if (fp) {
		if (fgets(cur, sizeof(cur), fp)) {
			cur_kb = (uint32_t)strtoul(cur, NULL, 10);
			have_cur = true;
		}
		fclose(fp);
	}

	if (have_cur) {
		if (cur_kb == kb) {
			/* The common case: every later lookup of a volume that was
			 * tuned on the first one. */
			return true;
		}

		/* Anything other than the kernel default means somebody decided
		 * this on purpose -- the operator, through RCOW_READ_AHEAD_KB, or
		 * a tuning layer above. Overwriting it would make that setting
		 * silently temporary, undone by the next lookup of the device,
		 * and 128 is a legitimate choice for a purely random workload.
		 *
		 * So this function only ever moves a device *off* the kernel
		 * default. That is what makes it safe to call on every lookup. */
		if (cur_kb != S3LVOL_KERNEL_DEFAULT_READ_AHEAD_KB) {
			SPDK_INFOLOG(s3lvol_nvmf, "%s: leaving readahead at %"
				     PRIu32 " KiB, which is not the kernel "
				     "default and so was set deliberately\n",
				     leaf, cur_kb);
			return false;
		}
	}

	fp = fopen(path, "w");
	if (!fp) {
		/* Not an error worth logging at error level: a volume with the
		 * kernel default readahead works, it just reads cold data more
		 * slowly. */
		SPDK_INFOLOG(s3lvol_nvmf, "cannot set readahead on %s: %s\n",
			     leaf, strerror(errno));
		return false;
	}

	if (fprintf(fp, "%" PRIu32 "\n", kb) < 0) {
		fclose(fp);
		return false;
	}
	if (fclose(fp) != 0) {
		return false;
	}

	SPDK_NOTICELOG("%s: readahead set to %" PRIu32 " KiB\n", leaf, kb);
	return true;
}

/* --------------------------------------------------------------------------
 * Quiesce before unload (B3)
 *
 * The lvstore unload unregisters every lvol bdev. NVMf holds a bdev channel per
 * namespace per poll group, and that channel is released when the namespace
 * goes away. If a host read is still waiting on S3 at that moment (a stalled GET
 * runs up to its 60 s deadline), the channel is destroyed with the bdev_io still
 * on its io_submitted list:
 *
 *   bdev_channel_destroy_resource: Assertion `TAILQ_EMPTY(&ch->io_submitted)'
 *
 * Observed three times on app2 (2026-11-18 14:47, 19:00, 2026-11-19 01:43), each
 * within 5 s of rcow_unload_lvstore on a keep-connected restart, each while
 * host I/O was stuck in S3 GETs; the 01:43 core was written 90 ms into the
 * unload, far too early for any drain to have waited on S3.
 *
 * The mechanism is in lib/nvmf: the unload unregisters every lvol at once, so
 * nvmf hot-removes many namespaces of the same subsystem concurrently. Each
 * hot-remove pauses its nsid, but state changes are queued per subsystem, and a
 * pause that finds the subsystem already PAUSED (by the previous nsid's
 * removal, whose resume is queued behind it) completes immediately without
 * looking at this nsid's io_outstanding. Its namespace is removed and the
 * following resume puts the channel with the read still on it.
 *
 * So every rcow subsystem is stopped (made INACTIVE) first and its namespaces
 * are removed while it is inactive. Stopping disconnects
 * the qpairs, waits for each qpair's outstanding requests, and only then puts
 * the namespace channels, so no channel can go away with I/O on it. To keep the
 * wait short, pending S3 deadlines are expired every 500 ms while it lasts:
 * stalled GETs complete with an error on a qpair that is already gone, so the
 * host never sees it and requeues the I/O on reconnect.
 *
 * The host is unaffected on a keep-connected restart: it sees a transport
 * disconnect, keeps the namespaces and requeues I/O, and reconnects to the new
 * process on the same NQN/NSID. Nothing here is undone; the process exits next.
 * -------------------------------------------------------------------------- */

#define S3LVOL_QUIESCE_POLL_US (500 * 1000)

struct nvmf_quiesce_ctx {
	struct spdk_poller	*poller;
	uint64_t		 start_tsc;
	uint64_t		 deadline_tsc;
	uint32_t		 pending;
	uint32_t		 stopped;
	uint32_t		 failed;
	uint32_t		 removed_ns;
	uint32_t		 expire_rounds;
	bool			 reported;
	s3lvol_nvmf_quiesce_cb	 cb_fn;
	void			*cb_arg;
};

static uint64_t
quiesce_elapsed_ms(struct nvmf_quiesce_ctx *ctx)
{
	return (spdk_get_ticks() - ctx->start_tsc) * 1000 / spdk_get_ticks_hz();
}

static void
quiesce_report(struct nvmf_quiesce_ctx *ctx, int status)
{
	struct s3lvol_nvmf_quiesce_result r = {
		.stopped       = ctx->stopped,
		.failed        = ctx->failed,
		.removed_ns    = ctx->removed_ns,
		.pending       = ctx->pending,
		.expire_rounds = ctx->expire_rounds,
		.elapsed_ms    = quiesce_elapsed_ms(ctx),
	};

	if (ctx->reported) {
		return;
	}
	ctx->reported = true;
	if (status == 0) {
		SPDK_NOTICELOG("NVMf quiesce: %u subsystem(s) stopped, %u namespace(s) "
			       "removed in %" PRIu64 " ms (%u S3 expire round(s))\n",
			       r.stopped, r.removed_ns, r.elapsed_ms, r.expire_rounds);
	} else {
		SPDK_ERRLOG("NVMf quiesce: %s after %" PRIu64 " ms: %u stopped, "
			    "%u failed, %u still draining\n", spdk_strerror(-status),
			    r.elapsed_ms, r.stopped, r.failed, r.pending);
	}
	if (ctx->cb_fn) {
		ctx->cb_fn(ctx->cb_arg, &r, status);
	}
}

static void
quiesce_maybe_free(struct nvmf_quiesce_ctx *ctx)
{
	if (ctx->pending > 0) {
		return;
	}
	spdk_poller_unregister(&ctx->poller);
	quiesce_report(ctx, ctx->failed ? -EIO : 0);
	free(ctx);
}

static int
quiesce_poll(void *arg)
{
	struct nvmf_quiesce_ctx *ctx = arg;

	if (ctx->pending == 0) {
		return SPDK_POLLER_IDLE;
	}
	/* Requests submitted after the previous round got a fresh deadline;
	 * expire those too. Cheap: one pass over the in-flight list. */
	s3_deadline_expire_all();
	ctx->expire_rounds++;
	if (!ctx->reported && spdk_get_ticks() >= ctx->deadline_tsc) {
		/* Report, but keep the context: the stop callbacks still have it. */
		quiesce_report(ctx, -ETIMEDOUT);
	}
	return SPDK_POLLER_BUSY;
}

static void
quiesce_stopped(struct spdk_nvmf_subsystem *subsystem, void *cb_arg, int status)
{
	struct nvmf_quiesce_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("NVMf quiesce: subsystem '%s' did not stop: %d\n",
			    spdk_nvmf_subsystem_get_nqn(subsystem), status);
		ctx->failed++;
	} else {
		struct spdk_nvmf_ns *ns, *next;
		uint32_t removed = 0;

		/* INACTIVE: the poll groups have dropped their channels, and
		 * remove_ns is legal without a pause. With no namespace left the
		 * unload's bdev unregister never reaches nvmf's hot-remove path,
		 * which is where concurrent removals in one subsystem skip the
		 * I/O drain (see the comment above). */
		for (ns = spdk_nvmf_subsystem_get_first_ns(subsystem); ns; ns = next) {
			next = spdk_nvmf_subsystem_get_next_ns(subsystem, ns);
			if (spdk_nvmf_subsystem_remove_ns(subsystem,
							  spdk_nvmf_ns_get_id(ns)) == 0) {
				removed++;
			} else {
				ctx->failed++;
			}
		}
		ctx->removed_ns += removed;
		ctx->stopped++;
	}
	assert(ctx->pending > 0);
	ctx->pending--;
	quiesce_maybe_free(ctx);
}

int
s3lvol_nvmf_quiesce_all(uint32_t timeout_ms, s3lvol_nvmf_quiesce_cb cb_fn,
			void *cb_arg)
{
	struct nvmf_quiesce_ctx *ctx;
	char nqn[SPDK_NVMF_NQN_MAX_LEN + 1];
	uint32_t i;
	int rc;

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		return -ENOMEM;
	}
	ctx->cb_fn = cb_fn;
	ctx->cb_arg = cb_arg;
	ctx->start_tsc = spdk_get_ticks();
	ctx->deadline_tsc = ctx->start_tsc +
			    (uint64_t)timeout_ms * spdk_get_ticks_hz() / 1000;
	ctx->poller = SPDK_POLLER_REGISTER(quiesce_poll, ctx, S3LVOL_QUIESCE_POLL_US);
	if (!ctx->poller) {
		free(ctx);
		return -ENOMEM;
	}

	/* Self reference: a stop that completes inline must not free ctx before
	 * the loop has submitted the rest. */
	ctx->pending = 1;
	s3_deadline_expire_all();
	ctx->expire_rounds++;
	for (i = 0; i < RCOW_NUM_SUBSYS; i++) {
		struct spdk_nvmf_subsystem *subsystem;

		s3lvol_nvmf_subsys_nqn(i, nqn, sizeof(nqn));
		subsystem = nvmf_find_subsystem(nqn);
		if (!subsystem) {
			continue;
		}
		ctx->pending++;
		rc = spdk_nvmf_subsystem_stop(subsystem, quiesce_stopped, ctx);
		if (rc != 0) {
			SPDK_ERRLOG("NVMf quiesce: cannot stop '%s': %s\n", nqn,
				    spdk_strerror(-rc));
			ctx->pending--;
			ctx->failed++;
		}
	}
	ctx->pending--;
	quiesce_maybe_free(ctx);
	return 0;
}
