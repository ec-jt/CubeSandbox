/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   vbdev_s3lvol -- module registration and global initialisation
 *
 *   This file owns two things that must happen at module init and can only
 *   happen at this layer:
 *
 *   1. **Start the thread spawner and compute its cpuset.**
 *      cpuset = all usable cores - this process's reactor cores. Computing the
 *      complement needs spdk_env_get_core_mask(), which is an env-layer API,
 *      while lib/s3bsdev must stay unit-testable outside the app framework,
 *      so the computation lives in the module layer and the cpuset is injected
 *      into s3_spawner_start().
 *
 *   2. **Initialise CRT.** s3_crt_global_init() must be called after the
 *      spawner -- it pthread_creates the event loop / resolver / logger
 *      threads internally, and those threads would inherit the reactor's
 *      single-core affinity if created on a reactor.
 */

#include "spdk/stdinc.h"
#include "spdk/bdev_module.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/thread.h"

#include "s3lvol/s3_client.h"
#include "s3lvol/s3_spawner.h"

#include "s3lvol/s3_peer.h"
#include "vbdev_s3lvol.h"

/* Number of CRT event loop threads. Kept below the reactor core count -- they
 * only do network I/O and do not need one thread per core. */
#define S3LVOL_CRT_THREADS 4

static bool g_s3lvol_initialized = false;

/* Compute "the set of cores allowed for background threads".
 *
 * 1. S3LVOL_BG_CPUS (a cpu list, e.g. "2" or "2-3") names the cores
 *    explicitly. Use it to give the CRT event loops a dedicated core so a
 *    busy reactor cannot starve S3 requests.
 * 2. Otherwise: the cores this process may use - reactor cores.
 *
 * The usable set is NOT sched_getaffinity(0): module init runs on the main
 * reactor, which DPDK has already pinned to its lcore, so that call returns
 * one core. With a 2-reactor mask the complement was empty, and the fallback
 * then put every CRT thread on reactor_0's core (seen live: AwsEventLoop on
 * CPU 0 next to a 96% busy reactor_0). s3_spawner_usable_cpus() reads the
 * online cores intersected with this process's cgroup cpuset instead.
 *
 * Reactor cores are enumerated with SPDK_ENV_FOREACH_CORE() and are always
 * removed, also from an explicit list.
 */
static int
s3lvol_build_spawner_cpuset(cpu_set_t *out)
{
	cpu_set_t online, want;
	const char *env;
	uint32_t core;
	int rc;

	CPU_ZERO(out);
	rc = s3_spawner_usable_cpus(&online);
	if (rc != 0) {
		SPDK_ERRLOG("cannot read the usable cpu set: %d\n", rc);
		return rc;
	}

	memcpy(out, &online, sizeof(online));
	env = getenv("S3LVOL_BG_CPUS");
	if (env && *env) {
		if (s3_spawner_parse_cpulist(env, &want) != 0) {
			SPDK_ERRLOG("S3LVOL_BG_CPUS=%s is not a cpu list; ignored\n", env);
		} else {
			CPU_AND(out, out, &want);
			if (CPU_COUNT(out) == 0) {
				SPDK_WARNLOG("S3LVOL_BG_CPUS=%s has no usable core; "
					     "using every non-reactor core\n", env);
				memcpy(out, &online, sizeof(online));
			}
		}
	}

	/* carve out the reactor cores */
	SPDK_ENV_FOREACH_CORE(core) {
		if (core < CPU_SETSIZE) {
			CPU_CLR(core, out);
		}
	}

	if (CPU_COUNT(out) == 0) {
		/* The reactors occupy every usable core. Hand the spawner all
		 * online cores -- sharing is better than "no core at all"
		 * (s3_spawner_start refuses to set an empty set's affinity,
		 * which would be back to square one). */
		SPDK_WARNLOG("Reactors occupy every available core; background "
			     "threads will share them\n");
		memcpy(out, &online, sizeof(online));
	}

	return 0;
}

static int
vbdev_s3lvol_init(void)
{
	cpu_set_t cpuset;
	uint32_t core;
	int num_reactors = 0;
	int rc;

	if (g_s3lvol_initialized) {
		return 0;
	}

	rc = s3lvol_build_spawner_cpuset(&cpuset);
	if (rc != 0) {
		return rc;
	}

	SPDK_ENV_FOREACH_CORE(core) {
		num_reactors++;
	}

	SPDK_NOTICELOG("s3lvol: %d reactor cores, %d cores left for background "
		       "threads\n", num_reactors, CPU_COUNT(&cpuset));

	/* The spawner must start first: CRT init runs on it, or CRT's I/O
	 * threads inherit the reactor's single-core affinity. */
	rc = s3_spawner_start(&cpuset);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to start thread spawner: %d\n", rc);
		return rc;
	}

	rc = s3_crt_global_init(S3LVOL_CRT_THREADS);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to init AWS CRT: %d\n", rc);
		s3_spawner_stop();
		return rc;
	}

	/* Peer pull server (S3LVOL_PEER_LISTEN). Optional: a failure is logged
	 * and importers of this node's exports read S3 instead. */
	if (s3_peer_serve_enabled()) {
		(void)s3_peer_server_start();
	}

	g_s3lvol_initialized = true;
	return 0;
}

static void
vbdev_s3lvol_fini(void)
{
	if (!g_s3lvol_initialized) {
		return;
	}

	s3_peer_server_stop();
	s3_crt_global_fini();
	s3_spawner_stop();
	g_s3lvol_initialized = false;
}

static int
vbdev_s3lvol_get_ctx_size(void)
{
	/* Keep in sync with struct s3lvol_bdev_io in vbdev_s3lvol_lvol.c: it
	 * holds a single spdk_blob_ext_io_opts. */
	return (int)sizeof(struct spdk_blob_ext_io_opts);
}

static struct spdk_bdev_module s3lvol_if = {
	.name            = "s3lvol",
	.module_init     = vbdev_s3lvol_init,
	.module_fini     = vbdev_s3lvol_fini,
	.get_ctx_size    = vbdev_s3lvol_get_ctx_size,
};

SPDK_BDEV_MODULE_REGISTER(s3lvol, &s3lvol_if)

struct spdk_bdev_module *
vbdev_s3lvol_get_module(void)
{
	return &s3lvol_if;
}

/* The log component. SPDK_INFOLOG(vbdev_s3lvol, ...) in vbdev_s3lvol_lvol.c
 * uses it; the registration lives here because this file is the module's
 * "primary" translation unit. */
SPDK_LOG_REGISTER_COMPONENT(vbdev_s3lvol)
