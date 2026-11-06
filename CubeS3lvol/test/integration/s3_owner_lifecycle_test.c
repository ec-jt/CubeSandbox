/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   Unit test for module/bdev/s3lvol/owner_lifecycle_ctl.h: when the adapter
 *   may release the S3 owner marker, and what an unload reports.
 *
 *   UNIT scope only. It proves the decision table the adapter calls, not that
 *   the adapter calls it on every path; the runtime fixture covers that.
 *
 *   cc -I module/bdev/s3lvol -fsanitize=address,undefined \
 *      test/integration/s3_owner_lifecycle_test.c -o /tmp/t && /tmp/t
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "owner_lifecycle_ctl.h"

static int g_checks;
static int g_failed;

#define CHECK(cond) do { \
	g_checks++; \
	if (!(cond)) { \
		g_failed++; \
		fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
	} \
} while (0)

static enum lvs_owner_action
decide(bool held, bool live, int teardown, int quiesce)
{
	struct lvs_owner_teardown t = {
		.owner_held = held, .resources_live = live,
		.teardown_status = teardown, .quiesce_status = quiesce,
	};
	return lvs_owner_decide(&t);
}

/* Clean unload: everything stopped and stopped cleanly -> release. */
static void
test_clean_release(void)
{
	CHECK(decide(true, false, 0, 0) == LVS_OWNER_RELEASE);
}

/* Nothing held is never "released" (no write is issued). */
static void
test_not_held(void)
{
	CHECK(decide(false, false, 0, 0) == LVS_OWNER_NOT_HELD);
	CHECK(decide(false, true, -EIO, -ETIMEDOUT) == LVS_OWNER_NOT_HELD);
}

/* Failed cleanup: SPDK still references the lvstore -> retain. */
static void
test_live_resources_retain(void)
{
	CHECK(decide(true, true, 0, 0) == LVS_OWNER_RETAIN);
}

/* Failed drain: the flusher timed out / WAL close failed -> retain, because
 * acknowledged writes may exist only in the local WAL. */
static void
test_teardown_failure_retains(void)
{
	CHECK(decide(true, false, -ETIMEDOUT, 0) == LVS_OWNER_RETAIN);
	CHECK(decide(true, false, -EIO, 0) == LVS_OWNER_RETAIN);
}

/* Registry writer not drained -> retain. */
static void
test_quiesce_failure_retains(void)
{
	CHECK(decide(true, false, 0, -ETIMEDOUT) == LVS_OWNER_RETAIN);
	CHECK(decide(true, false, 0, -ENOMEM) == LVS_OWNER_RETAIN);
}

/* Exhaustive: release happens iff held && !live && teardown==0 && quiesce==0. */
static void
test_exhaustive(void)
{
	static const int st[] = { 0, -EIO, -ETIMEDOUT };
	int a, b, c, d;

	for (a = 0; a < 2; a++) {
		for (b = 0; b < 2; b++) {
			for (c = 0; c < 3; c++) {
				for (d = 0; d < 3; d++) {
					enum lvs_owner_action r =
						decide(a, b, st[c], st[d]);
					bool want = a && !b && st[c] == 0 && st[d] == 0;

					CHECK((r == LVS_OWNER_RELEASE) == want);
					CHECK(a || r == LVS_OWNER_NOT_HELD);
				}
			}
		}
	}
}

/* An unload never reports success when the release failed. */
static void
test_final_status(void)
{
	CHECK(lvs_unload_final_status(0, 0, 0, 0) == 0);
	CHECK(lvs_unload_final_status(0, 0, 0, -EBUSY) == -EBUSY);
	CHECK(lvs_unload_final_status(0, 0, 0, -EIO) == -EIO);
	CHECK(lvs_unload_final_status(0, -ETIMEDOUT, 0, 0) == -ETIMEDOUT);
	CHECK(lvs_unload_final_status(0, 0, -ETIMEDOUT, 0) == -ETIMEDOUT);
	/* earliest failure wins */
	CHECK(lvs_unload_final_status(-EBUSY, -EIO, -ETIMEDOUT, -ENOENT) == -EBUSY);
	CHECK(lvs_unload_final_status(0, -EIO, -ETIMEDOUT, -ENOENT) == -EIO);
	CHECK(lvs_unload_final_status(0, 0, -ETIMEDOUT, -ENOENT) == -ETIMEDOUT);
}

/* Destroy never deletes the owner key as an object, in either mode: the claim
 * is released last through the owner protocol. */
static void
test_destroy_owner_key(void)
{
	CHECK(lvs_destroy_deletes_owner_key(false) == false);
	CHECK(lvs_destroy_deletes_owner_key(true) == false);
}

/* No delete unless the unload was clean AND handed the claim over. */
static void
test_destroy_may_delete(void)
{
	static const int st[] = { 0, -EIO, -ETIMEDOUT, -EBUSY, -EPERM };
	size_t i;
	int t;

	for (i = 0; i < sizeof(st) / sizeof(st[0]); i++) {
		for (t = 0; t < 2; t++) {
			CHECK(lvs_destroy_may_delete(st[i], t) ==
			      (st[i] == 0 && t));
		}
	}
}

/* Release only after every delete, every key recorded and a clean audit. */
static void
test_destroy_may_release(void)
{
	static const int st[] = { 0, -EIO, -EEXIST, -ETIMEDOUT };
	int c, d, l, a;

	for (c = 0; c < 2; c++) {
		for (d = 0; d < 4; d++) {
			for (l = 0; l < 2; l++) {
				for (a = 0; a < 4; a++) {
					bool want = c && st[d] == 0 && !l &&
						    st[a] == 0;

					CHECK(lvs_destroy_may_release(c, st[d], l,
								      st[a]) ==
					      want);
				}
			}
		}
	}
}

/* Destroy reports 0 only when every step succeeded; first failure wins. */
static void
test_destroy_final_status(void)
{
	CHECK(lvs_destroy_final_status(0, true, 0, false, 0, 0) == 0);
	/* unload failure: nothing else happened */
	CHECK(lvs_destroy_final_status(-EIO, false, 0, false, 0, 0) == -EIO);
	CHECK(lvs_destroy_final_status(-EBUSY, true, -EIO, true, -EEXIST,
				       -EIO) == -EBUSY);
	/* clean unload but no claim: not destroyed */
	CHECK(lvs_destroy_final_status(0, false, 0, false, 0, 0) == -EPERM);
	CHECK(lvs_destroy_final_status(0, true, -EIO, false, 0, 0) == -EIO);
	CHECK(lvs_destroy_final_status(0, true, 0, true, 0, 0) == -ENOMEM);
	CHECK(lvs_destroy_final_status(0, true, 0, false, -EEXIST, 0) ==
	      -EEXIST);
	/* a failed release is never hidden behind successful deletes */
	CHECK(lvs_destroy_final_status(0, true, 0, false, 0, -EIO) == -EIO);
	CHECK(lvs_destroy_final_status(0, true, 0, false, 0, -EBUSY) == -EBUSY);
	/* consistency: success implies the release was permitted */
	CHECK(lvs_destroy_may_release(true, 0, false, 0));
}

/* Strict guidance never mentions force; legacy guidance does. */
static void
test_retained_guidance(void)
{
	int s, d;

	for (s = 0; s < 2; s++) {
		for (d = 0; d < 2; d++) {
			const char *g = lvs_retained_guidance(s, d);
			bool has_force = strstr(g, "force=true") != NULL ||
					 strstr(g, "--force") != NULL;

			CHECK(g != NULL && g[0] != '\0');
			CHECK(has_force == !s);
			if (s) {
				CHECK(strstr(g, "no force") != NULL);
				CHECK(strstr(g, "takeover") != NULL);
			}
		}
	}
}

int
main(void)
{
	test_clean_release();
	test_not_held();
	test_live_resources_retain();
	test_teardown_failure_retains();
	test_quiesce_failure_retains();
	test_exhaustive();
	test_final_status();
	test_destroy_owner_key();
	test_destroy_may_delete();
	test_destroy_may_release();
	test_destroy_final_status();
	test_retained_guidance();

	printf("s3_owner_lifecycle_test: %d checks, %d failed\n",
	       g_checks, g_failed);
	return g_failed ? 1 : 0;
}
