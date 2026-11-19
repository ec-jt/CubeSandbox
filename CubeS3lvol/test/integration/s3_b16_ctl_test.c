/* SPDX-License-Identifier: Apache-2.0 */
/*
 * B16 offline unit test. No S3, no SPDK, no CRT: pthreads only.
 *
 *   [1] s3_deadline: the per-request watchdog. A fake target stands in for the
 *       CRT meta request, so what is tested is the ownership contract the real
 *       client relies on: cancel issued exactly once, never after finish(), the
 *       target never released below the references the watchdog took, and
 *       finish() reports whether the deadline fired (the client maps that to
 *       -ETIMEDOUT). What is NOT tested here: that aws_s3_meta_request_cancel()
 *       makes CRT deliver its finish callback, and the CRT error code it uses.
 *       That needs a live endpoint (s3_client_test).
 *   [2] export_drain_ctl: drain extended while chunks_flushed advances, fails
 *       after a no-progress window, fails at the overall cap.
 *   [3] export_reservation_ctl: a failure before any admission/manifest write
 *       drops the reservation (so a resubmit is admitted); after one, it keeps it.
 *   [4] export_abandon_ctl: abandon refuses on a present manifest, a present
 *       admission document, an in-flight export, a lookup error; succeeds only
 *       on two 404s.
 *
 * The asynchronous glue in vbdev_s3lvol_xfer.c that feeds [2]-[4] needs a live
 * target and is not covered here.
 */
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "s3lvol/s3_deadline.h"
#include "export_b16_ctl.h"
#include "s3lvol/s3_throttle.h"

static int g_pass;
static int g_fail;

static void
check(const char *what, int ok)
{
	printf("\t[%s] %s\n", ok ? "PASS" : "FAIL", what);
	if (ok) {
		g_pass++;
	} else {
		g_fail++;
	}
}

/* ==========================================================================
 * [1] s3_deadline with a fake target
 * ========================================================================== */

struct fake_target {
	atomic_int refs;      /* 1 = the "CRT" reference */
	atomic_int cancels;
	atomic_int underflow;
	atomic_int after_finish;
	atomic_bool finished;
};

static void
ft_acquire(void *t)
{
	atomic_fetch_add(&((struct fake_target *)t)->refs, 1);
}

static void
ft_release(void *t)
{
	struct fake_target *f = t;

	if (atomic_fetch_sub(&f->refs, 1) <= 0) {
		atomic_fetch_add(&f->underflow, 1);
	}
}

static void
ft_cancel(void *t)
{
	struct fake_target *f = t;

	if (atomic_load(&f->finished)) {
		atomic_fetch_add(&f->after_finish, 1);
	}
	atomic_fetch_add(&f->cancels, 1);
}

static const struct s3_deadline_ops g_ops = {
	.acquire = ft_acquire,
	.release = ft_release,
	.cancel  = ft_cancel,
};

static void
ft_init(struct fake_target *f)
{
	memset(f, 0, sizeof(*f));
	atomic_store(&f->refs, 1);
}

static void
sleep_ms(unsigned ms)
{
	struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };

	nanosleep(&ts, NULL);
}

/* Wait until the target was cancelled or \p ms passed. */
static void
wait_cancel(struct fake_target *f, unsigned ms)
{
	for (unsigned i = 0; i < ms && atomic_load(&f->cancels) == 0; i += 5) {
		sleep_ms(5);
	}
}

struct race_arg {
	struct fake_target  target;
	struct s3_deadline *d;
	bool                fired;
};

static void *
race_finish(void *arg)
{
	struct race_arg *r = arg;

	r->fired = s3_deadline_finish(r->d);
	atomic_store(&r->target.finished, true);
	return NULL;
}

static void
test_deadline(void)
{
	struct fake_target f;
	struct s3_deadline *d;
	uint64_t fired0;

	printf("\n[1] per-request deadline (s3_deadline)\n");

	check("0 ms means no deadline", s3_deadline_new(0) == NULL);
	check("finish(NULL) reports not fired", !s3_deadline_finish(NULL));
	s3_deadline_arm(NULL, &f, &g_ops);
	s3_deadline_abandon(NULL);

	/* Stalled request: cancelled exactly once, -ETIMEDOUT reported. */
	ft_init(&f);
	fired0 = s3_deadline_fired_total();
	d = s3_deadline_new(30);
	check("deadline registered", d != NULL && s3_deadline_pending() >= 1);
	s3_deadline_arm(d, &f, &g_ops);
	check("arm took a reference", atomic_load(&f.refs) == 2);
	wait_cancel(&f, 3000);
	sleep_ms(50);
	check("stalled request cancelled", atomic_load(&f.cancels) == 1);
	check("cancelled exactly once (no repeat while not finished)",
	      atomic_load(&f.cancels) == 1);
	check("fired counter advanced", s3_deadline_fired_total() == fired0 + 1);
	check("finish reports fired (maps to -ETIMEDOUT)", s3_deadline_finish(d));
	atomic_store(&f.finished, true);
	check("references balanced after finish", atomic_load(&f.refs) == 1);
	check("no release below zero", atomic_load(&f.underflow) == 0);

	/* Fast request: never cancelled. */
	ft_init(&f);
	d = s3_deadline_new(500);
	s3_deadline_arm(d, &f, &g_ops);
	check("fast request: finish reports not fired", !s3_deadline_finish(d));
	atomic_store(&f.finished, true);
	sleep_ms(600);
	check("fast request never cancelled", atomic_load(&f.cancels) == 0);
	check("fast request refs balanced", atomic_load(&f.refs) == 1);

	/* Completion before arm (CRT finished inline): nothing acquired, no
	 * cancel, both holds released in either order. */
	ft_init(&f);
	d = s3_deadline_new(10);
	check("inline finish before arm: not fired", !s3_deadline_finish(d));
	atomic_store(&f.finished, true);
	sleep_ms(30);
	s3_deadline_arm(d, &f, &g_ops);
	check("arm after finish acquires nothing", atomic_load(&f.refs) == 1);
	check("arm after finish never cancels", atomic_load(&f.cancels) == 0);

	/* Deadline passed before arm: arm issues the cancel itself. */
	ft_init(&f);
	d = s3_deadline_new(1);
	sleep_ms(20);
	s3_deadline_arm(d, &f, &g_ops);
	check("deadline passed before arm: cancelled from arm",
	      atomic_load(&f.cancels) == 1);
	check("and finish reports fired", s3_deadline_finish(d));
	check("refs balanced", atomic_load(&f.refs) == 1);

	/* Submit failed: abandon frees without touching anything. */
	d = s3_deadline_new(10);
	s3_deadline_abandon(d);
	check("abandon leaves nothing pending", s3_deadline_pending() == 0);

	/* Race: finish() from another thread around the deadline, many times.
	 * Never a cancel after finish returned, never an unbalanced release. */
	{
		int after = 0, under = 0, unbalanced = 0, mismatch = 0;

		for (int i = 0; i < 200; i++) {
			struct race_arg r;
			pthread_t t;

			ft_init(&r.target);
			r.d = s3_deadline_new(1 + (i % 3));
			s3_deadline_arm(r.d, &r.target, &g_ops);
			if (i % 2) {
				sleep_ms(1 + (i % 4));
			}
			pthread_create(&t, NULL, race_finish, &r);
			pthread_join(t, NULL);
			/* Let a watchdog scan that may have collected it finish. */
			sleep_ms(3);
			after += atomic_load(&r.target.after_finish);
			under += atomic_load(&r.target.underflow);
			unbalanced += atomic_load(&r.target.refs) != 1;
			/* fired is reported iff a cancel was issued. */
			mismatch += r.fired != (atomic_load(&r.target.cancels) == 1);
			mismatch += atomic_load(&r.target.cancels) > 1;
		}
		check("race: no cancel after finish returned", after == 0);
		check("race: no release below zero", under == 0);
		check("race: references balanced", unbalanced == 0);
		check("race: fired <=> exactly one cancel", mismatch == 0);
	}

	/* Teardown: expire_all cancels long-deadline requests at once. */
	{
		struct fake_target a, b;

		ft_init(&a);
		ft_init(&b);
		struct s3_deadline *da = s3_deadline_new(60000);
		struct s3_deadline *db = s3_deadline_new(60000);

		s3_deadline_arm(da, &a, &g_ops);
		s3_deadline_arm(db, &b, &g_ops);
		sleep_ms(30);
		check("60 s deadlines not yet fired", atomic_load(&a.cancels) == 0 &&
		      atomic_load(&b.cancels) == 0);
		s3_deadline_expire_all();
		wait_cancel(&a, 3000);
		wait_cancel(&b, 3000);
		check("expire_all: both cancelled within 3 s",
		      atomic_load(&a.cancels) == 1 && atomic_load(&b.cancels) == 1);
		check("expire_all: finish reports fired",
		      s3_deadline_finish(da) && s3_deadline_finish(db));
		check("expire_all: refs balanced",
		      atomic_load(&a.refs) == 1 && atomic_load(&b.refs) == 1);
	}

	check("nothing left pending", s3_deadline_pending() == 0);
	s3_deadline_shutdown();
	check("watchdog restarts after shutdown", (d = s3_deadline_new(5)) != NULL);
	s3_deadline_abandon(d);
	s3_deadline_shutdown();
}

/* ==========================================================================
 * [2] progress-based drain
 * ========================================================================== */

static void
test_drain(void)
{
	struct export_drain_progress p = {0};
	const uint64_t W = EXPORT_DRAIN_NO_PROGRESS_MS, CAP = 15ULL * 60 * 1000;
	uint64_t t = 1000, flushed = 10;
	bool moved;
	int again = 0;

	printf("\n[2] progress-based export drain\n");
	check("no-progress window 5 min", W == 5 * 60 * 1000);
	check("no overall cap by default", EXPORT_DRAIN_PROGRESS_CAP_MS == 0);

	export_drain_ctl_start(&p, t, flushed);
	/* Every 30 s drain timeout, the flusher completed some uploads. */
	for (int i = 0; i < 20; i++) {
		t += 30000;
		flushed += 3;
		if (export_drain_ctl_next(&p, t, flushed, W, CAP, &moved) == EXPORT_DRAIN_AGAIN &&
		    moved) {
			again++;
		}
	}
	check("extended on every progressing timeout (20/20)", again == 20);
	check("extensions counted", p.extensions == 20);

	/* Timeouts without progress stay inside the window until it is spent. */
	{
		uint64_t stall_start = t;
		enum export_drain_verdict v = EXPORT_DRAIN_AGAIN;

		while (v == EXPORT_DRAIN_AGAIN && t - stall_start < 2 * W) {
			t += 30000;
			v = export_drain_ctl_next(&p, t, flushed, W, CAP, &moved);
		}
		check("no progress: fail exactly at the window", v == EXPORT_DRAIN_FAIL &&
		      t - stall_start == W);
	}

	/* An explicit cap still stops a progressing drain. */
	memset(&p, 0, sizeof(p));
	t = 0;
	flushed = 0;
	export_drain_ctl_start(&p, t, flushed);
	{
		enum export_drain_verdict v = EXPORT_DRAIN_AGAIN;
		int n = 0;

		while (v == EXPORT_DRAIN_AGAIN && n < 1000) {
			t += 30000;
			flushed++;
			v = export_drain_ctl_next(&p, t, flushed, W, CAP, NULL);
			n++;
		}
		check("progressing drain stops at an explicit cap", v == EXPORT_DRAIN_FAIL);
		check("cap hit at 15 min", t == CAP);
	}

	/* Default (cap 0): a slow but progressing drain is never failed. Models
	 * the degraded bucket: one upload per ~70 s, for six hours. */
	memset(&p, 0, sizeof(p));
	t = 0;
	flushed = 0;
	export_drain_ctl_start(&p, t, flushed);
	{
		enum export_drain_verdict v = EXPORT_DRAIN_AGAIN;

		while (v == EXPORT_DRAIN_AGAIN && t < 6ULL * 3600 * 1000) {
			t += 30000;
			if ((t / 30000) % 3 == 0) {
				flushed++;
			}
			v = export_drain_ctl_next(&p, t, flushed, W, EXPORT_DRAIN_PROGRESS_CAP_MS,
						  NULL);
		}
		check("no default cap: slow progressing drain survives 6 h",
		      v == EXPORT_DRAIN_AGAIN);
	}

	/* Lazily started (first call is a timeout). */
	memset(&p, 0, sizeof(p));
	check("first look starts the clock",
	      export_drain_ctl_next(&p, 5, 7, W, CAP, &moved) == EXPORT_DRAIN_AGAIN && !moved);
}

/* ==========================================================================
 * [3] reservation cleanup
 * ========================================================================== */

static void
test_reservation(void)
{
	struct export_attempts a = {0};

	printf("\n[3] reservation cleanup on failure\n");
	check("drain failure before any write: drop (resubmit admitted)",
	      export_reservation_ctl_drop(&a, -ETIMEDOUT, true));
	check("chain walk / copy failure before manifest: drop",
	      export_reservation_ctl_drop(&a, -EIO, true));
	check("success never drops", !export_reservation_ctl_drop(&a, 0, true));
	check("non-pending entry never dropped", !export_reservation_ctl_drop(&a, -EIO, false));
	a.publish_started = true;
	check("manifest PUT attempted: keep (fail closed)",
	      !export_reservation_ctl_drop(&a, -EIO, true));
	a.publish_started = false;
	a.admission_write = true;
	check("admission write attempted: keep (fail closed)",
	      !export_reservation_ctl_drop(&a, -EIO, true));
}

/* ==========================================================================
 * [4] abandon verdict
 * ========================================================================== */

static struct export_abandon_facts
stuck(void)
{
	struct export_abandon_facts f;

	memset(&f, 0, sizeof(f));
	f.entry_exists = true;
	f.publication_pending = true;
	return f;
}

static void
test_abandon(void)
{
	struct export_abandon_facts f;

	printf("\n[4] rcow_abandon_unpublished_export verdict\n");

	f = stuck();
	f.entry_exists = false;
	check("unknown uuid: -ENOENT", export_abandon_ctl_verdict(&f) == -ENOENT);
	f = stuck();
	f.publication_pending = false;
	check("published entry: -EINVAL (release it instead)",
	      export_abandon_ctl_verdict(&f) == -EINVAL);
	f = stuck();
	f.inflight = true;
	check("export in flight: -EBUSY", export_abandon_ctl_precheck(&f) == -EBUSY);
	f = stuck();
	f.rewriting = true;
	check("rewrite in flight: -EBUSY", export_abandon_ctl_precheck(&f) == -EBUSY);
	f = stuck();
	f.reaping = true;
	check("reaping: -EBUSY", export_abandon_ctl_precheck(&f) == -EBUSY);

	f = stuck();
	check("precheck passes for a stuck entry", export_abandon_ctl_precheck(&f) == 0);
	check("nothing read yet: -EAGAIN", export_abandon_ctl_verdict(&f) == -EAGAIN);

	f.manifest_checked = true;
	f.manifest_head = 0;
	check("manifest exists: -EEXIST", export_abandon_ctl_verdict(&f) == -EEXIST);
	f.manifest_head = -EIO;
	check("manifest lookup error: refused with that error",
	      export_abandon_ctl_verdict(&f) == -EIO);
	f.manifest_head = -ETIMEDOUT;
	check("manifest lookup timeout: refused",
	      export_abandon_ctl_verdict(&f) == -ETIMEDOUT);
	f.manifest_head = -EACCES;
	check("manifest 403: refused", export_abandon_ctl_verdict(&f) == -EACCES);

	f.manifest_head = -ENOENT;
	check("manifest 404, admission unread: -EAGAIN",
	      export_abandon_ctl_verdict(&f) == -EAGAIN);
	f.admission_checked = true;
	f.admission_head = 0;
	check("admission exists: -EEXIST", export_abandon_ctl_verdict(&f) == -EEXIST);
	f.admission_head = -ECONNRESET;
	check("admission lookup error: refused",
	      export_abandon_ctl_verdict(&f) == -ECONNRESET);
	f.admission_head = -ENOENT;
	check("double 404: abandon allowed", export_abandon_ctl_verdict(&f) == 0);
	f.inflight = true;
	check("double 404 but an export started meanwhile: -EBUSY",
	      export_abandon_ctl_verdict(&f) == -EBUSY);
}

/* ==========================================================================
 * [5] reconcile route: which manifest proves a first publication
 * ========================================================================== */

static struct export_reconcile_manifest_facts
mf(uint32_t version, bool ref, bool has_base, int base_check)
{
	struct export_reconcile_manifest_facts f;

	memset(&f, 0, sizeof(f));
	f.version = version;
	f.layout_ref = ref;
	f.layout_dense = !ref;
	f.has_base = has_base;
	f.base_check = base_check;
	return f;
}

static void
test_reconcile_route(void)
{
	struct export_reconcile_manifest_facts f;

	printf("\n[5] rcow_reconcile_export route\n");

	f = mf(4, false, false, 0);
	check("v4 dense: admission route",
	      export_reconcile_ctl_route(&f) == EXPORT_RECONCILE_DENSE_ADMISSION);
	f = mf(4, true, false, 0);
	check("v4 ref: refused", export_reconcile_ctl_route(&f) == -ENOTSUP);

	f = mf(3, true, false, 0);
	check("v3 ref: manifest route", export_reconcile_ctl_route(&f) == EXPORT_RECONCILE_REF);
	f = mf(3, false, false, 0);
	check("v3 dense: refused", export_reconcile_ctl_route(&f) == -ENOTSUP);
	f = mf(3, true, true, 0);
	check("v3 with a base: malformed", export_reconcile_ctl_route(&f) == -EINVAL);

	/* The live case: v5 template delta whose manifest PUT landed but timed out. */
	f = mf(5, true, true, 0);
	check("v5 delta over the local base: manifest route",
	      export_reconcile_ctl_route(&f) == EXPORT_RECONCILE_REF);
	f = mf(5, true, true, -EXDEV);
	check("v5 delta over another base: refused -EXDEV",
	      export_reconcile_ctl_route(&f) == -EXDEV);
	f = mf(5, true, true, -ENOENT);
	check("v5 delta with no local base: refused -ENOENT",
	      export_reconcile_ctl_route(&f) == -ENOENT);
	f = mf(5, true, false, 0);
	check("v5 without a base: malformed", export_reconcile_ctl_route(&f) == -EINVAL);
	f = mf(5, false, true, 0);
	check("v5 dense: refused", export_reconcile_ctl_route(&f) == -ENOTSUP);

	f = mf(3, true, false, 0);
	f.generation = 1;
	check("rewritten history (generation 1): refused",
	      export_reconcile_ctl_route(&f) == -ENOTSUP);
	f = mf(2, true, false, 0);
	check("v2 ref: refused", export_reconcile_ctl_route(&f) == -ENOTSUP);
	f = mf(6, true, true, 0);
	check("v6 packed delta over the local base: manifest route",
	      export_reconcile_ctl_route(&f) == EXPORT_RECONCILE_REF);
	f = mf(6, true, true, -EXDEV);
	check("v6 packed delta over another base: refused -EXDEV",
	      export_reconcile_ctl_route(&f) == -EXDEV);
	f = mf(6, true, false, 0);
	check("v6 without a base: malformed", export_reconcile_ctl_route(&f) == -EINVAL);
	f = mf(7, true, true, 0);
	check("unknown future version: refused", export_reconcile_ctl_route(&f) == -ENOTSUP);
}

/* ==========================================================================
 * [T] WAL backlog throttle curve (S3LVOL_WAL_MAX_DIRTY_GB)
 * ========================================================================== */

static void
test_throttle(void)
{
	const uint64_t G = 1ULL << 30, cap = 8 * G;
	uint64_t prev = S3_THROTTLE_UNLIMITED, b;
	bool monotone = true;

	printf("\n[T] WAL backlog throttle curve\n");
	check("cap 0 means off", s3_throttle_rate(100 * G, 0) == S3_THROTTLE_UNLIMITED);
	check("below half the cap: unthrottled",
	      s3_throttle_rate(cap / 2 - 1, cap) == S3_THROTTLE_UNLIMITED);
	check("at half the cap: the high rate",
	      s3_throttle_rate(cap / 2, cap) == S3_THROTTLE_RATE_HI);
	check("just below the cap: close to the low rate",
	      s3_throttle_rate(cap - 1, cap) >= S3_THROTTLE_RATE_LO &&
	      s3_throttle_rate(cap - 1, cap) < S3_THROTTLE_RATE_LO + (1 << 20));
	check("at the cap: floor, never zero",
	      s3_throttle_rate(cap, cap) == S3_THROTTLE_RATE_FLOOR &&
	      S3_THROTTLE_RATE_FLOOR > 0);
	check("far past the cap: still the floor (writers slowed, not refused)",
	      s3_throttle_rate(40 * G, cap) == S3_THROTTLE_RATE_FLOOR);
	for (b = 0; b <= 2 * cap; b += cap / 64) {
		uint64_t r = s3_throttle_rate(b, cap);

		if (r > prev) {
			monotone = false;
		}
		prev = r;
	}
	check("rate never rises as the backlog grows", monotone);
	{
		const uint64_t W = 32 * G;   /* WAL capacity */

		check("WAL leg: healthy steady state (5 GiB of 32) is unthrottled",
		      s3_throttle_rate2(10ULL << 20, cap, 5 * G, W) == S3_THROTTLE_UNLIMITED);
		check("WAL leg: slope starts before the 85% wall",
		      s3_throttle_rate2(10ULL << 20, cap, W / 100 * 41, W) <= S3_THROTTLE_RATE_HI &&
		      s3_throttle_rate2(10ULL << 20, cap, W / 100 * 41, W) > S3_THROTTLE_RATE_LO);
		check("WAL leg: at 80% the floor, never zero",
		      s3_throttle_rate2(10ULL << 20, cap, W / 100 * 80, W) == S3_THROTTLE_RATE_FLOOR);
		check("the slower leg wins",
		      s3_throttle_rate2(cap, cap, 0, W) == S3_THROTTLE_RATE_FLOOR);
		check("cap 0 turns both legs off",
		      s3_throttle_rate2(100 * G, 0, W, W) == S3_THROTTLE_UNLIMITED);
	}
	/* The bound the throttle exists for: with S3 fully stalled, how far
	 * past the cap can a writer push the backlog in an hour? */
	check("stalled S3: backlog grows <= 7.1 GiB/h past the cap",
	      S3_THROTTLE_RATE_FLOOR * 3600 <= (uint64_t)(7.1 * (double)G));
}

int
main(void)
{
	printf("=== B16 export / deadline unit test ===\n");
	test_deadline();
	test_drain();
	test_throttle();
	test_reservation();
	test_abandon();
	test_reconcile_route();
	printf("\n=== %d passed, %d failed ===\n", g_pass, g_fail);
	return g_fail == 0 ? 0 : 1;
}
