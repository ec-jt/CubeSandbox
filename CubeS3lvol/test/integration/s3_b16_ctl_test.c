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
	const uint64_t W = EXPORT_DRAIN_NO_PROGRESS_MS, CAP = EXPORT_DRAIN_PROGRESS_CAP_MS;
	uint64_t t = 1000, flushed = 10;
	bool moved;
	int again = 0;

	printf("\n[2] progress-based export drain\n");
	check("no-progress window 60 s", W == 60000);
	check("overall cap 15 min", CAP == 15 * 60 * 1000);

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

	/* One timeout without progress is still inside the window. */
	t += 30000;
	check("no progress for 30 s: drain again",
	      export_drain_ctl_next(&p, t, flushed, W, CAP, &moved) == EXPORT_DRAIN_AGAIN &&
	      !moved);
	t += 30000;
	check("no progress for 60 s: fail",
	      export_drain_ctl_next(&p, t, flushed, W, CAP, &moved) == EXPORT_DRAIN_FAIL);

	/* Cap reached even though progress continues. */
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
		check("progressing drain stops at the cap", v == EXPORT_DRAIN_FAIL);
		check("cap hit at 15 min", t == CAP);
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

int
main(void)
{
	printf("=== B16 export / deadline unit test ===\n");
	test_deadline();
	test_drain();
	test_reservation();
	test_abandon();
	printf("\n=== %d passed, %d failed ===\n", g_pass, g_fail);
	return g_fail == 0 ? 0 : 1;
}
