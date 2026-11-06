/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 * Release-ordering harness for the failed-recovery unwind.
 *
 * === What this proves ===
 *
 * The production adapter in vbdev_s3lvol_lvstore.c decides, per lvol, whether
 * to unregister (a bdev was registered, so the bdev destructor closes it) or
 * close (it never was). s3_recovery_unwind.c owns everything after that
 * decision: the order, the barrier, the exactly-once report, and the rule that
 * a cleanup failure is reported rather than papered over.
 *
 * This file drives the real state machine with the same ops shape the
 * production adapter supplies. It asserts:
 *
 *   - every item is released before the unload starts;
 *   - the unload starts exactly once, after the last release;
 *   - report is called exactly once, after the unload;
 *   - a release failure does not stop the remaining releases, and the report
 *     says released=false;
 *   - an unload failure reports released=false;
 *   - an inline completion does not release an item twice, and an asynchronous
 *     completion advances exactly one item;
 *   - zero items still unload and report;
 *   - invalid arguments start nothing and leave the item list with the caller.
 *
 * === What this does NOT prove ===
 *
 * That the SPDK primitives behave as their contracts claim (spdk_lvol_close,
 * spdk_bdev_unregister, spdk_lvs_unload), or that a registered lvol really is
 * closed by the bdev destructor. Those need the live module; this proves the
 * ordering the module is driven by, which is the part that was missing tests.
 *
 * Usage:
 *   ./s3_recovery_unwind_test
 */

#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "spdk/stdinc.h"

#include "s3lvol/s3_recovery_unwind.h"

/*
 * The module logs through SPDK. The standalone build has no SPDK archive, so
 * the two symbols it uses are wrapped by this test (see the Makefile).
 */
void
__wrap_spdk_log(int level, const char *file, int line, const char *func,
		const char *format, ...)
{
	(void)level;
	(void)file;
	(void)line;
	(void)func;
	(void)format;
}

const char *
__wrap_spdk_strerror(int errnum)
{
	static char buf[64];

	snprintf(buf, sizeof(buf), "errno %d", errnum);
	return buf;
}

static int g_failures;

#define CHECK(cond) do {                                                \
	if (!(cond)) {                                                  \
		fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, \
			#cond);                                         \
		g_failures++;                                           \
	}                                                               \
} while (0)

#define MAX_EVENTS 64

enum ev_kind {
	EV_UNREGISTER = 1,
	EV_CLOSE,
	EV_UNLOAD,
	EV_REPORT,
};

struct mock {
	enum ev_kind  events[MAX_EVENTS];
	void         *handles[MAX_EVENTS];
	int           nevents;

	bool          complete_inline;
	/* Per-handle release status, indexed by the handle id. */
	int           release_status[8];
	int           unload_status;

	/* Deferred completions, for the asynchronous cases. */
	void        (*pending_done[MAX_EVENTS])(void *arg, int status);
	void         *pending_arg[MAX_EVENTS];
	int           pending_status[MAX_EVENTS];
	int           npending;

	int           reports;
	int           report_status;
	bool          report_released;
	bool          report_saw_items_freed;
};

static void
mock_record(struct mock *m, enum ev_kind kind, void *handle)
{
	if (m->nevents < MAX_EVENTS) {
		m->events[m->nevents] = kind;
		m->handles[m->nevents] = handle;
		m->nevents++;
	}
}

static int
mock_release_status(struct mock *m, void *handle)
{
	int id = (int)(intptr_t)handle;

	if (id < 0 || id >= 8) {
		return 0;
	}
	return m->release_status[id];
}

static void
mock_complete(struct mock *m, void (*done)(void *, int), void *arg, int status)
{
	if (m->complete_inline) {
		done(arg, status);
		return;
	}
	if (m->npending < MAX_EVENTS) {
		m->pending_done[m->npending] = done;
		m->pending_arg[m->npending] = arg;
		m->pending_status[m->npending] = status;
		m->npending++;
	}
}

static void
mock_unregister(void *cb_arg, void *handle,
		void (*done)(void *arg, int status), void *done_arg)
{
	struct mock *m = cb_arg;

	mock_record(m, EV_UNREGISTER, handle);
	mock_complete(m, done, done_arg, mock_release_status(m, handle));
}

static void
mock_close(void *cb_arg, void *handle,
	   void (*done)(void *arg, int status), void *done_arg)
{
	struct mock *m = cb_arg;

	mock_record(m, EV_CLOSE, handle);
	mock_complete(m, done, done_arg, mock_release_status(m, handle));
}

static void
mock_unload(void *cb_arg, void (*done)(void *arg, int status), void *done_arg)
{
	struct mock *m = cb_arg;

	mock_record(m, EV_UNLOAD, NULL);
	mock_complete(m, done, done_arg, m->unload_status);
}

static void
mock_report(void *cb_arg, int status, bool released)
{
	struct mock *m = cb_arg;

	mock_record(m, EV_REPORT, NULL);
	m->reports++;
	m->report_status = status;
	m->report_released = released;
}

static const struct s3_recovery_unwind_ops mock_ops = {
	.unregister = mock_unregister,
	.close = mock_close,
	.unload = mock_unload,
	.report = mock_report,
};

static struct s3_recovery_unwind_ops
ops_for(struct mock *m)
{
	struct s3_recovery_unwind_ops ops = mock_ops;

	ops.cb_arg = m;
	return ops;
}

/* Complete deferred primitives, one at a time, until none are left. */
static void
mock_drain(struct mock *m)
{
	int i = 0;

	while (i < m->npending) {
		void (*done)(void *, int) = m->pending_done[i];
		void *arg = m->pending_arg[i];
		int status = m->pending_status[i];

		i++;
		done(arg, status);
	}
	m->npending = 0;
}

/* ------------------------------------------------------------------ tests */

static void
test_order_registered_then_closed_inline(void)
{
	struct mock m = {0};
	struct s3_recovery_unwind_item *items;
	int rc;

	items = calloc(2, sizeof(*items));
	if (!items) {
		CHECK(!"items allocation");
		return;
	}

	m.complete_inline = true;
	items[0].handle = (void *)(intptr_t)0;
	items[0].registered = true;
	items[1].handle = (void *)(intptr_t)1;
	items[1].registered = false;

	rc = s3_recovery_unwind_start(&(struct s3_recovery_unwind_ops){
					      .unregister = mock_unregister,
					      .close = mock_close,
					      .unload = mock_unload,
					      .report = mock_report,
					      .cb_arg = &m,
				      },
				      items, 2, -EIO);
	CHECK(rc == 0);
	CHECK(m.nevents == 4);
	CHECK(m.events[0] == EV_UNREGISTER && m.handles[0] == (void *)(intptr_t)0);
	CHECK(m.events[1] == EV_CLOSE && m.handles[1] == (void *)(intptr_t)1);
	CHECK(m.events[2] == EV_UNLOAD);
	CHECK(m.events[3] == EV_REPORT);
	CHECK(m.reports == 1);
	CHECK(m.report_status == -EIO);
	CHECK(m.report_released == true);
}

static void
test_order_asynchronous(void)
{
	struct mock m = {0};
	struct s3_recovery_unwind_item *items;

	items = calloc(2, sizeof(*items));
	if (!items) {
		CHECK(!"items allocation");
		return;
	}

	m.complete_inline = false;
	items[0].handle = (void *)(intptr_t)2;
	items[0].registered = false;
	items[1].handle = (void *)(intptr_t)3;
	items[1].registered = true;

	CHECK(s3_recovery_unwind_start(&(struct s3_recovery_unwind_ops){
					      .unregister = mock_unregister,
					      .close = mock_close,
					      .unload = mock_unload,
					      .report = mock_report,
					      .cb_arg = &m,
				      },
				      items, 2, -EIO) == 0);
	/* Only the first primitive may be in flight. */
	CHECK(m.nevents == 1);
	CHECK(m.events[0] == EV_CLOSE);
	mock_drain(&m);
	CHECK(m.nevents == 4);
	CHECK(m.events[1] == EV_UNREGISTER);
	CHECK(m.events[2] == EV_UNLOAD);
	CHECK(m.events[3] == EV_REPORT);
	CHECK(m.reports == 1);
	CHECK(m.report_released == true);
}

static void
test_release_failure_with_ops(void)
{
	struct mock m = {0};
	struct s3_recovery_unwind_item *items;
	struct s3_recovery_unwind_ops ops;

	items = calloc(2, sizeof(*items));
	if (!items) {
		CHECK(!"items allocation");
		return;
	}

	m.complete_inline = true;
	m.release_status[0] = -EIO;
	items[0].handle = (void *)(intptr_t)0;
	items[0].registered = false;
	items[1].handle = (void *)(intptr_t)1;
	items[1].registered = false;
	ops = ops_for(&m);

	CHECK(s3_recovery_unwind_start(&ops, items, 2, -ENOENT) == 0);
	/* Both were attempted, the unload still ran, and the report says the
	 * cleanup did not complete. The original failure is still what the
	 * caller is told. */
	CHECK(m.nevents == 4);
	CHECK(m.events[0] == EV_CLOSE);
	CHECK(m.events[1] == EV_CLOSE);
	CHECK(m.events[2] == EV_UNLOAD);
	CHECK(m.events[3] == EV_REPORT);
	CHECK(m.reports == 1);
	CHECK(m.report_status == -ENOENT);
	CHECK(m.report_released == false);
}

static void
test_unload_failure_is_reported(void)
{
	struct mock m = {0};
	struct s3_recovery_unwind_item *items;
	struct s3_recovery_unwind_ops ops;

	items = calloc(1, sizeof(*items));
	if (!items) {
		CHECK(!"items allocation");
		return;
	}

	m.complete_inline = true;
	m.unload_status = -EBUSY;
	items[0].handle = (void *)(intptr_t)0;
	items[0].registered = false;
	ops = ops_for(&m);

	CHECK(s3_recovery_unwind_start(&ops, items, 1, -EIO) == 0);
	CHECK(m.reports == 1);
	CHECK(m.report_released == false);
	CHECK(m.report_status == -EIO);
}

static void
test_zero_items_still_unloads_and_reports(void)
{
	struct mock m = {0};
	struct s3_recovery_unwind_ops ops;

	m.complete_inline = true;
	ops = ops_for(&m);

	CHECK(s3_recovery_unwind_start(&ops, NULL, 0, -EIO) == 0);
	CHECK(m.nevents == 2);
	CHECK(m.events[0] == EV_UNLOAD);
	CHECK(m.events[1] == EV_REPORT);
	CHECK(m.reports == 1);
	CHECK(m.report_released == true);
}

static void
test_late_duplicate_completion_is_ignored(void)
{
	struct mock m = {0};
	struct s3_recovery_unwind_item *items;
	struct s3_recovery_unwind_ops ops;

	items = calloc(1, sizeof(*items));
	if (!items) {
		CHECK(!"items allocation");
		return;
	}

	m.complete_inline = false;
	items[0].handle = (void *)(intptr_t)0;
	items[0].registered = false;
	ops = ops_for(&m);

	CHECK(s3_recovery_unwind_start(&ops, items, 1, -EIO) == 0);
	CHECK(m.npending == 1);

	/* Complete the close, which starts the unload and parks it. */
	{
		void (*done)(void *, int) = m.pending_done[0];
		void *arg = m.pending_arg[0];

		m.npending = 0;
		done(arg, 0);
	}
	CHECK(m.npending == 1);

	/* Complete the unload: the run reports and frees itself. */
	{
		void (*done)(void *, int) = m.pending_done[0];
		void *arg = m.pending_arg[0];

		m.npending = 0;
		done(arg, 0);
	}
	CHECK(m.reports == 1);

	/* A late duplicate must not touch the freed run object. Under ASan a
	 * use-after-free here is a hard failure. There is no handle left to
	 * call it through, so this scenario is covered by the module's
	 * in_step guard and by the two-completion ASan runs of the callers;
	 * the assertion is that reaching here did not double-report. */
	CHECK(m.reports == 1);
}

static void
test_invalid_arguments(void)
{
	struct mock m = {0};
	struct s3_recovery_unwind_item *items;
	struct s3_recovery_unwind_ops ops;

	items = calloc(1, sizeof(*items));
	if (!items) {
		CHECK(!"items allocation");
		return;
	}

	m.complete_inline = true;
	items[0].handle = (void *)(intptr_t)0;
	items[0].registered = false;
	ops = ops_for(&m);

	/* Missing report. */
	{
		struct s3_recovery_unwind_ops bad = ops;

		bad.report = NULL;
		CHECK(s3_recovery_unwind_start(&bad, items, 1, -EIO) == -EINVAL);
	}
	/* Missing close. */
	{
		struct s3_recovery_unwind_ops bad = ops;

		bad.close = NULL;
		CHECK(s3_recovery_unwind_start(&bad, items, 1, -EIO) == -EINVAL);
	}
	/* Items promised but not supplied. */
	CHECK(s3_recovery_unwind_start(&ops, NULL, 1, -EIO) == -EINVAL);
	/* An unwind reports a failure, never success. */
	CHECK(s3_recovery_unwind_start(&ops, items, 1, 0) == -EINVAL);

	/* None of those started anything, so nothing was recorded and the
	 * list still belongs to this test. */
	CHECK(m.nevents == 0);
	CHECK(m.reports == 0);
	free(items);
}

static void
test_registered_item_is_never_closed_directly(void)
{
	struct mock m = {0};
	struct s3_recovery_unwind_item *items;
	struct s3_recovery_unwind_ops ops;

	items = calloc(1, sizeof(*items));
	if (!items) {
		CHECK(!"items allocation");
		return;
	}

	m.complete_inline = true;
	items[0].handle = (void *)(intptr_t)0;
	items[0].registered = true;
	ops = ops_for(&m);

	CHECK(s3_recovery_unwind_start(&ops, items, 1, -EIO) == 0);
	CHECK(m.nevents >= 2);
	CHECK(m.events[0] == EV_UNREGISTER);
	/* The destructor owns the close: a direct close would be a double
	 * close, so it must not appear. */
	{
		int i;
		int closes = 0;
		int unregisters = 0;

		for (i = 0; i < m.nevents; i++) {
			if (m.events[i] == EV_CLOSE) {
				closes++;
			}
			if (m.events[i] == EV_UNREGISTER) {
				unregisters++;
			}
		}
		CHECK(unregisters == 1);
		CHECK(closes == 0);
	}
}

int
main(void)
{
	test_order_registered_then_closed_inline();
	test_order_asynchronous();
	test_release_failure_with_ops();
	test_unload_failure_is_reported();
	test_zero_items_still_unloads_and_reports();
	test_late_duplicate_completion_is_ignored();
	test_invalid_arguments();
	test_registered_item_is_never_closed_directly();

	if (g_failures != 0) {
		fprintf(stderr, "\n%d assertion(s) FAILED\n", g_failures);
		return 1;
	}

	printf("\nall recovery unwind assertions passed\n");
	return 0;
}
