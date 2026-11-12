/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_deadline -- per-request total time limit, see include/s3lvol/s3_deadline.h
 *
 *   One watchdog thread for the whole process. Entries sit on one list under one
 *   mutex; the thread sleeps until the earliest deadline (at most a second) and
 *   cancels whatever has passed it. The volume is bounded by the number of S3
 *   requests in flight (a few hundred at most), so a scan per wakeup is cheaper
 *   than keeping a heap consistent.
 *
 *   Deliberately free of SPDK and CRT: it links into the offline unit test with
 *   nothing but pthreads.
 */

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <time.h>

#include "s3lvol/s3_deadline.h"

/* Upper bound on one watchdog sleep, so a deadline added while it sleeps is
 * noticed within this even if the wakeup signal were lost. */
#define S3_DEADLINE_MAX_SLEEP_MS 1000

/* Cancels issued per scan, collected under the lock and issued outside it. A
 * larger backlog is picked up by the next scan, which follows immediately. */
#define S3_DEADLINE_BATCH 64

struct s3_deadline {
	uint64_t                      deadline_ms;
	void                         *target;
	const struct s3_deadline_ops *ops;

	/* Holds: the submitter (until arm/abandon) and the completion (until
	 * finish/abandon). Freed at zero. */
	unsigned                      holds;

	bool                          armed;
	bool                          finished;
	bool                          fired;
	bool                          listed;

	TAILQ_ENTRY(s3_deadline)      link;
};

static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_cond;
static bool g_cond_ready;
static TAILQ_HEAD(, s3_deadline) g_list = TAILQ_HEAD_INITIALIZER(g_list);
static uint64_t g_pending;
static uint64_t g_fired_total;
static pthread_t g_thread;
static bool g_running;
static bool g_stop;

static uint64_t
deadline_now_ms(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/* Under g_lock. */
static void
deadline_unlist(struct s3_deadline *d)
{
	if (d->listed) {
		TAILQ_REMOVE(&g_list, d, link);
		d->listed = false;
		g_pending--;
	}
}

/* Under g_lock. Returns true when the caller must free \p d after unlocking. */
static bool
deadline_drop_hold(struct s3_deadline *d)
{
	if (--d->holds > 0) {
		return false;
	}
	deadline_unlist(d);
	return true;
}

static void *
deadline_thread(void *arg)
{
	(void)arg;

	pthread_mutex_lock(&g_lock);
	while (!g_stop) {
		struct {
			void *target;
			const struct s3_deadline_ops *ops;
		} fire[S3_DEADLINE_BATCH];
		unsigned n = 0, i;
		uint64_t now = deadline_now_ms();
		uint64_t next = now + S3_DEADLINE_MAX_SLEEP_MS;
		struct s3_deadline *d;

		TAILQ_FOREACH(d, &g_list, link) {
			if (d->fired || d->finished) {
				continue;
			}
			if (now < d->deadline_ms) {
				if (d->deadline_ms < next) {
					next = d->deadline_ms;
				}
				continue;
			}
			if (!d->armed) {
				/* Not handed a target yet: arm() issues the cancel
				 * itself once it sees the deadline has passed. */
				continue;
			}
			if (n == S3_DEADLINE_BATCH) {
				next = now;
				break;
			}
			d->fired = true;
			g_fired_total++;
			/* Our own reference for the cancel: finish() may drop the
			 * armed one the moment the lock is released. */
			d->ops->acquire(d->target);
			fire[n].target = d->target;
			fire[n].ops = d->ops;
			n++;
		}

		if (n > 0) {
			pthread_mutex_unlock(&g_lock);
			for (i = 0; i < n; i++) {
				fire[i].ops->cancel(fire[i].target);
				fire[i].ops->release(fire[i].target);
			}
			pthread_mutex_lock(&g_lock);
			continue;
		}

		{
			struct timespec ts;
			uint64_t wait_ms = next > now ? next - now : 0;

			clock_gettime(CLOCK_MONOTONIC, &ts);
			ts.tv_sec += (time_t)(wait_ms / 1000);
			ts.tv_nsec += (long)((wait_ms % 1000) * 1000000);
			if (ts.tv_nsec >= 1000000000L) {
				ts.tv_sec++;
				ts.tv_nsec -= 1000000000L;
			}
			pthread_cond_timedwait(&g_cond, &g_lock, &ts);
		}
	}
	pthread_mutex_unlock(&g_lock);
	return NULL;
}

/* Under g_lock. */
static void
deadline_start_locked(void)
{
	if (!g_cond_ready) {
		pthread_condattr_t attr;

		pthread_condattr_init(&attr);
		pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
		pthread_cond_init(&g_cond, &attr);
		pthread_condattr_destroy(&attr);
		g_cond_ready = true;
	}
	if (g_running) {
		return;
	}
	g_stop = false;
	if (pthread_create(&g_thread, NULL, deadline_thread, NULL) == 0) {
		g_running = true;
	}
}

struct s3_deadline *
s3_deadline_new(uint64_t timeout_ms)
{
	struct s3_deadline *d;

	if (timeout_ms == 0) {
		return NULL;
	}
	d = calloc(1, sizeof(*d));
	if (!d) {
		return NULL;
	}
	d->deadline_ms = deadline_now_ms() + timeout_ms;
	d->holds = 2;

	pthread_mutex_lock(&g_lock);
	deadline_start_locked();
	if (!g_running) {
		/* No watchdog, no deadline: degrade to the pre-B16 behaviour. */
		pthread_mutex_unlock(&g_lock);
		free(d);
		return NULL;
	}
	TAILQ_INSERT_TAIL(&g_list, d, link);
	d->listed = true;
	g_pending++;
	pthread_cond_signal(&g_cond);
	pthread_mutex_unlock(&g_lock);
	return d;
}

void
s3_deadline_arm(struct s3_deadline *d, void *target,
		const struct s3_deadline_ops *ops)
{
	bool cancel_now = false;
	bool free_it;

	if (!d) {
		return;
	}

	pthread_mutex_lock(&g_lock);
	if (!d->finished && target && ops) {
		/* finish() has not run, so the submitter's reference to the target
		 * is still alive: taking another one is safe. */
		ops->acquire(target);
		d->target = target;
		d->ops = ops;
		d->armed = true;
		if (!d->fired && deadline_now_ms() >= d->deadline_ms) {
			d->fired = true;
			g_fired_total++;
			ops->acquire(target);
			cancel_now = true;
		}
	}
	free_it = deadline_drop_hold(d);
	pthread_mutex_unlock(&g_lock);

	if (cancel_now) {
		ops->cancel(target);
		ops->release(target);
	}
	if (free_it) {
		free(d);
	}
}

void
s3_deadline_abandon(struct s3_deadline *d)
{
	if (!d) {
		return;
	}
	pthread_mutex_lock(&g_lock);
	deadline_unlist(d);
	pthread_mutex_unlock(&g_lock);
	free(d);
}

bool
s3_deadline_finish(struct s3_deadline *d)
{
	void *target = NULL;
	const struct s3_deadline_ops *ops = NULL;
	bool fired;
	bool free_it;

	if (!d) {
		return false;
	}

	pthread_mutex_lock(&g_lock);
	d->finished = true;
	fired = d->fired;
	if (d->armed) {
		target = d->target;
		ops = d->ops;
		d->armed = false;
		d->target = NULL;
	}
	deadline_unlist(d);
	free_it = deadline_drop_hold(d);
	pthread_mutex_unlock(&g_lock);

	if (target) {
		ops->release(target);
	}
	if (free_it) {
		free(d);
	}
	return fired;
}

uint64_t
s3_deadline_pending(void)
{
	uint64_t n;

	pthread_mutex_lock(&g_lock);
	n = g_pending;
	pthread_mutex_unlock(&g_lock);
	return n;
}

uint64_t
s3_deadline_fired_total(void)
{
	uint64_t n;

	pthread_mutex_lock(&g_lock);
	n = g_fired_total;
	pthread_mutex_unlock(&g_lock);
	return n;
}

void
s3_deadline_shutdown(void)
{
	pthread_t t;

	pthread_mutex_lock(&g_lock);
	if (!g_running) {
		pthread_mutex_unlock(&g_lock);
		return;
	}
	g_stop = true;
	pthread_cond_signal(&g_cond);
	t = g_thread;
	g_running = false;
	pthread_mutex_unlock(&g_lock);
	pthread_join(t, NULL);
}
