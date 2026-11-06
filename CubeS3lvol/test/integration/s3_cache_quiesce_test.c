/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 *   s3_cache teardown quiescence vs. local-descriptor lifetime
 *   (ASan/UBSan unit test, MOCKED bdev -- no real AIO)
 *
 *   Links the REAL lib/s3bsdev/s3_cache.c. Every SPDK symbol it uses is a fake
 *   defined here; in particular spdk_bdev_read/spdk_bdev_write go to a fake
 *   device whose completions the test parks and delivers on demand, delivers
 *   inline from the submit call, or refuses at submission (-ENOMEM).
 *
 *   The fake device has an explicit "descriptor": fake_desc_close() fails the
 *   test if any I/O is still outstanding on it, and any completion or
 *   submission after close also fails the test. That is the lifetime rule the
 *   bug broke (s3_bs_dev teardown let the owner close the local descriptor
 *   while a cache fill was still in flight).
 *
 *   The "owner" below models s3_bs_dev teardown exactly as the patched code
 *   does it: call s3_cache_quiesce(); on 0 continue inline, on -EINPROGRESS
 *   park; the cache's callback (or the inline continue) closes the descriptor
 *   and destroys the cache. No poller and no allocation exist on that path,
 *   and the test enforces it: after quiesce starts, malloc/calloc are made to
 *   fail (allocator-failure emulation) and every assertion still holds.
 *
 *   What it pins:
 *     Q1  idle cache: quiesce returns 0, callback never stored/called;
 *     Q2  multiple outstanding fills + reads: callback only after the LAST one,
 *         exactly once, with success and failure completions mixed;
 *     Q3  new reads/populates are refused once quiescing (no new device I/O);
 *     Q4  descriptor close + cache destroy happen only after every completion;
 *         the owner frees the cache FROM the quiesce callback (ASan oracle);
 *     Q5  inline completion during submit + user read callback that re-enters
 *         (starts quiesce from inside its callback): callback fires once, from
 *         the outermost frame, after the frame unwinds;
 *     Q6  submission errors (-ENOMEM) during/after quiesce unwind cleanly;
 *     Q7  quiesce while allocator fails: still completes (allocation-free);
 *     Q8  second quiesce -> -EBUSY, first callback still fires exactly once;
 *     Q9  destroy while not idle leaks (refuses) instead of freeing;
 *     Q10 late completion arriving after quiesce callback is impossible:
 *         the fake device would flag it.
 */

#include "spdk/stdinc.h"
#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/thread.h"
#include "spdk/uuid.h"

#include "s3lvol/s3_cache.h"

static int g_pass, g_fail;

/* Hard precondition, evaluated in every build (assert() vanishes under the
 * NDEBUG variant and would drop side effects). */
#define REQUIRE(cond) do { if (!(cond)) { \
	fprintf(stderr, "REQUIRE %s:%d: %s\n", __FILE__, __LINE__, #cond); \
	abort(); } } while (0)

#define CHECK(cond) do { \
	if (cond) { g_pass++; } else { g_fail++; \
		fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); } \
} while (0)

/* --------------------------------------------------------- allocator fail */

/* When set, every malloc/calloc in the process returns NULL (linked with
 * -Wl,--wrap=malloc,--wrap=calloc). The test itself allocates nothing while
 * it is set, so any attempt is the cache's. */
static bool g_fail_alloc;
static uint64_t g_alloc_attempts_while_failing;

void *__real_malloc(size_t n);
void *__real_calloc(size_t a, size_t b);

void *
__wrap_malloc(size_t n)
{
	if (g_fail_alloc) {
		g_alloc_attempts_while_failing++;
		return NULL;
	}
	return __real_malloc(n);
}

void *
__wrap_calloc(size_t a, size_t b)
{
	if (g_fail_alloc) {
		g_alloc_attempts_while_failing++;
		return NULL;
	}
	return __real_calloc(a, b);
}

/* ------------------------------------------------------------ SPDK fakes */

static struct spdk_thread *g_fake_thread = (struct spdk_thread *)0x1;
struct spdk_thread *spdk_get_thread(void) { return g_fake_thread; }

void *
spdk_dma_malloc(size_t size, size_t align, uint64_t *unused)
{
	void *p = NULL;

	(void)unused;
	if (posix_memalign(&p, align < sizeof(void *) ? sizeof(void *) : align,
			   size) != 0) {
		return NULL;
	}
	return p;
}

void spdk_dma_free(void *buf) { free(buf); }

int
spdk_uuid_compare(const struct spdk_uuid *a, const struct spdk_uuid *b)
{
	return memcmp(a, b, sizeof(*a));
}

void spdk_uuid_copy(struct spdk_uuid *d, const struct spdk_uuid *s) { memcpy(d, s, sizeof(*d)); }
void spdk_uuid_set_null(struct spdk_uuid *u) { memset(u, 0, sizeof(*u)); }

static int g_errlogs;

void
spdk_log(enum spdk_log_level level, const char *file, const int line,
	 const char *func, const char *format, ...)
{
	va_list ap;

	(void)file; (void)line;
	if (level == SPDK_LOG_ERROR) {
		g_errlogs++;
	}
	fprintf(stderr, "  [log %s] ", func);
	va_start(ap, format);
	vfprintf(stderr, format, ap);
	va_end(ap);
}

void spdk_log_register_flag(const char *name, struct spdk_log_flag *flag) { (void)name; (void)flag; }

/* ---------------------------------------------------------- fake device */

enum dev_mode { DEV_PARK, DEV_INLINE_OK, DEV_INLINE_FAIL, DEV_ENOMEM };

#define MAXIO 64

struct spdk_bdev_io { int idx; };

static struct {
	bool open;                 /* descriptor state */
	enum dev_mode mode;
	spdk_bdev_io_completion_cb cb[MAXIO];
	void *arg[MAXIO];
	bool is_write[MAXIO];
	struct spdk_bdev_io io[MAXIO];
	bool used[MAXIO];
	uint32_t outstanding;
	uint32_t submitted_reads, submitted_writes;
	uint32_t violations;       /* I/O after close, close with I/O live */
} g_dev;

#define FAKE_DESC ((struct spdk_bdev_desc *)&g_dev)
#define FAKE_CH   ((struct spdk_io_channel *)&g_dev.mode)

static void
fake_desc_open(void)
{
	memset(&g_dev, 0, sizeof(g_dev));
	g_dev.open = true;
}

static void
fake_desc_close(void)
{
	if (g_dev.outstanding != 0) {
		fprintf(stderr, "!! descriptor closed with %u I/O outstanding\n",
			g_dev.outstanding);
		g_dev.violations++;
	}
	g_dev.open = false;
}

static int
fake_submit(bool is_write, spdk_bdev_io_completion_cb cb, void *arg)
{
	int i;

	if (!g_dev.open) {
		fprintf(stderr, "!! I/O submitted on a closed descriptor\n");
		g_dev.violations++;
		return -EBADF;
	}
	if (g_dev.mode == DEV_ENOMEM) {
		return -ENOMEM;
	}
	for (i = 0; i < MAXIO && g_dev.used[i]; i++) {
	}
	REQUIRE(i < MAXIO);
	g_dev.used[i] = true;
	g_dev.io[i].idx = i;
	g_dev.cb[i] = cb;
	g_dev.arg[i] = arg;
	g_dev.is_write[i] = is_write;
	g_dev.outstanding++;
	if (is_write) {
		g_dev.submitted_writes++;
	} else {
		g_dev.submitted_reads++;
	}

	if (g_dev.mode == DEV_INLINE_OK || g_dev.mode == DEV_INLINE_FAIL) {
		/* Completion delivered from inside the submit call. */
		g_dev.used[i] = false;
		g_dev.outstanding--;
		cb(&g_dev.io[i], g_dev.mode == DEV_INLINE_OK, arg);
	}
	return 0;
}

int
spdk_bdev_read(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
	       void *buf, uint64_t offset, uint64_t nbytes,
	       spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	(void)desc; (void)ch; (void)offset;
	memset(buf, 0xAB, nbytes);
	return fake_submit(false, cb, cb_arg);
}

int
spdk_bdev_write(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		void *buf, uint64_t offset, uint64_t nbytes,
		spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	(void)desc; (void)ch; (void)buf; (void)offset; (void)nbytes;
	return fake_submit(true, cb, cb_arg);
}

void spdk_bdev_free_io(struct spdk_bdev_io *io) { (void)io; }

/* Deliver the oldest parked completion of the given kind (-1 any). */
static bool
complete_one(int want_write, bool success)
{
	for (int i = 0; i < MAXIO; i++) {
		if (!g_dev.used[i]) {
			continue;
		}
		if (want_write >= 0 && g_dev.is_write[i] != (bool)want_write) {
			continue;
		}
		if (!g_dev.open) {
			fprintf(stderr, "!! completion delivered after close\n");
			g_dev.violations++;
		}
		spdk_bdev_io_completion_cb cb = g_dev.cb[i];
		void *arg = g_dev.arg[i];

		g_dev.used[i] = false;
		g_dev.outstanding--;
		cb(&g_dev.io[i], success, arg);
		return true;
	}
	return false;
}

/* ----------------------------------------------------------------- owner */

/* Models the patched s3_bs_dev teardown: quiesce, then (inline or from the
 * callback) close the descriptor and free the cache. */
struct owner {
	struct s3_cache *cache;
	int quiesced_calls;
	uint32_t outstanding_at_cb;
	bool torn_down;
};

static void
owner_finish(struct owner *o)
{
	o->outstanding_at_cb = g_dev.outstanding;
	fake_desc_close();           /* flags a violation if I/O is live */
	s3_cache_destroy(o->cache);  /* ASan flags any later access */
	o->cache = NULL;
	o->torn_down = true;
}

static void
owner_quiesced(void *arg)
{
	struct owner *o = arg;

	o->quiesced_calls++;
	owner_finish(o);
}

static int
owner_teardown(struct owner *o)
{
	int rc = s3_cache_quiesce(o->cache, owner_quiesced, o);

	if (rc == 0) {
		owner_finish(o);
	}
	return rc;
}

/* --------------------------------------------------------------- helpers */

#define BS     4096u
#define CHUNK  (64u * 1024)
#define NSLOTS 8u

static struct s3_cache *
mk_cache(void)
{
	struct s3_cache *c = NULL;
	struct s3_cache_opts o = {
		.desc = FAKE_DESC, .ch = FAKE_CH,
		.region_offset = 1024 * 1024, .region_size = NSLOTS * CHUNK,
		.chunk_size = CHUNK, .block_size = BS, .num_chunks = 64,
	};

	REQUIRE(s3_cache_create(&o, &c) == 0);
	return c;
}

static struct spdk_uuid
mk_uuid(uint8_t v)
{
	struct spdk_uuid u;

	memset(&u, v, sizeof(u));
	return u;
}

static uint8_t g_src[CHUNK];

/* Populate a chunk and land the fill successfully so it can be read. */
static void
warm(struct s3_cache *c, uint64_t chunk, uint8_t v)
{
	struct spdk_uuid u = mk_uuid(v);
	enum dev_mode m = g_dev.mode;

	g_dev.mode = DEV_INLINE_OK;
	s3_cache_populate(c, chunk, &u, 0, g_src, CHUNK, CHUNK);
	g_dev.mode = m;
	CHECK(s3_cache_lookup(c, chunk, &u));
}

struct rd {
	int calls;
	int status;
	uint8_t buf[CHUNK];   /* fake bdev needs no DMA alignment */
};

static void
rd_cb(void *arg, int status)
{
	struct rd *r = arg;

	r->calls++;
	r->status = status;
}

/* ================================================================ tests */

static void
test_idle(void)
{
	struct owner o = {0};

	printf("[Q1] idle cache: quiesce answers 0 inline, no callback\n");
	fake_desc_open();
	o.cache = mk_cache();
	CHECK(owner_teardown(&o) == 0);
	CHECK(o.torn_down && o.quiesced_calls == 0);
	CHECK(g_dev.violations == 0);
}

static void
test_multi_outstanding(void)
{
	struct owner o = {0};
	struct rd *r1 = calloc(1, sizeof(*r1)), *r2 = calloc(1, sizeof(*r2));
	struct spdk_uuid u5 = mk_uuid(5), u6 = mk_uuid(6), u7 = mk_uuid(7);
	uint32_t w, rcount;

	printf("[Q2/Q3/Q4/Q7/Q8] multiple outstanding fills + reads, mixed results\n");
	fake_desc_open();
	o.cache = mk_cache();
	warm(o.cache, 1, 1);
	warm(o.cache, 2, 2);

	g_dev.mode = DEV_PARK;
	/* Three parked fills (staging bufs: 4) and two parked reads. */
	s3_cache_populate(o.cache, 5, &u5, 0, g_src, CHUNK, CHUNK);
	s3_cache_populate(o.cache, 6, &u6, 0, g_src, CHUNK, CHUNK);
	s3_cache_populate(o.cache, 7, &u7, 0, g_src, CHUNK, CHUNK);
	{
		struct spdk_uuid u1 = mk_uuid(1), u2 = mk_uuid(2);

		CHECK(s3_cache_read(o.cache, 1, &u1, 0, CHUNK, r1->buf, rd_cb, r1) == 0);
		CHECK(s3_cache_read(o.cache, 2, &u2, 0, CHUNK, r2->buf, rd_cb, r2) == 0);
	}
	CHECK(g_dev.outstanding == 5);
	CHECK(!s3_cache_is_quiesced(o.cache));

	/* Teardown starts. From here the allocator fails: quiesce must not need
	 * memory (Q7). */
	g_fail_alloc = true;
	CHECK(owner_teardown(&o) == -EINPROGRESS);
	CHECK(!o.torn_down && o.quiesced_calls == 0);

	/* Q8: a second quiesce does not replace the pending callback. */
	{
		int dummy = 0;

		CHECK(s3_cache_quiesce(o.cache, owner_quiesced, &dummy) == -EBUSY);
	}

	/* Q3: no new I/O is admitted. */
	w = g_dev.submitted_writes;
	rcount = g_dev.submitted_reads;
	{
		struct spdk_uuid u1 = mk_uuid(1), u9 = mk_uuid(9);
		struct rd r3 = {0};

		s3_cache_populate(o.cache, 9, &u9, 0, g_src, CHUNK, CHUNK);
		CHECK(s3_cache_read(o.cache, 1, &u1, 0, CHUNK, r3.buf, rd_cb, &r3) == -ENOENT);
		/* a smaller resident hit is refused too (no callback) */
		CHECK(s3_cache_read(o.cache, 1, &u1, 0, BS, r3.buf, rd_cb, &r3) == -ENOENT);
		CHECK(r3.calls == 0);
	}
	CHECK(g_dev.submitted_writes == w && g_dev.submitted_reads == rcount);

	/* Complete in a mixed order with mixed status. Each step but the last
	 * must leave the owner waiting and the descriptor open. */
	CHECK(complete_one(1, true));
	CHECK(!o.torn_down);
	CHECK(complete_one(0, false));          /* failed read */
	CHECK(r1->calls == 1 && r1->status == -EIO);
	CHECK(!o.torn_down);
	CHECK(complete_one(1, false));          /* failed fill */
	CHECK(!o.torn_down);
	CHECK(complete_one(0, true));
	CHECK(r2->calls == 1 && r2->status == 0);
	CHECK(!o.torn_down && g_dev.open);
	CHECK(complete_one(1, true));           /* LAST: frees the cache */
	CHECK(o.torn_down && o.quiesced_calls == 1);
	CHECK(o.outstanding_at_cb == 0);
	CHECK(g_dev.violations == 0);
	CHECK(!complete_one(-1, true));         /* Q10: nothing left to land */
	g_fail_alloc = false;
	printf("    allocation attempts while failing: %" PRIu64 "\n",
	       g_alloc_attempts_while_failing);
	free(r1);
	free(r2);
}

/* Q5: read callback that starts teardown from inside itself, with the read
 * completing INLINE from the submit call. The quiesce callback (which frees the
 * cache) must not run until s3_cache_read's frame has fully unwound. */
static struct owner g_reent_owner;
static int g_reent_rc = 1;
static bool g_reent_cache_alive_in_cb;

static void
reent_read_cb(void *arg, int status)
{
	struct rd *r = arg;

	r->calls++;
	r->status = status;
	g_reent_rc = owner_teardown(&g_reent_owner);
	/* Still inside the cache's frame: it must not have been freed. */
	g_reent_cache_alive_in_cb = (g_reent_owner.cache != NULL);
}

static void
test_reentrant_inline(void)
{
	struct rd *r = calloc(1, sizeof(*r));
	struct spdk_uuid u1 = mk_uuid(1);
	int rc;

	printf("[Q5] inline completion + re-entrant quiesce from the read callback\n");
	fake_desc_open();
	memset(&g_reent_owner, 0, sizeof(g_reent_owner));
	g_reent_owner.cache = mk_cache();
	warm(g_reent_owner.cache, 1, 1);

	g_dev.mode = DEV_INLINE_OK;
	rc = s3_cache_read(g_reent_owner.cache, 1, &u1, 0, CHUNK, r->buf,
			   reent_read_cb, r);
	/* The cache is freed by now, from the outermost frame's last action. */
	CHECK(rc == 0);
	CHECK(r->calls == 1 && r->status == 0);
	CHECK(g_reent_rc == -EINPROGRESS);      /* frames>0 => not idle yet */
	CHECK(g_reent_cache_alive_in_cb);
	CHECK(g_reent_owner.torn_down && g_reent_owner.quiesced_calls == 1);
	CHECK(g_dev.violations == 0);
	free(r);

	/* Same, inline FAILED read. */
	printf("[Q5b] inline failed completion + re-entrant quiesce\n");
	fake_desc_open();
	memset(&g_reent_owner, 0, sizeof(g_reent_owner));
	g_reent_rc = 1;
	g_reent_owner.cache = mk_cache();
	warm(g_reent_owner.cache, 1, 1);
	r = calloc(1, sizeof(*r));
	g_dev.mode = DEV_INLINE_FAIL;
	rc = s3_cache_read(g_reent_owner.cache, 1, &u1, 0, CHUNK, r->buf,
			   reent_read_cb, r);
	CHECK(rc == 0 && r->calls == 1 && r->status == -EIO);
	CHECK(g_reent_rc == -EINPROGRESS);
	CHECK(g_reent_owner.torn_down && g_reent_owner.quiesced_calls == 1);
	CHECK(g_dev.violations == 0);
	free(r);

	/* Past-the-end read answered inline (no device I/O) with re-entry. */
	printf("[Q5c] past-the-end inline answer + re-entrant quiesce\n");
	fake_desc_open();
	memset(&g_reent_owner, 0, sizeof(g_reent_owner));
	g_reent_rc = 1;
	g_reent_owner.cache = mk_cache();
	{
		struct spdk_uuid u3 = mk_uuid(3);

		g_dev.mode = DEV_INLINE_OK;
		/* Object of 1 block; read at block 4 is entirely past its end. */
		s3_cache_populate(g_reent_owner.cache, 3, &u3, 0, g_src, BS, BS);
		r = calloc(1, sizeof(*r));
		rc = s3_cache_read(g_reent_owner.cache, 3, &u3, 4 * BS, BS,
				   r->buf, reent_read_cb, r);
	}
	CHECK(rc == 0 && r->calls == 1 && r->status == 0);
	CHECK(g_reent_rc == -EINPROGRESS);
	CHECK(g_reent_owner.torn_down && g_reent_owner.quiesced_calls == 1);
	CHECK(g_dev.violations == 0);
	free(r);
}

/* Q6: submission errors with quiesce pending, and a quiesce that becomes
 * satisfied by a submission-error unwind. */
static void
test_submit_errors(void)
{
	struct owner o = {0};
	struct spdk_uuid u1 = mk_uuid(1), u5 = mk_uuid(5);
	struct rd *r = calloc(1, sizeof(*r));
	struct s3_cache_stats st;

	printf("[Q6] submission -ENOMEM unwinds, before and during quiesce\n");
	fake_desc_open();
	o.cache = mk_cache();
	warm(o.cache, 1, 1);

	g_dev.mode = DEV_ENOMEM;
	CHECK(s3_cache_read(o.cache, 1, &u1, 0, CHUNK, r->buf, rd_cb, r) == -ENOENT);
	s3_cache_populate(o.cache, 5, &u5, 0, g_src, CHUNK, CHUNK);
	CHECK(r->calls == 0);
	CHECK(s3_cache_is_quiesced(o.cache));
	s3_cache_get_stats(o.cache, &st);
	CHECK(st.hits == 0);                    /* rolled back */

	/* One parked fill, then quiesce, then the fill lands as a failure. */
	g_dev.mode = DEV_PARK;
	s3_cache_populate(o.cache, 5, &u5, 0, g_src, CHUNK, CHUNK);
	CHECK(owner_teardown(&o) == -EINPROGRESS);
	CHECK(complete_one(1, false));
	CHECK(o.torn_down && o.quiesced_calls == 1 && g_dev.violations == 0);
	free(r);
}

/* Q9: destroy while not idle must refuse (leak), not free under I/O. */
static void
test_destroy_refuses(void)
{
	struct s3_cache *c;
	struct spdk_uuid u5 = mk_uuid(5);
	int errs = g_errlogs;

	printf("[Q9] destroy with a fill outstanding refuses and leaks\n");
	fake_desc_open();
	c = mk_cache();
	g_dev.mode = DEV_PARK;
	s3_cache_populate(c, 5, &u5, 0, g_src, CHUNK, CHUNK);
	CHECK(g_dev.outstanding == 1);
#ifdef NDEBUG
	s3_cache_destroy(c);                    /* would assert in a debug build */
	CHECK(g_errlogs == errs + 1);
	/* The fill can still land safely on the refused (not freed) cache. */
	CHECK(complete_one(1, true));
	CHECK(s3_cache_is_quiesced(c));
	s3_cache_destroy(c);                    /* now idle: actually frees */
	CHECK(g_errlogs == errs + 1);
#else
	(void)errs;
	/* Debug build: the assert is the guard. Land the fill and destroy. */
	CHECK(complete_one(1, true));
	CHECK(s3_cache_is_quiesced(c));
	s3_cache_destroy(c);
#endif
	CHECK(g_dev.violations == 0);
}

int
main(void)
{
	memset(g_src, 0x5A, sizeof(g_src));

	test_idle();
	test_multi_outstanding();
	test_reentrant_inline();
	test_submit_errors();
	test_destroy_refuses();

	printf("s3_cache_quiesce_test (mocked bdev, no real AIO): %d checks, "
	       "%d failed\n", g_pass + g_fail, g_fail);
	return g_fail ? 1 : 0;
}
