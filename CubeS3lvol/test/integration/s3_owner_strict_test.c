/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */

/*
 * Deterministic harness for strict ownership (on-object format v2, CAS
 * tombstone protocol):
 *
 *   s3_owner_mode_from_env()
 *   s3_owner_strict_check_force()
 *   s3_owner_caps_valid()
 *   s3_owner_probe()
 *   s3_owner_acquire_strict()
 *   s3_owner_release_strict()
 *
 * plus a regression check that legacy acquire/release keep their old
 * unconditional behaviour.
 *
 * The S3 primitives are replaced by an in-process fake whose behaviour is the
 * thing under test. ETags are derived from object CONTENT (FNV-1a), like a
 * real single-part MD5 ETag, so identical bytes give identical ETags: ABA
 * protection must come from the protocol's per-version nonces, not from a
 * fake that never repeats an ETag. The fake can be configured to:
 *
 *   - honour or ignore If-None-Match on PUT (COS ignores it),
 *   - honour or ignore If-Match on PUT (CAS),
 *   - honour or ignore If-Match on DELETE (the pinned MinIO ignores it; v2
 *     must not care),
 *   - apply a PUT and then lose the acknowledgement (-ETIMEDOUT),
 *   - fail a PUT with a transport error before evaluating/applying it,
 *   - fail the next GET,
 *   - run a hook immediately before a PUT is evaluated, to interleave a
 *     second "process" between another process's read and its write.
 *
 * What this does NOT prove: real HTTP header handling or real backend
 * semantics (covered by the MinIO two-process run in
 * deps/recovery-owner-20261026/race), that a probed endpoint keeps enforcing
 * the headers, or anything about storage writer fencing / crash takeover.
 *
 * Usage: ./s3_owner_strict_test
 * Exit: 0 all assertions pass, 1 otherwise.
 */

#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "spdk/uuid.h"

#include "s3lvol/s3_client.h"
#include "s3lvol/s3_owner.h"

static int g_failures;
static int g_checks;

#define CHECK(cond) do {                                                \
	g_checks++;                                                     \
	if (!(cond)) {                                                  \
		fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, \
			#cond);                                         \
		g_failures++;                                           \
	}                                                               \
} while (0)

#define OWNER_KEY "lvs/meta/owner"

/* ------------------------------------------------------------------ fake */

#define FAKE_MAX_OBJECTS 16

struct fake_object {
	char     key[320];
	char    *data;
	size_t   len;
	bool     present;
};

typedef void (*fake_hook_fn)(void);

struct fake_backend {
	struct fake_object objs[FAKE_MAX_OBJECTS];

	bool     honour_if_none_match;
	bool     honour_if_match_put;
	bool     honour_if_match_delete;

	bool     lose_put_ack;           /* apply, then -ETIMEDOUT */
	bool     transport_fail_put;     /* -EIO, nothing evaluated/applied */
	int      fail_next_gets;         /* -EIO for the next N GETs */

	/* One-shot hook before the next PUT to the owner key with the given
	 * condition kind is evaluated (0 = INM, 1 = If-Match). */
	fake_hook_fn pre_put_hook;
	int          pre_put_hook_kind;

	int      puts;
	int      gets;
	int      deletes;
	int      owner_key_puts;
	int      owner_key_deletes;
	int      owner_key_uncond_puts;
};

static struct fake_backend g_fake;
static const char *g_fake_endpoint = "fake-endpoint";
static const char *g_fake_bucket = "fake-bucket";

static void
fake_reset(bool honour_inm, bool honour_ifmatch_put)
{
	int i;

	for (i = 0; i < FAKE_MAX_OBJECTS; i++) {
		free(g_fake.objs[i].data);
	}
	memset(&g_fake, 0, sizeof(g_fake));
	g_fake.honour_if_none_match = honour_inm;
	g_fake.honour_if_match_put = honour_ifmatch_put;
	/* Default to the measured MinIO behaviour: DELETE ignores If-Match. */
	g_fake.honour_if_match_delete = false;
}

static struct fake_object *
fake_find(const char *key)
{
	int i;

	for (i = 0; i < FAKE_MAX_OBJECTS; i++) {
		if (g_fake.objs[i].present &&
		    strcmp(g_fake.objs[i].key, key) == 0) {
			return &g_fake.objs[i];
		}
	}
	return NULL;
}

static struct fake_object *
fake_slot(const char *key)
{
	int i;

	for (i = 0; i < FAKE_MAX_OBJECTS; i++) {
		if (!g_fake.objs[i].present) {
			snprintf(g_fake.objs[i].key, sizeof(g_fake.objs[i].key),
				 "%s", key);
			return &g_fake.objs[i];
		}
	}
	fprintf(stderr, "fake backend is full\n");
	abort();
}

static void
fake_write_object(const char *key, const void *data, size_t len)
{
	struct fake_object *o = fake_find(key);

	if (!o) {
		o = fake_slot(key);
	}
	free(o->data);
	o->data = malloc(len ? len : 1);
	if (!o->data) {
		abort();
	}
	if (len) {
		memcpy(o->data, data, len);
	}
	o->len = len;
	o->present = true;
}

static void
fake_remove(struct fake_object *o)
{
	free(o->data);
	memset(o, 0, sizeof(*o));
}

/* Content-derived, like a single-part MD5 ETag. */
static void
fake_etag(const struct fake_object *o, char *out, size_t out_len)
{
	uint64_t h = 1469598103934665603ULL;
	size_t i;

	for (i = 0; i < o->len; i++) {
		h ^= (unsigned char)o->data[i];
		h *= 1099511628211ULL;
	}
	snprintf(out, out_len, "\"%016" PRIx64 "\"", h);
}

static bool
is_owner_key(const char *key)
{
	return strcmp(key, OWNER_KEY) == 0;
}

/* ------------------------------------------------------------------ stubs */

const char *
s3_client_endpoint(const struct s3_client *client)
{
	(void)client;
	return g_fake_endpoint;
}

const char *
s3_client_bucket(const struct s3_client *client)
{
	(void)client;
	return g_fake_bucket;
}

static void
fake_run_hook(const char *key, int kind)
{
	fake_hook_fn hook = g_fake.pre_put_hook;

	if (hook && is_owner_key(key) && g_fake.pre_put_hook_kind == kind) {
		g_fake.pre_put_hook = NULL;     /* one shot, disarm first */
		hook();
	}
}

/* kind: -1 unconditional, 0 If-None-Match:*, 1 If-Match:etag */
static int
fake_put(const char *key, struct iovec *iov, int iovcnt, int kind,
	 const char *etag, s3_op_cb cb, void *cb_arg)
{
	struct fake_object *o;
	char current[S3_OWNER_ETAG_MAX];
	size_t total = 0;
	size_t pos = 0;
	char *body;
	int i;

	g_fake.puts++;
	if (is_owner_key(key)) {
		g_fake.owner_key_puts++;
		if (kind < 0) {
			g_fake.owner_key_uncond_puts++;
		}
	}

	if (kind >= 0) {
		fake_run_hook(key, kind);
	}

	if (g_fake.transport_fail_put) {
		cb(cb_arg, -EIO);
		return 0;
	}

	o = fake_find(key);
	if (kind == 0 && g_fake.honour_if_none_match && o) {
		cb(cb_arg, -EEXIST);
		return 0;
	}
	if (kind == 1 && g_fake.honour_if_match_put) {
		if (!o) {
			cb(cb_arg, -ENOENT);
			return 0;
		}
		fake_etag(o, current, sizeof(current));
		if (!etag || strcmp(etag, current) != 0) {
			cb(cb_arg, -EEXIST);
			return 0;
		}
	}

	for (i = 0; i < iovcnt; i++) {
		total += iov[i].iov_len;
	}
	body = malloc(total ? total : 1);
	if (!body) {
		return -ENOMEM;
	}
	for (i = 0; i < iovcnt; i++) {
		memcpy(body + pos, iov[i].iov_base, iov[i].iov_len);
		pos += iov[i].iov_len;
	}
	fake_write_object(key, body, total);
	free(body);

	cb(cb_arg, g_fake.lose_put_ack ? -ETIMEDOUT : 0);
	return 0;
}

int
s3_put(struct s3_client *client, const char *key,
       struct iovec *iov, int iovcnt, bool if_none_match,
       s3_op_cb cb, void *cb_arg)
{
	(void)client;
	return fake_put(key, iov, iovcnt, if_none_match ? 0 : -1, NULL,
			cb, cb_arg);
}

int
s3_put_if_match(struct s3_client *client, const char *key,
		struct iovec *iov, int iovcnt, const char *etag,
		s3_op_cb cb, void *cb_arg)
{
	(void)client;
	if (!etag || etag[0] == '\0') {
		return -EINVAL;
	}
	return fake_put(key, iov, iovcnt, 1, etag, cb, cb_arg);
}

int
s3_get_range_ex(struct s3_client *client, const char *key,
		uint64_t offset, uint64_t len, void *buf,
		char *etag_out, size_t etag_len,
		s3_get_cb cb, void *cb_arg)
{
	struct fake_object *o;
	size_t n = 0;

	(void)client;

	g_fake.gets++;
	if (etag_out && etag_len) {
		etag_out[0] = '\0';
	}
	if (g_fake.fail_next_gets > 0) {
		g_fake.fail_next_gets--;
		if (cb) {
			cb(cb_arg, 0, -EIO);
		}
		return 0;
	}
	o = fake_find(key);
	if (!o) {
		if (cb) {
			cb(cb_arg, 0, -ENOENT);
		}
		return 0;
	}

	if (offset < o->len && len > 0) {
		n = o->len - (size_t)offset;
		if (n > len) {
			n = (size_t)len;
		}
		memcpy(buf, o->data + offset, n);
	}
	if (etag_out && etag_len) {
		fake_etag(o, etag_out, etag_len);
	}
	if (cb) {
		cb(cb_arg, (uint64_t)n, 0);
	}
	return 0;
}

int
s3_get_range(struct s3_client *client, const char *key,
	     uint64_t offset, uint64_t len, void *buf,
	     s3_get_cb cb, void *cb_arg)
{
	return s3_get_range_ex(client, key, offset, len, buf, NULL, 0, cb, cb_arg);
}

int
s3_head(struct s3_client *client, const char *key,
	uint64_t *out_size, s3_op_cb cb, void *cb_arg)
{
	struct fake_object *o = fake_find(key);

	(void)client;

	if (!o) {
		if (cb) {
			cb(cb_arg, -ENOENT);
		}
		return 0;
	}
	if (out_size) {
		*out_size = o->len;
	}
	if (cb) {
		cb(cb_arg, 0);
	}
	return 0;
}

int
s3_delete(struct s3_client *client, const char *key,
	  s3_op_cb cb, void *cb_arg)
{
	struct fake_object *o = fake_find(key);

	(void)client;

	g_fake.deletes++;
	if (is_owner_key(key)) {
		g_fake.owner_key_deletes++;
	}
	if (!o) {
		if (cb) {
			cb(cb_arg, -ENOENT);
		}
		return 0;
	}
	fake_remove(o);
	if (cb) {
		cb(cb_arg, 0);
	}
	return 0;
}

int
s3_delete_if_match(struct s3_client *client, const char *key,
		   const char *etag, s3_op_cb cb, void *cb_arg)
{
	struct fake_object *o = fake_find(key);
	char current[S3_OWNER_ETAG_MAX];

	(void)client;

	g_fake.deletes++;
	if (is_owner_key(key)) {
		g_fake.owner_key_deletes++;
	}
	if (!o) {
		if (cb) {
			cb(cb_arg, -ENOENT);
		}
		return 0;
	}
	if (g_fake.honour_if_match_delete) {
		fake_etag(o, current, sizeof(current));
		if (!etag || strcmp(etag, current) != 0) {
			if (cb) {
				cb(cb_arg, -EEXIST);
			}
			return 0;
		}
	}
	fake_remove(o);
	if (cb) {
		cb(cb_arg, 0);
	}
	return 0;
}

/* ------------------------------------------------------------- callbacks */

struct acquire_result {
	int                  calls;
	int                  status;
	bool                 has_claim;
	struct s3_owner_claim claim;
};

static void
on_acquire(void *cb_arg, const struct s3_owner_info *self,
	   const struct s3_owner_claim *claim, int status)
{
	struct acquire_result *r = cb_arg;

	r->calls++;
	r->status = status;
	if (claim) {
		r->has_claim = true;
		r->claim = *claim;
	}
	(void)self;
}

struct simple_result {
	int calls;
	int status;
};

static void
on_done(void *cb_arg, int status)
{
	struct simple_result *r = cb_arg;

	r->calls++;
	r->status = status;
}

struct legacy_result {
	int calls;
	int status;
};

static void
on_legacy_acquire(void *cb_arg, const struct s3_owner_info *holder, int status)
{
	struct legacy_result *r = cb_arg;

	(void)holder;
	r->calls++;
	r->status = status;
}

static struct s3_client *const g_cli = (struct s3_client *)&g_fake;
static struct s3_owner_caps g_caps;

static void
probe_caps(struct s3_owner_caps *caps)
{
	struct simple_result r = {0};

	s3_owner_probe(g_cli, "lvs", caps, on_done, &r);
	CHECK(r.calls == 1);
	CHECK(r.status == 0);
}

static void
acquire(struct acquire_result *r, const struct s3_owner_caps *caps)
{
	memset(r, 0, sizeof(*r));
	s3_owner_acquire_strict(g_cli, "lvs", NULL, caps, on_acquire, r);
	CHECK(r->calls == 1);
}

static int
release(const struct s3_owner_claim *claim, const struct s3_owner_caps *caps)
{
	struct simple_result r = {0};

	s3_owner_release_strict(g_cli, "lvs", claim, caps, on_done, &r);
	CHECK(r.calls == 1);
	return r.status;
}

/* Snapshot of the owner object for "unchanged" checks. */
struct snap {
	bool   present;
	size_t len;
	char   data[1024];
};

static void
snap_owner(struct snap *s)
{
	struct fake_object *o = fake_find(OWNER_KEY);

	memset(s, 0, sizeof(*s));
	if (o && o->len <= sizeof(s->data)) {
		s->present = true;
		s->len = o->len;
		memcpy(s->data, o->data, o->len);
	}
}

static bool
snap_equal_owner(const struct snap *s)
{
	struct fake_object *o = fake_find(OWNER_KEY);

	if (!s->present) {
		return o == NULL;
	}
	return o && o->len == s->len && memcmp(o->data, s->data, s->len) == 0;
}

static bool
owner_contains(const char *needle)
{
	struct fake_object *o = fake_find(OWNER_KEY);
	char tmp[1100];

	if (!o || o->len >= sizeof(tmp)) {
		return false;
	}
	memcpy(tmp, o->data, o->len);
	tmp[o->len] = '\0';
	return strstr(tmp, needle) != NULL;
}

/* ------------------------------------------------------------------ tests */

static void
test_probe_compliant(void)
{
	struct s3_owner_caps caps;
	struct simple_result r = {0};

	fake_reset(true, true);
	s3_owner_probe(g_cli, "lvs", &caps, on_done, &r);

	CHECK(r.calls == 1);
	CHECK(r.status == 0);
	CHECK(caps.probed);
	CHECK(caps.atomic_create);
	CHECK(caps.cas_replace);
	CHECK(strcmp(caps.endpoint, g_fake_endpoint) == 0);
	CHECK(strcmp(caps.bucket, g_fake_bucket) == 0);
	CHECK(strcmp(caps.lvs_name, "lvs") == 0);
	/* No probe object left, owner key never touched. */
	CHECK(g_fake.deletes == 1);
	CHECK(fake_find(OWNER_KEY) == NULL);
	CHECK(g_fake.owner_key_puts == 0);
	{
		int i, live = 0;

		for (i = 0; i < FAKE_MAX_OBJECTS; i++) {
			live += g_fake.objs[i].present ? 1 : 0;
		}
		CHECK(live == 0);
	}
}

/* The measured MinIO behaviour (DELETE ignores If-Match) must not block v2:
 * v2 never relies on conditional DELETE. */
static void
test_probe_ignores_delete_semantics(void)
{
	struct s3_owner_caps caps;
	struct simple_result r = {0};

	fake_reset(true, true);
	g_fake.honour_if_match_delete = false;
	s3_owner_probe(g_cli, "lvs", &caps, on_done, &r);
	CHECK(r.status == 0);
	CHECK(caps.cas_replace);
}

static void
test_probe_rejects_ignored_if_none_match(void)
{
	struct s3_owner_caps caps;
	struct simple_result r = {0};

	fake_reset(false, true);    /* COS behaviour */
	s3_owner_probe(g_cli, "lvs", &caps, on_done, &r);

	CHECK(r.calls == 1);
	CHECK(r.status == -ENOTSUP);
	CHECK(!caps.probed);
	CHECK(!caps.atomic_create);
	CHECK(g_fake.owner_key_puts == 0);
	CHECK(fake_find(OWNER_KEY) == NULL);
}

static void
test_probe_rejects_ignored_if_match_put(void)
{
	struct s3_owner_caps caps;
	struct simple_result r = {0};

	fake_reset(true, false);
	s3_owner_probe(g_cli, "lvs", &caps, on_done, &r);

	CHECK(r.calls == 1);
	CHECK(r.status == -ENOTSUP);
	CHECK(!caps.probed);
	CHECK(!caps.cas_replace);
	CHECK(g_fake.owner_key_puts == 0);
}

static void
test_probe_transport_error_is_not_capability(void)
{
	struct s3_owner_caps caps;
	struct simple_result r = {0};

	fake_reset(true, true);
	g_fake.transport_fail_put = true;
	s3_owner_probe(g_cli, "lvs", &caps, on_done, &r);
	CHECK(r.status == -EIO);
	CHECK(!caps.probed);
}

static void
test_acquire_requires_caps(void)
{
	struct s3_owner_caps caps;
	struct acquire_result r;

	fake_reset(true, true);
	memset(&caps, 0, sizeof(caps));

	acquire(&r, &caps);
	CHECK(r.status == -ENOTSUP);
	CHECK(!r.has_claim);
	CHECK(g_fake.puts == 0);
	CHECK(g_fake.gets == 0);

	/* A record missing the CAS capability is not valid either. */
	probe_caps(&caps);
	caps.cas_replace = false;
	CHECK(!s3_owner_caps_valid(&caps, g_cli, "lvs"));
	g_fake.puts = 0;
	acquire(&r, &caps);
	CHECK(r.status == -ENOTSUP);
	CHECK(g_fake.puts == 0);
}

static void
test_second_owner_is_refused(void)
{
	struct acquire_result a, b;
	struct snap s;

	fake_reset(true, true);
	probe_caps(&g_caps);

	acquire(&a, &g_caps);
	CHECK(a.status == 0);
	CHECK(a.has_claim);
	CHECK(owner_contains("\"state\":\"held\""));
	snap_owner(&s);

	acquire(&b, &g_caps);
	CHECK(b.status == -EBUSY);
	CHECK(!b.has_claim);
	CHECK(snap_equal_owner(&s));
}

static void
test_release_then_reacquire_never_deletes(void)
{
	struct acquire_result a, b, c;
	char tomb1[S3_OWNER_ETAG_MAX];
	struct fake_object *o;

	fake_reset(true, true);
	probe_caps(&g_caps);
	g_fake.deletes = 0;

	acquire(&a, &g_caps);
	CHECK(a.status == 0);
	CHECK(release(&a.claim, &g_caps) == 0);
	CHECK(owner_contains("\"state\":\"free\""));
	o = fake_find(OWNER_KEY);
	CHECK(o != NULL);
	fake_etag(o, tomb1, sizeof(tomb1));

	acquire(&b, &g_caps);
	CHECK(b.status == 0);
	CHECK(b.has_claim);
	CHECK(release(&b.claim, &g_caps) == 0);

	/* Each tombstone carries a fresh nonce, so its content-derived ETag
	 * differs from the previous tombstone: an old comparand is dead. */
	o = fake_find(OWNER_KEY);
	CHECK(o != NULL);
	{
		char tomb2[S3_OWNER_ETAG_MAX];

		fake_etag(o, tomb2, sizeof(tomb2));
		CHECK(strcmp(tomb1, tomb2) != 0);
	}

	acquire(&c, &g_caps);
	CHECK(c.status == 0);

	/* Strict v2 never deletes and never writes the owner key
	 * unconditionally. */
	CHECK(g_fake.owner_key_deletes == 0);
	CHECK(g_fake.owner_key_uncond_puts == 0);
}

static void
test_stale_release_cannot_replace_new_owner(void)
{
	struct acquire_result a, b;
	struct snap s;

	fake_reset(true, true);
	probe_caps(&g_caps);

	acquire(&a, &g_caps);
	CHECK(a.status == 0);
	CHECK(release(&a.claim, &g_caps) == 0);

	acquire(&b, &g_caps);
	CHECK(b.status == 0);
	snap_owner(&s);

	/* Replay of A's claim (ABA). */
	CHECK(release(&a.claim, &g_caps) == -EBUSY);
	CHECK(snap_equal_owner(&s));

	/* A forged claim with B's nonce but a wrong ETag is refused too. */
	{
		struct s3_owner_claim forged = b.claim;

		snprintf(forged.etag, sizeof(forged.etag), "\"forged\"");
		CHECK(release(&forged, &g_caps) == -EBUSY);
		CHECK(snap_equal_owner(&s));
	}
	/* And A's ETag with B's nonce. */
	{
		struct s3_owner_claim forged = b.claim;

		snprintf(forged.etag, sizeof(forged.etag), "%s", a.claim.etag);
		CHECK(release(&forged, &g_caps) == -EBUSY);
		CHECK(snap_equal_owner(&s));
	}

	CHECK(release(&b.claim, &g_caps) == 0);
	CHECK(g_fake.owner_key_deletes == 0);
}

/* A stale release racing against a new owner between the release's read and
 * its CAS: the CAS comparand is dead, the server rejects, nothing changes. */
static struct s3_owner_claim g_hook_claim;
static struct acquire_result g_hook_acq;

static void
hook_release_then_acquire(void)
{
	/* Another "process": the owner releases and B acquires in between. */
	CHECK(release(&g_hook_claim, &g_caps) == 0);
	acquire(&g_hook_acq, &g_caps);
}

static void
test_release_cas_loses_to_interleaved_owner(void)
{
	struct acquire_result a;
	struct simple_result r = {0};
	struct snap s;

	fake_reset(true, true);
	probe_caps(&g_caps);

	acquire(&a, &g_caps);
	CHECK(a.status == 0);
	g_hook_claim = a.claim;

	/* Duplicate release of the same claim: by the time the second CAS is
	 * evaluated, the first released and B acquired. */
	g_fake.pre_put_hook = hook_release_then_acquire;
	g_fake.pre_put_hook_kind = 1;
	s3_owner_release_strict(g_cli, "lvs", &a.claim, &g_caps, on_done, &r);
	CHECK(r.calls == 1);
	CHECK(g_hook_acq.status == 0);
	CHECK(r.status == -EBUSY);
	snap_owner(&s);
	CHECK(owner_contains("\"state\":\"held\""));
	CHECK(release(&g_hook_acq.claim, &g_caps) == 0);
	(void)s;
}

/* Two acquirers both read "absent"; the second one's conditional create is
 * evaluated after the first has won. */
static struct acquire_result g_racer;

static void
hook_racer_acquires(void)
{
	acquire(&g_racer, &g_caps);
}

static void
test_concurrent_create_one_winner(void)
{
	struct acquire_result a;

	fake_reset(true, true);
	probe_caps(&g_caps);

	g_fake.pre_put_hook = hook_racer_acquires;
	g_fake.pre_put_hook_kind = 0;
	acquire(&a, &g_caps);

	CHECK(g_racer.status == 0);
	CHECK(a.status == -EBUSY);
	CHECK(!a.has_claim);
	CHECK(release(&g_racer.claim, &g_caps) == 0);
}

/* Two acquirers both read the same free tombstone; only one CAS lands. */
static void
test_concurrent_cas_one_winner(void)
{
	struct acquire_result a, first;

	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&first, &g_caps);
	CHECK(release(&first.claim, &g_caps) == 0);

	memset(&g_racer, 0, sizeof(g_racer));
	g_fake.pre_put_hook = hook_racer_acquires;
	g_fake.pre_put_hook_kind = 1;
	acquire(&a, &g_caps);

	CHECK(g_racer.status == 0);
	CHECK(a.status == -EBUSY);
	CHECK(release(&g_racer.claim, &g_caps) == 0);
	CHECK(g_fake.owner_key_deletes == 0);
}

static void
test_lost_ack_create_resolved_by_readback(void)
{
	struct acquire_result a;

	fake_reset(true, true);
	probe_caps(&g_caps);

	g_fake.lose_put_ack = true;
	acquire(&a, &g_caps);
	CHECK(a.status == 0);
	CHECK(a.has_claim);
	g_fake.lose_put_ack = false;
	CHECK(release(&a.claim, &g_caps) == 0);
}

static void
test_lost_ack_cas_resolved_by_readback(void)
{
	struct acquire_result a, b;

	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&a, &g_caps);
	CHECK(release(&a.claim, &g_caps) == 0);

	g_fake.lose_put_ack = true;
	acquire(&b, &g_caps);
	g_fake.lose_put_ack = false;
	CHECK(b.status == 0);
	CHECK(b.has_claim);
	CHECK(release(&b.claim, &g_caps) == 0);
}

static void
test_transport_failure_absent_is_not_ownership(void)
{
	struct acquire_result a;

	fake_reset(true, true);
	probe_caps(&g_caps);

	g_fake.transport_fail_put = true;
	acquire(&a, &g_caps);
	CHECK(a.status == -EIO);
	CHECK(!a.has_claim);
	CHECK(fake_find(OWNER_KEY) == NULL);
}

static void
test_transport_failure_free_is_eio_not_success(void)
{
	struct acquire_result a, b;
	struct snap s;

	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&a, &g_caps);
	CHECK(release(&a.claim, &g_caps) == 0);
	snap_owner(&s);

	g_fake.transport_fail_put = true;
	acquire(&b, &g_caps);
	g_fake.transport_fail_put = false;
	/* Marker still the free tombstone: unknown outcome, fail closed. */
	CHECK(b.status == -EIO);
	CHECK(!b.has_claim);
	CHECK(snap_equal_owner(&s));
}

static void
hook_other_acquires_then_transport_fails(void)
{
	acquire(&g_racer, &g_caps);
	g_fake.transport_fail_put = true;
}

static void
test_ambiguous_ack_with_other_holder_is_busy(void)
{
	struct acquire_result a;

	fake_reset(true, true);
	probe_caps(&g_caps);
	memset(&g_racer, 0, sizeof(g_racer));

	g_fake.pre_put_hook = hook_other_acquires_then_transport_fails;
	g_fake.pre_put_hook_kind = 0;
	acquire(&a, &g_caps);
	g_fake.transport_fail_put = false;

	CHECK(g_racer.status == 0);
	CHECK(a.status == -EBUSY);
	CHECK(!a.has_claim);
	CHECK(release(&g_racer.claim, &g_caps) == 0);
}

static void
hook_fail_next_get(void)
{
	g_fake.fail_next_gets = 1;
	g_fake.lose_put_ack = true;
}

static void
test_ambiguous_ack_readback_failure_is_eio(void)
{
	struct acquire_result a;

	fake_reset(true, true);
	probe_caps(&g_caps);

	g_fake.pre_put_hook = hook_fail_next_get;
	g_fake.pre_put_hook_kind = 0;
	acquire(&a, &g_caps);
	g_fake.lose_put_ack = false;

	/* The write landed but cannot be proven: never "probably ours". */
	CHECK(a.status == -EIO);
	CHECK(!a.has_claim);
	CHECK(owner_contains("\"state\":\"held\""));
}

static void
test_release_lost_ack_applied(void)
{
	struct acquire_result a, b;

	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&a, &g_caps);

	g_fake.lose_put_ack = true;
	CHECK(release(&a.claim, &g_caps) == 0);
	g_fake.lose_put_ack = false;
	CHECK(owner_contains("\"state\":\"free\""));

	acquire(&b, &g_caps);
	CHECK(b.status == 0);
	CHECK(release(&b.claim, &g_caps) == 0);
}

static void
test_release_transport_failure_not_applied(void)
{
	struct acquire_result a;
	struct snap s;

	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&a, &g_caps);
	snap_owner(&s);

	g_fake.transport_fail_put = true;
	CHECK(release(&a.claim, &g_caps) == -EIO);
	g_fake.transport_fail_put = false;
	CHECK(snap_equal_owner(&s));

	/* The claim is still valid and can be released later. */
	CHECK(release(&a.claim, &g_caps) == 0);
	CHECK(owner_contains("\"state\":\"free\""));
}

static void
test_release_lost_ack_unreadable_is_eio(void)
{
	struct acquire_result a;

	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&a, &g_caps);

	g_fake.transport_fail_put = true;
	g_fake.fail_next_gets = 0;
	{
		struct simple_result r = {0};

		/* read OK, CAS transport error, read-back fails */
		g_fake.pre_put_hook = hook_fail_next_get;
		g_fake.pre_put_hook_kind = 1;
		s3_owner_release_strict(g_cli, "lvs", &a.claim, &g_caps,
					on_done, &r);
		CHECK(r.calls == 1);
		CHECK(r.status == -EIO);
	}
	g_fake.transport_fail_put = false;
	g_fake.lose_put_ack = false;
	CHECK(owner_contains("\"state\":\"held\""));
	CHECK(release(&a.claim, &g_caps) == 0);
}

static void
put_owner_raw(const char *doc)
{
	fake_write_object(OWNER_KEY, doc, strlen(doc));
}

static void
test_foreign_markers_are_never_overwritten(void)
{
	static const char *const docs[] = {
		/* strict v1 / legacy layout: no "v", no "state" */
		"{\"node_id\":\"old\",\"pid\":1,\"attach_ts\":1,"
		"\"lvs_uuid\":\"00000000-0000-0000-0000-000000000000\","
		"\"nonce\":\"11111111-2222-3333-4444-555555555555\"}",
		/* v2 with an unknown state */
		"{\"v\":2,\"state\":\"maybe\","
		"\"nonce\":\"11111111-2222-3333-4444-555555555555\"}",
		/* v2 free with an extra, unknown key */
		"{\"v\":2,\"state\":\"free\",\"x\":1,"
		"\"nonce\":\"11111111-2222-3333-4444-555555555555\"}",
		/* future version */
		"{\"v\":3,\"state\":\"free\","
		"\"nonce\":\"11111111-2222-3333-4444-555555555555\"}",
		/* free with a null nonce */
		"{\"v\":2,\"state\":\"free\","
		"\"nonce\":\"00000000-0000-0000-0000-000000000000\"}",
		/* garbage */
		"not json at all",
	};
	size_t i;

	for (i = 0; i < sizeof(docs) / sizeof(docs[0]); i++) {
		struct acquire_result a;
		struct snap s;

		fake_reset(true, true);
		probe_caps(&g_caps);
		put_owner_raw(docs[i]);
		snap_owner(&s);
		g_fake.owner_key_puts = 0;

		acquire(&a, &g_caps);
		CHECK(a.status == -EBUSY);
		CHECK(!a.has_claim);
		CHECK(g_fake.owner_key_puts == 0);
		CHECK(snap_equal_owner(&s));
	}
}

/* A marker written by the legacy path is foreign to strict v2. */
static void
test_legacy_marker_blocks_strict(void)
{
	struct legacy_result lr = {0};
	struct acquire_result a;
	struct snap s;

	fake_reset(true, true);
	probe_caps(&g_caps);

	s3_owner_acquire(g_cli, "lvs", NULL, false, on_legacy_acquire, &lr);
	CHECK(lr.calls == 1);
	CHECK(lr.status == 0);
	snap_owner(&s);

	acquire(&a, &g_caps);
	CHECK(a.status == -EBUSY);
	CHECK(snap_equal_owner(&s));
}

/* Legacy behaviour is unchanged: unconditional write, unconditional delete. */
static void
test_legacy_unchanged(void)
{
	struct legacy_result lr = {0};
	struct simple_result rel = {0};

	fake_reset(false, false);   /* even on a backend with no conditions */

	s3_owner_acquire(g_cli, "lvs", NULL, false, on_legacy_acquire, &lr);
	CHECK(lr.status == 0);
	CHECK(g_fake.owner_key_uncond_puts == 1);
	CHECK(fake_find(OWNER_KEY) != NULL);

	s3_owner_release(g_cli, "lvs", on_done, &rel);
	CHECK(rel.calls == 1);
	CHECK(rel.status == 0);
	CHECK(g_fake.owner_key_deletes == 1);
	CHECK(fake_find(OWNER_KEY) == NULL);
}

static void
test_release_absent_or_bad_claim(void)
{
	struct acquire_result a;
	struct s3_owner_claim bad;

	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&a, &g_caps);

	/* Out-of-band removal: nothing to release, nothing written. */
	fake_remove(fake_find(OWNER_KEY));
	g_fake.owner_key_puts = 0;
	CHECK(release(&a.claim, &g_caps) == -ENOENT);
	CHECK(g_fake.owner_key_puts == 0);
	CHECK(fake_find(OWNER_KEY) == NULL);

	bad = a.claim;
	memset(&bad.info.nonce, 0, sizeof(bad.info.nonce));
	CHECK(release(&bad, &g_caps) == -EINVAL);
	bad = a.claim;
	bad.etag[0] = '\0';
	CHECK(release(&bad, &g_caps) == -EINVAL);
	bad = a.claim;
	bad.valid = false;
	CHECK(release(&bad, &g_caps) == -EINVAL);

	{
		struct s3_owner_caps none;

		memset(&none, 0, sizeof(none));
		g_fake.puts = 0;
		g_fake.gets = 0;
		CHECK(release(&a.claim, &none) == -ENOTSUP);
		CHECK(g_fake.puts == 0);
		CHECK(g_fake.gets == 0);
	}
}

static void
test_force_refused(void)
{
	CHECK(s3_owner_strict_check_force(true) == -EINVAL);
	CHECK(s3_owner_strict_check_force(false) == 0);
}

static void
test_mode_env(void)
{
	/* Strict is the default: unset and empty both mean strict. */
	unsetenv(S3_OWNER_MODE_ENV);
	CHECK(s3_owner_mode_from_env() == S3_OWNER_MODE_STRICT);

	setenv(S3_OWNER_MODE_ENV, "", 1);
	CHECK(s3_owner_mode_from_env() == S3_OWNER_MODE_STRICT);

	setenv(S3_OWNER_MODE_ENV, "strict", 1);
	CHECK(s3_owner_mode_from_env() == S3_OWNER_MODE_STRICT);

	/* Explicit legacy is still recognised (the dispatch layer refuses it). */
	setenv(S3_OWNER_MODE_ENV, "legacy", 1);
	CHECK(s3_owner_mode_from_env() == S3_OWNER_MODE_LEGACY);

	setenv(S3_OWNER_MODE_ENV, "on", 1);
	CHECK(s3_owner_mode_from_env() == S3_OWNER_MODE_INVALID);

	unsetenv(S3_OWNER_MODE_ENV);
}

static void
test_caps_are_bound_to_target(void)
{
	struct s3_owner_caps caps;

	fake_reset(true, true);
	probe_caps(&caps);

	CHECK(s3_owner_caps_valid(&caps, g_cli, "lvs"));
	CHECK(!s3_owner_caps_valid(&caps, g_cli, "other-lvs"));

	g_fake_endpoint = "different-endpoint";
	CHECK(!s3_owner_caps_valid(&caps, g_cli, "lvs"));
	g_fake_endpoint = "fake-endpoint";

	g_fake_bucket = "different-bucket";
	CHECK(!s3_owner_caps_valid(&caps, g_cli, "lvs"));
	g_fake_bucket = "fake-bucket";

	CHECK(s3_owner_caps_valid(&caps, g_cli, "lvs"));
}

static int
verify(const struct s3_owner_claim *claim, const struct spdk_uuid *lvs_uuid,
       const struct s3_owner_caps *caps)
{
	struct simple_result r = {0};

	s3_owner_verify_claim_strict(g_cli, "lvs", claim, lvs_uuid, caps,
				     on_done, &r);
	CHECK(r.calls == 1);
	return r.status;
}

/* Unchanged held claim: verified with exactly one GET and no write. */
static void
test_verify_claim_unchanged(void)
{
	struct acquire_result a;
	struct snap s;
	int puts, dels;

	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&a, &g_caps);
	CHECK(a.status == 0 && a.has_claim);
	snap_owner(&s);
	puts = g_fake.puts;
	dels = g_fake.deletes;
	g_fake.gets = 0;

	CHECK(verify(&a.claim, NULL, &g_caps) == 0);
	CHECK(g_fake.gets == 1);
	CHECK(g_fake.puts == puts);
	CHECK(g_fake.deletes == dels);
	CHECK(snap_equal_owner(&s));
	/* Repeatable: verifying does not consume the claim. */
	CHECK(verify(&a.claim, NULL, &g_caps) == 0);
	/* And the claim still releases normally afterwards. */
	CHECK(release(&a.claim, &g_caps) == 0);
	CHECK(owner_contains("\"state\":\"free\""));
}

/* Released (free tombstone), replaced by a new owner, absent, foreign, or
 * unreadable: all refused, nothing written. */
static void
test_verify_claim_changed_fails_closed(void)
{
	struct acquire_result a, b;
	struct snap s;
	int puts;

	/* free */
	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&a, &g_caps);
	CHECK(release(&a.claim, &g_caps) == 0);
	puts = g_fake.puts;
	CHECK(verify(&a.claim, NULL, &g_caps) == -EBUSY);
	CHECK(g_fake.puts == puts);

	/* replaced: another claim acquired the free marker (ABA) */
	acquire(&b, &g_caps);
	CHECK(b.status == 0);
	snap_owner(&s);
	puts = g_fake.puts;
	CHECK(verify(&a.claim, NULL, &g_caps) == -EBUSY);
	CHECK(g_fake.puts == puts && snap_equal_owner(&s));
	CHECK(verify(&b.claim, NULL, &g_caps) == 0);

	/* absent (out-of-band removal) */
	fake_remove(fake_find(OWNER_KEY));
	puts = g_fake.puts;
	CHECK(verify(&b.claim, NULL, &g_caps) == -ENOENT);
	CHECK(g_fake.puts == puts && fake_find(OWNER_KEY) == NULL);

	/* foreign (legacy) document at the key */
	fake_write_object(OWNER_KEY, "{\"node_id\":\"h\",\"pid\":1}", 24);
	snap_owner(&s);
	puts = g_fake.puts;
	CHECK(verify(&b.claim, NULL, &g_caps) == -EBUSY);
	CHECK(g_fake.puts == puts && snap_equal_owner(&s));

	/* unreadable */
	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&a, &g_caps);
	snap_owner(&s);
	puts = g_fake.puts;
	g_fake.fail_next_gets = 1;
	CHECK(verify(&a.claim, NULL, &g_caps) == -EIO);
	CHECK(g_fake.puts == puts && snap_equal_owner(&s));
	/* the claim is still intact and usable after a failed verify */
	CHECK(verify(&a.claim, NULL, &g_caps) == 0);
}

/* Same nonce, different ETag (bytes rewritten out of band): refused. */
static void
test_verify_claim_etag_mismatch(void)
{
	struct acquire_result a;
	struct s3_owner_claim c;

	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&a, &g_caps);
	c = a.claim;
	snprintf(c.etag, sizeof(c.etag), "\"0000000000000000\"");
	CHECK(verify(&c, NULL, &g_caps) == -EBUSY);
	CHECK(verify(&a.claim, NULL, &g_caps) == 0);
}

/* lvstore uuid binding: a held claim naming another uuid is -ESTALE. */
static void
test_verify_claim_uuid_binding(void)
{
	struct acquire_result a;
	struct spdk_uuid u1, u2;
	struct simple_result r = {0};

	fake_reset(true, true);
	probe_caps(&g_caps);
	spdk_uuid_generate(&u1);
	spdk_uuid_generate(&u2);
	memset(&a, 0, sizeof(a));
	s3_owner_acquire_strict(g_cli, "lvs", &u1, &g_caps, on_acquire, &a);
	CHECK(a.status == 0);
	CHECK(verify(&a.claim, &u1, &g_caps) == 0);
	CHECK(verify(&a.claim, &u2, &g_caps) == -ESTALE);
	(void)r;

	/* Create-path claim: acquired before the lvstore uuid existed, so the
	 * marker records zeroes. It verifies against the real uuid (the device
	 * super block carries that binding), and a claim whose recorded uuid
	 * does not match the marker's is refused. */
	CHECK(release(&a.claim, &g_caps) == 0);
	memset(&a, 0, sizeof(a));
	s3_owner_acquire_strict(g_cli, "lvs", NULL, &g_caps, on_acquire, &a);
	CHECK(a.status == 0);
	CHECK(spdk_uuid_is_null(&a.claim.info.lvs_uuid));
	CHECK(verify(&a.claim, &u1, &g_caps) == 0);
	{
		struct s3_owner_claim c = a.claim;

		spdk_uuid_copy(&c.info.lvs_uuid, &u2);
		CHECK(verify(&c, &u2, &g_caps) == -EBUSY);
	}
}

/* Bad inputs and invalid caps: refused before any request. */
static void
test_verify_claim_requires_caps_and_claim(void)
{
	struct acquire_result a;
	struct s3_owner_caps bad;
	struct s3_owner_claim c;

	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&a, &g_caps);
	g_fake.gets = 0;

	memset(&bad, 0, sizeof(bad));
	CHECK(verify(&a.claim, NULL, &bad) == -ENOTSUP);
	c = a.claim;
	c.valid = false;
	CHECK(verify(&c, NULL, &g_caps) == -EINVAL);
	c = a.claim;
	c.etag[0] = '\0';
	CHECK(verify(&c, NULL, &g_caps) == -EINVAL);
	c = a.claim;
	memset(&c.info.nonce, 0, sizeof(c.info.nonce));
	CHECK(verify(&c, NULL, &g_caps) == -EINVAL);
	CHECK(g_fake.gets == 0);
	g_fake_bucket = "other-bucket";
	CHECK(verify(&a.claim, NULL, &g_caps) == -ENOTSUP);
	g_fake_bucket = "fake-bucket";
	CHECK(g_fake.gets == 0);
}

/* A verified retained claim does not let a contender in, and normal acquire
 * still refuses this process's own held marker (no hostname/pid shortcut). */
static void
test_verify_does_not_weaken_acquire(void)
{
	struct acquire_result a, b;

	fake_reset(true, true);
	probe_caps(&g_caps);
	acquire(&a, &g_caps);
	CHECK(verify(&a.claim, NULL, &g_caps) == 0);
	acquire(&b, &g_caps);   /* same host, same pid */
	CHECK(b.status == -EBUSY && !b.has_claim);
	CHECK(verify(&a.claim, NULL, &g_caps) == 0);
}

int
main(void)
{
	test_probe_compliant();
	test_probe_ignores_delete_semantics();
	test_probe_rejects_ignored_if_none_match();
	test_probe_rejects_ignored_if_match_put();
	test_probe_transport_error_is_not_capability();
	test_acquire_requires_caps();
	test_second_owner_is_refused();
	test_release_then_reacquire_never_deletes();
	test_stale_release_cannot_replace_new_owner();
	test_release_cas_loses_to_interleaved_owner();
	test_concurrent_create_one_winner();
	test_concurrent_cas_one_winner();
	test_lost_ack_create_resolved_by_readback();
	test_lost_ack_cas_resolved_by_readback();
	test_transport_failure_absent_is_not_ownership();
	test_transport_failure_free_is_eio_not_success();
	test_ambiguous_ack_with_other_holder_is_busy();
	test_ambiguous_ack_readback_failure_is_eio();
	test_release_lost_ack_applied();
	test_release_transport_failure_not_applied();
	test_release_lost_ack_unreadable_is_eio();
	test_foreign_markers_are_never_overwritten();
	test_legacy_marker_blocks_strict();
	test_legacy_unchanged();
	test_release_absent_or_bad_claim();
	test_force_refused();
	test_mode_env();
	test_caps_are_bound_to_target();
	test_verify_claim_unchanged();
	test_verify_claim_changed_fails_closed();
	test_verify_claim_etag_mismatch();
	test_verify_claim_uuid_binding();
	test_verify_claim_requires_caps_and_claim();
	test_verify_does_not_weaken_acquire();

	if (g_failures) {
		fprintf(stderr, "s3_owner_strict_test: %d failure(s) of %d "
			"checks\n", g_failures, g_checks);
		return 1;
	}
	printf("s3_owner_strict_test: all %d checks passed (34 tests)\n",
	       g_checks);
	return 0;
}
