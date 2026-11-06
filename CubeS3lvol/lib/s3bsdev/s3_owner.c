/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */

#include "spdk/stdinc.h"
#include "spdk/json.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/uuid.h"

#include "s3lvol/s3_owner.h"

/* Same layout rule as the data objects (<prefix>/data/<uuid>): the prefix is the
 * lvstore name. */
#define OWNER_KEY_FMT   "%s/meta/owner"

/* Enough for "<lvs_name>/meta/owner". The lvstore name is bounded by
 * S3_LVS_NAME_MAX (64) but this file must not depend on s3_local_dev.h, so give
 * it room for any sane name and check at runtime. */
#define OWNER_KEY_MAX   256

struct owner_ctx {
	struct s3_client       *client;
	char                    key[OWNER_KEY_MAX];
	bool                    force;

	/* What we intend to write. */
	struct s3_owner_info    self;

	/* Read buffer, reused for both the "who is there" read and the read-back.
	 * Its lifetime has to cover the whole s3_get_range, hence living here. */
	char                   *buf;

	/* The document we PUT. Kept alive for the same reason. */
	char                   *doc;
	struct iovec            iov;

	struct s3_owner_info    holder;
	bool                    holder_valid;

	/* Distinguishes the read that looks for an existing owner from the one
	 * that confirms our own write landed. */
	bool                    verifying;

	s3_owner_acquire_cb     acq_cb;
	s3_owner_cb             cb;
	void                   *cb_arg;
};

/* ==========================================================================
 * Encoding / decoding the document
 * ========================================================================== */

/* Sanitise anything that goes into the JSON as a string.
 *
 * The hostname is the only field that is not a number or a uuid, and it comes
 * from the OS rather than from us. A quote or a backslash in it would produce a
 * document that no longer parses -- or worse, one that parses into something
 * else. Restricting it to an obviously safe set is cheaper than escaping, and a
 * mangled hostname in a diagnostic field costs nothing. */
static void
sanitise_ident(const char *in, char *out, size_t out_len)
{
	size_t i;

	if (out_len == 0) {
		return;
	}

	for (i = 0; i + 1 < out_len && in && in[i] != '\0'; i++) {
		unsigned char c = (unsigned char)in[i];

		if (isalnum(c) || c == '.' || c == '-' || c == '_') {
			out[i] = (char)c;
		} else {
			out[i] = '_';
		}
	}
	out[i] = '\0';
}

static int
owner_encode(const struct s3_owner_info *info, char **out_doc)
{
	char uuid_str[SPDK_UUID_STRING_LEN];
	char nonce_str[SPDK_UUID_STRING_LEN];
	char *doc;
	int len;

	spdk_uuid_fmt_lower(uuid_str, sizeof(uuid_str), &info->lvs_uuid);
	spdk_uuid_fmt_lower(nonce_str, sizeof(nonce_str), &info->nonce);

	doc = calloc(1, S3_OWNER_DOC_MAX);
	if (!doc) {
		return -ENOMEM;
	}

	len = snprintf(doc, S3_OWNER_DOC_MAX,
		       "{\"node_id\":\"%s\",\"pid\":%" PRId32 ","
		       "\"attach_ts\":%" PRIu64 ",\"lvs_uuid\":\"%s\","
		       "\"nonce\":\"%s\"}",
		       info->node_id, info->pid, info->attach_ts,
		       uuid_str, nonce_str);
	if (len < 0 || len >= S3_OWNER_DOC_MAX) {
		free(doc);
		return -ENAMETOOLONG;
	}

	*out_doc = doc;
	return len;
}

struct owner_decoded {
	char *node_id;
	uint32_t pid;
	uint64_t attach_ts;
	char *lvs_uuid;
	char *nonce;
};

static const struct spdk_json_object_decoder owner_decoders[] = {
	{"node_id",   offsetof(struct owner_decoded, node_id),   spdk_json_decode_string, true},
	{"pid",       offsetof(struct owner_decoded, pid),       spdk_json_decode_uint32, true},
	{"attach_ts", offsetof(struct owner_decoded, attach_ts), spdk_json_decode_uint64, true},
	{"lvs_uuid",  offsetof(struct owner_decoded, lvs_uuid),  spdk_json_decode_string, true},
	{"nonce",     offsetof(struct owner_decoded, nonce),     spdk_json_decode_string, true},
};

/* Parse a document read back from S3.
 *
 * Every field is optional and a parse failure is not fatal to the caller: the
 * point of reading is to find out *whether* somebody is there, and a marker we
 * cannot read still means somebody wrote one. Returning -EINVAL here would turn
 * "an older or corrupt marker" into "attach is impossible", which is worse than
 * reporting an unknown holder. */
static int
owner_decode(const char *buf, size_t len, struct s3_owner_info *out)
{
	struct owner_decoded dec = {0};
	struct spdk_json_val *values = NULL;
	char *copy;
	ssize_t nvals;
	int rc = -EINVAL;

	memset(out, 0, sizeof(*out));

	/* spdk_json_parse rewrites the buffer in place (it unescapes strings), so
	 * it must not see the caller's. */
	copy = malloc(len + 1);
	if (!copy) {
		return -ENOMEM;
	}
	memcpy(copy, buf, len);
	copy[len] = '\0';

	nvals = spdk_json_parse(copy, len, NULL, 0, NULL,
				SPDK_JSON_PARSE_FLAG_DECODE_IN_PLACE);
	if (nvals <= 0) {
		goto out;
	}

	values = calloc((size_t)nvals, sizeof(*values));
	if (!values) {
		rc = -ENOMEM;
		goto out;
	}

	nvals = spdk_json_parse(copy, len, values, (size_t)nvals, NULL,
				SPDK_JSON_PARSE_FLAG_DECODE_IN_PLACE);
	if (nvals <= 0) {
		goto out;
	}

	if (spdk_json_decode_object(values, owner_decoders,
				    SPDK_COUNTOF(owner_decoders), &dec) != 0) {
		goto out;
	}

	if (dec.node_id) {
		snprintf(out->node_id, sizeof(out->node_id), "%s", dec.node_id);
	}
	out->pid = (int32_t)dec.pid;
	out->attach_ts = dec.attach_ts;
	if (dec.lvs_uuid) {
		spdk_uuid_parse(&out->lvs_uuid, dec.lvs_uuid);
	}
	if (dec.nonce) {
		spdk_uuid_parse(&out->nonce, dec.nonce);
	}
	rc = 0;

out:
	free(dec.node_id);
	free(dec.lvs_uuid);
	free(dec.nonce);
	free(values);
	free(copy);
	return rc;
}

void
s3_owner_info_str(const struct s3_owner_info *info, char *out, size_t out_len)
{
	char uuid_str[SPDK_UUID_STRING_LEN];

	if (!out || out_len == 0) {
		return;
	}
	if (!info) {
		snprintf(out, out_len, "<unknown holder>");
		return;
	}

	spdk_uuid_fmt_lower(uuid_str, sizeof(uuid_str), &info->lvs_uuid);

	snprintf(out, out_len,
		 "node=%s pid=%" PRId32 " since=%" PRIu64 " (unix) lvs_uuid=%s",
		 info->node_id[0] ? info->node_id : "?", info->pid,
		 info->attach_ts, uuid_str);
}

/* ==========================================================================
 * acquire
 * ========================================================================== */

static void owner_do_put(struct owner_ctx *ctx);
static void owner_do_read(struct owner_ctx *ctx, bool verifying);

static void
owner_acquire_finish(struct owner_ctx *ctx, int status)
{
	s3_owner_acquire_cb cb_fn = ctx->acq_cb;
	void *cb_arg = ctx->cb_arg;
	struct s3_owner_info holder = ctx->holder;
	bool holder_valid = ctx->holder_valid;

	free(ctx->buf);
	free(ctx->doc);
	free(ctx);

	if (cb_fn) {
		cb_fn(cb_arg, (status == -EBUSY && holder_valid) ? &holder : NULL,
		      status);
	}
}

static void
owner_read_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct owner_ctx *ctx = cb_arg;
	struct s3_owner_info got;
	char desc[256];

	if (status == -ENOENT) {
		if (ctx->verifying) {
			/* Our own marker vanished between writing and reading it
			 * back. Somebody is actively managing this lvstore, which
			 * is exactly what must not happen. */
			SPDK_ERRLOG("owner marker for '%s' disappeared right after "
				    "it was written; another process is touching "
				    "this lvstore\n", ctx->key);
			owner_acquire_finish(ctx, -EBUSY);
			return;
		}
		/* Nobody there. */
		owner_do_put(ctx);
		return;
	}

	if (status != 0) {
		SPDK_ERRLOG("Failed to read the owner marker '%s': %d\n",
			    ctx->key, status);
		owner_acquire_finish(ctx, status);
		return;
	}

	if (bytes_read == 0 || bytes_read > S3_OWNER_DOC_MAX) {
		SPDK_ERRLOG("Owner marker '%s' has an implausible size %" PRIu64
			    "\n", ctx->key, bytes_read);
		owner_acquire_finish(ctx, -EIO);
		return;
	}

	if (owner_decode(ctx->buf, bytes_read, &got) != 0) {
		/* Unreadable, but present. Treated as "occupied by someone
		 * unknown" rather than as a hard error -- see owner_decode. */
		if (ctx->verifying) {
			SPDK_ERRLOG("Could not parse the owner marker '%s' we just "
				    "wrote; treating it as contended\n", ctx->key);
			owner_acquire_finish(ctx, -EBUSY);
			return;
		}
		SPDK_WARNLOG("Owner marker '%s' exists but could not be parsed; "
			     "treating it as held by an unknown owner\n", ctx->key);
		memset(&ctx->holder, 0, sizeof(ctx->holder));
		ctx->holder_valid = false;
		if (ctx->force) {
			owner_do_put(ctx);
			return;
		}
		owner_acquire_finish(ctx, -EBUSY);
		return;
	}

	if (ctx->verifying) {
		/* The whole point of the read-back: confirm the marker still says
		 * what we wrote. If a concurrent acquire overwrote it, its nonce
		 * is there instead of ours and we must not proceed.
		 *
		 * This narrows the race, it does not remove it -- see the header. */
		if (spdk_uuid_compare(&got.nonce, &ctx->self.nonce) != 0) {
			s3_owner_info_str(&got, desc, sizeof(desc));
			SPDK_ERRLOG("Lost the race for '%s': the marker now belongs "
				    "to %s\n", ctx->key, desc);
			ctx->holder = got;
			ctx->holder_valid = true;
			owner_acquire_finish(ctx, -EBUSY);
			return;
		}
		owner_acquire_finish(ctx, 0);
		return;
	}

	/* Somebody holds it. */
	ctx->holder = got;
	ctx->holder_valid = true;
	s3_owner_info_str(&got, desc, sizeof(desc));

	if (!ctx->force) {
		SPDK_ERRLOG("'%s' is already held by %s -- refusing to attach. "
			    "If that owner is gone (a crash, say), confirm it and "
			    "retry with force=true.\n", ctx->key, desc);
		owner_acquire_finish(ctx, -EBUSY);
		return;
	}

	/* Overwriting somebody else's marker leaves a trace on purpose. */
	SPDK_WARNLOG("force=true: overwriting the owner marker '%s', which was "
		     "held by %s. If that owner is still running, both processes "
		     "are now writing to the same objects.\n", ctx->key, desc);
	owner_do_put(ctx);
}

static void
owner_do_read(struct owner_ctx *ctx, bool verifying)
{
	int rc;

	ctx->verifying = verifying;

	/* A fixed-length range read rather than HEAD-then-GET.
	 *
	 * S3 truncates a range whose end is past the object instead of failing,
	 * so one round trip gets both "does it exist" (-ENOENT) and the content.
	 * The HEAD was only ever needed to learn the size. */
	rc = s3_get_range(ctx->client, ctx->key, 0, S3_OWNER_DOC_MAX, ctx->buf,
			  owner_read_done, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit the owner marker read for '%s': %d\n",
			    ctx->key, rc);
		owner_acquire_finish(ctx, rc);
	}
}

static void
owner_put_done(void *cb_arg, int status)
{
	struct owner_ctx *ctx = cb_arg;

	if (status != 0) {
		SPDK_ERRLOG("Failed to write the owner marker '%s': %d\n",
			    ctx->key, status);
		owner_acquire_finish(ctx, status);
		return;
	}

	/* Read it back. Without this, two processes that both saw "nobody there"
	 * would both carry on. */
	owner_do_read(ctx, true);
}

static void
owner_do_put(struct owner_ctx *ctx)
{
	int len;
	int rc;

	/* if_none_match is deliberately false. COS ignores it and returns 0
	 * regardless, so passing true would look like an atomic create-if-absent
	 * while providing none of it -- the read-back below is what this relies
	 * on instead. */
	len = owner_encode(&ctx->self, &ctx->doc);
	if (len < 0) {
		owner_acquire_finish(ctx, len);
		return;
	}

	ctx->iov.iov_base = ctx->doc;
	ctx->iov.iov_len = (size_t)len;

	rc = s3_put(ctx->client, ctx->key, &ctx->iov, 1, false,
		    owner_put_done, ctx);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to submit the owner marker write for '%s': "
			    "%d\n", ctx->key, rc);
		owner_acquire_finish(ctx, rc);
	}
}

void
s3_owner_acquire(struct s3_client *client, const char *lvs_name,
		 const struct spdk_uuid *lvs_uuid, bool force,
		 s3_owner_acquire_cb cb_fn, void *cb_arg)
{
	struct owner_ctx *ctx;
	char host[S3_OWNER_NODE_ID_MAX] = {0};
	int rc;

	assert(cb_fn != NULL);

	if (!client || !lvs_name || lvs_name[0] == '\0') {
		cb_fn(cb_arg, NULL, -EINVAL);
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		cb_fn(cb_arg, NULL, -ENOMEM);
		return;
	}
	ctx->client = client;
	ctx->force  = force;
	ctx->acq_cb = cb_fn;
	ctx->cb_arg = cb_arg;

	rc = snprintf(ctx->key, sizeof(ctx->key), OWNER_KEY_FMT, lvs_name);
	if (rc < 0 || (size_t)rc >= sizeof(ctx->key)) {
		free(ctx);
		cb_fn(cb_arg, NULL, -ENAMETOOLONG);
		return;
	}

	if (gethostname(host, sizeof(host) - 1) != 0) {
		snprintf(host, sizeof(host), "unknown");
	}
	sanitise_ident(host, ctx->self.node_id, sizeof(ctx->self.node_id));
	ctx->self.pid = (int32_t)getpid();
	ctx->self.attach_ts = (uint64_t)time(NULL);
	if (lvs_uuid) {
		spdk_uuid_copy(&ctx->self.lvs_uuid, lvs_uuid);
	}
	spdk_uuid_generate(&ctx->self.nonce);

	ctx->buf = calloc(1, S3_OWNER_DOC_MAX);
	if (!ctx->buf) {
		free(ctx);
		cb_fn(cb_arg, NULL, -ENOMEM);
		return;
	}

	owner_do_read(ctx, false);
}

/* ==========================================================================
 * release
 * ========================================================================== */

static void
owner_delete_done(void *cb_arg, int status)
{
	struct owner_ctx *ctx = cb_arg;
	s3_owner_cb cb_fn = ctx->cb;
	void *user_arg = ctx->cb_arg;

	if (status != 0 && status != -ENOENT) {
		/* Worth a line, not worth failing over: a marker left behind only
		 * means the next attach needs force=true. */
		SPDK_WARNLOG("Failed to remove the owner marker '%s' (%d); the "
			     "next attach will need force=true\n",
			     ctx->key, status);
	}

	free(ctx->buf);
	free(ctx->doc);
	free(ctx);

	if (cb_fn) {
		cb_fn(user_arg, status == -ENOENT ? 0 : status);
	}
}

void
s3_owner_release(struct s3_client *client, const char *lvs_name,
		 s3_owner_cb cb_fn, void *cb_arg)
{
	struct owner_ctx *ctx;
	int rc;

	if (!client || !lvs_name || lvs_name[0] == '\0') {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		if (cb_fn) {
			cb_fn(cb_arg, -ENOMEM);
		}
		return;
	}
	ctx->client = client;
	ctx->cb     = cb_fn;
	ctx->cb_arg = cb_arg;

	rc = snprintf(ctx->key, sizeof(ctx->key), OWNER_KEY_FMT, lvs_name);
	if (rc < 0 || (size_t)rc >= sizeof(ctx->key)) {
		free(ctx);
		if (cb_fn) {
			cb_fn(cb_arg, -ENAMETOOLONG);
		}
		return;
	}

	rc = s3_delete(ctx->client, ctx->key, owner_delete_done, ctx);
	if (rc != 0) {
		SPDK_WARNLOG("Failed to submit the owner marker delete for '%s': "
			     "%d\n", ctx->key, rc);
		free(ctx);
		if (cb_fn) {
			cb_fn(cb_arg, rc);
		}
	}
}

/* ==========================================================================
	* Strict mode: mode selection, capability probe, atomic acquire, fenced release
	*
	* The legacy path above stays byte-for-byte as it was: it is the default and
	* a deployment that has not opted in must keep behaving the same way. Nothing
	* below runs unless create/attach/recover explicitly ask for strict mode.
	* ========================================================================== */

enum s3_owner_mode
s3_owner_mode_from_env(void)
{
	const char *v = getenv(S3_OWNER_MODE_ENV);

	/* Strict is the default: an unset or empty setting must not silently
	 * downgrade the ownership guarantee. Legacy is only entered when it is
	 * explicitly requested, and the dispatch layer refuses it before any
	 * ownership or destructive mutation (lvs_owner_acquire). */
	if (!v || v[0] == '\0' || strcmp(v, "strict") == 0) {
		return S3_OWNER_MODE_STRICT;
	}
	if (strcmp(v, "legacy") == 0) {
		return S3_OWNER_MODE_LEGACY;
	}

	/* No aliases, and in particular no value that means "trust me". An
	 * unrecognised setting is a configuration error: the caller refuses. */
	SPDK_ERRLOG("%s='%s' is not a recognised ownership mode; only 'legacy' "
		    "and 'strict' are accepted. Ownership operations will refuse "
		    "until it is corrected.\n", S3_OWNER_MODE_ENV, v);
	return S3_OWNER_MODE_INVALID;
}

int
s3_owner_strict_check_force(bool force)
{
	if (force) {
		SPDK_ERRLOG("strict ownership mode does not accept force: taking "
			    "over an existing marker is an out-of-band operator "
			    "action (stop the old writer, remove the marker), not "
			    "an API option. Refusing.\n");
		return -EINVAL;
	}
	return 0;
}

static void
owner_copy_str(char *dst, size_t dst_len, const char *src)
{
	if (dst_len == 0) {
		return;
	}
	snprintf(dst, dst_len, "%s", src ? src : "");
}

bool
s3_owner_caps_valid(const struct s3_owner_caps *caps,
		    const struct s3_client *client, const char *lvs_name)
{
	const char *endpoint;
	const char *bucket;

	/* Strict v2 relies on conditional create plus whole-object CAS. It
	 * never relies on conditional DELETE (measured to be ignored by the
	 * pinned local MinIO, see deps/recovery-owner-20261026/raw). */
	if (!caps || !caps->probed || !caps->atomic_create ||
	    !caps->cas_replace) {
		return false;
	}
	if (!client || !lvs_name || lvs_name[0] == '\0') {
		return false;
	}

	endpoint = s3_client_endpoint(client);
	bucket = s3_client_bucket(client);

	/* Bound to the exact target the probe measured. A capability result is
	 * not a property of "S3" in general; it is a property of this endpoint,
	 * bucket and prefix policy at the time it was probed. */
	if (caps->endpoint[0] == '\0' || caps->bucket[0] == '\0' ||
	    caps->lvs_name[0] == '\0') {
		return false;
	}
	if (strcmp(caps->endpoint, endpoint ? endpoint : "") != 0 ||
	    strcmp(caps->bucket, bucket ? bucket : "") != 0 ||
	    strcmp(caps->lvs_name, lvs_name) != 0) {
		return false;
	}
	return true;
}

/* ==========================================================================
 * Strict v2: CAS tombstone protocol
 *
 * Why not conditional DELETE: the strict v1 release was
 * `DELETE If-Match: <etag>`. An independent raw SigV4 probe against the pinned
 * local MinIO (RELEASE.2025-09-07T16-13-09Z, image sha256:69b2ec20...) showed
 * that a DELETE with a wrong If-Match returns 204 and removes the object,
 * whether or not If-Match is in SignedHeaders, while GET If-Match and
 * PUT If-Match on the same server both return 412. The header reached the
 * server; the backend simply does not evaluate it on DELETE. So the fault is
 * backend semantics, not the client, and no client change can make a
 * conditional DELETE safe there. See deps/recovery-owner-20261026/raw/.
 *
 * Protocol (owner key `<lvs>/meta/owner`, never deleted by strict code):
 *
 *   absent              free, never used
 *   {"v":2,"state":"free", nonce:R}   free (tombstone), R fresh per release
 *   {"v":2,"state":"held", nonce:N}   held by the claim whose nonce is N
 *   anything else       foreign (legacy marker, strict v1 marker, garbage):
 *                       always treated as held by somebody else
 *
 *   acquire: GET (body+ETag)
 *            absent -> PUT If-None-Match:*      held(N)
 *            free   -> PUT If-Match:<ETag>      held(N)
 *            other  -> -EBUSY, no write
 *            then GET read-back: body must be held(N); its ETag is the claim.
 *   release: GET; must be held(N) with the claim ETag
 *            -> PUT If-Match:<claim ETag>       free(R)
 *
 * Invariants:
 *
 *   I1  Every strict write is conditional (If-None-Match:* or If-Match:<etag>).
 *       There is no unconditional write and no DELETE of the owner key.
 *   I2  Every strict write carries a fresh 128-bit random nonce, so no two
 *       versions of the owner object have the same bytes. A content-derived
 *       ETag therefore never repeats, and a stale ETag cannot match a later
 *       version (ABA). The nonce is checked as well, independently of ETag.
 *   I3  A held document is only ever replaced by a CAS whose comparand is the
 *       ETag captured by the claim that wrote it. Others never write over a
 *       held or foreign document.
 *   I4  An acknowledgement that is lost is never interpreted as success; the
 *       object is read back and only the nonce decides. If the outcome cannot
 *       be proven, the result is an error (fail closed), never "probably ours".
 *   I5  No fallback: missing capability is -ENOTSUP before any owner-key write.
 *
 * What this is NOT: a lease, a storage writer fence, or crash takeover. A
 * crashed holder leaves held(N) forever; recovery from that is an
 * out-of-band operator action. Data-object writes are not fenced by this
 * marker at all.
 * ========================================================================== */

#define OWNER_V2                2
#define OWNER_STATE_HELD        "held"
#define OWNER_STATE_FREE        "free"

enum owner_doc_kind {
	OWNER_DOC_FOREIGN = 0,  /* legacy, strict v1, unparseable, unknown */
	OWNER_DOC_HELD,
	OWNER_DOC_FREE,
};

static int
owner_encode_v2(const struct s3_owner_info *info, const char *state,
		char **out_doc)
{
	char uuid_str[SPDK_UUID_STRING_LEN];
	char nonce_str[SPDK_UUID_STRING_LEN];
	char *doc;
	int len;

	spdk_uuid_fmt_lower(uuid_str, sizeof(uuid_str), &info->lvs_uuid);
	spdk_uuid_fmt_lower(nonce_str, sizeof(nonce_str), &info->nonce);

	doc = calloc(1, S3_OWNER_DOC_MAX);
	if (!doc) {
		return -ENOMEM;
	}

	len = snprintf(doc, S3_OWNER_DOC_MAX,
		       "{\"v\":%d,\"state\":\"%s\",\"node_id\":\"%s\","
		       "\"pid\":%" PRId32 ",\"attach_ts\":%" PRIu64 ","
		       "\"lvs_uuid\":\"%s\",\"nonce\":\"%s\"}",
		       OWNER_V2, state, info->node_id, info->pid,
		       info->attach_ts, uuid_str, nonce_str);
	if (len < 0 || len >= S3_OWNER_DOC_MAX) {
		free(doc);
		return -ENAMETOOLONG;
	}

	*out_doc = doc;
	return len;
}

struct owner_decoded_v2 {
	uint32_t v;
	char *state;
	char *node_id;
	uint32_t pid;
	uint64_t attach_ts;
	char *lvs_uuid;
	char *nonce;
};

static const struct spdk_json_object_decoder owner_decoders_v2[] = {
	{"v",         offsetof(struct owner_decoded_v2, v),         spdk_json_decode_uint32, true},
	{"state",     offsetof(struct owner_decoded_v2, state),     spdk_json_decode_string, true},
	{"node_id",   offsetof(struct owner_decoded_v2, node_id),   spdk_json_decode_string, true},
	{"pid",       offsetof(struct owner_decoded_v2, pid),       spdk_json_decode_uint32, true},
	{"attach_ts", offsetof(struct owner_decoded_v2, attach_ts), spdk_json_decode_uint64, true},
	{"lvs_uuid",  offsetof(struct owner_decoded_v2, lvs_uuid),  spdk_json_decode_string, true},
	{"nonce",     offsetof(struct owner_decoded_v2, nonce),     spdk_json_decode_string, true},
};

/* Classify a document read from the owner key. Anything that is not exactly a
 * v2 held/free document with a parseable non-null nonce is FOREIGN, and
 * FOREIGN is never written over by strict code. */
static enum owner_doc_kind
owner_classify(const char *buf, size_t len, struct s3_owner_info *out)
{
	struct owner_decoded_v2 dec = {0};
	struct spdk_json_val *values = NULL;
	enum owner_doc_kind kind = OWNER_DOC_FOREIGN;
	char *copy;
	ssize_t nvals;

	memset(out, 0, sizeof(*out));
	if (len == 0 || len > S3_OWNER_DOC_MAX) {
		return OWNER_DOC_FOREIGN;
	}

	copy = malloc(len + 1);
	if (!copy) {
		return OWNER_DOC_FOREIGN;
	}
	memcpy(copy, buf, len);
	copy[len] = '\0';

	nvals = spdk_json_parse(copy, len, NULL, 0, NULL,
				SPDK_JSON_PARSE_FLAG_DECODE_IN_PLACE);
	if (nvals <= 0) {
		goto out;
	}
	values = calloc((size_t)nvals, sizeof(*values));
	if (!values) {
		goto out;
	}
	nvals = spdk_json_parse(copy, len, values, (size_t)nvals, NULL,
				SPDK_JSON_PARSE_FLAG_DECODE_IN_PLACE);
	if (nvals <= 0) {
		goto out;
	}
	/* Not relaxed: an unknown key makes the document FOREIGN. */
	if (spdk_json_decode_object(values, owner_decoders_v2,
				    SPDK_COUNTOF(owner_decoders_v2), &dec) != 0) {
		goto out;
	}

	if (dec.node_id) {
		snprintf(out->node_id, sizeof(out->node_id), "%s", dec.node_id);
	}
	out->pid = (int32_t)dec.pid;
	out->attach_ts = dec.attach_ts;
	if (dec.lvs_uuid) {
		spdk_uuid_parse(&out->lvs_uuid, dec.lvs_uuid);
	}

	if (dec.v != OWNER_V2 || !dec.state || !dec.nonce ||
	    spdk_uuid_parse(&out->nonce, dec.nonce) != 0 ||
	    spdk_uuid_is_null(&out->nonce)) {
		goto out;       /* legacy / strict v1 / malformed: FOREIGN */
	}
	if (strcmp(dec.state, OWNER_STATE_HELD) == 0) {
		kind = OWNER_DOC_HELD;
	} else if (strcmp(dec.state, OWNER_STATE_FREE) == 0) {
		kind = OWNER_DOC_FREE;
	}

out:
	free(dec.state);
	free(dec.node_id);
	free(dec.lvs_uuid);
	free(dec.nonce);
	free(values);
	free(copy);
	return kind;
}

static bool
owner_status_is_precondition(int status)
{
	/* 412 -> -EEXIST, 409 -> -EBUSY, and 404 (-ENOENT) for an If-Match
	 * against a key that no longer exists. In every case the server
	 * evaluated the condition and did not write. */
	return status == -EEXIST || status == -EBUSY || status == -ENOENT;
}

static void
owner_self_identity(struct s3_owner_info *info, const struct spdk_uuid *lvs_uuid)
{
	char host[S3_OWNER_NODE_ID_MAX] = {0};

	memset(info, 0, sizeof(*info));
	if (gethostname(host, sizeof(host) - 1) != 0) {
		snprintf(host, sizeof(host), "unknown");
	}
	sanitise_ident(host, info->node_id, sizeof(info->node_id));
	info->pid = (int32_t)getpid();
	info->attach_ts = (uint64_t)time(NULL);
	if (lvs_uuid) {
		spdk_uuid_copy(&info->lvs_uuid, lvs_uuid);
	}
	spdk_uuid_generate(&info->nonce);
}

/* One context for all three strict state machines. */
struct owner_strict_ctx {
	struct s3_client       *client;
	char                    key[OWNER_KEY_MAX];
	char                    probe_key[OWNER_KEY_MAX];
	char                    lvs_name[S3_OWNER_LVS_NAME_MAX];

	struct s3_owner_info    self;       /* acquire: held doc identity */
	struct s3_owner_info    rel;        /* release: tombstone identity */
	struct s3_owner_caps   *out_caps;   /* probe only, caller-owned */
	struct s3_owner_claim   claim;      /* acquire result / release input */

	char                   *buf;        /* body read buffer */
	char                   *doc;        /* body being written */
	size_t                  doc_len;
	struct iovec            iov;

	/* Probe bodies: A (create), B (dup create), C (wrong CAS), D (right
	 * CAS), E (stale CAS). */
	char                   *pdoc[5];
	size_t                  pdoc_len[5];
	int                     probe_step;
	char                    etag_a[S3_OWNER_ETAG_MAX];
	char                    etag_d[S3_OWNER_ETAG_MAX];

	char                    etag[S3_OWNER_ETAG_MAX];  /* last read ETag */

	struct s3_owner_info    holder;
	bool                    holder_valid;

	bool                    ambiguous;  /* write completion inconclusive */

	s3_owner_claim_cb       claim_cb;
	s3_owner_cb             cb;
	void                   *cb_arg;
};

static void
owner_strict_free(struct owner_strict_ctx *ctx)
{
	int i;

	free(ctx->buf);
	free(ctx->doc);
	for (i = 0; i < 5; i++) {
		free(ctx->pdoc[i]);
	}
	free(ctx);
}

static void
owner_strict_finish(struct owner_strict_ctx *ctx, int status)
{
	s3_owner_claim_cb claim_cb = ctx->claim_cb;
	s3_owner_cb cb = ctx->cb;
	void *cb_arg = ctx->cb_arg;
	struct s3_owner_claim claim = ctx->claim;
	struct s3_owner_info self = ctx->self;
	struct s3_owner_caps *out_caps = ctx->out_caps;

	if (out_caps && status != 0) {
		memset(out_caps, 0, sizeof(*out_caps));
	}

	owner_strict_free(ctx);

	if (claim_cb) {
		claim_cb(cb_arg, status == 0 ? &self : NULL,
			 status == 0 ? &claim : NULL, status);
	}
	if (cb) {
		cb(cb_arg, status);
	}
}

static int
owner_probe_key(char *out, size_t out_len, const char *lvs_name)
{
	struct spdk_uuid probe_uuid;
	char uuid_str[SPDK_UUID_STRING_LEN];
	int rc;

	spdk_uuid_generate(&probe_uuid);
	spdk_uuid_fmt_lower(uuid_str, sizeof(uuid_str), &probe_uuid);
	rc = snprintf(out, out_len, "%s/meta/owner-probe-%s", lvs_name,
		      uuid_str);
	if (rc < 0 || (size_t)rc >= out_len) {
		return -ENAMETOOLONG;
	}
	return 0;
}

/* ==========================================================================
 * Probe
 *
 * Steps on a unique probe key (never the owner key):
 *   1 PUT If-None-Match:* A          -> 0
 *   2 PUT If-None-Match:* B          -> 412/409
 *   3 GET                            -> A, ETag EA
 *   4 PUT If-Match:"wrong" C         -> 412/409
 *   5 GET                            -> A, ETag EA (unchanged)
 *   6 PUT If-Match:EA D              -> 0
 *   7 GET                            -> D, ETag ED != EA
 *   8 PUT If-Match:EA E (stale)      -> 412/409
 *   9 GET                            -> D, ETag ED (unchanged)
 *  10 DELETE (unconditional, unique key that only this probe ever wrote)
 * ========================================================================== */

static void probe_next(struct owner_strict_ctx *ctx);

static void
probe_cleanup_and_fail(struct owner_strict_ctx *ctx, int status)
{
	/* The probe key is unique to this probe; removing it unconditionally
	 * cannot touch anybody else's object. Best effort. */
	if (s3_delete(ctx->client, ctx->probe_key, NULL, NULL) != 0) {
		SPDK_WARNLOG("could not remove probe object '%s'; it is unique "
			     "and harmless\n", ctx->probe_key);
	}
	owner_strict_finish(ctx, status);
}

static void
probe_write_done(void *cb_arg, int status)
{
	struct owner_strict_ctx *ctx = cb_arg;
	int step = ctx->probe_step;

	switch (step) {
	case 1:
	case 6:
		if (status != 0) {
			if (step == 1 && owner_status_is_precondition(status)) {
				SPDK_ERRLOG("probe key '%s' unexpectedly exists; "
					    "refusing strict mode\n",
					    ctx->probe_key);
				owner_strict_finish(ctx, -EBUSY);
				return;
			}
			SPDK_ERRLOG("strict ownership probe step %d on '%s' "
				    "failed (%d)%s\n", step, ctx->probe_key,
				    status, step == 6 ? ": a CAS with the "
				    "correct ETag was not applied" : "");
			probe_cleanup_and_fail(ctx, step == 6 &&
					       owner_status_is_precondition(status) ?
					       -ENOTSUP : status);
			return;
		}
		break;
	case 2:
	case 4:
	case 8:
		if (status == 0) {
			SPDK_ERRLOG("strict ownership probe failed at step %d: "
				    "the endpoint accepted a %s PUT on '%s'. "
				    "%s is not enforced; strict mode is "
				    "refused.\n", step,
				    step == 2 ? "duplicate 'If-None-Match: *'" :
				    step == 4 ? "wrong-ETag 'If-Match'" :
				    "stale-ETag 'If-Match'", ctx->probe_key,
				    step == 2 ? "Conditional create" :
				    "Compare-and-swap replace");
			probe_cleanup_and_fail(ctx, -ENOTSUP);
			return;
		}
		if (status != -EEXIST && status != -EBUSY) {
			SPDK_ERRLOG("strict ownership probe step %d on '%s': "
				    "expected 412/409, got %d\n", step,
				    ctx->probe_key, status);
			probe_cleanup_and_fail(ctx, status == -ENOENT ?
					       -ENOTSUP : status);
			return;
		}
		break;
	default:
		probe_cleanup_and_fail(ctx, -EIO);
		return;
	}

	ctx->probe_step++;
	probe_next(ctx);
}

static void
probe_read_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct owner_strict_ctx *ctx = cb_arg;
	int step = ctx->probe_step;
	int want = (step == 3 || step == 5) ? 0 : 3;   /* A or D */

	if (status != 0) {
		probe_cleanup_and_fail(ctx, status);
		return;
	}
	if (bytes_read != ctx->pdoc_len[want] ||
	    memcmp(ctx->buf, ctx->pdoc[want], ctx->pdoc_len[want]) != 0) {
		SPDK_ERRLOG("strict ownership probe failed at step %d: '%s' "
			    "does not hold the expected bytes. A rejected or "
			    "applied condition did not behave atomically.\n",
			    step, ctx->probe_key);
		probe_cleanup_and_fail(ctx, -ENOTSUP);
		return;
	}
	if (ctx->etag[0] == '\0') {
		SPDK_ERRLOG("strict ownership probe: no ETag for '%s'; CAS "
			    "cannot be used\n", ctx->probe_key);
		probe_cleanup_and_fail(ctx, -EIO);
		return;
	}

	switch (step) {
	case 3:
		owner_copy_str(ctx->etag_a, sizeof(ctx->etag_a), ctx->etag);
		break;
	case 5:
		if (strcmp(ctx->etag, ctx->etag_a) != 0) {
			SPDK_ERRLOG("strict ownership probe: ETag of '%s' changed "
				    "after a rejected CAS\n", ctx->probe_key);
			probe_cleanup_and_fail(ctx, -ENOTSUP);
			return;
		}
		break;
	case 7:
		if (strcmp(ctx->etag, ctx->etag_a) == 0) {
			SPDK_ERRLOG("strict ownership probe: ETag of '%s' did not "
				    "change when its bytes changed; ETag cannot "
				    "serve as a CAS comparand\n", ctx->probe_key);
			probe_cleanup_and_fail(ctx, -ENOTSUP);
			return;
		}
		owner_copy_str(ctx->etag_d, sizeof(ctx->etag_d), ctx->etag);
		break;
	case 9:
		if (strcmp(ctx->etag, ctx->etag_d) != 0) {
			SPDK_ERRLOG("strict ownership probe: ETag of '%s' changed "
				    "after a rejected stale CAS\n", ctx->probe_key);
			probe_cleanup_and_fail(ctx, -ENOTSUP);
			return;
		}
		break;
	default:
		probe_cleanup_and_fail(ctx, -EIO);
		return;
	}

	ctx->probe_step++;
	probe_next(ctx);
}

static void
probe_delete_done(void *cb_arg, int status)
{
	struct owner_strict_ctx *ctx = cb_arg;
	struct s3_owner_caps *caps = ctx->out_caps;

	if (status != 0 && status != -ENOENT) {
		SPDK_ERRLOG("strict ownership probe could not remove its own "
			    "probe object '%s' (%d); strict mode cannot claim a "
			    "clean probe\n", ctx->probe_key, status);
		owner_strict_finish(ctx, status);
		return;
	}

	memset(caps, 0, sizeof(*caps));
	caps->probed = true;
	caps->atomic_create = true;
	caps->cas_replace = true;
	owner_copy_str(caps->endpoint, sizeof(caps->endpoint),
		       s3_client_endpoint(ctx->client));
	owner_copy_str(caps->bucket, sizeof(caps->bucket),
		       s3_client_bucket(ctx->client));
	owner_copy_str(caps->lvs_name, sizeof(caps->lvs_name), ctx->lvs_name);
	caps->probed_at = (uint64_t)time(NULL);

	owner_strict_finish(ctx, 0);
}

static void
probe_next(struct owner_strict_ctx *ctx)
{
	int rc;
	int idx;

	switch (ctx->probe_step) {
	case 1:
	case 2:
		idx = ctx->probe_step - 1;
		ctx->iov.iov_base = ctx->pdoc[idx];
		ctx->iov.iov_len = ctx->pdoc_len[idx];
		rc = s3_put(ctx->client, ctx->probe_key, &ctx->iov, 1, true,
			    probe_write_done, ctx);
		break;
	case 4:
		ctx->iov.iov_base = ctx->pdoc[2];
		ctx->iov.iov_len = ctx->pdoc_len[2];
		rc = s3_put_if_match(ctx->client, ctx->probe_key, &ctx->iov, 1,
				     "\"s3lvol-owner-probe-wrong-etag\"",
				     probe_write_done, ctx);
		break;
	case 6:
		ctx->iov.iov_base = ctx->pdoc[3];
		ctx->iov.iov_len = ctx->pdoc_len[3];
		rc = s3_put_if_match(ctx->client, ctx->probe_key, &ctx->iov, 1,
				     ctx->etag_a, probe_write_done, ctx);
		break;
	case 8:
		ctx->iov.iov_base = ctx->pdoc[4];
		ctx->iov.iov_len = ctx->pdoc_len[4];
		rc = s3_put_if_match(ctx->client, ctx->probe_key, &ctx->iov, 1,
				     ctx->etag_a, probe_write_done, ctx);
		break;
	case 3:
	case 5:
	case 7:
	case 9:
		ctx->etag[0] = '\0';
		rc = s3_get_range_ex(ctx->client, ctx->probe_key, 0,
				     S3_OWNER_DOC_MAX, ctx->buf, ctx->etag,
				     sizeof(ctx->etag), probe_read_done, ctx);
		break;
	case 10:
		rc = s3_delete(ctx->client, ctx->probe_key, probe_delete_done,
			       ctx);
		break;
	default:
		rc = -EIO;
		break;
	}

	if (rc != 0) {
		if (ctx->probe_step == 1) {
			owner_strict_finish(ctx, rc);
		} else {
			probe_cleanup_and_fail(ctx, rc);
		}
	}
}

void
s3_owner_probe(struct s3_client *client, const char *lvs_name,
	       struct s3_owner_caps *caps, s3_owner_cb cb_fn, void *cb_arg)
{
	static const char *const names[5] = {
		"probe-a", "probe-b", "probe-c", "probe-d", "probe-e"
	};
	struct owner_strict_ctx *ctx;
	struct s3_owner_info info;
	int rc;
	int i;

	if (!client || !lvs_name || lvs_name[0] == '\0' || !caps) {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}
	memset(caps, 0, sizeof(*caps));

	if (strlen(lvs_name) >= S3_OWNER_LVS_NAME_MAX) {
		if (cb_fn) {
			cb_fn(cb_arg, -ENAMETOOLONG);
		}
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		if (cb_fn) {
			cb_fn(cb_arg, -ENOMEM);
		}
		return;
	}
	ctx->client = client;
	ctx->cb = cb_fn;
	ctx->cb_arg = cb_arg;
	ctx->out_caps = caps;
	owner_copy_str(ctx->lvs_name, sizeof(ctx->lvs_name), lvs_name);

	rc = owner_probe_key(ctx->probe_key, sizeof(ctx->probe_key), lvs_name);
	if (rc != 0) {
		owner_strict_finish(ctx, rc);
		return;
	}

	for (i = 0; i < 5; i++) {
		memset(&info, 0, sizeof(info));
		owner_copy_str(info.node_id, sizeof(info.node_id), names[i]);
		info.pid = (int32_t)getpid();
		info.attach_ts = (uint64_t)time(NULL);
		spdk_uuid_generate(&info.nonce);
		rc = owner_encode_v2(&info, "probe", &ctx->pdoc[i]);
		if (rc <= 0) {
			owner_strict_finish(ctx, rc == 0 ? -EINVAL : rc);
			return;
		}
		ctx->pdoc_len[i] = (size_t)rc;
	}

	ctx->buf = calloc(1, S3_OWNER_DOC_MAX);
	if (!ctx->buf) {
		owner_strict_finish(ctx, -ENOMEM);
		return;
	}

	ctx->probe_step = 1;
	probe_next(ctx);
}

/* ==========================================================================
 * Strict acquire
 * ========================================================================== */

static void acq_step_read_back(struct owner_strict_ctx *ctx);
static void acq_step_read_holder(struct owner_strict_ctx *ctx);

static void
acq_write_done(void *cb_arg, int status)
{
	struct owner_strict_ctx *ctx = cb_arg;

	if (status == 0) {
		ctx->ambiguous = false;
		acq_step_read_back(ctx);
		return;
	}

	if (owner_status_is_precondition(status)) {
		/* The server evaluated the condition and did not write: some
		 * other process changed the marker since our read. */
		acq_step_read_holder(ctx);
		return;
	}

	/* Transport error / timeout / 5xx: the write may or may not have been
	 * applied. Never retry blindly; read back and let the nonce decide. */
	SPDK_WARNLOG("strict acquire of '%s': conditional PUT completion was "
		     "inconclusive (%d); reading the marker back to decide\n",
		     ctx->key, status);
	ctx->ambiguous = true;
	acq_step_read_back(ctx);
}

static void
acq_read_back_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct owner_strict_ctx *ctx = cb_arg;
	struct s3_owner_info got;
	enum owner_doc_kind kind;
	char desc[256];

	if (status == -ENOENT) {
		SPDK_ERRLOG("strict acquire of '%s': no marker after the "
			    "conditional PUT%s; ownership is unproven\n",
			    ctx->key, ctx->ambiguous ? " (ack lost)" : "");
		owner_strict_finish(ctx, ctx->ambiguous ? -EIO : -EBUSY);
		return;
	}
	if (status != 0) {
		SPDK_ERRLOG("strict acquire of '%s': cannot read the marker back "
			    "(%d); ownership is unproven, refusing to proceed\n",
			    ctx->key, status);
		owner_strict_finish(ctx, -EIO);
		return;
	}

	kind = owner_classify(ctx->buf, bytes_read, &got);
	if (kind != OWNER_DOC_HELD ||
	    spdk_uuid_compare(&got.nonce, &ctx->self.nonce) != 0) {
		if (kind == OWNER_DOC_HELD) {
			ctx->holder = got;
			ctx->holder_valid = true;
		}
		s3_owner_info_str(kind == OWNER_DOC_HELD ? &got : NULL, desc,
				  sizeof(desc));
		SPDK_ERRLOG("strict acquire of '%s' not proven: the marker is %s "
			    "(%s)%s\n", ctx->key,
			    kind == OWNER_DOC_HELD ? "held by another claim" :
			    kind == OWNER_DOC_FREE ? "still free" : "foreign",
			    desc, ctx->ambiguous ? "; the PUT ack was lost" : "");
		/* Not ours. If the ack was lost and the marker is still free,
		 * the write may yet land later; report -EIO, never success. */
		owner_strict_finish(ctx, (ctx->ambiguous &&
					  kind != OWNER_DOC_HELD) ? -EIO : -EBUSY);
		return;
	}

	if (ctx->etag[0] == '\0') {
		/* Ours, but a later release could not be conditional. Fail
		 * closed and leave the marker; an operator must clear it. */
		SPDK_ERRLOG("strict acquire of '%s': marker is ours but the "
			    "response carried no ETag; failing the acquire and "
			    "leaving the marker in place\n", ctx->key);
		owner_strict_finish(ctx, -EIO);
		return;
	}

	ctx->claim.info = ctx->self;
	owner_copy_str(ctx->claim.etag, sizeof(ctx->claim.etag), ctx->etag);
	ctx->claim.valid = true;
	owner_strict_finish(ctx, 0);
}

static void
acq_step_read_back(struct owner_strict_ctx *ctx)
{
	int rc;

	ctx->etag[0] = '\0';
	rc = s3_get_range_ex(ctx->client, ctx->key, 0, S3_OWNER_DOC_MAX,
			     ctx->buf, ctx->etag, sizeof(ctx->etag),
			     acq_read_back_done, ctx);
	if (rc != 0) {
		owner_strict_finish(ctx, -EIO);
	}
}

static void
acq_holder_read_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct owner_strict_ctx *ctx = cb_arg;
	struct s3_owner_info got;

	if (status == 0 &&
	    owner_classify(ctx->buf, bytes_read, &got) == OWNER_DOC_HELD) {
		ctx->holder = got;
		ctx->holder_valid = true;
	}
	/* Whatever is there now, our conditional write was rejected. */
	owner_strict_finish(ctx, -EBUSY);
}

static void
acq_step_read_holder(struct owner_strict_ctx *ctx)
{
	int rc;

	rc = s3_get_range_ex(ctx->client, ctx->key, 0, S3_OWNER_DOC_MAX,
			     ctx->buf, NULL, 0, acq_holder_read_done, ctx);
	if (rc != 0) {
		owner_strict_finish(ctx, -EBUSY);
	}
}

static void
acq_initial_read_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct owner_strict_ctx *ctx = cb_arg;
	struct s3_owner_info got;
	enum owner_doc_kind kind;
	char desc[256];
	int rc;

	ctx->iov.iov_base = ctx->doc;
	ctx->iov.iov_len = ctx->doc_len;

	if (status == -ENOENT) {
		/* Never used: create exactly once. */
		rc = s3_put(ctx->client, ctx->key, &ctx->iov, 1, true,
			    acq_write_done, ctx);
		if (rc != 0) {
			owner_strict_finish(ctx, rc);
		}
		return;
	}
	if (status != 0) {
		SPDK_ERRLOG("strict acquire of '%s': cannot read the marker "
			    "(%d)\n", ctx->key, status);
		owner_strict_finish(ctx, status);
		return;
	}

	kind = owner_classify(ctx->buf, bytes_read, &got);
	if (kind == OWNER_DOC_FREE) {
		if (ctx->etag[0] == '\0') {
			SPDK_ERRLOG("strict acquire of '%s': free marker has no "
				    "ETag; cannot CAS\n", ctx->key);
			owner_strict_finish(ctx, -EIO);
			return;
		}
		rc = s3_put_if_match(ctx->client, ctx->key, &ctx->iov, 1,
				     ctx->etag, acq_write_done, ctx);
		if (rc != 0) {
			owner_strict_finish(ctx, rc);
		}
		return;
	}

	if (kind == OWNER_DOC_HELD) {
		ctx->holder = got;
		ctx->holder_valid = true;
		s3_owner_info_str(&got, desc, sizeof(desc));
		SPDK_ERRLOG("strict acquire of '%s' refused: held by %s. Strict "
			    "mode has no force; a crashed holder must be cleared "
			    "out of band after its writer is stopped.\n",
			    ctx->key, desc);
	} else {
		SPDK_ERRLOG("strict acquire of '%s' refused: the marker is not a "
			    "strict v2 document (legacy, strict v1 or "
			    "unparseable) and is treated as held by an unknown "
			    "writer. Mixed-version deployments are not "
			    "supported.\n", ctx->key);
	}
	owner_strict_finish(ctx, -EBUSY);
}

void
s3_owner_acquire_strict(struct s3_client *client, const char *lvs_name,
			const struct spdk_uuid *lvs_uuid,
			const struct s3_owner_caps *caps,
			s3_owner_claim_cb cb_fn, void *cb_arg)
{
	struct owner_strict_ctx *ctx;
	int rc;

	assert(cb_fn != NULL);

	if (!client || !lvs_name || lvs_name[0] == '\0') {
		cb_fn(cb_arg, NULL, NULL, -EINVAL);
		return;
	}

	/* Fail closed before any ownership request. */
	if (!s3_owner_caps_valid(caps, client, lvs_name)) {
		SPDK_ERRLOG("strict acquire of '%s' refused: no valid capability "
			    "record for this endpoint/bucket/lvstore. Probe the "
			    "backend first; there is no fallback to legacy.\n",
			    lvs_name);
		cb_fn(cb_arg, NULL, NULL, -ENOTSUP);
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		cb_fn(cb_arg, NULL, NULL, -ENOMEM);
		return;
	}
	ctx->client = client;
	ctx->claim_cb = cb_fn;
	ctx->cb_arg = cb_arg;

	rc = snprintf(ctx->key, sizeof(ctx->key), OWNER_KEY_FMT, lvs_name);
	if (rc < 0 || (size_t)rc >= sizeof(ctx->key)) {
		owner_strict_finish(ctx, -ENAMETOOLONG);
		return;
	}

	owner_self_identity(&ctx->self, lvs_uuid);

	rc = owner_encode_v2(&ctx->self, OWNER_STATE_HELD, &ctx->doc);
	if (rc <= 0) {
		owner_strict_finish(ctx, rc == 0 ? -EINVAL : rc);
		return;
	}
	ctx->doc_len = (size_t)rc;
	ctx->buf = calloc(1, S3_OWNER_DOC_MAX);
	if (!ctx->buf) {
		owner_strict_finish(ctx, -ENOMEM);
		return;
	}

	ctx->etag[0] = '\0';
	rc = s3_get_range_ex(client, ctx->key, 0, S3_OWNER_DOC_MAX, ctx->buf,
			     ctx->etag, sizeof(ctx->etag),
			     acq_initial_read_done, ctx);
	if (rc != 0) {
		owner_strict_finish(ctx, rc);
	}
}

/* ==========================================================================
 * Strict release: CAS held(N) -> free(R)
 * ========================================================================== */

static void
rel_read_back_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct owner_strict_ctx *ctx = cb_arg;
	struct s3_owner_info got;
	enum owner_doc_kind kind;

	if (status != 0) {
		SPDK_ERRLOG("strict release of '%s': ack lost and the marker "
			    "cannot be read (%d); release outcome unknown\n",
			    ctx->key, status);
		owner_strict_finish(ctx, -EIO);
		return;
	}

	kind = owner_classify(ctx->buf, bytes_read, &got);
	if (kind == OWNER_DOC_FREE &&
	    spdk_uuid_compare(&got.nonce, &ctx->rel.nonce) == 0) {
		/* Our tombstone is there: the release was applied. */
		owner_strict_finish(ctx, 0);
		return;
	}
	if (kind == OWNER_DOC_HELD &&
	    spdk_uuid_compare(&got.nonce, &ctx->claim.info.nonce) == 0 &&
	    strcmp(ctx->etag, ctx->claim.etag) == 0) {
		/* Still exactly our claim: not applied (yet). The claim stays
		 * valid; the caller may retry. */
		SPDK_ERRLOG("strict release of '%s': ack lost and the marker "
			    "is still held by this claim; release not "
			    "applied\n", ctx->key);
		owner_strict_finish(ctx, -EIO);
		return;
	}
	/* Something else is there. Our held version is gone, which can only
	 * have happened through our CAS (or an out-of-band operator action).
	 * Either way this claim no longer holds the marker, and nothing was
	 * removed or replaced that belonged to somebody else. */
	SPDK_WARNLOG("strict release of '%s': ack lost; the marker no longer "
		     "holds this claim (now %s)\n", ctx->key,
		     kind == OWNER_DOC_HELD ? "held by another claim" :
		     kind == OWNER_DOC_FREE ? "a different free tombstone" :
		     "foreign");
	owner_strict_finish(ctx, 0);
}

static void
rel_write_done(void *cb_arg, int status)
{
	struct owner_strict_ctx *ctx = cb_arg;
	int rc;

	if (status == 0) {
		owner_strict_finish(ctx, 0);
		return;
	}
	if (owner_status_is_precondition(status)) {
		SPDK_ERRLOG("strict release of '%s': the conditional replace was "
			    "rejected (the marker changed); nothing was "
			    "written\n", ctx->key);
		owner_strict_finish(ctx, -EBUSY);
		return;
	}

	SPDK_WARNLOG("strict release of '%s': completion inconclusive (%d); "
		     "reading back\n", ctx->key, status);
	ctx->etag[0] = '\0';
	rc = s3_get_range_ex(ctx->client, ctx->key, 0, S3_OWNER_DOC_MAX,
			     ctx->buf, ctx->etag, sizeof(ctx->etag),
			     rel_read_back_done, ctx);
	if (rc != 0) {
		owner_strict_finish(ctx, -EIO);
	}
}

static void
rel_read_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct owner_strict_ctx *ctx = cb_arg;
	struct s3_owner_info got;
	enum owner_doc_kind kind;
	char desc[256];
	int rc;

	if (status == -ENOENT) {
		/* Strict code never deletes the owner key, so this is an
		 * out-of-band removal. Nothing to release. */
		owner_strict_finish(ctx, -ENOENT);
		return;
	}
	if (status != 0) {
		owner_strict_finish(ctx, status);
		return;
	}

	kind = owner_classify(ctx->buf, bytes_read, &got);
	if (kind != OWNER_DOC_HELD ||
	    spdk_uuid_compare(&got.nonce, &ctx->claim.info.nonce) != 0) {
		s3_owner_info_str(kind == OWNER_DOC_HELD ? &got : NULL, desc,
				  sizeof(desc));
		SPDK_ERRLOG("strict release of '%s' refused: the marker no "
			    "longer holds this claim (%s %s); a stale release "
			    "must not replace it\n", ctx->key,
			    kind == OWNER_DOC_HELD ? "held by" :
			    kind == OWNER_DOC_FREE ? "free," : "foreign,",
			    desc);
		if (kind == OWNER_DOC_HELD) {
			ctx->holder = got;
			ctx->holder_valid = true;
		}
		owner_strict_finish(ctx, -EBUSY);
		return;
	}
	if (ctx->etag[0] == '\0' || strcmp(ctx->etag, ctx->claim.etag) != 0) {
		SPDK_ERRLOG("strict release of '%s' refused: the marker's ETag "
			    "changed since this claim acquired it\n", ctx->key);
		owner_strict_finish(ctx, -EBUSY);
		return;
	}

	/* CAS against the claim's ETag, not the one just read (they are equal
	 * here, but the claim is the authority). */
	ctx->iov.iov_base = ctx->doc;
	ctx->iov.iov_len = ctx->doc_len;
	rc = s3_put_if_match(ctx->client, ctx->key, &ctx->iov, 1,
			     ctx->claim.etag, rel_write_done, ctx);
	if (rc != 0) {
		owner_strict_finish(ctx, rc);
	}
}

void
s3_owner_release_strict(struct s3_client *client, const char *lvs_name,
			const struct s3_owner_claim *claim,
			const struct s3_owner_caps *caps,
			s3_owner_cb cb_fn, void *cb_arg)
{
	struct owner_strict_ctx *ctx;
	int rc;

	if (!client || !lvs_name || lvs_name[0] == '\0' || !claim ||
	    !claim->valid || claim->etag[0] == '\0' ||
	    spdk_uuid_is_null(&claim->info.nonce)) {
		if (cb_fn) {
			cb_fn(cb_arg, -EINVAL);
		}
		return;
	}
	if (!s3_owner_caps_valid(caps, client, lvs_name)) {
		SPDK_ERRLOG("strict release of '%s' refused: no valid capability "
			    "record. An unconditional write or delete is not an "
			    "acceptable fallback.\n", lvs_name);
		if (cb_fn) {
			cb_fn(cb_arg, -ENOTSUP);
		}
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		if (cb_fn) {
			cb_fn(cb_arg, -ENOMEM);
		}
		return;
	}
	ctx->client = client;
	ctx->cb = cb_fn;
	ctx->cb_arg = cb_arg;
	ctx->claim = *claim;

	rc = snprintf(ctx->key, sizeof(ctx->key), OWNER_KEY_FMT, lvs_name);
	if (rc < 0 || (size_t)rc >= sizeof(ctx->key)) {
		owner_strict_finish(ctx, -ENAMETOOLONG);
		return;
	}

	/* The tombstone gets its own fresh nonce (I2), so every free version
	 * is unique and a later acquirer's CAS comparand cannot be replayed. */
	owner_self_identity(&ctx->rel, &claim->info.lvs_uuid);
	rc = owner_encode_v2(&ctx->rel, OWNER_STATE_FREE, &ctx->doc);
	if (rc <= 0) {
		owner_strict_finish(ctx, rc == 0 ? -EINVAL : rc);
		return;
	}
	ctx->doc_len = (size_t)rc;

	ctx->buf = calloc(1, S3_OWNER_DOC_MAX);
	if (!ctx->buf) {
		owner_strict_finish(ctx, -ENOMEM);
		return;
	}

	ctx->etag[0] = '\0';
	rc = s3_get_range_ex(client, ctx->key, 0, S3_OWNER_DOC_MAX,
			     ctx->buf, ctx->etag, sizeof(ctx->etag),
			     rel_read_done, ctx);
	if (rc != 0) {
		owner_strict_finish(ctx, rc);
	}
}

/* ==========================================================================
	* Strict verify: read-only proof that the marker is still exactly a claim
	*
	* Used before a claim this process retained (after a failed teardown) is used
	* again. It issues one GET and nothing else: no PUT, no DELETE, no CAS. The
	* claim's nonce AND ETag must both match a v2 held document; hostname and pid
	* are display fields and are not consulted. The lvstore uuid recorded in the
	* held document must match too, so a claim cannot be carried over to a
	* recreated lvstore of the same name.
	* ========================================================================== */

static void
verify_read_done(void *cb_arg, uint64_t bytes_read, int status)
{
	struct owner_strict_ctx *ctx = cb_arg;
	struct s3_owner_info got;
	enum owner_doc_kind kind;

	if (status != 0) {
		/* -ENOENT: absent. Strict code never deletes the key, so this
		 * is an out-of-band removal: the claim does not hold it. Any
		 * other error: cannot prove anything. Both fail closed. */
		SPDK_ERRLOG("strict verify of '%s': marker %s (%d); the retained "
			    "claim is not proven and is not used\n", ctx->key,
			    status == -ENOENT ? "is absent" : "cannot be read",
			    status);
		owner_strict_finish(ctx, status);
		return;
	}

	kind = owner_classify(ctx->buf, bytes_read, &got);
	if (kind != OWNER_DOC_HELD ||
	    spdk_uuid_compare(&got.nonce, &ctx->claim.info.nonce) != 0) {
		SPDK_ERRLOG("strict verify of '%s': the marker no longer holds the "
			    "retained claim (%s); it is not used\n", ctx->key,
			    kind == OWNER_DOC_HELD ? "held by another nonce" :
			    kind == OWNER_DOC_FREE ? "free" : "foreign");
		owner_strict_finish(ctx, -EBUSY);
		return;
	}
	if (ctx->etag[0] == '\0' || strcmp(ctx->etag, ctx->claim.etag) != 0) {
		SPDK_ERRLOG("strict verify of '%s': the marker's ETag changed since "
			    "the claim acquired it; it is not used\n", ctx->key);
		owner_strict_finish(ctx, -EBUSY);
		return;
	}
	/* The held document must carry exactly the lvstore uuid this claim
	 * wrote. A claim taken on the create path predates the uuid (all
	 * zeroes); that is what it wrote, so it is what must be there. */
	if (spdk_uuid_compare(&got.lvs_uuid, &ctx->claim.info.lvs_uuid) != 0) {
		SPDK_ERRLOG("strict verify of '%s': the held document's lvstore "
			    "uuid differs from the one this claim wrote; it is "
			    "not used\n", ctx->key);
		owner_strict_finish(ctx, -EBUSY);
		return;
	}
	/* And a claim that did name an lvstore must name the expected one. */
	if (!spdk_uuid_is_null(&ctx->self.lvs_uuid) &&
	    !spdk_uuid_is_null(&ctx->claim.info.lvs_uuid) &&
	    spdk_uuid_compare(&ctx->claim.info.lvs_uuid,
			      &ctx->self.lvs_uuid) != 0) {
		SPDK_ERRLOG("strict verify of '%s': the held claim names a "
			    "different lvstore uuid; it is not used\n", ctx->key);
		owner_strict_finish(ctx, -ESTALE);
		return;
	}
	owner_strict_finish(ctx, 0);
}

void
s3_owner_verify_claim_strict(struct s3_client *client, const char *lvs_name,
			     const struct s3_owner_claim *claim,
			     const struct spdk_uuid *lvs_uuid,
			     const struct s3_owner_caps *caps,
			     s3_owner_cb cb_fn, void *cb_arg)
{
	struct owner_strict_ctx *ctx;
	int rc;

	assert(cb_fn != NULL);

	if (!client || !lvs_name || lvs_name[0] == '\0' || !claim ||
	    !claim->valid || claim->etag[0] == '\0' ||
	    spdk_uuid_is_null(&claim->info.nonce)) {
		cb_fn(cb_arg, -EINVAL);
		return;
	}
	if (!s3_owner_caps_valid(caps, client, lvs_name)) {
		SPDK_ERRLOG("strict verify of '%s' refused: no valid capability "
			    "record for this endpoint/bucket/lvstore\n", lvs_name);
		cb_fn(cb_arg, -ENOTSUP);
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		cb_fn(cb_arg, -ENOMEM);
		return;
	}
	ctx->client = client;
	ctx->cb = cb_fn;
	ctx->cb_arg = cb_arg;
	ctx->claim = *claim;
	if (lvs_uuid) {
		spdk_uuid_copy(&ctx->self.lvs_uuid, lvs_uuid);
	}

	rc = snprintf(ctx->key, sizeof(ctx->key), OWNER_KEY_FMT, lvs_name);
	if (rc < 0 || (size_t)rc >= sizeof(ctx->key)) {
		owner_strict_finish(ctx, -ENAMETOOLONG);
		return;
	}
	ctx->buf = calloc(1, S3_OWNER_DOC_MAX);
	if (!ctx->buf) {
		owner_strict_finish(ctx, -ENOMEM);
		return;
	}

	ctx->etag[0] = '\0';
	rc = s3_get_range_ex(client, ctx->key, 0, S3_OWNER_DOC_MAX,
			     ctx->buf, ctx->etag, sizeof(ctx->etag),
			     verify_read_done, ctx);
	if (rc != 0) {
		owner_strict_finish(ctx, rc);
	}
}
