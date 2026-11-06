/* SPDX-License-Identifier: Apache-2.0
 * Included by the RPC translation unit. Trusted local control-plane only.
 * Records are create-only, bounded, outside sealed metadata, and never deleted.
 * This certifies publication identity, NOT retention or rootfs byte integrity.
 */
#define CHECKPOINT_RECORD_MAX (128U * 1024U)
#define CHECKPOINT_MANIFEST_MAX (16U * 1024U * 1024U)

struct checkpoint_rpc {
	char *action, *key, *body, *name, *uuid, *attempt;
};
struct checkpoint_io {
	struct spdk_jsonrpc_request *request;
	struct s3_client *client;
	char key[256], probe[256], uuid[SPDK_UUID_STRING_LEN];
	char snapshot_uuid[SPDK_UUID_STRING_LEN], snapshot[SPDK_LVOL_NAME_MAX];
	char *body, *read;
	uint64_t size;
	struct iovec iov;
	int phase;
	bool evidence, write;
	bool hold;
	uint8_t attempt[32], commit[32];
};

static void checkpoint_head(void *arg, int status);
static void checkpoint_read(void *arg, uint64_t bytes, int status);
static void checkpoint_put(void *arg, int status);

static int checkpoint_hex(const char *s, uint8_t out[32])
{
	unsigned i;
	if (!s || strlen(s) != 64 || strspn(s, "0123456789abcdef") != 64) return -EINVAL;
	for (i = 0; i < 32; i++) {
		char pair[3] = {s[2*i], s[2*i+1], 0};
		out[i] = (uint8_t)strtoul(pair, NULL, 16);
	}
	return 0;
}

static void
checkpoint_finish(struct checkpoint_io *c, int status, const char *result)
{
	if (status) rpc_lvol_respond_err(c->request, status, "checkpoint publication not verified");
	else {
		struct spdk_json_write_ctx *w = spdk_jsonrpc_begin_result(c->request);
		spdk_json_write_object_begin(w);
		spdk_json_write_named_string(w, "string_value", result);
		spdk_json_write_object_end(w);
		spdk_jsonrpc_end_result(c->request, w);
	}
	s3_client_put(c->client);
	free(c->body); free(c->read); free(c);
}

static void checkpoint_held(void *arg, int status)
{
	checkpoint_finish(arg, status, "held_nonexpiring_remote_only");
}

static void checkpoint_source_drained(void *arg, int status)
{
	struct checkpoint_io *c = arg;
	/* c owns a copy of the binding until the native waiter detaches. A node
	 * cancellation closes only its transport; it cannot free native state. */
	checkpoint_finish(c, status, c->body);
}

static int checkpoint_contains_export(char *body, size_t len, const char *uuid)
{
	struct spdk_json_val *v = calloc(4096, sizeof(*v)), *objects = NULL, *role = NULL, *value = NULL;
	const char *roles[] = {"rootfs", "metadata", "memory"};
	int rc = -EINVAL;
	unsigned i;
	if (!v) return -ENOMEM;
	if (spdk_json_parse(body, len, v, 4096, NULL, 0) <= 0) goto out;
	if (spdk_json_find(v, "objects", NULL, &objects, SPDK_JSON_VAL_OBJECT_BEGIN)) goto out;
	for (i = 0; i < SPDK_COUNTOF(roles); i++) {
		if (spdk_json_find(objects, roles[i], NULL, &role, SPDK_JSON_VAL_OBJECT_BEGIN)) continue;
		if (!spdk_json_find_string(role, "export_uuid", NULL, &value) &&
		    value->len == strlen(uuid) && !memcmp(value->start, uuid, value->len)) { rc = 0; break; }
	}
out:
	free(v); return rc;
}

static void
checkpoint_start_read(struct checkpoint_io *c)
{
	int rc = s3_head(c->client, c->phase < 3 && c->write ? c->probe : c->key,
			 &c->size, checkpoint_head, c);
	if (rc) checkpoint_finish(c, rc, NULL);
}

static void
checkpoint_head(void *arg, int status)
{
	struct checkpoint_io *c = arg;
	int rc;
	if (status || !c->size || c->size > (c->evidence ? CHECKPOINT_MANIFEST_MAX : CHECKPOINT_RECORD_MAX)) {
		checkpoint_finish(c, status ? status : -EFBIG, NULL); return;
	}
	free(c->read); c->read = calloc(1, c->size + 1);
	if (!c->read) { checkpoint_finish(c, -ENOMEM, NULL); return; }
	rc = s3_get_range(c->client, c->phase < 3 && c->write ? c->probe : c->key,
			  0, c->size, c->read, checkpoint_read, c);
	if (rc) checkpoint_finish(c, rc, NULL);
}

static void
checkpoint_submit_put(struct checkpoint_io *c, const char *key, const char *body)
{
	int rc;
	c->iov.iov_base = (void *)body; c->iov.iov_len = strlen(body);
	rc = s3_put(c->client, key, &c->iov, 1, true, checkpoint_put, c);
	if (rc) checkpoint_put(c, rc);
}

static void
checkpoint_put(void *arg, int status)
{
	struct checkpoint_io *c = arg;
	if (c->phase == 0) {
		if (status) { checkpoint_finish(c, status, NULL); return; }
		c->phase = 1;
		checkpoint_submit_put(c, c->probe, "B");
	} else if (c->phase == 1) {
		/* A backend ignoring conditional create must NEVER see the real key. */
		if (status != -EEXIST && status != -EBUSY) {
			checkpoint_finish(c, -ENOTSUP, NULL); return;
		}
		c->phase = 2; checkpoint_start_read(c);
	} else {
		/* Including transport failure: the PUT may have committed. A single
		 * read-back resolves success; a missing/different record fails closed.
		 * Retrying identical bytes is safe; no overwrite/CAS/delete exists. */
		c->phase = 4; checkpoint_start_read(c);
	}
}

static void
checkpoint_read(void *arg, uint64_t bytes, int status)
{
	struct checkpoint_io *c = arg;
	struct s3_export_manifest *m = NULL;
	struct rpc_json_buf buf;
	struct spdk_json_write_ctx *w;
	int rc;
	if (status || bytes != c->size || strlen(c->read) != bytes) {
		checkpoint_finish(c, status ? status : -EINVAL, NULL); return;
	}
	if (c->write && c->phase == 2) {
		if (strcmp(c->read, "A")) { checkpoint_finish(c, -ENOTSUP, NULL); return; }
		c->phase = 3; checkpoint_submit_put(c, c->key, c->body); return;
	}
	if (c->write && strcmp(c->body, c->read)) {
		checkpoint_finish(c, -EEXIST, NULL); return;
	}
	if (c->hold) {
		struct s3lvol_lvstore *lvs;
		rc = checkpoint_contains_export(c->read, bytes, c->uuid);
		if (rc) { checkpoint_finish(c, rc, NULL); return; }
		/* Resolve again after asynchronous reads; never retain an unpinned
		 * lvstore pointer. The client reference is held throughout. */
		lvs = rpc_lvstore_for(c->request, NULL);
		if (!lvs) { s3_client_put(c->client); free(c->read); free(c); return; }
		if (s3lvol_lvstore_get_client(lvs) != c->client) { checkpoint_finish(c, -ESTALE, NULL); return; }
		rc = s3lvol_restore_hold(lvs, c->uuid, c->attempt, c->commit, checkpoint_held, c);
		if (rc) checkpoint_finish(c, rc, NULL);
		return;
	}
	if (!c->evidence) { checkpoint_finish(c, 0, c->read); return; }
	rc = s3_export_manifest_parse(c->read, bytes, &m);
	if (rc) { checkpoint_finish(c, rc, NULL); return; }
	if (strcmp(m->uuid_str, c->uuid) || !m->src.snapshot_uuid[0] ||
	    strcmp(m->src.snapshot_uuid, c->snapshot_uuid) || strcmp(m->src.snapshot, c->snapshot) ||
	    strcmp(m->src.endpoint, s3_client_endpoint(c->client)) ||
	    strcmp(m->src.bucket, s3_client_bucket(c->client))) {
		s3_export_manifest_unref(m); checkpoint_finish(c, -ESTALE, NULL); return;
	}
	w = rpc_json_buf_begin(&buf);
	if (!w) { s3_export_manifest_unref(m); checkpoint_finish(c, -ENOMEM, NULL); return; }
	spdk_json_write_object_begin(w);
#define CP_STR(field, value) spdk_json_write_named_string(w, field, value)
#define CP_NUM(field, value) spdk_json_write_named_uint64(w, field, value)
	CP_STR("export_uuid", m->uuid_str); CP_STR("snapshot_uuid", m->src.snapshot_uuid);
	CP_STR("snapshot", m->src.snapshot); CP_STR("endpoint", m->src.endpoint);
	CP_STR("bucket", m->src.bucket); CP_STR("prefix", m->src.prefix); CP_STR("lvs_name", m->src.lvs_name);
	CP_NUM("source_blob_id", m->src.blob_id); CP_NUM("manifest_version", m->version);
	CP_NUM("generation", m->generation); CP_NUM("size_bytes", m->size_bytes);
	CP_NUM("cluster_size", m->cluster_size); CP_NUM("chunk_size", m->chunk_size); CP_NUM("block_size", m->block_size);
#undef CP_STR
#undef CP_NUM
	spdk_json_write_object_end(w); spdk_json_write_end(w);
	s3_export_manifest_unref(m);
	checkpoint_finish(c, 0, buf.data); free(buf.data);
}

static void
rpc_rcow_checkpoint_record(struct spdk_jsonrpc_request *request, const struct spdk_json_val *params)
{
	struct checkpoint_rpc r = {0};
	const struct spdk_json_object_decoder dec[] = {
		{"action", offsetof(struct checkpoint_rpc, action), spdk_json_decode_string, false},
		{"key", offsetof(struct checkpoint_rpc, key), spdk_json_decode_string, true},
		{"body", offsetof(struct checkpoint_rpc, body), spdk_json_decode_string, true},
		{"name", offsetof(struct checkpoint_rpc, name), spdk_json_decode_string, true},
		{"uuid", offsetof(struct checkpoint_rpc, uuid), spdk_json_decode_string, true},
		{"attempt", offsetof(struct checkpoint_rpc, attempt), spdk_json_decode_string, true},
	};
	struct checkpoint_io *c = NULL;
	struct s3lvol_lvstore *lvs;
	struct spdk_lvol *lvol;
	struct spdk_uuid nonce;
	char nonce_str[SPDK_UUID_STRING_LEN];
	enum s3lvol_export_state state;
	bool deletable;
	int rc = -EINVAL;
	if (!params || spdk_json_decode_object(params, dec, SPDK_COUNTOF(dec), &r)) goto out;
	/* Same configured target selection as ordinary import/export. Never accept
	 * endpoint, bucket, prefix, credentials, or arbitrary object keys from RPC. */
	lvs = rpc_lvstore_for(request, NULL);
	if (!lvs) { rc = 0; goto out; }
	if (!strcmp(r.action, "source-inventory")) {
		struct rpc_json_buf buf;
		struct spdk_json_write_ctx *w = rpc_json_buf_begin(&buf);
		if (!w) { rc = -ENOMEM; goto out; }
		rc = s3lvol_source_inventory(lvs, w);
		if (rc) { spdk_json_write_end(w); free(buf.data); goto out; }
		rpc_json_buf_respond(request, w, &buf);
		goto out;
	}
	c = calloc(1, sizeof(*c));
	if (!c) { rc = -ENOMEM; goto out; }
	c->request = request;
	c->client = s3lvol_lvstore_get_client(lvs); s3_client_get(c->client);
	if (!strcmp(r.action, "source-prepare") || !strcmp(r.action, "source-drain")) {
		if (!r.body || strlen(r.body) > 120 * 1024) goto out;
		c->body = strdup(r.body);
		if (!c->body) { rc = -ENOMEM; goto out; }
		if (!strcmp(r.action, "source-prepare")) {
			rc = s3lvol_scoped_source_prepare(lvs, c->body);
			if (!rc) { checkpoint_finish(c, 0, c->body); c = NULL; }
		} else {
			rc = s3lvol_scoped_source_drain(lvs, c->body, checkpoint_source_drained, c);
			if (!rc) c = NULL;
		}
		goto out;
	}
	if (!strcmp(r.action, "evidence")) {
		if (!r.name || !r.uuid || spdk_uuid_parse(&nonce, r.uuid)) goto out;
		lvol = s3lvol_lvol_find(lvs, r.name);
		if (!lvol || !spdk_blob_is_read_only(lvol->blob) || lvol->action_in_progress) goto out;
		rc = s3lvol_export_query(r.uuid, &state, &deletable);
		if (rc || state != S3LVOL_EXPORT_STATE_DONE) { rc = -EAGAIN; goto out; }
		c->evidence = true;
		snprintf(c->uuid, sizeof(c->uuid), "%s", r.uuid);
		snprintf(c->snapshot, sizeof(c->snapshot), "%s", r.name);
		snprintf(c->snapshot_uuid, sizeof(c->snapshot_uuid), "%s", lvol->uuid_str);
		s3_export_manifest_key(r.uuid, c->key, sizeof(c->key));
	} else {
		if (!r.key || strlen(r.key) != 64 || strspn(r.key, "0123456789abcdef") != 64) goto out;
		snprintf(c->key, sizeof(c->key), "checkpoint-commits/v1/%s.json", r.key);
		if (!strcmp(r.action, "index-put") || !strcmp(r.action, "index-get"))
			snprintf(c->key, sizeof(c->key), "checkpoint-commits/export-index-v1/%s.json", r.key);
		if (!strcmp(r.action, "attempt-put"))
			snprintf(c->key, sizeof(c->key), "restore-attempts/v1/%s.json", r.key);
		if (!strcmp(r.action, "hold")) {
			if (!r.uuid || spdk_uuid_parse(&nonce, r.uuid) ||
			    checkpoint_hex(r.attempt, c->attempt) || checkpoint_hex(r.key, c->commit)) goto out;
			c->hold = true; snprintf(c->uuid, sizeof(c->uuid), "%s", r.uuid);
		} else if (!strcmp(r.action, "put") || !strcmp(r.action, "index-put") || !strcmp(r.action, "attempt-put")) {
			if (!r.body || !*r.body || strlen(r.body) > CHECKPOINT_RECORD_MAX) goto out;
			c->write = true; c->body = strdup(r.body);
			if (!c->body) { rc = -ENOMEM; goto out; }
			spdk_uuid_generate(&nonce); spdk_uuid_fmt_lower(nonce_str, sizeof(nonce_str), &nonce);
			snprintf(c->probe, sizeof(c->probe), "checkpoint-commits/v1/probe-%s", nonce_str);
			/* Unique capability artifacts remain: no release, TTL or delete. */
			checkpoint_submit_put(c, c->probe, "A"); c = NULL; rc = 0; goto out;
		} else if (strcmp(r.action, "get") && strcmp(r.action, "index-get")) goto out;
	}
	checkpoint_start_read(c); c = NULL; rc = 0;
out:
	if (c) { s3_client_put(c->client); free(c->body); free(c); }
	if (rc) rpc_lvol_respond_err(request, rc, "invalid or incomplete checkpoint operation");
	free(r.action); free(r.key); free(r.body); free(r.name); free(r.uuid); free(r.attempt);
}
SPDK_RPC_REGISTER("rcow_checkpoint_record", rpc_rcow_checkpoint_record, SPDK_RPC_RUNTIME)
