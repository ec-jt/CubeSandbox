/* SPDX-License-Identifier: Apache-2.0 */
/* Included by xfer.c: one asynchronous, bounded CAS operation. */
#include "s3lvol/s3_owner.h"
#include "restore_admission_ctl.h"

typedef void (*admission_cb)(void *, const struct ea_doc *, int);
struct admission_io {
	struct s3_client *client;
	struct s3_owner_caps caps;
	struct er_doc wire;
	uint8_t attempt[32], commit[32];
	bool attempt_op, acquire;
	struct iovec iov;
	char key[S3_EXPORT_KEY_MAX];
	char etag[128];
	struct spdk_uuid uuid;
	struct spdk_uuid reader;
	enum ea_action action;
	unsigned retries;
	uint64_t size;
	bool create;
	admission_cb cb;
	void *arg;
};

static void admission_read(struct admission_io *a);
static void admission_done(struct admission_io *a, int status)
{
	a->cb(a->arg, &a->wire.base, status);
	s3_client_put(a->client);
	free(a);
}
static void admission_written(void *arg, int status)
{
	struct admission_io *a = arg;
	if (!a->create && (status == -EEXIST || status == -EBUSY) && a->retries++ < 16) {
		admission_read(a);
		return;
	}
	admission_done(a, status);
}
static void admission_got(void *arg, uint64_t bytes, int status)
{
	struct admission_io *a = arg;
	struct spdk_uuid nonce;
	int rc = status;
	if (!rc && bytes != a->size) rc = -EINVAL;
	if (!rc) rc = er_validate(&a->wire, bytes);
	if (!rc && (memcmp(a->wire.base.export_id, &a->uuid, 16) || !a->etag[0])) rc = -EINVAL;
	if (!rc && !a->attempt_op && a->action >= EA_CLOSE &&
	    memcmp(a->wire.base.owner_id, &a->reader, 16)) rc = -EPERM;
	if (!rc && a->attempt_op) {
		rc = er_attempt_change(&a->wire, bytes, a->acquire, a->attempt, a->commit);
		if (!rc) a->size = sizeof(a->wire);
	} else if (!rc) rc = er_reader_change(&a->wire, bytes, a->action, (const uint8_t *)&a->reader);
	if (rc) { admission_done(a, rc); return; }
	spdk_uuid_generate(&nonce);
	memcpy(a->wire.base.nonce, &nonce, 16);
	a->iov = (struct iovec){ &a->wire, a->size };
	rc = s3_put_if_match(a->client, a->key, &a->iov, 1, a->etag,
			     admission_written, a);
	if (rc) admission_done(a, rc);
}
static void admission_head(void *arg, int status)
{
	struct admission_io *a = arg;
	int rc;
	if (status || (a->size != sizeof(struct ea_doc) && a->size != sizeof(a->wire))) {
		admission_done(a, status ? status : -EINVAL);
		return;
	}
	memset(&a->wire, 0, sizeof(a->wire));
	rc = s3_get_range_ex(a->client, a->key, 0, a->size, &a->wire,
				 a->etag, sizeof(a->etag), admission_got, a);
	if (rc) admission_done(a, rc);
}
static void admission_read(struct admission_io *a)
{
	int rc = s3_head(a->client, a->key, &a->size, admission_head, a);
	if (rc) admission_done(a, rc);
}
static void admission_probed(void *arg, int status)
{
	struct admission_io *a = arg;
	int rc;
	if (status) { admission_done(a, status); return; }
	if (!a->create) { admission_read(a); return; }
	a->iov = (struct iovec){ &a->wire, sizeof(struct ea_doc) };
	rc = s3_put(a->client, a->key, &a->iov, 1, true, admission_written, a);
	if (rc) admission_done(a, rc);
}
static int admission_start(struct s3_client *client, const char *uuid,
			   enum ea_action action, const struct spdk_uuid *reader,
			   const struct ea_doc *initial, admission_cb cb, void *arg)
{
	struct admission_io *a = calloc(1, sizeof(*a));
	if (!a) return -ENOMEM;
	if (spdk_uuid_parse(&a->uuid, uuid)) { free(a); return -EINVAL; }
	a->client = client;
	s3_client_get(client);
	a->action = action;
	if (reader) a->reader = *reader;
	a->cb = cb;
	a->arg = arg;
	if (initial) { a->wire.base = *initial; a->create = true; }
	snprintf(a->key, sizeof(a->key), "exports/admission/%s", uuid);
	/* Capability evidence is endpoint/bucket bound. Never infer CAS from 200. */
	s3_owner_probe(client, "exports", &a->caps, admission_probed, a);
	return 0;
}

static int __attribute__((unused))
admission_attempt_start(struct s3_client *client, const char *uuid, bool acquire,
			const uint8_t attempt[32], const uint8_t commit[32], admission_cb cb, void *arg)
{
	struct admission_io *a = calloc(1, sizeof(*a));
	if (!a) return -ENOMEM;
	if (spdk_uuid_parse(&a->uuid, uuid)) { free(a); return -EINVAL; }
	a->client = client; s3_client_get(client);
	a->attempt_op = true; a->acquire = acquire;
	memcpy(a->attempt, attempt, 32); memcpy(a->commit, commit, 32);
	a->cb = cb; a->arg = arg;
	snprintf(a->key, sizeof(a->key), "exports/admission/%s", uuid);
	s3_owner_probe(client, "exports", &a->caps, admission_probed, a);
	return 0;
}
