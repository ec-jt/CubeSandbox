/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/* White-box, offline request-boundary test. Include the production TU without
 * changing its build. Real CRT HTTP messages/streams and global initialization;
 * fake client/submission only. No S3 I/O, SPDK environment, devices or daemon.
 * Global init body runs directly because this is not a pinned reactor. */
#include "spdk/stdinc.h"
#include <aws/s3/s3_client.h>
#include <aws/http/request_response.h>

static struct aws_s3_client *fake_client_new(struct aws_allocator *, const struct aws_s3_client_config *);
static struct aws_s3_client *fake_client_release(struct aws_s3_client *);
static struct aws_s3_meta_request *capture_request(struct aws_s3_client *, const struct aws_s3_meta_request_options *);
static struct aws_s3_meta_request *fake_request_release(struct aws_s3_meta_request *);
static int maybe_add_header(struct aws_http_message *, struct aws_http_header);

#define aws_s3_client_new fake_client_new
#define aws_s3_client_release fake_client_release
#define aws_s3_client_make_meta_request capture_request
#define aws_s3_meta_request_release fake_request_release
#define aws_http_message_add_header maybe_add_header
#include "../../lib/s3bsdev/s3_client_aws.c"
#undef aws_s3_client_new
#undef aws_s3_client_release
#undef aws_s3_client_make_meta_request
#undef aws_s3_meta_request_release
#undef aws_http_message_add_header

static unsigned checks, submits, clients_created, callbacks;
static int last_status;
static bool fail_condition_header;
static struct aws_s3_meta_request_options pending;
#define CHECK(x) do { checks++; if (!(x)) { \
	fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #x); exit(1); \
} } while (0)

/* Link stubs: never initialize a SPDK environment or inspect host affinity. */
bool s3_spawner_is_started(void) { return false; }
void *s3_spawner_run_task(void *(*fn)(void *), void *arg) { return fn(arg); }

struct fake_client {
	void (*shutdown)(void *);
	void *arg;
};

static struct aws_s3_client *
fake_client_new(struct aws_allocator *allocator, const struct aws_s3_client_config *config)
{
	(void)allocator;
	struct fake_client *client = calloc(1, sizeof(*client));
	CHECK(client != NULL);
	client->shutdown = config->shutdown_callback;
	client->arg = config->shutdown_callback_user_data;
	clients_created++;
	return (struct aws_s3_client *)client;
}

static struct aws_s3_client *
fake_client_release(struct aws_s3_client *opaque)
{
	struct fake_client *client = (struct fake_client *)opaque;
	client->shutdown(client->arg);
	free(client);
	return NULL;
}

static struct aws_s3_meta_request *
capture_request(struct aws_s3_client *client, const struct aws_s3_meta_request_options *options)
{
	(void)client;
	CHECK(pending.message == NULL);
	pending = *options;
	aws_http_message_acquire(pending.message);
	submits++;
	return (struct aws_s3_meta_request *)&pending;
}

static struct aws_s3_meta_request *
fake_request_release(struct aws_s3_meta_request *request)
{
	CHECK(request == (struct aws_s3_meta_request *)&pending);
	aws_http_message_release(pending.message);
	pending.message = NULL;
	return NULL;
}

static int
maybe_add_header(struct aws_http_message *message, struct aws_http_header header)
{
	if (fail_condition_header && aws_byte_cursor_eq_c_str(&header.name, "If-Match")) {
		return AWS_OP_ERR;
	}
	return aws_http_message_add_header(message, header);
}

static void callback(void *arg, int status)
{
	(void)arg;
	callbacks++;
	last_status = status;
}

static void
header_is(const char *name, const char *expected)
{
	struct aws_byte_cursor value;
	int rc = aws_http_headers_get(aws_http_message_get_headers(pending.message),
		aws_byte_cursor_from_c_str(name), &value);
	CHECK(expected ? rc == AWS_OP_SUCCESS : rc != AWS_OP_SUCCESS);
	if (expected) {
		CHECK(aws_byte_cursor_eq_c_str(&value, expected));
	}
}

static void
finish(int http_status)
{
	struct aws_s3_meta_request_result result = {
		.response_status = http_status,
		.error_code = http_status == 200 ? AWS_ERROR_SUCCESS : AWS_ERROR_S3_INVALID_RESPONSE_STATUS,
	};
	unsigned before = callbacks;
	pending.finish_callback((struct aws_s3_meta_request *)&pending, &result, pending.user_data);
	CHECK(callbacks == before + 1);
	CHECK(last_status == (http_status == 200 ? 0 : -EEXIST));
}

static void
exercise_mode(const char *setting, bool bare)
{
	struct s3_target target = {
		.endpoint = "offline.invalid", .region = "test", .bucket = "test",
		.use_path_style = true, .auth_mode = S3_AUTH_STATIC,
		.access_key = "test-only", .secret_key = "test-only",
	};
	struct s3_client *client, *reused, *other;
	struct iovec iov = {.iov_base = "payload", .iov_len = 7};
	char etag[128] = "\"abc-2\"";
	const char *expected = bare ? "abc-2" : "\"abc-2\"";
	if (setting) { setenv("S3LVOL_PUT_IF_MATCH_BARE_ETAG", setting, 1); }
	else { unsetenv("S3LVOL_PUT_IF_MATCH_BARE_ETAG"); }
	CHECK(s3_crt_global_init_body(1) == 0);
	CHECK(g_app_ctx.put_if_match_bare_etag == bare);
	/* The production GET callback must retain the exact response identity,
	 * independent of the outgoing PUT wire format. */
	char returned_etag[128] = {0};
	struct s3_request get_req = {.etag_out = returned_etag, .etag_cap = sizeof(returned_etag)};
	struct aws_http_headers *headers = aws_http_headers_new(g_app_ctx.allocator);
	CHECK(headers != NULL);
	CHECK(aws_http_headers_add(headers, aws_byte_cursor_from_c_str("ETag"),
		aws_byte_cursor_from_c_str("\"abc-2\"")) == AWS_OP_SUCCESS);
	CHECK(s3_get_headers_callback(NULL, headers, 200, &get_req) == AWS_OP_SUCCESS);
	CHECK(strcmp(returned_etag, "\"abc-2\"") == 0);
	aws_http_headers_release(headers);
	unsigned count = clients_created;
	CHECK(s3_client_get_or_create(&target, &client) == 0);
	/* Changing env, including to invalid text, cannot affect existing OR new
	 * clients in this lifecycle. Repeated init must not re-read it either. */
	setenv("S3LVOL_PUT_IF_MATCH_BARE_ETAG", bare ? "0" : "1", 1);
	CHECK(s3_crt_global_init_body(1) == 0);
	CHECK(s3_client_get_or_create(&target, &reused) == 0 && reused == client);
	setenv("S3LVOL_PUT_IF_MATCH_BARE_ETAG", "invalid-after-init", 1);
	target.bucket = "second";
	CHECK(s3_client_get_or_create(&target, &other) == 0 && other != client);
	CHECK(clients_created == count + 2);
	CHECK(s3_put_if_match(client, "key", &iov, 1, etag, callback, NULL) == 0);
	CHECK(strcmp(etag, "\"abc-2\"") == 0);
	strcpy(etag, "caller-reused-storage");
	header_is("If-Match", expected);
	header_is("If-None-Match", NULL);
	CHECK(strcmp(((struct s3_request *)pending.user_data)->if_match_buf, expected) == 0);
	CHECK(((struct s3_request *)pending.user_data)->has_if_match);
	finish(200);
	CHECK(s3_put_if_match(other, "key", &iov, 1, "\"abc-2\"", callback, NULL) == 0);
	header_is("If-Match", expected);
	unsigned before = submits;
	finish(412);
	CHECK(submits == before); /* no format retry or blind fallback */
	CHECK(s3_put_if_match(client, "key", &iov, 1, "abc-2", callback, NULL) == 0);
	header_is("If-Match", "abc-2");
	finish(200);
	const char *invalid[] = {NULL, "", "*", "\"*\"", "W/\"abc\"", "a,b", "\"a\",\"b\"", "\"abc", "a\r\nb"};
	before = submits;
	for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
		CHECK(s3_put_if_match(client, "key", &iov, 1, invalid[i], callback, NULL) == -EINVAL);
	}
	CHECK(submits == before);
	fail_condition_header = true;
	CHECK(s3_put_if_match(client, "key", &iov, 1, "abc", callback, NULL) == -ENOMEM);
	CHECK(submits == before && S3_STAT_GET(client, inflight) == 0);
	fail_condition_header = false;
	CHECK(s3_put(client, "key", &iov, 1, true, callback, NULL) == 0);
	header_is("If-None-Match", "*");
	header_is("If-Match", NULL);
	finish(200);
	CHECK(s3_put(client, "key", &iov, 1, false, callback, NULL) == 0);
	header_is("If-None-Match", NULL);
	header_is("If-Match", NULL);
	finish(200);
	/* DELETE uses the original quote-preserving path even in bare PUT mode. */
	CHECK(s3_delete_if_match(client, "key", "\"abc-2\"", callback, NULL) == 0);
	header_is("If-Match", "\"abc-2\"");
	finish(200);
	s3_client_put(reused);
	s3_client_put(client);
	s3_client_put(other);
	s3_crt_global_fini_body();
	CHECK(!g_app_ctx_initialized && g_num_clients == 0);
}

int
main(void)
{
	setenv("S3LVOL_CRT_LOG_LEVEL", "none", 1);
	setenv("S3LVOL_PUT_IF_MATCH_BARE_ETAG", "typo", 1);
	CHECK(s3_crt_global_init_body(1) == -EINVAL && !g_app_ctx_initialized);
	exercise_mode(NULL, false);
	exercise_mode("1", true);
	exercise_mode("0", false);
	printf("s3_put_etag_request_test: %u checks passed, %u captured requests, no network\n", checks, submits);
	return 0;
}
