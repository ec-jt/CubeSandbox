/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */

/*
 * Strict-ownership driver for a real endpoint (local MinIO only).
 *
 * Commands:
 *
 *   probe    Probe the endpoint and print the capability record.
 *   acquire  Probe, then acquire atomically. Prints the claim. With
 *            --wait-release-file, a winner holds the claim until the named file
 *            appears, then releases it (used to keep the loser's attempt inside
 *            the winner's critical section).
 *   release  Release the claim stored in --claim-in.
 *   inspect  Read the owner marker and print it (or NONE).
 *
 * The point of this program is that it is a *process*, not a thread: the
 * two-process race in the runner uses two of these against the same prefix.
 *
 * Output is newline key=value records terminated by a line "END", so the
 * runner can parse it without a JSON dependency:
 *
 *   CMD=acquire
 *   STATUS=0
 *   NONCE=...
 *   ETAG="..."
 *   PID=...
 *   END
 *
 * A negative STATUS is a negative errno (-16 is -EBUSY, -95 is -ENOTSUP).
 *
 * Usage:
 *   export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
 *   ./s3_owner_race acquire --endpoint minio:9000 --bucket b --lvs lvs \
 *       --claim-out /tmp/a.claim --wait-release-file /tmp/a.release
 */

#include "spdk/stdinc.h"
#include "spdk/log.h"
#include "spdk/uuid.h"

#include "s3lvol/s3_client.h"
#include "s3lvol/s3_owner.h"
#include "s3lvol/s3_spawner.h"

#include <getopt.h>

struct latch {
	pthread_mutex_t mutex;
	pthread_cond_t  cond;
	bool            done;
	int             status;
	uint64_t        bytes;
	struct s3_owner_claim claim;
	bool            has_claim;
};

static void
latch_init(struct latch *l)
{
	/* done/status/claim must start zeroed: the previous version left them
	 * as stack garbage and a lucky zero made a failed probe look like
	 * success. */
	memset(l, 0, sizeof(*l));
	pthread_mutex_init(&l->mutex, NULL);
	pthread_cond_init(&l->cond, NULL);
}

static void
latch_set(struct latch *l, int status, uint64_t bytes)
{
	pthread_mutex_lock(&l->mutex);
	l->status = status;
	l->bytes = bytes;
	l->done = true;
	pthread_cond_signal(&l->cond);
	pthread_mutex_unlock(&l->mutex);
}

static int
latch_wait(struct latch *l, int timeout_s)
{
	struct timespec deadline;
	int rc = 0;

	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += timeout_s;

	pthread_mutex_lock(&l->mutex);
	while (!l->done && rc == 0) {
		rc = pthread_cond_timedwait(&l->cond, &l->mutex, &deadline);
	}
	pthread_mutex_unlock(&l->mutex);
	return rc == ETIMEDOUT ? -ETIMEDOUT : l->status;
}

static void
on_done(void *cb_arg, int status)
{
	latch_set(cb_arg, status, 0);
}

static void
on_get(void *cb_arg, uint64_t bytes, int status)
{
	latch_set(cb_arg, status, bytes);
}

static void
on_acquire(void *cb_arg, const struct s3_owner_info *self,
	   const struct s3_owner_claim *claim, int status)
{
	struct latch *l = cb_arg;

	pthread_mutex_lock(&l->mutex);
	l->status = status;
	l->done = true;
	if (claim) {
		l->claim = *claim;
		l->has_claim = true;
	}
	pthread_cond_signal(&l->cond);
	pthread_mutex_unlock(&l->mutex);
	(void)self;
}

static bool
wait_for_file(const char *path, int timeout_s)
{
	struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000 };
	int waited_ms = 0;

	while (access(path, F_OK) != 0) {
		nanosleep(&ts, NULL);
		waited_ms++;
		if (waited_ms > timeout_s * 1000) {
			return false;
		}
	}
	return true;
}

static void
print_claim(const char *cmd, int status, const struct s3_owner_claim *claim)
{
	char nonce[SPDK_UUID_STRING_LEN] = "";

	if (claim && claim->valid) {
		spdk_uuid_fmt_lower(nonce, sizeof(nonce), &claim->info.nonce);
	}
	printf("CMD=%s\n", cmd);
	printf("STATUS=%d\n", status);
	printf("NONCE=%s\n", nonce);
	printf("ETAG=%s\n", claim && claim->valid ? claim->etag : "");
	printf("PID=%d\n", (int)getpid());
	printf("END\n");
	fflush(stdout);
}

static int
write_claim_file(const char *path, const struct s3_owner_claim *claim)
{
	char nonce[SPDK_UUID_STRING_LEN];
	char lvs_uuid[SPDK_UUID_STRING_LEN];
	FILE *f;

	spdk_uuid_fmt_lower(nonce, sizeof(nonce), &claim->info.nonce);
	spdk_uuid_fmt_lower(lvs_uuid, sizeof(lvs_uuid), &claim->info.lvs_uuid);

	f = fopen(path, "w");
	if (!f) {
		return -errno;
	}
	fprintf(f, "nonce=%s\n", nonce);
	fprintf(f, "etag=%s\n", claim->etag);
	fprintf(f, "lvs_uuid=%s\n", lvs_uuid);
	fprintf(f, "node_id=%s\n", claim->info.node_id);
	fprintf(f, "pid=%d\n", claim->info.pid);
	fclose(f);
	return 0;
}

static int
read_claim_file(const char *path, struct s3_owner_claim *claim)
{
	FILE *f = fopen(path, "r");
	char line[512];

	if (!f) {
		return -errno;
	}
	memset(claim, 0, sizeof(*claim));
	while (fgets(line, sizeof(line), f)) {
		char *nl = strchr(line, '\n');

		if (nl) {
			*nl = '\0';
		}
		if (strncmp(line, "nonce=", 6) == 0) {
			if (spdk_uuid_parse(&claim->info.nonce, line + 6) != 0) {
				fclose(f);
				return -EINVAL;
			}
		} else if (strncmp(line, "etag=", 5) == 0) {
			snprintf(claim->etag, sizeof(claim->etag), "%s",
				 line + 5);
		} else if (strncmp(line, "lvs_uuid=", 9) == 0) {
			spdk_uuid_parse(&claim->info.lvs_uuid, line + 9);
		} else if (strncmp(line, "node_id=", 8) == 0) {
			snprintf(claim->info.node_id,
				 sizeof(claim->info.node_id), "%s", line + 8);
		} else if (strncmp(line, "pid=", 4) == 0) {
			claim->info.pid = (int32_t)atoi(line + 4);
		}
	}
	fclose(f);
	claim->valid = claim->etag[0] != '\0';
	return claim->valid ? 0 : -EINVAL;
}

static void
usage(const char *prog)
{
	fprintf(stderr,
		"usage: %s <probe|acquire|release|inspect> --endpoint HOST:PORT "
		"--bucket B --lvs NAME [options]\n"
		"  --region R            default us-east-1\n"
		"  --path-style          use path-style addressing\n"
		"  --no-tls              plain HTTP\n"
		"  --claim-out PATH      write the acquired claim\n"
		"  --claim-in PATH       read the claim for release\n"
		"  --wait-release-file PATH  winner holds until this file exists\n"
		"  --barrier PATH        wait for this file before probing\n",
		prog);
}

int
main(int argc, char **argv)
{
	const char *cmd;
	const char *endpoint = NULL;
	const char *bucket = NULL;
	const char *region = "us-east-1";
	const char *lvs = NULL;
	const char *claim_out = NULL;
	const char *claim_in = NULL;
	const char *wait_release = NULL;
	const char *barrier = NULL;
	const char *raw_key = NULL;
	const char *raw_etag = NULL;
	bool path_style = false;
	bool verify_tls = true;
	uint32_t threads = 4;

	static const struct option long_opts[] = {
		{ "endpoint",          required_argument, NULL, 'e' },
		{ "bucket",            required_argument, NULL, 'b' },
		{ "region",            required_argument, NULL, 'r' },
		{ "lvs",               required_argument, NULL, 'l' },
		{ "claim-out",         required_argument, NULL, 'o' },
		{ "claim-in",          required_argument, NULL, 'i' },
		{ "wait-release-file", required_argument, NULL, 'w' },
		{ "barrier",           required_argument, NULL, 'B' },
		{ "key",               required_argument, NULL, 'k' },
		{ "etag",              required_argument, NULL, 'E' },
		{ "path-style",        no_argument,       NULL, 'P' },
		{ "no-tls",            no_argument,       NULL, 'T' },
		{ "threads",           required_argument, NULL, 't' },
		{ "help",              no_argument,       NULL, 'h' },
		{ NULL, 0, NULL, 0 },
	};
	int opt;
	int rc;
	struct s3_target target;
	struct s3_client *client = NULL;
	struct s3_owner_caps caps;
	struct latch l;

	if (argc < 2) {
		usage(argv[0]);
		return 2;
	}
	cmd = argv[1];
	optind = 2;

	while ((opt = getopt_long(argc, argv, "e:b:r:l:o:i:w:B:k:E:PTt:h",
				  long_opts, NULL)) != -1) {
		switch (opt) {
		case 'e': endpoint = optarg; break;
		case 'b': bucket = optarg; break;
		case 'r': region = optarg; break;
		case 'l': lvs = optarg; break;
		case 'o': claim_out = optarg; break;
		case 'i': claim_in = optarg; break;
		case 'w': wait_release = optarg; break;
		case 'B': barrier = optarg; break;
		case 'k': raw_key = optarg; break;
		case 'E': raw_etag = optarg; break;
		case 'P': path_style = true; break;
		case 'T': verify_tls = false; break;
		case 't': threads = (uint32_t)strtoul(optarg, NULL, 10); break;
		case 'h': usage(argv[0]); return 0;
		default: usage(argv[0]); return 2;
		}
	}

	if (!endpoint || !bucket || !lvs) {
		fprintf(stderr, "error: --endpoint, --bucket and --lvs are required\n");
		return 2;
	}
	if (!getenv("AWS_ACCESS_KEY_ID") || !getenv("AWS_SECRET_ACCESS_KEY")) {
		fprintf(stderr, "error: AWS credentials are not in the environment\n");
		return 2;
	}

	spdk_log_set_print_level(SPDK_LOG_ERROR);
	spdk_log_open(NULL);

	{
		cpu_set_t allowed;

		CPU_ZERO(&allowed);
		if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0) {
			fprintf(stderr, "sched_getaffinity: %s\n", strerror(errno));
			return 3;
		}
		rc = s3_spawner_start(&allowed);
		if (rc != 0) {
			fprintf(stderr, "s3_spawner_start: %d\n", rc);
			return 3;
		}
	}

	rc = s3_crt_global_init(threads);
	if (rc != 0) {
		fprintf(stderr, "s3_crt_global_init: %d\n", rc);
		s3_spawner_stop();
		return 3;
	}

	memset(&target, 0, sizeof(target));
	target.endpoint = (char *)endpoint;
	target.region = (char *)region;
	target.bucket = (char *)bucket;
	target.auth_mode = S3_AUTH_ENV;
	target.use_path_style = path_style;
	target.verify_tls = verify_tls;

	rc = s3_client_get_or_create(&target, &client);
	if (rc != 0) {
		fprintf(stderr, "s3_client_get_or_create: %d\n", rc);
		goto out_fini;
	}

	latch_init(&l);

	if (barrier && !wait_for_file(barrier, 30)) {
		fprintf(stderr, "barrier %s never appeared\n", barrier);
		rc = 3;
		goto out_client;
	}

	rc = 0;
	if (strcmp(cmd, "probe") == 0) {
		memset(&caps, 0, sizeof(caps));
		s3_owner_probe(client, lvs, &caps, on_done, &l);
		rc = latch_wait(&l, 60);
		printf("CMD=probe\nSTATUS=%d\n", rc);
		printf("ATOMIC_CREATE=%d\n", caps.atomic_create ? 1 : 0);
		printf("CAS_REPLACE=%d\n", caps.cas_replace ? 1 : 0);
		printf("LVS=%s\n", caps.lvs_name);
		printf("PATH_STYLE=%d\nENDPOINT=%s\nEND\n", path_style ? 1 : 0,
		       endpoint);
	} else if (strcmp(cmd, "acquire") == 0) {
		memset(&caps, 0, sizeof(caps));
		l.done = false;
		s3_owner_probe(client, lvs, &caps, on_done, &l);
		rc = latch_wait(&l, 60);
		if (rc != 0) {
			print_claim("acquire", rc, NULL);
			goto out_client;
		}

		memset(&l, 0, sizeof(l));
		latch_init(&l);
		s3_owner_acquire_strict(client, lvs, NULL, &caps, on_acquire, &l);
		rc = latch_wait(&l, 60);
		print_claim("acquire", rc, l.has_claim ? &l.claim : NULL);

		if (rc == 0 && claim_out) {
			rc = write_claim_file(claim_out, &l.claim);
			if (rc != 0) {
				fprintf(stderr, "claim file: %s\n", strerror(-rc));
			}
		}

		if (rc == 0 && wait_release) {
			struct latch rel;

			if (!wait_for_file(wait_release, 60)) {
				fprintf(stderr, "release file %s never appeared\n",
					wait_release);
				rc = 3;
				goto out_client;
			}
			memset(&rel, 0, sizeof(rel));
			latch_init(&rel);
			s3_owner_release_strict(client, lvs, &l.claim, &caps,
						on_done, &rel);
			rc = latch_wait(&rel, 60);
			printf("CMD=release\nSTATUS=%d\nEND\n", rc);
		}
	} else if (strcmp(cmd, "release") == 0) {
		struct s3_owner_claim claim;

		memset(&caps, 0, sizeof(caps));
		l.done = false;
		s3_owner_probe(client, lvs, &caps, on_done, &l);
		rc = latch_wait(&l, 60);
		if (rc != 0) {
			printf("CMD=release\nSTATUS=%d\nEND\n", rc);
			goto out_client;
		}
		if (!claim_in) {
			fprintf(stderr, "release needs --claim-in\n");
			rc = 2;
			goto out_client;
		}
		rc = read_claim_file(claim_in, &claim);
		if (rc != 0) {
			printf("CMD=release\nSTATUS=%d\nEND\n", rc);
			goto out_client;
		}
		memset(&l, 0, sizeof(l));
		latch_init(&l);
		s3_owner_release_strict(client, lvs, &claim, &caps,
					on_done, &l);
		rc = latch_wait(&l, 60);
		printf("CMD=release\nSTATUS=%d\nEND\n", rc);
	} else if (strcmp(cmd, "inspect") == 0) {
		char key[512];
		char buf[S3_OWNER_DOC_MAX];
		char etag[S3_OWNER_ETAG_MAX];

		snprintf(key, sizeof(key), "%s/meta/owner", lvs);
		memset(buf, 0, sizeof(buf));
		memset(&l, 0, sizeof(l));
		latch_init(&l);
		s3_get_range_ex(client, key, 0, sizeof(buf), buf, etag,
				sizeof(etag), on_get, &l);
		rc = latch_wait(&l, 60);
		printf("CMD=inspect\nSTATUS=%d\n", rc);
		if (rc == 0) {
			printf("BYTES=%" PRIu64 "\n", l.bytes);
			printf("ETAG=%s\n", etag);
			printf("BODY=%.*s\n", (int)l.bytes, buf);
		}
		printf("END\n");
	} else if (strcmp(cmd, "raw-delete") == 0) {
		if (!raw_key || !raw_etag) {
			fprintf(stderr, "raw-delete needs --key and --etag\n");
			rc = 2;
			goto out_client;
		}
		memset(&l, 0, sizeof(l));
		latch_init(&l);
		s3_delete_if_match(client, raw_key, raw_etag, on_done, &l);
		rc = latch_wait(&l, 60);
		printf("CMD=raw-delete\nSTATUS=%d\nEND\n", rc);
	} else {
		usage(argv[0]);
		rc = 2;
	}

out_client:
	if (client) {
		s3_client_put(client);
	}
out_fini:
	s3_crt_global_fini();
	s3_spawner_stop();
	return rc == 0 ? 0 : 1;
}
