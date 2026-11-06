/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/*
 * Unit test for module/bdev/s3lvol/retained_claim_ctl.h: the same-process
 * retained strict-claim record (eligibility, identity match, exactly-once
 * take/put_back/consume, lifetime). Pure C, no SPDK runtime, no S3. Built
 * with -fsanitize=address,undefined to check record lifetime.
 */
#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "retained_claim_ctl.h"

static int g_checks, g_failures;
#define CHECK(c) do { g_checks++; if (!(c)) { g_failures++; \
	fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } } while (0)

static void
mk_ident(struct lvs_retained_ident *id, const char *name, bool dual)
{
	memset(id, 0, sizeof(*id));
	snprintf(id->lvs_name, sizeof(id->lvs_name), "%s", name);
	snprintf(id->endpoint, sizeof(id->endpoint), "minio:9000");
	snprintf(id->bucket, sizeof(id->bucket), "bkt");
	spdk_uuid_parse(&id->lvs_uuid, "11111111-1111-1111-1111-111111111111");
	snprintf(id->wal_bdev, sizeof(id->wal_bdev), "walbdev");
	spdk_uuid_parse(&id->wal_bdev_uuid, "22222222-2222-2222-2222-222222222222");
	if (dual) {
		snprintf(id->cache_bdev, sizeof(id->cache_bdev), "cachebdev");
		spdk_uuid_parse(&id->cache_bdev_uuid,
				"33333333-3333-3333-3333-333333333333");
	}
}

static void
mk_claim(struct s3_owner_claim *c)
{
	memset(c, 0, sizeof(*c));
	c->valid = true;
	snprintf(c->etag, sizeof(c->etag), "\"abc\"");
	spdk_uuid_parse(&c->info.nonce, "44444444-4444-4444-4444-444444444444");
}

static void
test_eligibility(void)
{
	/* the U/D bug case: strict, held, all stopped, teardown failed */
	CHECK(lvs_retained_eligible(true, true, true, true, true, 0, -ETIMEDOUT));
	/* clean teardown: release path, never retained */
	CHECK(!lvs_retained_eligible(true, true, true, true, true, 0, 0));
	/* legacy marker: not a fenced claim */
	CHECK(!lvs_retained_eligible(false, true, true, true, true, 0, -EIO));
	CHECK(!lvs_retained_eligible(true, false, true, true, true, 0, -EIO));
	/* a writer may remain: blobstore or bs_dev not finished */
	CHECK(!lvs_retained_eligible(true, true, false, true, true, 0, -EIO));
	CHECK(!lvs_retained_eligible(true, true, true, false, true, 0, -EIO));
	/* registry writer not proven quiesced */
	CHECK(!lvs_retained_eligible(true, true, true, true, false, 0, -EIO));
	CHECK(!lvs_retained_eligible(true, true, true, true, true, -ETIMEDOUT,
				     -EIO));
}

static void
test_ident_check(void)
{
	struct lvs_retained_ident a, b;

	mk_ident(&a, "lv", false);
	b = a;
	CHECK(lvs_retained_ident_check(&a, &b) == 0);

	b = a; snprintf(b.wal_bdev, sizeof(b.wal_bdev), "otherbdev");
	CHECK(lvs_retained_ident_check(&a, &b) == -ENODEV);
	b = a; spdk_uuid_parse(&b.wal_bdev_uuid,
			       "99999999-9999-9999-9999-999999999999");
	CHECK(lvs_retained_ident_check(&a, &b) == -ENODEV);   /* same name, new device */
	b = a; spdk_uuid_parse(&b.lvs_uuid,
			       "99999999-9999-9999-9999-999999999999");
	CHECK(lvs_retained_ident_check(&a, &b) == -ESTALE);
	b = a; snprintf(b.bucket, sizeof(b.bucket), "other");
	CHECK(lvs_retained_ident_check(&a, &b) == -EXDEV);
	b = a; snprintf(b.endpoint, sizeof(b.endpoint), "other:9000");
	CHECK(lvs_retained_ident_check(&a, &b) == -EXDEV);
	b = a; snprintf(b.lvs_name, sizeof(b.lvs_name), "lv2");
	CHECK(lvs_retained_ident_check(&a, &b) == -EXDEV);

	/* single vs dual layout, and dual cache device swapped */
	mk_ident(&b, "lv", true);
	CHECK(lvs_retained_ident_check(&a, &b) == -ENODEV);
	mk_ident(&a, "lv", true);
	CHECK(lvs_retained_ident_check(&a, &b) == 0);
	spdk_uuid_parse(&b.cache_bdev_uuid, "99999999-9999-9999-9999-999999999999");
	CHECK(lvs_retained_ident_check(&a, &b) == -ENODEV);

	/* incomplete identity never matches (null bdev uuid, blank lvs uuid) */
	mk_ident(&a, "lv", false);
	b = a; memset(&b.wal_bdev_uuid, 0, sizeof(b.wal_bdev_uuid));
	CHECK(lvs_retained_ident_check(&a, &b) == -EINVAL);
	b = a; memset(&b.lvs_uuid, 0, sizeof(b.lvs_uuid));
	CHECK(lvs_retained_ident_check(&a, &b) == -EINVAL);
	CHECK(lvs_retained_ident_check(&b, &b) == -EINVAL);
	CHECK(lvs_retained_ident_check(NULL, &a) == -EINVAL);
}

static void
test_copy_refuses_truncation(void)
{
	char small[4];

	CHECK(lvs_retained_copy(small, sizeof(small), "abc") == 0);
	CHECK(lvs_retained_copy(small, sizeof(small), "abcd") == -ENAMETOOLONG);
	CHECK(lvs_retained_copy(small, sizeof(small), NULL) == -EINVAL);
}

static void
test_add_rules(void)
{
	struct lvs_retained_list l = TAILQ_HEAD_INITIALIZER(l);
	struct lvs_retained_ident id;
	struct s3_owner_caps caps = {0};
	struct s3_owner_claim c, bad;

	mk_ident(&id, "lv", false);
	mk_claim(&c);
	bad = c; bad.valid = false;
	CHECK(lvs_retained_add(&l, &id, &caps, &bad) == -EINVAL);
	bad = c; bad.etag[0] = '\0';
	CHECK(lvs_retained_add(&l, &id, &caps, &bad) == -EINVAL);
	bad = c; memset(&bad.info.nonce, 0, sizeof(bad.info.nonce));
	CHECK(lvs_retained_add(&l, &id, &caps, &bad) == -EINVAL);
	CHECK(TAILQ_EMPTY(&l));

	CHECK(lvs_retained_add(&l, &id, &caps, &c) == 0);
	/* never overwritten: a second record for the name is refused */
	CHECK(lvs_retained_add(&l, &id, &caps, &c) == -EEXIST);
	CHECK(lvs_retained_find(&l, "lv") != NULL);
	CHECK(lvs_retained_find(&l, "nope") == NULL);
	CHECK(lvs_retained_clear_idle(&l) == 1);
	CHECK(TAILQ_EMPTY(&l));
}

/* Concurrent retry, failed attaches and success: exactly once. */
static void
test_take_put_back_consume(void)
{
	struct lvs_retained_list l = TAILQ_HEAD_INITIALIZER(l);
	struct lvs_retained_ident id;
	struct s3_owner_caps caps = {0};
	struct s3_owner_claim c;
	struct lvs_retained *r1, *r2;
	int i;

	mk_ident(&id, "lv", false);
	mk_claim(&c);
	CHECK(lvs_retained_add(&l, &id, &caps, &c) == 0);

	CHECK(lvs_retained_take(&l, "missing", &r1) == -ENOENT && r1 == NULL);
	CHECK(lvs_retained_take(&l, "lv", &r1) == 0 && r1 && r1->busy);
	/* concurrent retry refused while the first attach owns it */
	CHECK(lvs_retained_take(&l, "lv", &r2) == -EBUSY && r2 == NULL);
	/* busy records survive clear_idle (no free under an in-flight attach) */
	CHECK(lvs_retained_clear_idle(&l) == 0);
	CHECK(lvs_retained_find(&l, "lv") == r1);

	/* repeated failures: claim bytes unchanged, record reusable */
	for (i = 0; i < 3; i++) {
		CHECK(lvs_retained_put_back(r1) == 0);
		CHECK(!r1->busy && r1->failed_attempts == (uint32_t)(i + 1));
		CHECK(memcmp(&r1->claim, &c, sizeof(c)) == 0);
		CHECK(lvs_retained_put_back(r1) == -EINVAL);  /* not taken */
		CHECK(lvs_retained_take(&l, "lv", &r1) == 0);
	}
	/* consume needs it taken, and frees it exactly once */
	CHECK(lvs_retained_consume(&l, r1) == 0);
	CHECK(TAILQ_EMPTY(&l));
	CHECK(lvs_retained_find(&l, "lv") == NULL);
	CHECK(lvs_retained_take(&l, "lv", &r2) == -ENOENT);

	/* consume of an untaken record is refused and nothing is freed */
	CHECK(lvs_retained_add(&l, &id, &caps, &c) == 0);
	r1 = lvs_retained_find(&l, "lv");
	CHECK(lvs_retained_consume(&l, r1) == -EINVAL);
	CHECK(lvs_retained_find(&l, "lv") == r1);
	CHECK(lvs_retained_clear_idle(&l) == 1);
}

/* Records hold copies: mutating or freeing the source has no effect. */
static void
test_copies_not_pointers(void)
{
	struct lvs_retained_list l = TAILQ_HEAD_INITIALIZER(l);
	struct lvs_retained_ident *id = calloc(1, sizeof(*id));
	struct s3_owner_claim *c = calloc(1, sizeof(*c));
	struct s3_owner_caps caps = {0};
	struct lvs_retained *r;

	mk_ident(id, "lv", true);
	mk_claim(c);
	CHECK(lvs_retained_add(&l, id, &caps, c) == 0);
	memset(id, 0xA5, sizeof(*id));
	free(id);
	memset(c, 0xA5, sizeof(*c));
	free(c);
	r = lvs_retained_find(&l, "lv");
	CHECK(r && strcmp(r->ident.wal_bdev, "walbdev") == 0);
	CHECK(r && strcmp(r->claim.etag, "\"abc\"") == 0);
	CHECK(lvs_retained_clear_idle(&l) == 1);
}

int
main(void)
{
	test_eligibility();
	test_ident_check();
	test_copy_refuses_truncation();
	test_add_rules();
	test_take_put_back_consume();
	test_copies_not_pointers();
	if (g_failures) {
		fprintf(stderr, "retained_claim_ctl_test: %d failure(s) of %d checks\n",
			g_failures, g_checks);
		return 1;
	}
	printf("retained_claim_ctl_test: all %d checks passed (6 tests)\n",
	       g_checks);
	return 0;
}
