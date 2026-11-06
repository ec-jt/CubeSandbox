/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#include <stdio.h>
#include <stdlib.h>
#include "s3_put_etag.h"

static unsigned checks;
#define CHECK(x) do { checks++; if (!(x)) { \
	fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #x); exit(1); \
} } while (0)

int
main(void)
{
	const char *bad[] = {"", "*", "\"*\"", "W/abc", "W/\"abc\"", "\"W/abc\"",
		"\"\"", "\"", "\"abc", "abc\"", "a\"b", "\"a\"b\"", "a,b", "\"a,b\"",
		"\"a\",\"b\"", " abc", "abc ", "\"a b\"", "a\tb", "a\r\nb", "a\\b",
		"a\x7f", "a\x80"};
	const char *good[] = {"abc", "\"abc\"", "deadbeef-12", "\"deadbeef-12\""};
	char out[128], input[130], original[130];
	bool mode = true;
	CHECK(s3_put_etag_mode_parse(NULL, &mode) == 0 && !mode);
	CHECK(s3_put_etag_mode_parse("0", &mode) == 0 && !mode);
	CHECK(s3_put_etag_mode_parse("1", &mode) == 0 && mode);
	const char *bad_modes[] = {"", "true", "false", "yes", "2", "01", " 1", "1 "};
	for (size_t i = 0; i < sizeof(bad_modes) / sizeof(bad_modes[0]); i++) {
		CHECK(s3_put_etag_mode_parse(bad_modes[i], &mode) == -EINVAL && mode);
	}
	for (int bare = 0; bare <= 1; bare++) {
		for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
			strcpy(out, "untouched");
			CHECK(s3_put_etag_format(out, sizeof(out), bad[i], bare) == -EINVAL);
			CHECK(strcmp(out, "untouched") == 0);
		}
		for (size_t i = 0; i < sizeof(good) / sizeof(good[0]); i++) {
			strcpy(input, good[i]);
			strcpy(original, input);
			CHECK(s3_put_etag_format(out, sizeof(out), input, bare) == 0);
			CHECK(strcmp(input, original) == 0);
			if (bare && input[0] == '"') {
				CHECK(strlen(out) == strlen(input) - 2);
				CHECK(memcmp(out, input + 1, strlen(out)) == 0);
			} else {
				CHECK(strcmp(out, input) == 0);
			}
		}
		memset(input, 'a', 127); input[127] = 0;
		CHECK(s3_put_etag_format(out, sizeof(out), input, bare) == 0);
		CHECK(strlen(out) == 127);
		input[0] = '"'; input[126] = '"';
		CHECK(s3_put_etag_format(out, sizeof(out), input, bare) == 0);
		CHECK(strlen(out) == (bare ? 125U : 127U));
		input[126] = 'a'; input[127] = '"'; input[128] = 0;
		CHECK(s3_put_etag_format(out, sizeof(out), input, bare) == -ENAMETOOLONG);
		memset(input, 'a', sizeof(input)); /* bounded non-NUL input */
		CHECK(s3_put_etag_format(out, sizeof(out), input, bare) == -ENAMETOOLONG);
		CHECK(s3_put_etag_format(out, sizeof(out), NULL, bare) == -EINVAL);
		for (unsigned c = 1; c <= 255; c++) {
			char tag[] = {'a', (char)c, 'b', 0};
			bool invalid = c <= 0x20 || c >= 0x7f || c == '"' || c == ',' || c == '\\';
			CHECK(s3_put_etag_format(out, sizeof(out), tag, bare) == (invalid ? -EINVAL : 0));
		}
	}
	CHECK(s3_put_etag_format(out, 0, "abc", true) == -EINVAL);
	CHECK(s3_put_etag_format(NULL, 128, "abc", true) == -EINVAL);
	printf("s3_put_etag_test: %u checks passed\n", checks);
	return 0;
}
