/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#ifndef S3LVOL_PUT_ETAG_H
#define S3LVOL_PUT_ETAG_H

#include <stdbool.h>
#include <stddef.h>
#include <errno.h>
#include <string.h>

/* Deliberately explicit: typos must not silently select a wire format. */
static inline int
s3_put_etag_mode_parse(const char *value, bool *bare)
{
	if (!value || strcmp(value, "0") == 0) {
		*bare = false;
	} else if (strcmp(value, "1") == 0) {
		*bare = true;
	} else {
		return -EINVAL;
	}
	return 0;
}

/* One nonempty strong comparand, never a list or wildcard. Preserve valid
 * input byte-for-byte by default. Compatibility mode removes ONLY an outer
 * quote pair; response/stored ETags never pass through this helper.
 *
 * Reject ambiguous HTTP syntax even inside quotes: after stripping, commas,
 * whitespace, backslashes and weak-tag prefixes must not acquire new meaning.
 * The input bound is enforced BEFORE stripping, matching the old API limit.
 * On error, dst is untouched. src and dst must not overlap. */
static inline int
s3_put_etag_format(char *dst, size_t capacity, const char *src, bool bare)
{
	size_t len, begin = 0, end, i;
	bool quoted;

	if (!dst || !src || capacity == 0) {
		return -EINVAL;
	}
	len = strnlen(src, capacity);
	if (len == capacity) {
		return -ENAMETOOLONG;
	}
	if (len == 0) {
		return -EINVAL;
	}
	end = len;
	quoted = src[0] == '"';
	if (quoted) {
		if (len < 3 || src[len - 1] != '"') {
			return -EINVAL;
		}
		begin = 1;
		end--;
	}
	if ((end - begin == 1 && src[begin] == '*') ||
	    (end - begin >= 2 && src[begin] == 'W' && src[begin + 1] == '/')) {
		return -EINVAL;
	}
	for (i = begin; i < end; i++) {
		unsigned char c = (unsigned char)src[i];

		if (c <= 0x20 || c >= 0x7f || c == '"' || c == ',' || c == '\\') {
			return -EINVAL;
		}
	}
	if (!bare || !quoted) {
		begin = 0;
		end = len;
	}
	memcpy(dst, src + begin, end - begin);
	dst[end - begin] = '\0';
	return 0;
}

#endif
