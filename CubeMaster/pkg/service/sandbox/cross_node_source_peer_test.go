// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package sandbox

import "testing"

func TestCrossNodeSourcePeer(t *testing.T) {
	for _, tc := range []struct{ origin, target, want string }{
		{"10.0.1.1", "10.0.1.2", "10.0.1.1"},
		{" 10.0.1.2 ", "10.0.1.1", "10.0.1.2"},
		{"10.0.1.1", "10.0.1.1", ""},
		{"", "10.0.1.1", ""},
		{"0.0.0.0", "10.0.1.1", ""},
		{"127.0.0.1", "10.0.1.1", ""},
		{"app1", "10.0.1.1", ""},
		{"fe80::1", "10.0.1.1", ""},
	} {
		if got := crossNodeSourcePeer(tc.origin, tc.target); got != tc.want {
			t.Errorf("crossNodeSourcePeer(%q, %q) = %q, want %q", tc.origin, tc.target, got, tc.want)
		}
	}
}
