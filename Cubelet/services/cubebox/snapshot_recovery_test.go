// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestSnapshotRecoveryOutcome(t *testing.T) {
	// Synthetic errors deliberately include storage/metadata faults. None can
	// authorize a cold boot by looking like a memory restore error.
	for _, cause := range []string{"", "memory missing", "memory corrupt", "incompatible snapshot", "ownership denied", "disk integrity failure", "metadata missing", "probe failure", "RPC timeout"} {
		t.Run(cause, func(t *testing.T) {
			labels := map[string]string{"user-data": "preserved"}
			var history []string
			finish := beginSnapshotRecovery(true, func(update map[string]string) {
				for k, v := range update {
					labels[k] = v
				}
				history = append(history, labels[recoveryModeLabel])
			})
			if labels[recoveryModeLabel] != "pending" {
				t.Fatal(labels)
			}
			var input error
			if cause != "" {
				input = errors.New(cause)
			}
			err := finish(input)
			want := "unknown"
			if input != nil {
				want = "failed"
				if !errors.Is(err, input) || !strings.Contains(err.Error(), "disk_only_fallback=refused") {
					t.Fatalf("lost cause/refusal: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(history, []string{"pending", want}) {
				t.Fatal(history)
			}
			if labels[recoveryProcessLabel] != "unverified" || labels["user-data"] != "preserved" {
				t.Fatal(labels)
			}
		})
	}
}

func TestSnapshotRecoveryNormalCreateUnchanged(t *testing.T) {
	finish := beginSnapshotRecovery(false, func(map[string]string) { t.Fatal("normal create got recovery labels") })
	if finish(nil) != nil {
		t.Fatal("success changed")
	}
	err := errors.New("original failure")
	if finish(err) != err {
		t.Fatal("error changed")
	}
}
