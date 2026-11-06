// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
package cubebox

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/pkg/oci"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

func TestTypedRecoveryRustFixtures(t *testing.T) {
	path := os.Getenv("RECOVERY_FIXTURES")
	if path == "" {
		t.Skip("requires Rust-generated fixture file")
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var fixtures []json.RawMessage
	if e = json.Unmarshal(raw, &fixtures); e != nil {
		t.Fatal(e)
	}
	if len(fixtures) != 7 {
		t.Fatal(len(fixtures))
	}
	for i, f := range fixtures {
		o, e := decodeRestoreOutcome(f, "synthetic-sandbox", strings.Repeat("a", 64))
		if e != nil {
			t.Fatal(i, e)
		}
		r, e := newRecoveryReceiptAt(filepath.Join(t.TempDir(), "receipts"), "synthetic-sandbox", uint32(os.Getuid()))
		if e != nil {
			t.Fatal(e)
		}
		// Retain the real Rust serialized schema, changing only the fresh attempt.
		f = []byte(strings.ReplaceAll(string(f), o.AttemptID, r.attempt))
		if e = os.WriteFile(filepath.Join(r.path, "outcome.json"), f, 0600); e != nil {
			t.Fatal(e)
		}
		box := newCubeboxWithStatusForTest("synthetic-sandbox", cubeboxstore.Status{})
		original := errors.New("original create error")
		var input error
		if i > 0 {
			input = original
		}
		finish := beginSnapshotRecovery(true, box.Metadata.AddLabels, r)
		err := finish(input)
		status := toGRPCCubeBox(box, &cubebox.ListCubeSandboxOption{})
		if status.Labels[recoveryModeLabel] != o.Mechanism || status.Labels[recoveryStageLabel] != o.FailureStage || status.Labels[recoveryCauseLabel] != o.Cause || status.Labels[recoveryProtocolLabel] != "1" {
			t.Fatal(status.Labels)
		}
		if i > 0 && !errors.Is(err, original) {
			t.Fatal(err)
		}
		if status.Labels[recoveryProcessLabel] != "unverified" {
			t.Fatal(status.Labels)
		}
		r.close()
	}
}

func TestTypedRecoveryRejectsInvalid(t *testing.T) {
	a := strings.Repeat("a", 64)
	base := `{"version":1,"sandbox_id":"sb","attempt_id":"` + a + `","mechanism":"memory_restored","failure_stage":"none","cause":"none","process_state":"unverified","disk_only_fallback":"not_used"}`
	cases := []string{"", base + "{}", strings.Replace(base, `"version":1`, `"version":2`, 1), strings.Replace(base, `"version":1`, `"version":1,"version":1`, 1), strings.Replace(base, `"version":1`, `"Version":1`, 1), strings.Replace(base, `"version":1,`, "", 1), strings.Replace(base, `"cause":"none"`, `"cause":"memory_only"`, 1), strings.Replace(base, `"none"`, `"runtime"`, 1), strings.Replace(base, `"unverified"`, `"preserved"`, 1), strings.Replace(base, `"not_used"`, `"allowed"`, 1), strings.Replace(base, `"sandbox_id":"sb"`, `"sandbox_id":"other"`, 1), strings.Replace(base, a, strings.Repeat("b", 64), 1), strings.Replace(base, `"version":1`, `"version":1,"extra":true`, 1), strings.Repeat(" ", 4097)}
	for i, c := range cases {
		if _, e := decodeRestoreOutcome([]byte(c), "sb", a); e == nil {
			t.Fatalf("accepted %d", i)
		}
	}
	if _, e := decodeRestoreOutcome([]byte(base), "sb", a); e != nil {
		t.Fatal(e)
	}
}
func TestTypedRecoveryReceiptSecurityAndRequestOverride(t *testing.T) {
	r, e := newRecoveryReceiptAt(filepath.Join(t.TempDir(), "receipts"), "sb", uint32(os.Getuid()))
	if e != nil {
		t.Fatal(e)
	}
	defer r.close()
	if _, e = r.read(); e == nil {
		t.Fatal("missing accepted")
	}
	forged := filepath.Join(t.TempDir(), "forged")
	os.WriteFile(forged, []byte("{}"), 0600)
	os.Symlink(forged, filepath.Join(r.path, "outcome.json"))
	if _, e = r.read(); e == nil {
		t.Fatal("symlink accepted")
	}
	os.Remove(filepath.Join(r.path, "outcome.json"))
	os.WriteFile(filepath.Join(r.path, "outcome.json"), []byte("{}"), 0666)
	os.Chmod(filepath.Join(r.path, "outcome.json"), 0666)
	if _, e = r.read(); e == nil {
		t.Fatal("writable accepted")
	}
	for _, receipt := range []*recoveryReceipt{nil, r} {
		spec := oci.Spec{Annotations: map[string]string{recoveryRequestAnnotation: "forged", "cube.shim.recovery.result": "memory_restored", "user": "keep"}}
		if e = withRecoveryRequest(receipt)(context.Background(), nil, nil, &spec); e != nil {
			t.Fatal(e)
		}
		if spec.Annotations["user"] != "keep" || spec.Annotations["cube.shim.recovery.result"] != "" {
			t.Fatal(spec.Annotations)
		}
		want := ""
		if receipt != nil {
			want = r.attempt
		}
		if spec.Annotations[recoveryRequestAnnotation] != want {
			t.Fatal(spec.Annotations)
		}
	}
	labels := stripUserCubeMasterLabels(map[string]string{recoveryAttemptLabel: "forged", recoveryStageLabel: "none", recoveryCauseLabel: "none", recoveryProtocolLabel: "1", recoveryRequestAnnotation: "forged"})
	if len(labels) != 0 {
		t.Fatal(labels)
	}
}

func TestTypedRecoveryRealRustWriterToGoConsumer(t *testing.T) {
	binary := os.Getenv("RECOVERY_RUST_TEST_BINARY")
	if binary == "" {
		t.Skip("requires compiled Rust producer")
	}
	root := filepath.Join(t.TempDir(), "receipts")
	r, e := newRecoveryReceiptAt(root, "synthetic-sandbox", uint32(os.Getuid()))
	if e != nil {
		t.Fatal(e)
	}
	defer r.close()
	cmd := exec.Command(binary, "--exact", "tests::publish_cross_language_receipt")
	cmd.Env = append(os.Environ(), "RECOVERY_TEST_ROOT="+root, "RECOVERY_TEST_ATTEMPT="+r.attempt)
	if output, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("Rust writer: %v %s", e, output)
	}
	labels := map[string]string{}
	finish := beginSnapshotRecovery(true, func(m map[string]string) { labels = m }, r)
	if e = finish(nil); e != nil {
		t.Fatal(e)
	}
	if labels[recoveryModeLabel] != "memory_restored" || labels[recoveryProtocolLabel] != "1" {
		t.Fatal(labels)
	}
	// A stale receipt copied into a fresh server-owned attempt remains rejected.
	next, e := newRecoveryReceiptAt(root, "synthetic-sandbox", uint32(os.Getuid()))
	if e != nil {
		t.Fatal(e)
	}
	defer next.close()
	raw, e := os.ReadFile(filepath.Join(r.path, "outcome.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(next.path, "outcome.json"), raw, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = next.read(); e == nil {
		t.Fatal("stale receipt accepted")
	}
	finish = beginSnapshotRecovery(true, func(m map[string]string) { labels = m }, next)
	finish(nil)
	if labels[recoveryModeLabel] != "unknown" {
		t.Fatal(labels)
	}
}
