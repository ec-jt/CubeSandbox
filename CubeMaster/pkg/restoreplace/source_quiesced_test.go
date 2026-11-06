// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package restoreplace

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/nodemeta"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/scheduler/selctx"
)

const (
	testDigest = "b27d4a66044e3bc47447fe98595713142ee20a58dd436269d0bb6e8d5264f9bd"
	testOther  = "0000000000000000000000000000000000000000000000000000000000000001"
)

func quiescedRecordJSON(t *testing.T, sandbox, checkpoint, digest string) []byte {
	t.Helper()
	raw, err := json.Marshal(SourceQuiescedRecord{
		Version: 1,
		Binding: SourceQuiescedBinding{
			OperationID:      "op-1",
			SandboxID:        sandbox,
			NodeID:           "node-a",
			Incarnation:      "inc-1",
			CheckpointID:     checkpoint,
			CheckpointDigest: digest,
		},
		Stage:         LocalQuiesced,
		NativeBinding: json.RawMessage(`{"native":{"volume":"sb-x-root"}}`),
	})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return raw
}

// crossNodeInput is a placement that WOULD leave the origin: origin is
// unhealthy, the snapshot is S3 remote_ready, and a compatible peer exists.
func crossNodeInput() Input {
	in := s3ReadyInput("node-a")
	in.OriginNodeIP = "10.0.0.1"
	in.SourceSandboxID = "sb-1"
	in.SourceCheckpointID = "snap-1"
	in.SourceCheckpointDigest = testDigest
	return in
}

// crossNodeSeams installs the minimum seams for a cross-node decision and
// returns the peer the scheduler will pick.
func crossNodeSeams(t *testing.T) *node.Node {
	t.Helper()
	resetPlacementSeams(t)
	origin := &node.Node{InsID: "node-a", IP: "10.0.0.1", Healthy: false}
	peer := &node.Node{InsID: "node-b", IP: "10.0.0.2", Healthy: true}
	lookupNodeFn = func(nodeID, nodeIP string) *node.Node { return origin }
	listCompatibleFn = func(ctx context.Context, facts *nodemeta.HostFacts) ([]compatibleCandidate, error) {
		return []compatibleCandidate{
			{NodeID: "node-a", NodeIP: "10.0.0.1"},
			{NodeID: "node-b", NodeIP: "10.0.0.2"},
		}, nil
	}
	selectNodeFn = func(ctx context.Context, instanceType string, reqRes *selctx.RequestResource, scope []string) (*node.Node, error) {
		for _, s := range scope {
			if s == "node-b" {
				return peer, nil
			}
		}
		return nil, nil
	}
	return peer
}

func setQuiescedFlag(t *testing.T, value string) {
	t.Helper()
	t.Setenv(SourceQuiescedEnv, value)
}

func setQuiescedFetch(t *testing.T, fn func(context.Context, string, string, string) (*SourceQuiescedRecord, error)) {
	t.Helper()
	orig := fetchSourceQuiescedFn
	t.Cleanup(func() { fetchSourceQuiescedFn = orig })
	fetchSourceQuiescedFn = fn
}

// Flag off: placement is byte-for-byte the previous behaviour. The fetch seam
// must never be called, so a broken origin cannot change the outcome.
func TestDecideCrossNodeFlagOffIgnoresQuiescedRecord(t *testing.T) {
	peer := crossNodeSeams(t)
	setQuiescedFlag(t, "")
	called := false
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		called = true
		return nil, errors.New("must not be consulted")
	})

	got, err := Decide(context.Background(), crossNodeInput())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if !got.CrossNode || got.NodeID != peer.ID() {
		t.Fatalf("want cross-node %s, got %+v", peer.ID(), got)
	}
	if called {
		t.Fatal("flag off must not read the origin record")
	}
}

func TestDecideCrossNodeFlagExplicitFalseIgnoresQuiescedRecord(t *testing.T) {
	peer := crossNodeSeams(t)
	setQuiescedFlag(t, "false")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		t.Fatal("flag false must not read the origin record")
		return nil, nil
	})

	got, err := Decide(context.Background(), crossNodeInput())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if !got.CrossNode || got.NodeID != peer.ID() {
		t.Fatalf("want cross-node %s, got %+v", peer.ID(), got)
	}
}

// Matching record: the origin stopped its runtime and drained this exact
// checkpoint, so the restore may leave the origin.
func TestDecideCrossNodeMatchingQuiescedRecordAllowed(t *testing.T) {
	peer := crossNodeSeams(t)
	setQuiescedFlag(t, "true")
	var gotSandbox, gotIP, gotNode string
	setQuiescedFetch(t, func(_ context.Context, ip, nodeID, sandbox string) (*SourceQuiescedRecord, error) {
		gotIP, gotNode, gotSandbox = ip, nodeID, sandbox
		return decodeSourceQuiescedRecord(quiescedRecordJSON(t, "sb-1", "snap-1", testDigest))
	})

	got, err := Decide(context.Background(), crossNodeInput())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if !got.CrossNode || got.NodeID != peer.ID() {
		t.Fatalf("want cross-node %s, got %+v", peer.ID(), got)
	}
	if gotSandbox != "sb-1" || gotIP != "10.0.0.1" || gotNode != "node-a" {
		t.Fatalf("record read must target the origin, got ip=%q node=%q sandbox=%q", gotIP, gotNode, gotSandbox)
	}
}

// Missing record: the origin never drained, so the restore must not leave it.
func TestDecideCrossNodeMissingQuiescedRecordBlocked(t *testing.T) {
	crossNodeSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		return nil, errors.New("no such file or directory")
	})

	got, err := Decide(context.Background(), crossNodeInput())
	if err == nil {
		t.Fatalf("want refusal, got %+v", got)
	}
	if !strings.Contains(err.Error(), "local-quiesced record unavailable") {
		t.Fatalf("want unavailable-record refusal, got %v", err)
	}
}

// An empty record body is a missing record, not an implicit pass.
func TestDecideCrossNodeEmptyQuiescedRecordBlocked(t *testing.T) {
	crossNodeSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		return decodeSourceQuiescedRecord(nil)
	})

	got, err := Decide(context.Background(), crossNodeInput())
	if err == nil {
		t.Fatalf("want refusal, got %+v", got)
	}
	if !strings.Contains(err.Error(), "record absent") {
		t.Fatalf("want absent-record refusal, got %v", err)
	}
}

func TestDecideCrossNodeMismatchedCheckpointIDBlocked(t *testing.T) {
	crossNodeSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		return decodeSourceQuiescedRecord(quiescedRecordJSON(t, "sb-1", "snap-2", testDigest))
	})

	got, err := Decide(context.Background(), crossNodeInput())
	if err == nil {
		t.Fatalf("want refusal, got %+v", got)
	}
	if !strings.Contains(err.Error(), "checkpoint_id") {
		t.Fatalf("want checkpoint_id refusal, got %v", err)
	}
}

func TestDecideCrossNodeMismatchedDigestBlocked(t *testing.T) {
	crossNodeSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		return decodeSourceQuiescedRecord(quiescedRecordJSON(t, "sb-1", "snap-1", testOther))
	})

	got, err := Decide(context.Background(), crossNodeInput())
	if err == nil {
		t.Fatalf("want refusal, got %+v", got)
	}
	if !strings.Contains(err.Error(), "checkpoint_digest") {
		t.Fatalf("want checkpoint_digest refusal, got %v", err)
	}
}

func TestDecideCrossNodeMismatchedSandboxIDBlocked(t *testing.T) {
	crossNodeSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		return decodeSourceQuiescedRecord(quiescedRecordJSON(t, "sb-other", "snap-1", testDigest))
	})

	got, err := Decide(context.Background(), crossNodeInput())
	if err == nil {
		t.Fatalf("want refusal, got %+v", got)
	}
	if !strings.Contains(err.Error(), "sandbox_id") {
		t.Fatalf("want sandbox_id refusal, got %v", err)
	}
}

// A record that names a different origin node is foreign: refuse.
func TestDecideCrossNodeMismatchedOriginNodeBlocked(t *testing.T) {
	crossNodeSeams(t)
	setQuiescedFlag(t, "true")
	rec, err := decodeSourceQuiescedRecord(quiescedRecordJSON(t, "sb-1", "snap-1", testDigest))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	rec.Binding.NodeID = "node-z"
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		return rec, nil
	})

	got, err := Decide(context.Background(), crossNodeInput())
	if err == nil {
		t.Fatalf("want refusal, got %+v", got)
	}
	if !strings.Contains(err.Error(), "node_id") {
		t.Fatalf("want node_id refusal, got %v", err)
	}
}

// Unreachable origin: fail closed. No cross-node placement is produced.
func TestDecideCrossNodeUnreachableOriginFailsClosed(t *testing.T) {
	crossNodeSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		return nil, errors.New("rpc error: code = Unavailable desc = connection refused")
	})

	got, err := Decide(context.Background(), crossNodeInput())
	if err == nil {
		t.Fatalf("want refusal, got %+v", got)
	}
	if got != nil {
		t.Fatalf("fail closed must not return a placement, got %+v", got)
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("want transport cause in refusal, got %v", err)
	}
}

// An origin with no address cannot be asked, so it cannot be left.
func TestDecideCrossNodeMissingOriginAddressFailsClosed(t *testing.T) {
	crossNodeSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		t.Fatal("must not read without an origin address")
		return nil, nil
	})
	in := crossNodeInput()
	in.OriginNodeIP = ""

	got, err := Decide(context.Background(), in)
	if err == nil {
		t.Fatalf("want refusal, got %+v", got)
	}
	if !strings.Contains(err.Error(), "origin node address unknown") {
		t.Fatalf("want origin-address refusal, got %v", err)
	}
}

// An unknown expected sandbox identity is a refusal, never a wildcard.
func TestDecideCrossNodeMissingExpectedIdentityFailsClosed(t *testing.T) {
	crossNodeSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		t.Fatal("must not read without an expected sandbox identity")
		return nil, nil
	})
	in := crossNodeInput()
	in.SourceSandboxID = ""

	got, err := Decide(context.Background(), in)
	if err == nil {
		t.Fatalf("want refusal, got %+v", got)
	}
	if !strings.Contains(err.Error(), "source sandbox identity unknown") {
		t.Fatalf("want identity refusal, got %v", err)
	}
}

// An unparsable flag value must fail closed rather than silently disable.
func TestDecideCrossNodeUnparsableFlagFailsClosed(t *testing.T) {
	crossNodeSeams(t)
	setQuiescedFlag(t, "yes-please")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		return nil, errors.New("origin unreachable")
	})

	if _, err := Decide(context.Background(), crossNodeInput()); err == nil {
		t.Fatal("unparsable flag must still enforce the precondition")
	}
}

// Origin preference is untouched: a schedulable origin wins even with the
// precondition enabled and no record at all.
func TestDecideOriginPreferredWithQuiescedFlagOn(t *testing.T) {
	resetPlacementSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		t.Fatal("origin-preferred placement must not read the origin record")
		return nil, nil
	})
	origin := &node.Node{InsID: "node-a", IP: "10.0.0.1", Healthy: true}
	lookupNodeFn = func(nodeID, nodeIP string) *node.Node { return origin }
	selectNodeFn = func(ctx context.Context, instanceType string, reqRes *selctx.RequestResource, scope []string) (*node.Node, error) {
		return origin, nil
	}

	got, err := Decide(context.Background(), crossNodeInput())
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if got.CrossNode || got.NodeID != "node-a" {
		t.Fatalf("want origin node-a, got %+v", got)
	}
}

// PinToOrigin still wins over everything, including a valid record.
func TestDecidePinToOriginBeatsQuiescedRecord(t *testing.T) {
	crossNodeSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		t.Fatal("pinned placement must not read the origin record")
		return nil, nil
	})
	in := crossNodeInput()
	in.PinToOrigin = true

	got, err := Decide(context.Background(), in)
	if err == nil {
		t.Fatalf("want refusal, got %+v", got)
	}
	if !strings.Contains(err.Error(), "host-mount") {
		t.Fatalf("want host-mount refusal, got %v", err)
	}
}

// A non-S3 or not-ready snapshot never reaches the precondition.
func TestDecideNonS3DoesNotConsultQuiescedRecord(t *testing.T) {
	resetPlacementSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		t.Fatal("non-S3 placement must not read the origin record")
		return nil, nil
	})
	lookupNodeFn = func(nodeID, nodeIP string) *node.Node { return nil }
	in := crossNodeInput()
	in.Backend = constants.SnapshotBackendXFS

	if _, err := Decide(context.Background(), in); err == nil {
		t.Fatal("xfs must not cross-node")
	}
}

func TestDecodeSourceQuiescedRecordRejectsForeignShapes(t *testing.T) {
	good := quiescedRecordJSON(t, "sb-1", "snap-1", testDigest)
	if _, err := decodeSourceQuiescedRecord(good); err != nil {
		t.Fatalf("good record must decode: %v", err)
	}

	cases := map[string]string{
		"unknown field":    `{"version":1,"binding":{},"stage":"local-quiesced-not-global-fenced","native_binding":{},"verified":true}`,
		"wrong stage":      `{"version":1,"binding":{"operation_id":"o","sandbox_id":"s","node_id":"n","incarnation":"i","checkpoint_id":"c","checkpoint_digest":"` + testDigest + `"},"stage":"admitted","native_binding":{}}`,
		"wrong version":    `{"version":2,"binding":{"operation_id":"o","sandbox_id":"s","node_id":"n","incarnation":"i","checkpoint_id":"c","checkpoint_digest":"` + testDigest + `"},"stage":"local-quiesced-not-global-fenced","native_binding":{}}`,
		"uppercase digest": `{"version":1,"binding":{"operation_id":"o","sandbox_id":"s","node_id":"n","incarnation":"i","checkpoint_id":"c","checkpoint_digest":"` + strings.ToUpper(testDigest) + `"},"stage":"local-quiesced-not-global-fenced","native_binding":{}}`,
		"short digest":     `{"version":1,"binding":{"operation_id":"o","sandbox_id":"s","node_id":"n","incarnation":"i","checkpoint_id":"c","checkpoint_digest":"abcd"},"stage":"local-quiesced-not-global-fenced","native_binding":{}}`,
		"no native":        `{"version":1,"binding":{"operation_id":"o","sandbox_id":"s","node_id":"n","incarnation":"i","checkpoint_id":"c","checkpoint_digest":"` + testDigest + `"},"stage":"local-quiesced-not-global-fenced"}`,
		"trailing data":    string(good) + `{}`,
		"not json":         `not-json`,
	}
	for name, raw := range cases {
		if _, err := decodeSourceQuiescedRecord([]byte(raw)); err == nil {
			t.Fatalf("%s: want decode error", name)
		}
	}
}

// A caller-supplied "verified" boolean has no field to land in, so it cannot
// authorize anything: the record must come from the origin.
func TestValidateSourceQuiescedRequiresExactIdentity(t *testing.T) {
	rec, err := decodeSourceQuiescedRecord(quiescedRecordJSON(t, "sb-1", "snap-1", testDigest))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	base := crossNodeInput()
	if err := ValidateSourceQuiesced(base, rec); err != nil {
		t.Fatalf("matching identity must validate: %v", err)
	}
	for name, mutate := range map[string]func(*Input){
		"sandbox":    func(in *Input) { in.SourceSandboxID = "sb-2" },
		"checkpoint": func(in *Input) { in.SourceCheckpointID = "snap-2" },
		"digest":     func(in *Input) { in.SourceCheckpointDigest = testOther },
		"node":       func(in *Input) { in.OriginNodeID = "node-z" },
		"empty sand": func(in *Input) { in.SourceSandboxID = "" },
		"empty ckpt": func(in *Input) { in.SourceCheckpointID = "" },
		"empty dgst": func(in *Input) { in.SourceCheckpointDigest = "" },
	} {
		in := base
		mutate(&in)
		if err := ValidateSourceQuiesced(in, rec); err == nil {
			t.Fatalf("%s: want validation error", name)
		}
	}
	if err := ValidateSourceQuiesced(base, nil); err == nil {
		t.Fatal("nil record must not validate")
	}
}
