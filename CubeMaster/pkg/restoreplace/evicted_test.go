// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package restoreplace

import (
	"context"
	"errors"
	"testing"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/nodemeta"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/scheduler/selctx"
)

// evictedSeams: the origin is healthy and schedulable (it would win origin
// preference) and a compatible peer exists.
func evictedSeams(t *testing.T) (origin, peer *node.Node) {
	t.Helper()
	resetPlacementSeams(t)
	origin = &node.Node{InsID: "node-a", IP: "10.0.0.1", Healthy: true}
	peer = &node.Node{InsID: "node-b", IP: "10.0.0.2", Healthy: true}
	lookupNodeFn = func(string, string) *node.Node { return origin }
	listCompatibleFn = func(context.Context, *nodemeta.HostFacts) ([]compatibleCandidate, error) {
		return []compatibleCandidate{{NodeID: "node-a", NodeIP: "10.0.0.1"}, {NodeID: "node-b", NodeIP: "10.0.0.2"}}, nil
	}
	selectNodeFn = func(_ context.Context, _ string, _ *selctx.RequestResource, scope []string) (*node.Node, error) {
		for _, s := range scope {
			if s == "node-a" {
				return origin, nil
			}
		}
		for _, s := range scope {
			if s == "node-b" {
				return peer, nil
			}
		}
		return nil, nil
	}
	return origin, peer
}

// An evicted sandbox must leave the origin even when the origin has capacity,
// and the restore goes through the source-quiesced gate bound to the exact
// sandbox, checkpoint and committed digest.
func TestDecideExcludeOriginGoesCrossNodeThroughGate(t *testing.T) {
	_, peer := evictedSeams(t)
	setQuiescedFlag(t, "true")
	var gotSandbox string
	setQuiescedFetch(t, func(_ context.Context, _, _, sandbox string) (*SourceQuiescedRecord, error) {
		gotSandbox = sandbox
		return decodeSourceQuiescedRecord(quiescedRecordJSON(t, "sb-1", "snap-1", testDigest))
	})
	in := crossNodeInput()
	in.ExcludeOrigin = true

	got, err := Decide(context.Background(), in)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if !got.CrossNode || got.NodeID != peer.ID() {
		t.Fatalf("evicted restore must go cross-node to %s, got %+v", peer.ID(), got)
	}
	if gotSandbox != "sb-1" {
		t.Fatalf("gate must read the origin record for sb-1, got %q", gotSandbox)
	}

	// Without ExcludeOrigin the same healthy origin wins.
	in.ExcludeOrigin = false
	home, err := Decide(context.Background(), in)
	if err != nil || home.CrossNode || home.NodeID != "node-a" {
		t.Fatalf("non-evicted restore must stay home, got %+v err=%v", home, err)
	}
}

func TestDecideExcludeOriginRefusedByGateAndPolicy(t *testing.T) {
	evictedSeams(t)
	setQuiescedFlag(t, "true")

	// Digest mismatch: the record proves a different checkpoint.
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		return decodeSourceQuiescedRecord(quiescedRecordJSON(t, "sb-1", "snap-1", testOther))
	})
	in := crossNodeInput()
	in.ExcludeOrigin = true
	if _, err := Decide(context.Background(), in); err == nil {
		t.Fatal("gate must refuse a foreign checkpoint digest")
	}

	// Unreachable origin record.
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		return nil, errors.New("origin down")
	})
	if _, err := Decide(context.Background(), in); err == nil {
		t.Fatal("gate must refuse when the origin record is unavailable")
	}

	// ExcludeOrigin never weakens PinToOrigin or the remote_ready rule.
	setQuiescedFlag(t, "")
	pinned := crossNodeInput()
	pinned.ExcludeOrigin, pinned.PinToOrigin = true, true
	if _, err := Decide(context.Background(), pinned); err == nil {
		t.Fatal("host-mount pin must refuse an evicted restore")
	}
	notReady := crossNodeInput()
	notReady.ExcludeOrigin, notReady.RemoteStatus = true, "inprogress"
	if _, err := Decide(context.Background(), notReady); err == nil {
		t.Fatal("non-remote-ready checkpoint must refuse an evicted restore")
	}
}
