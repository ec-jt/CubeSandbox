// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package restoreplace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

var evictedCommit = []byte(`{"version":1,"binding":{"sandbox_id":"sb-1"},"objects":{}}`)

func evictedCommitDigest() string {
	s := sha256.Sum256(evictedCommit)
	return hex.EncodeToString(s[:])
}

func evictionRecordJSON(t *testing.T, sandbox, checkpoint, node, digest string, commit []byte) []byte {
	t.Helper()
	native, err := json.Marshal(evictedNativeBinding{Kind: EvictedNativeKind, CheckpointCommit: commit})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(SourceQuiescedRecord{
		Version: 1,
		Binding: SourceQuiescedBinding{OperationID: "evict-op", SandboxID: sandbox, NodeID: node,
			Incarnation: "inc", CheckpointID: checkpoint, CheckpointDigest: digest},
		Stage: LocalQuiesced, NativeBinding: native,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func evictedInput(record []byte) Input {
	in := crossNodeInput()
	in.SourceCheckpointDigest = evictedCommitDigest()
	in.ExcludeOrigin = true
	in.PersistedSourceQuiescedRecord = record
	return in
}

// Gate ON, evicted binding with a valid persisted record: the restore goes
// cross-node while the origin is unreachable, and the origin is never asked.
func TestDecideEvictedPersistedRecordAllowedWithOriginUnreachable(t *testing.T) {
	_, peer := evictedSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		t.Fatal("origin must not be consulted when a persisted eviction record exists")
		return nil, errors.New("origin unreachable")
	})
	in := evictedInput(evictionRecordJSON(t, "sb-1", "snap-1", "node-a", evictedCommitDigest(), evictedCommit))
	got, err := Decide(context.Background(), in)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if !got.CrossNode || got.NodeID != peer.ID() {
		t.Fatalf("want cross-node to %s, got %+v", peer.ID(), got)
	}
}

// Every mismatched, tampered, empty or truncated persisted record is refused,
// and a present-but-invalid record never falls back to the origin.
func TestDecideEvictedPersistedRecordRefusals(t *testing.T) {
	evictedSeams(t)
	setQuiescedFlag(t, "true")
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		// Even a "valid" origin answer must not rescue a bad persisted record.
		return decodeSourceQuiescedRecord(quiescedRecordJSON(t, "sb-1", "snap-1", evictedCommitDigest()))
	})
	d := evictedCommitDigest()
	good := evictionRecordJSON(t, "sb-1", "snap-1", "node-a", d, evictedCommit)
	tampered := []byte(strings.Replace(string(good), `"evict-op"`, `"evict-op","extra":1`, 1))
	otherCommit := []byte(`{"version":1,"objects":{"x":{}}}`)
	cases := map[string][]byte{
		"sandbox mismatch":     evictionRecordJSON(t, "sb-2", "snap-1", "node-a", d, evictedCommit),
		"checkpoint mismatch":  evictionRecordJSON(t, "sb-1", "snap-2", "node-a", d, evictedCommit),
		"node mismatch":        evictionRecordJSON(t, "sb-1", "snap-1", "node-b", d, evictedCommit),
		"digest mismatch":      evictionRecordJSON(t, "sb-1", "snap-1", "node-a", testOther, evictedCommit),
		"commit does not hash": evictionRecordJSON(t, "sb-1", "snap-1", "node-a", d, otherCommit),
		"unknown field":        tampered,
		"truncated":            good[:len(good)-5],
		"trailing data":        append(append([]byte(nil), good...), []byte(`{}`)...),
		"fence native binding": quiescedRecordJSON(t, "sb-1", "snap-1", d),
		"wrong stage":          []byte(strings.Replace(string(good), LocalQuiesced, "runtime-stopped-storage-unproven", 1)),
		"whitespace-only":      []byte("   "),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Decide(context.Background(), evictedInput(raw)); err == nil {
				t.Fatalf("%s must be refused", name)
			}
		})
	}
}

// Evicted binding without any persisted record (legacy, pre-column) falls
// back to the origin read, unchanged.
func TestDecideEvictedWithoutPersistedRecordFallsBackToOrigin(t *testing.T) {
	evictedSeams(t)
	setQuiescedFlag(t, "true")
	called := false
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		called = true
		return nil, errors.New("origin down")
	})
	if _, err := Decide(context.Background(), evictedInput(nil)); err == nil {
		t.Fatal("no persisted record and unreachable origin must refuse")
	}
	if !called {
		t.Fatal("evicted binding without persisted record must read the origin")
	}
}

// A non-evicted cross-node restore ignores any persisted record and keeps the
// origin read exactly as before.
func TestDecideNonEvictedIgnoresPersistedRecord(t *testing.T) {
	peer := crossNodeSeams(t)
	setQuiescedFlag(t, "true")
	called := false
	setQuiescedFetch(t, func(context.Context, string, string, string) (*SourceQuiescedRecord, error) {
		called = true
		return decodeSourceQuiescedRecord(quiescedRecordJSON(t, "sb-1", "snap-1", testDigest))
	})
	in := crossNodeInput()
	in.PersistedSourceQuiescedRecord = evictionRecordJSON(t, "sb-1", "snap-1", "node-a", evictedCommitDigest(), evictedCommit)
	got, err := Decide(context.Background(), in)
	if err != nil || !got.CrossNode || got.NodeID != peer.ID() {
		t.Fatalf("non-evicted restore: got %+v err=%v", got, err)
	}
	if !called {
		t.Fatal("non-evicted cross-node restore must read the origin")
	}
}

// Flag off: the persisted record is never even decoded.
func TestDecideEvictedFlagOffIgnoresPersistedRecord(t *testing.T) {
	evictedSeams(t)
	setQuiescedFlag(t, "")
	if _, err := Decide(context.Background(), evictedInput([]byte("garbage"))); err != nil {
		t.Fatalf("flag off must not consult the record: %v", err)
	}
}
