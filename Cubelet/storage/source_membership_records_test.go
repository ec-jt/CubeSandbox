// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
)

// useRecordsTestLocal installs a minimal local store whose db and failover dir
// are usable by recoveryStorageRecords. It never touches a real node path.
func useRecordsTestLocal(t *testing.T) *local {
	t.Helper()
	root := t.TempDir()
	db, err := utils.NewCubeStore(filepath.Join(root, "storage.db"), nil)
	if err != nil {
		t.Fatalf("new cube store: %v", err)
	}
	l := &local{config: &Config{StorageBackend: "cubecow", RootPath: root}, db: db}
	if err := os.MkdirAll(l.failoverDir(), 0o755); err != nil {
		t.Fatalf("mkdir failover: %v", err)
	}
	previous := localStorage
	localStorage = l
	t.Cleanup(func() {
		localStorage = previous
		db.Close()
	})
	return l
}

func putStorageRecord(t *testing.T, l *local, id string, info *StorageInfo) {
	t.Helper()
	raw, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("marshal %s: %v", id, err)
	}
	if err := l.db.Set(bucketName, id, raw); err != nil {
		t.Fatalf("set %s: %v", id, err)
	}
}

// A zero-length value is a torn write artifact. It must not abort the whole
// recovery membership read: that is what produced
// "unexpected end of JSON input" from sourceIncarnation.
func TestRecoveryStorageRecordsSkipsZeroLengthValue(t *testing.T) {
	l := useRecordsTestLocal(t)

	putStorageRecord(t, l, "sb-good", &StorageInfo{SandboxID: "sb-good"})
	if err := l.db.Set(bucketName, "sb-torn", []byte{}); err != nil {
		t.Fatalf("set torn: %v", err)
	}

	got, err := recoveryStorageRecords()
	if err != nil {
		t.Fatalf("recoveryStorageRecords returned error for a zero-length value: %v", err)
	}
	if _, ok := got["sb-torn"]; ok {
		t.Fatalf("zero-length record must not be reported as a record")
	}
	if _, ok := got["sb-good"]; !ok {
		t.Fatalf("valid record missing from result")
	}
}

// A zero-length failover file is the same artifact on the other storage path.
func TestRecoveryStorageRecordsSkipsZeroLengthFailoverFile(t *testing.T) {
	l := useRecordsTestLocal(t)

	putStorageRecord(t, l, "sb-good", &StorageInfo{SandboxID: "sb-good"})
	if err := os.WriteFile(filepath.Join(l.failoverDir(), "sb-torn"), nil, 0o644); err != nil {
		t.Fatalf("write failover: %v", err)
	}

	got, err := recoveryStorageRecords()
	if err != nil {
		t.Fatalf("recoveryStorageRecords returned error for a zero-length failover file: %v", err)
	}
	if _, ok := got["sb-torn"]; ok {
		t.Fatalf("zero-length failover entry must not be reported as a record")
	}
	if _, ok := got["sb-good"]; !ok {
		t.Fatalf("valid record missing from result")
	}
}

// A non-empty record that does not decode must still fail closed: recovery
// membership may never silently ignore a corrupt record.
func TestRecoveryStorageRecordsFailsClosedOnMalformedRecord(t *testing.T) {
	l := useRecordsTestLocal(t)

	if err := l.db.Set(bucketName, "sb-bad", []byte(`{"sandbox_id":`)); err != nil {
		t.Fatalf("set bad: %v", err)
	}

	if _, err := recoveryStorageRecords(); err == nil {
		t.Fatalf("malformed non-empty record must fail closed")
	}
}

// A record whose key and embedded identity disagree must still fail closed.
func TestRecoveryStorageRecordsFailsClosedOnIdentityMismatch(t *testing.T) {
	l := useRecordsTestLocal(t)

	putStorageRecord(t, l, "sb-a", &StorageInfo{SandboxID: "sb-b"})

	if _, err := recoveryStorageRecords(); err == nil {
		t.Fatalf("identity mismatch must fail closed")
	}
}
