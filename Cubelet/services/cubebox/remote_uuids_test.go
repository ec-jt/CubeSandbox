// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"testing"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

func TestScheduleRemoteExportIfS3SkipsXFS(t *testing.T) {
	prev := scheduleSnapshotExportFn
	scheduleSnapshotExportFn = func(string, string) (bool, error) {
		t.Fatal("xfs must not schedule an export")
		return false, nil
	}
	t.Cleanup(func() { scheduleSnapshotExportFn = prev })
	for _, backend := range []string{"", cow.BackendXFS, "cubecow"} {
		if pending, err := scheduleRemoteExportIfS3(backend, "snap-1"); pending || err != nil {
			t.Fatalf("backend %q: pending=%v err=%v", backend, pending, err)
		}
	}
}

func TestScheduleRemoteExportIfS3HandsOffWithoutWaiting(t *testing.T) {
	prev := scheduleSnapshotExportFn
	var got string
	scheduleSnapshotExportFn = func(backend, id string) (bool, error) {
		got = backend + "/" + id
		return true, nil
	}
	t.Cleanup(func() { scheduleSnapshotExportFn = prev })
	pending, err := scheduleRemoteExportIfS3("s3", "snap-1")
	if err != nil || !pending || got != cow.BackendS3+"/snap-1" {
		t.Fatalf("pending=%v err=%v got=%s", pending, err, got)
	}
}
