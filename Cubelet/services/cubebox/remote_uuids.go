// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"fmt"
	"strings"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// scheduleRemoteExportIfS3 hands a sealed S3 package to the asynchronous
// export worker (B15) after Pause / CommitSandbox sealed it locally. It never
// waits for S3. Templates (AppSnapshot) do not export. XFS is a no-op and
// reports no pending export. The returned flag is what the RPC response
// carries so Master records remote_status=inprogress with empty export_uuids.
func scheduleRemoteExportIfS3(backend, snapshotID string) (bool, error) {
	normalized, err := cow.NormalizeBackend(backend)
	if err != nil {
		return false, err
	}
	if normalized != cow.BackendS3 {
		return false, nil
	}
	return scheduleSnapshotExportFn(normalized, snapshotID)
}

// scheduleSnapshotExportFn is swapped in tests.
var scheduleSnapshotExportFn = storage.ScheduleSnapshotExport

func acceptedSnapshotUUIDs(uuids *cow.RemoteUUIDs) (string, error) {
	if uuids == nil || strings.TrimSpace(uuids.Rootfs) == "" || strings.TrimSpace(uuids.Memory) == "" || strings.TrimSpace(uuids.Metadata) == "" {
		return "", fmt.Errorf("S3 package export did not accept all rootfs, memory, and metadata roles")
	}
	return uuids.JSON(), nil
}
