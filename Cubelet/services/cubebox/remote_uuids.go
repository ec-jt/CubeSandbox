// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"context"
	"fmt"
	"strings"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// uploadRemoteUUIDsIfS3 runs cubecow_export_snapshot (via UploadSnapshot)
// after Pause / CommitSandbox has already sealed package disks to
// snapshots. Templates (AppSnapshot) do not export. XFS is a no-op.
// Export acceptance is not remote readiness. The caller must propagate errors;
// Master keeps accepted exports inprogress until status polling proves DONE.
func uploadRemoteUUIDsIfS3(ctx context.Context, backend, snapshotID string) (string, error) {
	normalized, err := cow.NormalizeBackend(backend)
	if err != nil {
		return "", err
	}
	if normalized != cow.BackendS3 {
		return "", nil
	}
	uuids, err := storage.UploadSnapshot(ctx, normalized, snapshotID)
	if err != nil {
		return "", err
	}
	return acceptedSnapshotUUIDs(uuids)
}

func acceptedSnapshotUUIDs(uuids *cow.RemoteUUIDs) (string, error) {
	if uuids == nil || strings.TrimSpace(uuids.Rootfs) == "" || strings.TrimSpace(uuids.Memory) == "" || strings.TrimSpace(uuids.Metadata) == "" {
		return "", fmt.Errorf("S3 package export did not accept all rootfs, memory, and metadata roles")
	}
	return uuids.JSON(), nil
}
