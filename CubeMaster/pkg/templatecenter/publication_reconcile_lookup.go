// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package templatecenter

import (
	"context"
	"errors"
	"strings"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox"
)

// The operator publication reconcile (B15) lives in the sandbox package, which
// this package imports. Register the snapshot-table lookup it needs for
// commit holds here instead of importing back.
func init() {
	sandbox.SetCommitSnapshotLookup(commitSnapshotStateForReconcile)
}

func commitSnapshotStateForReconcile(ctx context.Context, snapshotID string) (sandbox.CommitSnapshotState, error) {
	rec, err := getSnapshotRecord(ctx, snapshotID)
	if errors.Is(err, ErrSnapshotNotFound) {
		return sandbox.CommitSnapshotState{}, nil
	}
	if err != nil {
		return sandbox.CommitSnapshotState{}, err
	}
	return sandbox.CommitSnapshotState{
		Found:        true,
		Ready:        strings.EqualFold(strings.TrimSpace(rec.Status), StatusReady),
		OriginNodeIP: strings.TrimSpace(rec.OriginNodeIP),
		SandboxID:    strings.TrimSpace(rec.OriginSandboxID),
	}, nil
}
