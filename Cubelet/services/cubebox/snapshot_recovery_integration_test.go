// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

func TestSnapshotRecoveryLabelsCannotBeSuppliedByCaller(t *testing.T) {
	labels := stripUserCubeMasterLabels(map[string]string{
		recoveryModeLabel: "disk_only", recoveryProcessLabel: "preserved",
		recoveryFallbackLabel: "allowed", "user-label": "keep",
	})
	if len(labels) != 1 || labels["user-label"] != "keep" {
		t.Fatal(labels)
	}
}

func TestSnapshotRecoveryStatusRoundTrip(t *testing.T) {
	// Synthetic CubeBox, real metadata update and status serializer.
	box := newCubeboxWithStatusForTest("synthetic-recovery", cubeboxstore.Status{})
	finish := beginSnapshotRecovery(true, box.Metadata.AddLabels)
	if err := finish(errors.New("memory corrupt")); err == nil {
		t.Fatal("failure lost")
	}
	status := toGRPCCubeBox(box, &cubebox.ListCubeSandboxOption{})
	if status.Labels[recoveryModeLabel] != "failed" || status.Labels[recoveryFallbackLabel] != "refused" || status.Labels[recoveryProcessLabel] != "unverified" {
		t.Fatal(status.Labels)
	}
	finish = beginSnapshotRecovery(true, box.Metadata.AddLabels)
	if err := finish(nil); err != nil {
		t.Fatal(err)
	}
	status = toGRPCCubeBox(box, &cubebox.ListCubeSandboxOption{})
	if status.Labels[recoveryModeLabel] != "unknown" || status.Labels[recoveryProcessLabel] != "unverified" {
		t.Fatal(status.Labels)
	}
}

func TestSnapshotRecoveryCrossNodeRefusalsUnchanged(t *testing.T) {
	// Synthetic requests fail before any storage import or VM operation.
	for _, tc := range []struct{ backend, remote, want string }{
		{"xfs", "", "needs the s3 backend"},
		{"s3", "", "carries no remote_uuids"},
		{"s3", "not-json", "carries no remote_uuids"},
		{"s3", `{"rootfs":"synthetic-root","memory":"synthetic-memory"}`, "carries no metadata export"},
	} {
		req := &cubebox.RunCubeSandboxRequest{Annotations: map[string]string{
			constants.MasterAnnotationSnapshotCrossNode:   "true",
			constants.MasterAnnotationStorageBackend:      tc.backend,
			constants.MasterAnnotationSnapshotRemoteUUIDs: tc.remote,
			constants.MasterAnnotationDesiredSandboxID:    "synthetic-recovery",
		}}
		err := prepareCrossNodeRestore(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("want %q, got %v", tc.want, err)
		}
	}
	if err := prepareCrossNodeRestore(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}
