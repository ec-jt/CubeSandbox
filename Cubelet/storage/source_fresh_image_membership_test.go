// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// freshImageMembershipFixture mirrors a real fresh-image S3 source: the rootfs
// is the default-medium volume Create minted (sb-<id>-root), the record carries
// the typed ownership marker, and the inventory contains that live volume plus
// an unrelated sandbox volume and the selected native objects.
func freshImageMembershipFixture() (sourcefence.Binding, *SourceStorageBinding, map[string]*StorageInfo, SourceMembership, map[string]*cubecow.Volume) {
	b := sourcefence.Binding{OperationID: "op", SandboxID: "sb", NodeID: "node", Incarnation: strings.Repeat("b", 64), CheckpointID: "cp", CheckpointDigest: strings.Repeat("a", 64)}
	e := &SourceStorageBinding{
		Version:      2,
		Cohort:       sourcefence.CohortFreshImageS3,
		RootfsMount:  "root",
		CommitSHA256: b.CheckpointDigest,
		LiveName:     "sb-sb-root",
		LiveUUID:     "live",
		LiveSize:     4096,
		Commit: CheckpointCommit{Binding: CheckpointDiskBinding{SandboxID: "sb", CheckpointID: "cp"}, Objects: map[string]CheckpointExport{
			"rootfs": {Snapshot: "cp-root", SnapshotUUID: "selected", SizeBytes: 4096, Endpoint: "native:9000", Bucket: "bucket", Prefix: "store", LvsName: "store"},
		}},
	}
	all := map[string]*StorageInfo{
		"sb": {
			SandboxID:     "sb",
			Backend:       cow.BackendS3,
			OwnershipMode: OwnershipModeFreshImageS3,
			Volumes: map[string]*BackendFileInfo{
				"root": {Name: "root", VolumeName: "sb-sb-root", Kind: cowKindVolume, Gen: 0},
			},
		},
		"other": {SandboxID: "other", Backend: cow.BackendS3, Volumes: map[string]*BackendFileInfo{"disk": {VolumeName: "other", Kind: cowKindVolume}}},
	}
	n := SourceMembership{
		StoreUUID:  "store-uuid",
		OwnerNonce: "owner",
		Endpoint:   "native:9000",
		Bucket:     "bucket",
		Prefix:     "store",
		Members: []SourceMember{
			{"sb-sb-root", "live", 4096, false, false},
			{"other", "other-uuid", 4096, false, false},
			{"cp-root", "selected", 4096, true, false},
		},
	}
	live := map[string]*cubecow.Volume{
		"sb-sb-root": {Name: "sb-sb-root", VolumeUUID: "live", SizeBytes: 4096},
		"other":      {Name: "other", VolumeUUID: "other-uuid", SizeBytes: 4096},
	}
	return b, e, all, n, live
}

func TestRecoveryMembershipFreshImageCohort(t *testing.T) {
	b, e, all, n, live := freshImageMembershipFixture()
	got, err := resolveRecoveryMembership(context.Background(), &S3Cow{engine: &fakeCowEngine{volumeInfos: live}}, b, e, all, &s3MetadataState{}, n)
	require.NoError(t, err)
	require.Len(t, got.Token, 64)
	require.Equal(t, "sb", got.Sandbox)
	require.Equal(t, uint32(0), got.Generation)
	for _, v := range got.Members {
		require.Equal(t, v.Name != "other", v.Source, v.Name)
	}
}

func TestRecoveryMembershipFreshImageCohortIsEnforced(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(e *SourceStorageBinding, all map[string]*StorageInfo)
	}{
		{"missing typed marker", func(_ *SourceStorageBinding, all map[string]*StorageInfo) {
			all["sb"].OwnershipMode = ""
		}},
		{"unknown cohort", func(e *SourceStorageBinding, _ map[string]*StorageInfo) {
			e.Cohort = "template"
		}},
		{"template cohort with a fresh-image mount", func(e *SourceStorageBinding, _ map[string]*StorageInfo) {
			e.Cohort = ""
			e.Version = 1
		}},
		{"fresh-image cohort without a mount", func(e *SourceStorageBinding, _ map[string]*StorageInfo) {
			e.RootfsMount = ""
		}},
		{"persisted record lost the volume", func(_ *SourceStorageBinding, all map[string]*StorageInfo) {
			delete(all["sb"].Volumes, "root")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, e, all, n, live := freshImageMembershipFixture()
			tc.mutate(e, all)
			got, err := resolveRecoveryMembership(context.Background(), &S3Cow{engine: &fakeCowEngine{volumeInfos: live}}, b, e, all, &s3MetadataState{}, n)
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
}
