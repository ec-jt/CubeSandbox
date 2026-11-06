// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

// OwnershipModeFreshImageS3 is the typed storage ownership marker the node
// writes at Create time when it mints a brand-new native default-medium volume
// for the sandbox rootfs instead of deriving it from a template or selected
// snapshot. It is node-decided data: no request field or annotation can set it.
//
// This marker exists because an S3/cubecow node refuses template creation
// (local.go CreateCubeboxBaseStorage and the IsCreateSnapshot path), so a
// fresh-image sandbox can never carry a LocalRunTemplate. Recovery admission
// therefore needs a distinct, typed way to name that cohort instead of
// accepting a nil template as "unidentified".
const OwnershipModeFreshImageS3 = "fresh-image-s3-v1"

// FreshImageSourceVolume is the resolved identity of a fresh-image S3 source.
// Every field comes from authoritative persisted records plus the live native
// volume, never from a request.
type FreshImageSourceVolume struct {
	Backend   string
	MountName string
	Name      string
	Kind      string
	Gen       uint32
	UUID      string
	SizeBytes uint64
}

func (v *FreshImageSourceVolume) object() *CowSnapshotObject {
	return &CowSnapshotObject{Name: v.Name, MountName: v.MountName, Kind: v.Kind, Gen: v.Gen, SizeBytes: v.SizeBytes}
}

// freshImageVolumeName mirrors the S3 default-medium naming rule exactly
// ("sb-<sandboxID>-<rootfs mount name>"). Create and recovery share it so a
// recovered source can never name a volume Create would not have produced.
func freshImageVolumeName(sandboxID, mountName string) string {
	return fmt.Sprintf("sb-%s-%s", sandboxID, mountName)
}

func validSourceIdentifier(s string) bool {
	return s != "" && s == strings.TrimSpace(s) && len(s) <= 128 && !strings.ContainsAny(s, "\x00\r\n/")
}

// ResolveFreshImageSourceVolume derives the fresh-image source identity from the
// persisted StorageInfo ownership marker, the persisted rootfs volume record and
// the live native volume. It fails closed for anything else: a missing marker, a
// non-S3 backend, external disks, a mount name the record does not describe, a
// name that is not the default-medium name Create would have produced, a volume
// claimed by another sandbox, or a live volume whose uuid/size cannot corroborate
// the record.
func ResolveFreshImageSourceVolume(ctx context.Context, sandboxID, rootfsMount string) (*FreshImageSourceVolume, error) {
	all, err := recoveryStorageRecords()
	if err != nil {
		return nil, err
	}
	m, err := requireS3Cow()
	if err != nil {
		return nil, err
	}
	return resolveFreshImageSourceVolume(ctx, m, all, sandboxID, rootfsMount)
}

func resolveFreshImageSourceVolume(ctx context.Context, m *S3Cow, all map[string]*StorageInfo, sandboxID, rootfsMount string) (*FreshImageSourceVolume, error) {
	if !validSourceIdentifier(sandboxID) || !validSourceIdentifier(rootfsMount) {
		return nil, fmt.Errorf("invalid fresh-image source identity input")
	}
	info := all[sandboxID]
	if info == nil || info.SandboxID != sandboxID {
		return nil, fmt.Errorf("source storage record absent or foreign")
	}
	if storageBackendFromInfo(info) != cow.BackendS3 {
		return nil, fmt.Errorf("source storage backend is not s3")
	}
	if info.OwnershipMode != OwnershipModeFreshImageS3 {
		return nil, fmt.Errorf("source storage ownership is not the typed fresh-image cohort")
	}
	if info.CubePCIDiskInfo != nil || info.CubePCISystemDiskInfo != nil || len(info.HostDirBackendInfos) > 0 || len(info.PluginVolumeBackendInfos) > 0 {
		return nil, fmt.Errorf("source contains unsupported external storage")
	}
	volume := info.Volumes[rootfsMount]
	if volume == nil || volume.Name != rootfsMount {
		return nil, fmt.Errorf("persisted source rootfs volume absent")
	}
	name := freshImageVolumeName(sandboxID, rootfsMount)
	if volume.VolumeName != name || volume.Kind != cowKindVolume || volume.Gen != 0 || volume.Medium != cubebox.StorageMedium_StorageMediumDefault {
		return nil, fmt.Errorf("persisted source rootfs volume is not a fresh-image default medium")
	}
	// The same native object must not be the recorded rootfs of another sandbox.
	for id, other := range all {
		if other == nil || id == sandboxID {
			continue
		}
		for _, v := range other.Volumes {
			if v != nil && v.VolumeName == name {
				return nil, fmt.Errorf("fresh-image source volume shared with %s", id)
			}
		}
	}
	if m == nil {
		return nil, fmt.Errorf("s3 cow store unavailable")
	}
	live, err := m.GetVolumeInfo(ctx, name)
	if err != nil {
		return nil, err
	}
	if live == nil || strings.TrimSpace(live.VolumeUUID) == "" || live.SizeBytes == 0 || live.ImportOrigin != nil {
		return nil, fmt.Errorf("live native source volume unavailable, substituted or imported")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return &FreshImageSourceVolume{
		Backend:   cow.BackendS3,
		MountName: rootfsMount,
		Name:      name,
		Kind:      volume.Kind,
		Gen:       volume.Gen,
		UUID:      live.VolumeUUID,
		SizeBytes: live.SizeBytes,
	}, nil
}
