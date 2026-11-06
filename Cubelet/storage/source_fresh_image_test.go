// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

func freshImageRecord() *StorageInfo {
	return &StorageInfo{
		SandboxID:     "sb",
		Backend:       cow.BackendS3,
		OwnershipMode: OwnershipModeFreshImageS3,
		Volumes: map[string]*BackendFileInfo{
			"root": {
				Name:       "root",
				VolumeName: "sb-sb-root",
				Kind:       cowKindVolume,
				Gen:        0,
				Medium:     cubebox.StorageMedium_StorageMediumDefault,
			},
		},
	}
}

func TestResolveFreshImageSourceVolume(t *testing.T) {
	validLive := &cubecow.Volume{Name: "sb-sb-root", VolumeUUID: "live-uuid", SizeBytes: 1073741824}
	cases := []struct {
		name    string
		mutate  func(all map[string]*StorageInfo, live map[string]*cubecow.Volume, mount *string)
		wantErr bool
	}{
		{name: "valid", mutate: func(map[string]*StorageInfo, map[string]*cubecow.Volume, *string) {}},
		{name: "no typed marker", mutate: func(all map[string]*StorageInfo, _ map[string]*cubecow.Volume, _ *string) {
			all["sb"].OwnershipMode = ""
		}, wantErr: true},
		{name: "legacy template marker", mutate: func(all map[string]*StorageInfo, _ map[string]*cubecow.Volume, _ *string) {
			all["sb"].OwnershipMode = "template"
		}, wantErr: true},
		{name: "xfs backend", mutate: func(all map[string]*StorageInfo, _ map[string]*cubecow.Volume, _ *string) {
			all["sb"].Backend = cow.BackendXFS
		}, wantErr: true},
		{name: "host dir", mutate: func(all map[string]*StorageInfo, _ map[string]*cubecow.Volume, _ *string) {
			all["sb"].HostDirBackendInfos = map[string]*HostDirBackendInfo{"host": {}}
		}, wantErr: true},
		{name: "plugin volume", mutate: func(all map[string]*StorageInfo, _ map[string]*cubecow.Volume, _ *string) {
			all["sb"].PluginVolumeBackendInfos = map[string]*PluginVolumeBackendInfo{"plugin": {}}
		}, wantErr: true},
		{name: "record missing", mutate: func(all map[string]*StorageInfo, _ map[string]*cubecow.Volume, _ *string) {
			delete(all, "sb")
		}, wantErr: true},
		{name: "record identity mismatch", mutate: func(all map[string]*StorageInfo, _ map[string]*cubecow.Volume, _ *string) {
			all["sb"].SandboxID = "other"
		}, wantErr: true},
		{name: "mount unknown", mutate: func(_ map[string]*StorageInfo, _ map[string]*cubecow.Volume, mount *string) {
			*mount = "cube_rootfs_rw"
		}, wantErr: true},
		{name: "persisted key mismatch", mutate: func(all map[string]*StorageInfo, _ map[string]*cubecow.Volume, _ *string) {
			all["sb"].Volumes["root"].Name = "other"
		}, wantErr: true},
		{name: "generation-root name", mutate: func(all map[string]*StorageInfo, live map[string]*cubecow.Volume, _ *string) {
			all["sb"].Volumes["root"].VolumeName = "sb-sb-rootfs-gen0"
			live["sb-sb-rootfs-gen0"] = &cubecow.Volume{Name: "sb-sb-rootfs-gen0", VolumeUUID: "live-uuid", SizeBytes: 1073741824}
		}, wantErr: true},
		{name: "arbitrary name", mutate: func(all map[string]*StorageInfo, live map[string]*cubecow.Volume, _ *string) {
			all["sb"].Volumes["root"].VolumeName = "sb-sb-anything"
			live["sb-sb-anything"] = &cubecow.Volume{Name: "sb-sb-anything", VolumeUUID: "live-uuid", SizeBytes: 1073741824}
		}, wantErr: true},
		{name: "snapshot kind", mutate: func(all map[string]*StorageInfo, _ map[string]*cubecow.Volume, _ *string) {
			all["sb"].Volumes["root"].Kind = cowKindSnapshot
		}, wantErr: true},
		{name: "non zero generation", mutate: func(all map[string]*StorageInfo, _ map[string]*cubecow.Volume, _ *string) {
			all["sb"].Volumes["root"].Gen = 1
		}, wantErr: true},
		{name: "non default medium", mutate: func(all map[string]*StorageInfo, _ map[string]*cubecow.Volume, _ *string) {
			all["sb"].Volumes["root"].Medium = cubebox.StorageMedium_StorageMediumMemory
		}, wantErr: true},
		{name: "shared with another sandbox", mutate: func(all map[string]*StorageInfo, _ map[string]*cubecow.Volume, _ *string) {
			all["other"] = &StorageInfo{SandboxID: "other", Backend: cow.BackendS3, Volumes: map[string]*BackendFileInfo{
				"root": {Name: "root", VolumeName: "sb-sb-root", Kind: cowKindVolume},
			}}
		}, wantErr: true},
		{name: "live missing", mutate: func(_ map[string]*StorageInfo, live map[string]*cubecow.Volume, _ *string) {
			delete(live, "sb-sb-root")
		}, wantErr: true},
		{name: "live uuid missing", mutate: func(_ map[string]*StorageInfo, live map[string]*cubecow.Volume, _ *string) {
			live["sb-sb-root"] = &cubecow.Volume{Name: "sb-sb-root", SizeBytes: 1073741824}
		}, wantErr: true},
		{name: "live size zero", mutate: func(_ map[string]*StorageInfo, live map[string]*cubecow.Volume, _ *string) {
			live["sb-sb-root"] = &cubecow.Volume{Name: "sb-sb-root", VolumeUUID: "live-uuid"}
		}, wantErr: true},
		{name: "live imported", mutate: func(_ map[string]*StorageInfo, live map[string]*cubecow.Volume, _ *string) {
			live["sb-sb-root"] = &cubecow.Volume{Name: "sb-sb-root", VolumeUUID: "live-uuid", SizeBytes: 1073741824, ImportOrigin: &cubecow.ImportOrigin{}}
		}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			all := map[string]*StorageInfo{"sb": freshImageRecord()}
			live := map[string]*cubecow.Volume{"sb-sb-root": {Name: validLive.Name, VolumeUUID: validLive.VolumeUUID, SizeBytes: validLive.SizeBytes}}
			mount := "root"
			tc.mutate(all, live, &mount)
			store := &S3Cow{engine: &fakeCowEngine{volumeInfos: live}}
			got, err := resolveFreshImageSourceVolume(context.Background(), store, all, "sb", mount)
			if tc.wantErr {
				require.Error(t, err, "expected refusal")
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, &FreshImageSourceVolume{
				Backend:   cow.BackendS3,
				MountName: "root",
				Name:      "sb-sb-root",
				Kind:      cowKindVolume,
				Gen:       0,
				UUID:      validLive.VolumeUUID,
				SizeBytes: validLive.SizeBytes,
			}, got)
		})
	}
}

func TestResolveFreshImageSourceVolumeRejectsCallerNames(t *testing.T) {
	all := map[string]*StorageInfo{"sb": freshImageRecord()}
	live := map[string]*cubecow.Volume{"sb-sb-root": {Name: "sb-sb-root", VolumeUUID: "live-uuid", SizeBytes: 4096}}
	store := &S3Cow{engine: &fakeCowEngine{volumeInfos: live}}
	for _, mount := range []string{"", " root", "root/", "root\n", "../root"} {
		if mount == "root" {
			continue
		}
		if _, err := resolveFreshImageSourceVolume(context.Background(), store, all, "sb", mount); err == nil {
			t.Fatalf("mount %q must be refused", mount)
		}
	}
	for _, id := range []string{"", " sb", "sb ", "sb/1"} {
		if _, err := resolveFreshImageSourceVolume(context.Background(), store, all, id, "root"); err == nil {
			t.Fatalf("sandbox %q must be refused", id)
		}
	}
}

func TestFreshImageVolumeNameIsTheCreateName(t *testing.T) {
	// Create and recovery must build the same name from the same inputs.
	require.Equal(t, "sb-sb-root", freshImageVolumeName("sb", "root"))
	require.Equal(t, "sb-sb-cube_rootfs_rw", freshImageVolumeName("sb", "cube_rootfs_rw"))
}
