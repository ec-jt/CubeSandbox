// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// useTestCowStorageAt is useTestCowStorage with the work path under a temp
// dir, for imports that mount at paths derived from it.
func useTestCowStorageAt(t *testing.T, engine *fakeCowEngine) string {
	t.Helper()
	root := t.TempDir()
	previous := localStorage
	localStorage = &local{
		config:       &Config{StorageBackend: "cubecow", RootPath: root},
		cowManager:   &XfsCow{engine: engine},
		s3CowManager: &S3Cow{engine: engine},
	}
	t.Cleanup(func() {
		localStorage = previous
	})
	return root
}

func crossNodeAnnotations() map[string]string {
	return map[string]string{
		constants.MasterAnnotationSnapshotCrossNode:   "true",
		constants.MasterAnnotationStorageBackend:      cow.BackendS3,
		constants.MasterAnnotationDesiredSandboxID:    "sb-1",
		constants.MasterAnnotationSnapshotRemoteUUIDs: `{"rootfs":"u-root","memory":"u-mem","metadata":"u-meta"}`,
	}
}

func TestCrossNodeSandboxImportReadsTheRequest(t *testing.T) {
	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	require.NotNil(t, imp)
	require.Equal(t, "sb-1", imp.SandboxID)
	require.Equal(t, "u-root", imp.UUIDs.Rootfs)
}

// Master sends remote_uuids on same-node restores too, so only its cross_node
// verdict may turn a clone into an import. Everything else stays local.
func TestCrossNodeSandboxImportNeedsEveryPrecondition(t *testing.T) {
	for name, mutate := range map[string]func(map[string]string){
		"same node":    func(ann map[string]string) { delete(ann, constants.MasterAnnotationSnapshotCrossNode) },
		"xfs":          func(ann map[string]string) { ann[constants.MasterAnnotationStorageBackend] = cow.BackendXFS },
		"no sandbox":   func(ann map[string]string) { delete(ann, constants.MasterAnnotationDesiredSandboxID) },
		"no uuids":     func(ann map[string]string) { delete(ann, constants.MasterAnnotationSnapshotRemoteUUIDs) },
		"empty uuids":  func(ann map[string]string) { ann[constants.MasterAnnotationSnapshotRemoteUUIDs] = `{}` },
		"no backend":   func(ann map[string]string) { delete(ann, constants.MasterAnnotationStorageBackend) },
		"not a create": func(ann map[string]string) { delete(ann, constants.MasterAnnotationSnapshotCrossNode) },
	} {
		t.Run(name, func(t *testing.T) {
			ann := crossNodeAnnotations()
			mutate(ann)
			require.Nil(t, CrossNodeSandboxImport(ann))
		})
	}
}

// The metadata disk lands under the sandbox's own name and mount, so destroy
// takes it along and no later create is left short of the package's copy.
func TestSandboxImportMetadataIsSandboxOwned(t *testing.T) {
	stubS3MetadataMounts(t)
	engine := &fakeCowEngine{}
	root := useTestCowStorageAt(t, engine)

	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	require.NotNil(t, imp)

	dir, err := imp.EnsureMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "s3", "snapshots", "sb-1", "metadata"), dir)

	name := S3MetadataVolumeName("sb-1")
	require.Equal(t, [][2]string{{name, "u-meta"}}, engine.importedLvols)
	require.Contains(t, engine.activatedVolumes, name)
	require.True(t, s3MetadataIsMounted(dir))
	require.Empty(t, engine.createVolumeFromSnapshots)

	require.NoError(t, ReleaseS3MetadataVolume(context.Background(), cow.BackendS3, "sb-1"))
	require.Contains(t, engine.deletedVolumes, name)
	require.False(t, s3MetadataIsMounted(dir))
}

// Resume imports at the Create entry and the create workflow asks again on
// its way to the run template; the second call must not import twice.
func TestSandboxImportMetadataIsIdempotent(t *testing.T) {
	stubS3MetadataMounts(t)
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)

	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	require.NotNil(t, imp)

	first, err := imp.EnsureMetadata(context.Background())
	require.NoError(t, err)
	second, err := imp.EnsureMetadata(context.Background())
	require.NoError(t, err)

	require.Equal(t, first, second)
	require.Len(t, engine.importedLvols, 1)
}

func TestSandboxImportRootfsAndMemoryUseSandboxNames(t *testing.T) {
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)

	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	require.NotNil(t, imp)

	rootfs, dev, err := imp.Rootfs(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, "sb-sb-1-rootfs-gen0", rootfs)
	require.Equal(t, "/dev/mapper/sb-sb-1-rootfs-gen0", dev)

	memory, _, err := imp.Memory(context.Background())
	require.NoError(t, err)
	require.Equal(t, "sb-sb-1-memory", memory)

	require.Equal(t, [][2]string{
		{"sb-sb-1-rootfs-gen0", "u-root"},
		{"sb-sb-1-memory", "u-mem"},
	}, engine.importedLvols)
}

// A rootfs-only package has no memory image, and the sandbox cold-starts
// instead of failing.
func TestSandboxImportMemoryOptional(t *testing.T) {
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)

	ann := crossNodeAnnotations()
	ann[constants.MasterAnnotationSnapshotRemoteUUIDs] = `{"rootfs":"u-root","metadata":"u-meta"}`
	imp := CrossNodeSandboxImport(ann)
	require.NotNil(t, imp)

	name, _, err := imp.Memory(context.Background())
	require.NoError(t, err)
	require.Empty(t, name)
	require.Empty(t, engine.importedLvols)
}

// s3lvol creates the lvol before it fails, so treating "it exists now" as
// success would hand the sandbox an empty disk and report a corrupt
// filesystem several steps later. The import has to fail loudly.
func TestSandboxImportFailsWhenImportFails(t *testing.T) {
	engine := &fakeCowEngine{importErr: errors.New("s3lvol rpc: File exists")}
	useTestCowStorageAt(t, engine)

	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	require.NotNil(t, imp)

	_, _, err := imp.Rootfs(context.Background(), 0)
	require.ErrorContains(t, err, "File exists")
}

func TestSandboxImportOriginStopsBeforeActivation(t *testing.T) {
	for _, test := range []string{"wrong-export", "legacy", "source-export-is-not-origin", "wrong-volume", "unknown-version"} {
		t.Run(test, func(t *testing.T) {
			engine := &fakeCowEngine{}
			useTestCowStorageAt(t, engine)
			name := SandboxRootfsName("sb-1", 0)
			_, err := engine.ImportLvol(name, "u-root")
			require.NoError(t, err)
			info := engine.volumeInfos[name]
			switch test {
			case "wrong-export":
				info.ImportOrigin.ExportUUID = "different"
			case "legacy":
				info.ImportOrigin = nil
			case "source-export-is-not-origin":
				info.ImportOrigin = nil
				info.ExportUUID = "u-root"
			case "wrong-volume":
				info.ImportOrigin.VolumeUUID = "different"
			case "unknown-version":
				info.ImportOrigin.Version = 2
			}
			engine.importedLvols = nil
			imp := CrossNodeSandboxImport(crossNodeAnnotations())
			_, _, err = imp.Rootfs(context.Background(), 0)
			require.ErrorContains(t, err, "native import origin")
			require.Empty(t, engine.activatedVolumes)
			require.Empty(t, engine.importedLvols)
			require.Empty(t, engine.deletedVolumes)
			require.Empty(t, engine.deactivatedVolumes)
			require.Same(t, info, engine.volumeInfos[name])
		})
	}
}

func TestSandboxImportAlreadyExistsIsNotEvidence(t *testing.T) {
	engine := &fakeCowEngine{importErr: &cubecow.CowError{Code: cubecow.SemAlreadyExists}}
	useTestCowStorageAt(t, engine)
	_, _, err := CrossNodeSandboxImport(crossNodeAnnotations()).Rootfs(context.Background(), 0)
	require.Error(t, err)
	require.Empty(t, engine.activatedVolumes)
	require.Empty(t, engine.deletedVolumes)
}

func TestSandboxImportNativeFFIJSON(t *testing.T) {
	path := os.Getenv("ORIGIN_FFI_FIXTURE")
	if path == "" {
		t.Skip("optional captured native/Rust FFI fixture")
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var info cubecow.Volume
	require.NoError(t, json.Unmarshal(raw, &info))
	require.NoError(t, requireImportedOrigin(&info, "target", "export-a"))
	require.NotEmpty(t, info.ImportOrigin.SnapshotUUID)
	require.Equal(t, uint64(1073741824), info.ImportOrigin.SizeBytes)
	require.Equal(t, "snap-a", info.ImportOrigin.Snapshot)
	require.Error(t, requireImportedOrigin(&info, "target", "export-b"))
}

// B18: a failed cross-node Create releases exactly the disks it imported
// under the sandbox's own names and its snapshot dir. Deleting an imported
// lvol never touches the source export: no export, snapshot or package
// object delete is issued, only delete_volume on sandbox-minted names.
func TestReleaseCrossNodeSandboxImportsDeletesOnlyImports(t *testing.T) {
	stubS3MetadataMounts(t)
	engine := &fakeCowEngine{}
	root := useTestCowStorageAt(t, engine)
	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	require.NotNil(t, imp)
	dir, err := imp.EnsureMetadata(context.Background())
	require.NoError(t, err)
	_, _, err = imp.Rootfs(context.Background(), 0)
	require.NoError(t, err)
	_, _, err = imp.Memory(context.Background())
	require.NoError(t, err)
	// A same-name clone without import origin must be left alone.
	engine.volumeInfos["unrelated-package-snap"] = &cubecow.Volume{Name: "unrelated-package-snap"}

	require.NoError(t, ReleaseCrossNodeSandboxImports(context.Background(), "sb-1"))
	require.ElementsMatch(t, []string{"sb-sb-1-rootfs-gen0", "sb-sb-1-memory", S3MetadataVolumeName("sb-1")}, engine.deletedVolumes)
	require.Empty(t, engine.deletedSnapshots, "no package or export snapshot may be deleted")
	require.Contains(t, engine.deactivatedVolumes, "sb-sb-1-rootfs-gen0")
	require.False(t, s3MetadataIsMounted(dir))
	_, statErr := os.Stat(filepath.Join(root, "s3", "snapshots", "sb-1"))
	require.True(t, os.IsNotExist(statErr), "per-sandbox snapshot dir removed")

	// Idempotent: a second release finds nothing and succeeds.
	require.NoError(t, ReleaseCrossNodeSandboxImports(context.Background(), "sb-1"))
}

func TestReleaseCrossNodeSandboxImportsSkipsNonImported(t *testing.T) {
	stubS3MetadataMounts(t)
	engine := &fakeCowEngine{volumeInfos: map[string]*cubecow.Volume{
		// Same-node clone (no import origin): owned by regular storage destroy.
		"sb-sb-2-rootfs-gen0": {Name: "sb-sb-2-rootfs-gen0"},
	}}
	useTestCowStorageAt(t, engine)
	require.NoError(t, ReleaseCrossNodeSandboxImports(context.Background(), "sb-2"))
	require.NotContains(t, engine.deletedVolumes, "sb-sb-2-rootfs-gen0")
}
