package storage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

func bindingFixture(t *testing.T, dir string) *SnapshotCatalogEntry {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "snapshot"), 0700))
	for _, file := range bindingMetadataFiles {
		require.NoError(t, os.WriteFile(filepath.Join(dir, file), []byte(`{"fixture":true}`), 0600))
	}
	e := &SnapshotCatalogEntry{SnapshotID: "snap-1", MetaDir: dir, SnapshotPath: filepath.Dir(dir), RootfsVol: "tpl-snap-1-rootfs", RootfsKind: CowKindSnapshot, RootfsSizeBytes: 4096, Backend: cow.BackendS3}
	require.NoError(t, PrepareCheckpointDiskBinding(context.Background(), e, "sb-1", 7))
	return e
}

func putBinding(t *testing.T, dir string, e *SnapshotCatalogEntry) {
	t.Helper()
	b, err := json.Marshal(e)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, snapshotCatalogFileName), b, 0600))
}

func TestCheckpointDiskBindingImportedCaller(t *testing.T) {
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)
	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	dir := imp.MetadataDir()
	e := bindingFixture(t, dir)
	putBinding(t, dir, e)
	for _, memory := range []string{"u-mem", ""} {
		imp.UUIDs.Memory = memory
		r := imp.ObserveDiskAdmission(context.Background(), "snap-1", strings.Repeat("a", 64))
		require.Equal(t, "capture_metadata_consistent", r.MetadataBinding)
		require.Equal(t, "unverified", r.DiskIdentity)
		require.Equal(t, "unverified", r.Publication)
		require.Equal(t, "unverified", r.Retention)
		require.False(t, r.BootAllowed)
	}
	// Foreign UUIDs cannot become native identity evidence, even with a valid
	// metadata binding. No backend operation is invoked by observation.
	imp.UUIDs.Rootfs = "foreign-root"
	r := imp.ObserveDiskAdmission(context.Background(), "snap-1", strings.Repeat("b", 64))
	require.Equal(t, "unverified", r.DiskIdentity)
	require.Equal(t, strings.Repeat("b", 64), r.AttemptID)
	require.Empty(t, engine.importedLvols)
	require.Empty(t, engine.activatedVolumes)
	require.Empty(t, engine.deletedVolumes)
	require.Empty(t, engine.deletedSnapshots)
}

func TestCheckpointDiskBindingNativeOriginComparison(t *testing.T) {
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)
	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	e := bindingFixture(t, imp.MetadataDir())
	putBinding(t, imp.MetadataDir(), e)
	rootName := SandboxRootfsName(imp.SandboxID, 0)
	_, err := engine.ImportLvol(rootName, imp.UUIDs.Rootfs)
	require.NoError(t, err)
	_, err = engine.ImportLvol(S3MetadataVolumeName(imp.SandboxID), imp.UUIDs.Metadata)
	require.NoError(t, err)
	o := engine.volumeInfos[rootName].ImportOrigin
	o.SnapshotUUID = "source-snapshot-uuid"
	o.Snapshot = e.RootfsVol
	o.SizeBytes = e.RootfsSizeBytes
	o.Generation = 100 // manifest generation is NOT source rootfs generation 7
	r := imp.ObserveDiskAdmission(context.Background(), "snap-1", strings.Repeat("a", 64))
	require.Equal(t, "native_source_snapshot_name_size_consistent", r.NativeOriginBinding)
	require.Equal(t, "unverified", r.DiskIdentity)
	require.Equal(t, "unverified", r.Publication)
	require.Equal(t, "unverified", r.Retention)
	require.False(t, r.BootAllowed)
	for _, mutate := range []func(*cubecow.ImportOrigin){
		func(o *cubecow.ImportOrigin) { o.ExportUUID = "wrong" },
		func(o *cubecow.ImportOrigin) { o.Snapshot = "wrong" },
		func(o *cubecow.ImportOrigin) { o.SnapshotUUID = "" },
		func(o *cubecow.ImportOrigin) { o.SizeBytes++ },
	} {
		copy := *o
		mutate(o)
		r = imp.ObserveDiskAdmission(context.Background(), "snap-1", strings.Repeat("a", 64))
		require.Equal(t, "unverified", r.NativeOriginBinding)
		*o = copy
	}
}

func TestCheckpointDiskBindingRefusals(t *testing.T) {
	for _, tc := range []string{"legacy", "checkpoint", "rootfs", "generation", "size", "sandbox", "backend", "role", "damaged", "missing", "symlink", "cancel", "attempt"} {
		t.Run(tc, func(t *testing.T) {
			dir := t.TempDir()
			e := bindingFixture(t, dir)
			ctx := context.Background()
			attempt := strings.Repeat("a", 64)
			switch tc {
			case "legacy":
				e.DiskBinding = nil
			case "checkpoint":
				e.DiskBinding.CheckpointID = "other"
			case "rootfs":
				e.DiskBinding.RootfsObject = "other"
			case "generation":
				e.DiskBinding.RootfsGeneration++
			case "size":
				e.DiskBinding.RootfsSizeBytes++
			case "sandbox":
				e.DiskBinding.SandboxID = "foreign"
			case "backend":
				e.DiskBinding.Backend = cow.BackendXFS
			case "role":
				e.DiskBinding.RootfsRole = "memory"
			case "damaged":
				require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot/state.json"), []byte("damaged"), 0600))
			case "missing":
				require.NoError(t, os.Remove(filepath.Join(dir, "snapshot/state.json")))
			case "symlink":
				require.NoError(t, os.Rename(filepath.Join(dir, "snapshot"), filepath.Join(dir, "real")))
				require.NoError(t, os.Symlink("real", filepath.Join(dir, "snapshot")))
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "attempt":
				attempt = "stale-or-invalid"
			}
			putBinding(t, dir, e)
			before, err := os.ReadFile(filepath.Join(dir, snapshotCatalogFileName))
			require.NoError(t, err)
			r := ObserveCheckpointDiskBinding(ctx, dir, "snap-1", "sb-1", cow.BackendS3, attempt)
			require.Equal(t, "unverified", r.MetadataBinding)
			require.False(t, r.BootAllowed)
			after, err := os.ReadFile(filepath.Join(dir, snapshotCatalogFileName))
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestCheckpointDiskBindingMutationAndClone(t *testing.T) {
	dir := t.TempDir()
	e := bindingFixture(t, dir)
	putBinding(t, dir, e)
	copy := cloneCatalogEntry(e)
	copy.DiskBinding.MetadataSHA256["snapshot/state.json"] = "changed"
	require.NotEqual(t, copy.DiskBinding.MetadataSHA256, e.DiskBinding.MetadataSHA256)
	r := ObserveCheckpointDiskBinding(context.Background(), dir, "snap-1", "sb-1", cow.BackendS3, strings.Repeat("a", 64))
	require.Equal(t, "capture_metadata_consistent", r.MetadataBinding)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "snapshot/state.json"), []byte("changed"), 0600))
	r = ObserveCheckpointDiskBinding(context.Background(), dir, "snap-1", "sb-1", cow.BackendS3, strings.Repeat("a", 64))
	require.Equal(t, "unverified", r.MetadataBinding)
}
