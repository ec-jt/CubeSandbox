package cubebox

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckpointDiskBindingPublicationCaller(t *testing.T) {
	dir := t.TempDir()
	meta := filepath.Join(dir, "metadata")
	require.NoError(t, os.MkdirAll(filepath.Join(meta, "snapshot"), 0700))
	for _, file := range []string{"config.json", "state.json"} {
		require.NoError(t, os.WriteFile(filepath.Join(meta, "snapshot", file), []byte(`{"fixture":true}`), 0600))
	}
	entry := &storage.SnapshotCatalogEntry{SnapshotID: "binding-test", Backend: cow.BackendXFS, SnapshotPath: dir, MetaDir: meta,
		RootfsVol: "root-snapshot", RootfsKind: storage.CowKindSnapshot, RootfsSizeBytes: 4096}
	defer storage.DeleteSnapshotCatalog(entry.SnapshotID)
	// Actual producer wrapper: XFS publication has no S3/device side effects.
	_, err := publishSnapshotPackage(context.Background(), cow.BackendXFS, entry, "sandbox", 9)
	require.NoError(t, err)
	r := storage.ObserveCheckpointDiskBinding(context.Background(), meta, "binding-test", "sandbox", cow.BackendXFS, strings.Repeat("a", 64))
	require.Equal(t, "capture_metadata_consistent", r.MetadataBinding)
	require.False(t, r.BootAllowed)
	require.Equal(t, "unverified", r.Publication)
	before, err := os.ReadFile(filepath.Join(meta, "catalog.json"))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = publishSnapshotPackage(ctx, cow.BackendXFS, entry, "sandbox", 9)
	require.Error(t, err)
	after, err := os.ReadFile(filepath.Join(meta, "catalog.json"))
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRestoreDiskAdmissionDoesNotTrustLabels(t *testing.T) {
	ann := map[string]string{"cube.master.recovery.disk_identity": "verified", "cube.master.recovery.disk_retention": "verified"}
	r := observeRestoreDiskAdmission(context.Background(), ann, "checkpoint", strings.Repeat("a", 64))
	for _, field := range []string{"disk_admission", "disk_metadata", "disk_identity", "disk_retention", "disk_publication"} {
		require.Equal(t, "unverified", r["cube.master.recovery."+field])
	}
	require.Equal(t, "pre_runtime_only", r["cube.master.recovery.disk_observation_phase"])
	for k := range r {
		require.True(t, rejectUserCubeMasterLabel(k))
	}
}
