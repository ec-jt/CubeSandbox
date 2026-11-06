package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// Fixture RPC transport persists records across S3Cow instances. Native S3
// conditional-write semantics are covered separately by the MinIO harness.
type commitEngine struct {
	*fakeCowEngine
	mu        sync.Mutex
	records   map[string]string
	evidence  map[string]CheckpointExport
	ambiguous bool
}

func (e *commitEngine) CheckpointRecord(raw string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var r map[string]string
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return "", err
	}
	switch r["action"] {
	case "evidence":
		v, ok := e.evidence[r["uuid"]]
		if !ok || v.Snapshot != r["name"] {
			return "", fmt.Errorf("no evidence")
		}
		b, _ := json.Marshal(v)
		return string(b), nil
	case "get", "index-get":
		v, ok := e.records[r["key"]]
		if !ok {
			return "", fmt.Errorf("not found")
		}
		return v, nil
	case "put", "index-put":
		if v, ok := e.records[r["key"]]; ok && v != r["body"] {
			return "", fmt.Errorf("conflict")
		}
		e.records[r["key"]] = r["body"]
		if e.ambiguous {
			e.ambiguous = false
			return "", fmt.Errorf("lost reply")
		}
		return r["body"], nil
	}
	return "", fmt.Errorf("bad action")
}
func commitFixture(t *testing.T) (*S3Cow, *commitEngine, *SnapshotCatalogEntry) {
	f := &fakeCowEngine{}
	useTestCowStorageAt(t, f)
	e := &commitEngine{fakeCowEngine: f, records: map[string]string{}, evidence: map[string]CheckpointExport{}}
	m := &S3Cow{engine: e}
	localStorage.s3CowManager = m
	dir := SnapshotMetaDir(cow.BackendS3, SnapshotKindNormal, "snap-1")
	// Prepare the existing fixture without a native lookup; add exact capture
	// identity below to model the producer's live UUID read.
	localStorage.s3CowManager = nil
	entry := bindingFixture(t, dir)
	localStorage.s3CowManager = m
	entry.MemoryVol = "tpl-snap-1-memory-snap"
	entry.MemoryKind = cowKindSnapshot
	entry.MetadataVol = S3MetadataSnapshotName(entry.SnapshotID)
	entry.MetadataKind = cowKindSnapshot
	entry.DiskBinding.NativeRootfsUUID = "rootfs-native"
	f.volumeInfos = map[string]*cubecow.Volume{}
	for role, name := range map[string]string{"rootfs": entry.RootfsVol, "memory": entry.MemoryVol, "metadata": entry.MetadataVol} {
		uuid := role + "-export"
		f.volumeInfos[name] = &cubecow.Volume{Name: name, VolumeUUID: role + "-native", SizeBytes: 4096, ExportUUID: uuid, ExportStatus: cow.ExportStatusDone}
		e.evidence[uuid] = CheckpointExport{ExportUUID: uuid, SnapshotUUID: role + "-native", Snapshot: name, Endpoint: "minio:9000", Bucket: "bucket", Prefix: "source", LvsName: "source", SourceBlobID: 4, ManifestVersion: 4, Generation: 9, SizeBytes: 4096, ClusterSize: 4096, ChunkSize: 4096, BlockSize: 512}
	}
	require.NoError(t, WriteSnapshotCatalogFor(cow.BackendS3, entry))
	require.NoError(t, writeS3FinalizeCatalog(entry))
	require.NoError(t, m.recordCheckpointSeal(context.Background(), entry))
	return m, e, entry
}
func TestCheckpointCommitProducerRestartAndConsumer(t *testing.T) {
	m, e, entry := commitFixture(t)
	ctx := context.Background()
	uuids, err := m.Upload(ctx, entry.SnapshotID)
	require.NoError(t, err)
	require.NotNil(t, uuids)
	// Fake ExportSnapshot may update IDs; use native simulated completed IDs.
	for _, info := range e.volumeInfos {
		info.ExportUUID = strings.TrimSuffix(info.VolumeUUID, "-native") + "-export"
		info.ExportStatus = cow.ExportStatusDone
	}
	e.ambiguous = true
	_, err = m.UploadStatus(ctx, entry.SnapshotID)
	require.Error(t, err)
	require.Len(t, e.records, 1)
	m = &S3Cow{engine: e}
	localStorage.s3CowManager = m
	DeleteSnapshotCatalogFor(cow.BackendS3, entry.SnapshotID)
	st, err := m.UploadStatus(ctx, entry.SnapshotID)
	require.NoError(t, err)
	require.Equal(t, cow.RemoteStateReady, st.State)
	require.Contains(t, st.Message, "committed capture")
	imp := &SandboxImport{SandboxID: "sb-1", UUIDs: &cow.RemoteUUIDs{Rootfs: "rootfs-export", Metadata: "metadata-export", Memory: "memory-export"}}
	require.NoError(t, os.MkdirAll(imp.MetadataDir(), 0700))
	for _, name := range bindingMetadataFiles {
		b, err := os.ReadFile(filepath.Join(entry.MetaDir, name))
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(imp.MetadataDir(), name)), 0700))
		require.NoError(t, os.WriteFile(filepath.Join(imp.MetadataDir(), name), b, 0600))
	}
	putBinding(t, imp.MetadataDir(), entry)
	for role, name := range map[string]string{"rootfs": SandboxRootfsName(imp.SandboxID, 0), "metadata": S3MetadataVolumeName(imp.SandboxID), "memory": SandboxMemoryName(imp.SandboxID)} {
		evidence := e.evidence[role+"-export"]
		raw, _ := json.Marshal(evidence)
		origin := &cubecow.ImportOrigin{}
		require.NoError(t, json.Unmarshal(raw, origin))
		origin.Version = 1
		origin.VolumeUUID = role + "-target"
		origin.LvstoreUUID = "target-store"
		e.volumeInfos[name] = &cubecow.Volume{Name: name, VolumeUUID: origin.VolumeUUID, ImportOrigin: origin}
	}
	observation := imp.ObserveDiskAdmission(ctx, entry.SnapshotID, strings.Repeat("a", 64))
	require.Equal(t, "committed_capture", observation.Publication)
	require.Equal(t, "committed_native_pair", observation.DiskIdentity)
	require.False(t, observation.BootAllowed)
	require.Equal(t, "unverified", observation.Retention)
	root := e.volumeInfos[SandboxRootfsName(imp.SandboxID, 0)].ImportOrigin
	for _, mutate := range []func(*cubecow.ImportOrigin){func(o *cubecow.ImportOrigin) { o.Generation++ }, func(o *cubecow.ImportOrigin) { o.SnapshotUUID = "foreign" }, func(o *cubecow.ImportOrigin) { o.Bucket = "foreign" }, func(o *cubecow.ImportOrigin) { o.ExportUUID = "metadata-export" }, func(o *cubecow.ImportOrigin) { o.ChunkSize++ }} {
		saved := *root
		mutate(root)
		require.NotEqual(t, "committed_capture", imp.ObserveDiskAdmission(ctx, entry.SnapshotID, strings.Repeat("b", 64)).Publication)
		*root = saved
	}
	entry.DiskBinding.CaptureID = strings.Repeat("c", 64)
	putBinding(t, imp.MetadataDir(), entry)
	require.NotEqual(t, "committed_capture", imp.ObserveDiskAdmission(ctx, entry.SnapshotID, strings.Repeat("d", 64)).Publication)
	require.Empty(t, e.deletedVolumes)
	require.Empty(t, e.deletedSnapshots)
}
func TestCheckpointCommitIncompleteAndConflicting(t *testing.T) {
	for _, which := range []string{"pending", "failed", "uuid", "size", "scope", "evidence", "duplicate"} {
		t.Run(which, func(t *testing.T) {
			m, e, entry := commitFixture(t)
			info := e.volumeInfos[entry.MemoryVol]
			switch which {
			case "pending":
				info.ExportStatus = cow.ExportStatusInProgress
			case "failed":
				info.ExportStatus = cow.ExportStatusFailed
			case "uuid":
				info.VolumeUUID = "recreated"
			case "size":
				info.SizeBytes++
			case "scope":
				v := e.evidence[info.ExportUUID]
				v.Bucket = "foreign"
				e.evidence[info.ExportUUID] = v
			case "evidence":
				delete(e.evidence, info.ExportUUID)
			case "duplicate":
				info.ExportUUID = "rootfs-export"
			}
			_, _ = m.UploadStatus(context.Background(), entry.SnapshotID)
			require.Empty(t, e.records)
		})
	}
	m, e, entry := commitFixture(t)
	ctx := context.Background()
	ok, err := m.commitCheckpoint(ctx, entry.SnapshotID)
	require.NoError(t, err)
	require.True(t, ok)
	before := e.records[checkpointKey(*entry.DiskBinding)]
	info := e.volumeInfos[entry.RootfsVol]
	v := e.evidence[info.ExportUUID]
	v.Generation++
	e.evidence[info.ExportUUID] = v
	_, err = m.commitCheckpoint(ctx, entry.SnapshotID)
	require.Error(t, err)
	require.Equal(t, before, e.records[checkpointKey(*entry.DiskBinding)])
}
func TestCheckpointCommitImmutableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); require.NoError(t, immutableCheckpointFile(path, []byte("same"))) }()
	}
	wg.Wait()
	require.Error(t, immutableCheckpointFile(path, []byte("different")))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "same", string(b))
}

func TestCheckpointCommitFinalizationReceipt(t *testing.T) {
	f := newFinalizeFixture(t)
	f.entry.RootfsSizeBytes = 8 << 20
	f.entry.DiskBinding = &CheckpointDiskBinding{Version: 1, CaptureID: strings.Repeat("a", 64), SandboxID: "source", CheckpointID: f.entry.SnapshotID, Backend: cow.BackendS3, RootfsObject: f.entry.RootfsVol, NativeRootfsUUID: "root-native", RootfsSizeBytes: 8 << 20}
	f.engine.volumeInfos[f.entry.RootfsVol].VolumeUUID = "root-native"
	f.engine.afterSeal = func(name string) { f.engine.volumeInfos[name].VolumeUUID = name + "-native" }
	require.NoError(t, WriteSnapshotCatalogFor(cow.BackendS3, f.entry))
	require.NoError(t, f.run())
	before, err := os.ReadFile(filepath.Join(f.entry.SnapshotPath, checkpointSealFile))
	require.NoError(t, err)
	f.hideMetadata(t)
	f.restart()
	require.NoError(t, f.run())
	after, err := os.ReadFile(filepath.Join(f.entry.SnapshotPath, checkpointSealFile))
	require.NoError(t, err)
	require.Equal(t, before, after)
	f.engine.volumeInfos[f.entry.RootfsVol].VolumeUUID = "recreated"
	require.Error(t, f.run())
}

func TestCheckpointCommitPartialSealFailsClosed(t *testing.T) {
	f := newFinalizeFixture(t)
	f.entry.DiskBinding = &CheckpointDiskBinding{Version: 1, NativeRootfsUUID: "root-native"}
	f.engine.volumeInfos[f.entry.RootfsVol].VolumeUUID = "root-native"
	f.engine.afterSeal = func(name string) { f.engine.volumeInfos[name].VolumeUUID = name + "-native" }
	f.engine.sealErrors = map[string]error{S3MetadataSnapshotName(f.entry.SnapshotID): fmt.Errorf("partial seal")}
	require.NoError(t, WriteSnapshotCatalogFor(cow.BackendS3, f.entry))
	require.Error(t, f.run())
	f.hideMetadata(t)
	f.restart()
	require.ErrorContains(t, f.run(), "ambiguous prior seal")
	require.Empty(t, f.engine.deletedVolumes)
	require.Empty(t, f.engine.deletedSnapshots)
}
