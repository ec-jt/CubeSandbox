package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// SourceStorageBinding is native evidence captured under the source lifecycle
// and package barriers before runtime stop. It is NOT a drain or ownership
// receipt. SelectedGeneration and LiveGeneration are deliberately independent.
type SourceStorageBinding struct {
	Version int    `json:"version"`
	Cohort  string `json:"cohort,omitempty"`

	// RootfsMount and the cohort are populated only for the fresh-image S3
	// cohort. They are omitted for template-derived sources, so those records
	// keep their original bytes.
	RootfsMount    string           `json:"rootfs_mount,omitempty"`
	Commit         CheckpointCommit `json:"commit"`
	CommitSHA256   string           `json:"commit_sha256"`
	LiveName       string           `json:"live_name"`
	LiveUUID       string           `json:"live_uuid"`
	LiveGeneration uint32           `json:"live_generation"`
	LiveSize       uint64           `json:"live_size"`
}

// BindRecoverySource resolves the digest through the configured native storage,
// never caller JSON, cached reconstructed catalogs or a caller-supplied path.
// XFS has no native UUID/commit API yet and fails closed, not a fallback to S3.
// This entry point is the template-derived cohort and keeps its original
// generation-root name check and binding version.
func BindRecoverySource(ctx context.Context, backend, sandbox, checkpoint, digest string, live *CowSnapshotObject) (*SourceStorageBinding, error) {
	backend, err := cow.NormalizeBackend(backend)
	if err != nil {
		return nil, err
	}
	if backend != cow.BackendS3 {
		return nil, fmt.Errorf("xfs source binding unsupported: native volume UUID and selected-checkpoint commit are unavailable")
	}
	store, err := requireS3Cow()
	if err != nil {
		return nil, err
	}
	return store.bindRecoverySource(ctx, sourcefence.CohortTemplateGenerationRoot, sandbox, checkpoint, digest, "", live)
}

// BindRecoverySourceFreshImage binds the fresh-image S3 cohort. The rootfs
// volume name is taken from the persisted StorageInfo ownership record plus the
// live native volume, not from the sb-<id>-rootfs-gen<N> convention, which this
// cohort never produces. The caller supplies only the persisted mount name.
func BindRecoverySourceFreshImage(ctx context.Context, sandbox, checkpoint, digest, rootfsMount string) (*SourceStorageBinding, error) {
	v, err := ResolveFreshImageSourceVolume(ctx, sandbox, rootfsMount)
	if err != nil {
		return nil, err
	}
	store, err := requireS3Cow()
	if err != nil {
		return nil, err
	}
	return store.bindRecoverySource(ctx, sourcefence.CohortFreshImageS3, sandbox, checkpoint, digest, rootfsMount, v.object())
}

func (m *S3Cow) bindRecoverySource(ctx context.Context, cohort, sandbox, checkpoint, digest, rootfsMount string, live *CowSnapshotObject) (*SourceStorageBinding, error) {
	var liveUUID string
	if sandbox == "" || checkpoint == "" || live == nil || live.Kind != cowKindVolume || live.SizeBytes == 0 {
		return nil, fmt.Errorf("invalid native source generation")
	}
	switch cohort {
	case sourcefence.CohortTemplateGenerationRoot:
		if live.Name != SandboxRootfsName(sandbox, live.Gen) {
			return nil, fmt.Errorf("invalid native source generation")
		}
	case sourcefence.CohortFreshImageS3:
		// Never trust the identity handed in: re-derive it from the persisted
		// ownership record plus the live native volume and compare.
		v, err := ResolveFreshImageSourceVolume(ctx, sandbox, rootfsMount)
		if err != nil {
			return nil, err
		}
		if v.Name != live.Name || v.Kind != live.Kind || v.Gen != live.Gen || v.SizeBytes != live.SizeBytes {
			return nil, fmt.Errorf("fresh-image native source identity changed")
		}
		liveUUID = v.UUID
	default:
		return nil, fmt.Errorf("unknown source ownership cohort")
	}
	key := checkpointKey(CheckpointDiskBinding{Backend: cow.BackendS3, SandboxID: sandbox, CheckpointID: checkpoint})
	raw, err := m.checkpointRPC(ctx, map[string]string{"action": "get", "key": key})
	if err != nil {
		return nil, fmt.Errorf("selected native checkpoint commit: %w", err)
	}
	var commit CheckpointCommit
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if err = d.Decode(&commit); err != nil {
		return nil, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, fmt.Errorf("trailing native commit")
	}
	b := commit.Binding
	validHash := func(s string) bool {
		v, e := hex.DecodeString(s)
		return e == nil && len(v) == 32 && strings.ToLower(s) == s
	}
	if commit.Version != 1 || b.Version != 1 || b.Backend != cow.BackendS3 || b.SandboxID != sandbox || b.CheckpointID != checkpoint || !validHash(b.CaptureID) || b.RootfsRole != "rootfs" || b.MetadataRole != "metadata" || b.NativeRootfsUUID == "" || b.RootfsObject == "" || b.RootfsSizeBytes == 0 || len(commit.Objects) < 2 || len(commit.Objects) > 3 {
		return nil, fmt.Errorf("foreign or incomplete native checkpoint commit")
	}
	for _, name := range []string{"snapshot/config.json", "snapshot/state.json"} {
		if !validHash(b.MetadataSHA256[name]) {
			return nil, fmt.Errorf("missing native capture metadata digest")
		}
	}
	for name, hash := range b.MetadataSHA256 {
		known := false
		for _, allowed := range bindingMetadataFiles {
			if name == allowed {
				known = true
			}
		}
		if !known || !validHash(hash) {
			return nil, fmt.Errorf("invalid native capture metadata digest")
		}
	}
	root, rootOK := commit.Objects["rootfs"]
	_, metaOK := commit.Objects["metadata"]
	if !rootOK || !metaOK || root.SnapshotUUID != b.NativeRootfsUUID || root.Snapshot != b.RootfsObject || root.SizeBytes != b.RootfsSizeBytes {
		return nil, fmt.Errorf("native checkpoint rootfs mismatch")
	}
	seenUUID, seenExport := map[string]bool{}, map[string]bool{}
	for role, object := range commit.Objects {
		if (role != "rootfs" && role != "memory" && role != "metadata") || !validCheckpointExport(object) || seenUUID[object.SnapshotUUID] || seenExport[object.ExportUUID] || object.Endpoint != root.Endpoint || object.Bucket != root.Bucket || object.Prefix != root.Prefix || object.LvsName != root.LvsName {
			return nil, fmt.Errorf("invalid or mixed native checkpoint object set")
		}
		seenUUID[object.SnapshotUUID] = true
		seenExport[object.ExportUUID] = true
		// Corroborate the retained native snapshot and completed export. A stale
		// commit alone does not prove the original source objects still exist.
		info, e := m.GetVolumeInfo(ctx, object.Snapshot)
		if e != nil {
			return nil, e
		}
		if info == nil || info.VolumeUUID != object.SnapshotUUID || info.SizeBytes != object.SizeBytes || info.ExportUUID != object.ExportUUID || info.ExportStatus != cow.ExportStatusDone {
			return nil, fmt.Errorf("selected native object missing or substituted")
		}
		evidence, e := m.checkpointRPC(ctx, map[string]string{"action": "evidence", "name": object.Snapshot, "uuid": object.ExportUUID})
		if e != nil {
			return nil, e
		}
		var observed CheckpointExport
		if e = json.Unmarshal([]byte(evidence), &observed); e != nil || !reflect.DeepEqual(observed, object) {
			return nil, fmt.Errorf("selected native export changed")
		}
	}
	body, err := json.Marshal(commit)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(body)
	if !validHash(digest) || !bytes.Equal([]byte(digest), []byte(hex.EncodeToString(hash[:]))) {
		return nil, fmt.Errorf("selected native checkpoint digest mismatch")
	}
	info, err := m.GetVolumeInfo(ctx, live.Name)
	if err != nil {
		return nil, err
	}
	if info == nil || info.VolumeUUID == "" || info.SizeBytes != live.SizeBytes || seenUUID[info.VolumeUUID] {
		return nil, fmt.Errorf("live native source identity unavailable or substituted")
	}
	if liveUUID != "" && info.VolumeUUID != liveUUID {
		return nil, fmt.Errorf("live native source identity changed")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	version, cohortName, mount := 1, "", ""
	if cohort == sourcefence.CohortFreshImageS3 {
		version, cohortName, mount = 2, sourcefence.CohortFreshImageS3, rootfsMount
	}
	return &SourceStorageBinding{Version: version, Cohort: cohortName, RootfsMount: mount, Commit: commit, CommitSHA256: digest, LiveName: live.Name, LiveUUID: info.VolumeUUID, LiveGeneration: live.Gen, LiveSize: info.SizeBytes}, nil
}
