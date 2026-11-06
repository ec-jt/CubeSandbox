// SPDX-License-Identifier: Apache-2.0
package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// CheckpointCommit is authoritative only when fetched through the configured
// native storage service. A JSON copy supplied by a caller is never evidence.
// No member authorizes boot or promises retention or whole-rootfs integrity.
type CheckpointCommit struct {
	Version int                         `json:"version"`
	Binding CheckpointDiskBinding       `json:"binding"`
	Objects map[string]CheckpointExport `json:"objects"`
}

type CheckpointExport struct {
	ExportUUID      string `json:"export_uuid"`
	SnapshotUUID    string `json:"snapshot_uuid"`
	Snapshot        string `json:"snapshot"`
	Endpoint        string `json:"endpoint"`
	Bucket          string `json:"bucket"`
	Prefix          string `json:"prefix"`
	LvsName         string `json:"lvs_name"`
	SourceBlobID    uint64 `json:"source_blob_id"`
	ManifestVersion uint32 `json:"manifest_version"`
	Generation      uint32 `json:"generation"`
	SizeBytes       uint64 `json:"size_bytes"`
	ClusterSize     uint32 `json:"cluster_size"`
	ChunkSize       uint32 `json:"chunk_size"`
	BlockSize       uint32 `json:"block_size"`
}

type checkpointNative interface{ CheckpointRecord(string) (string, error) }
type checkpointSeal struct {
	Version int                         `json:"version"`
	Binding CheckpointDiskBinding       `json:"binding"`
	Objects map[string]CheckpointExport `json:"objects"`
}

var checkpointCommitMu sync.Mutex

const checkpointSealFile = ".checkpoint-sealed-v1.json"

func checkpointKey(b CheckpointDiskBinding) string {
	// Stable checkpoint scope, not capture ID: a second capture cannot bless a
	// different export set by choosing a fresh nonce for the same checkpoint.
	raw, _ := json.Marshal([]string{b.Backend, b.SandboxID, b.CheckpointID})
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func immutableCheckpointFile(path string, body []byte) error {
	// Link is atomic no-replace. Sync before publication and sync the directory
	// even on equal retry, including a previous ambiguous directory-sync result.
	f, err := os.CreateTemp(filepath.Dir(path), ".checkpoint-write-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(body); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Link(tmp, path); err != nil {
		if !os.IsExist(err) {
			return err
		}
		old, readErr := os.ReadFile(path)
		if readErr != nil || !bytes.Equal(old, body) {
			return fmt.Errorf("conflicting immutable checkpoint record")
		}
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Persist the exact native sealed set while finalization still owns the package.
// This is outside metadata/, never a rewrite of its catalog or sealed image.
func (m *S3Cow) recordCheckpointSeal(ctx context.Context, entry *SnapshotCatalogEntry) error {
	if entry.DiskBinding == nil || entry.DiskBinding.NativeRootfsUUID == "" {
		return nil
	} // legacy: no strong admission
	r := checkpointSeal{Version: 1, Binding: *entry.DiskBinding, Objects: map[string]CheckpointExport{}}
	for role, name := range map[string]string{"rootfs": entry.RootfsVol, "memory": entry.MemoryVol, "metadata": entry.MetadataVol} {
		if name == "" {
			if role == "memory" {
				continue
			}
			return fmt.Errorf("missing required %s", role)
		}
		info, err := m.GetVolumeInfo(ctx, name)
		if err != nil {
			return err
		}
		if info == nil || info.VolumeUUID == "" || info.SizeBytes == 0 {
			return fmt.Errorf("missing native sealed identity for %s", role)
		}
		r.Objects[role] = CheckpointExport{Snapshot: name, SnapshotUUID: info.VolumeUUID, SizeBytes: info.SizeBytes}
	}
	if r.Binding.NativeRootfsUUID == "" || r.Objects["rootfs"].SnapshotUUID != r.Binding.NativeRootfsUUID {
		return fmt.Errorf("capture rootfs native identity changed")
	}
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return immutableCheckpointFile(filepath.Join(entry.SnapshotPath, checkpointSealFile), body)
}

func (m *S3Cow) checkpointRPC(ctx context.Context, request map[string]string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	native, ok := m.engine.(checkpointNative)
	if !ok {
		return "", fmt.Errorf("native checkpoint record API unavailable")
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	reply, err := native.CheckpointRecord(string(raw))
	if err != nil {
		return "", err
	}
	if len(reply) > 128<<10 {
		return "", fmt.Errorf("checkpoint reply exceeds limit")
	}
	return reply, ctx.Err()
}

func validCheckpointExport(e CheckpointExport) bool {
	return e.ExportUUID != "" && e.SnapshotUUID != "" && e.Snapshot != "" && e.Endpoint != "" && e.Bucket != "" && e.Prefix != "" && e.LvsName != "" && e.SizeBytes > 0 && e.ClusterSize > 0 && e.ChunkSize > 0 && e.BlockSize > 0 && e.ManifestVersion >= 2 && e.ManifestVersion <= 4
}

// Called from the real UploadStatus terminal branch. No polling/background task:
// every required role must already be DONE, and native evidence independently
// checks DONE, the live read-only snapshot UUID and the stored manifest.
func (m *S3Cow) commitCheckpoint(ctx context.Context, id string) (bool, error) {
	checkpointCommitMu.Lock()
	defer checkpointCommitMu.Unlock()
	entry, prepared, err := readS3FinalizeCatalog(id)
	if errors.Is(err, ErrSnapshotCatalogNotFound) {
		return false, nil
	} // legacy status only, never strong admission
	if err != nil {
		return false, err
	}
	if entry.DiskBinding == nil || entry.DiskBinding.NativeRootfsUUID == "" {
		return false, nil
	}
	if !prepared {
		return false, fmt.Errorf("checkpoint not sealed")
	}
	raw, err := os.ReadFile(filepath.Join(entry.SnapshotPath, checkpointSealFile))
	if err != nil {
		return false, err
	}
	if len(raw) > 128<<10 {
		return false, fmt.Errorf("seal record too large")
	}
	var seal checkpointSeal
	if err = json.Unmarshal(raw, &seal); err != nil {
		return false, err
	}
	if seal.Version != 1 || !reflect.DeepEqual(seal.Binding, *entry.DiskBinding) || len(seal.Objects) < 2 || len(seal.Objects) > 3 {
		return false, fmt.Errorf("capture seal mismatch")
	}
	record := CheckpointCommit{Version: 1, Binding: seal.Binding, Objects: map[string]CheckpointExport{}}
	for _, role := range []string{"rootfs", "metadata", "memory"} {
		expected, ok := seal.Objects[role]
		if !ok {
			if role == "memory" {
				continue
			}
			return false, fmt.Errorf("missing sealed role %s", role)
		}
		info, err := m.GetVolumeInfo(ctx, expected.Snapshot)
		if err != nil {
			return false, err
		}
		if info == nil || info.ExportStatus != cow.ExportStatusDone || info.ExportUUID == "" || info.VolumeUUID != expected.SnapshotUUID || info.SizeBytes != expected.SizeBytes {
			return false, fmt.Errorf("incomplete or changed checkpoint %s", role)
		}
		reply, err := m.checkpointRPC(ctx, map[string]string{"action": "evidence", "name": expected.Snapshot, "uuid": info.ExportUUID})
		if err != nil {
			return false, err
		}
		var evidence CheckpointExport
		if err = json.Unmarshal([]byte(reply), &evidence); err != nil {
			return false, err
		}
		if !validCheckpointExport(evidence) || evidence.Snapshot != expected.Snapshot || evidence.SnapshotUUID != expected.SnapshotUUID || evidence.SizeBytes != expected.SizeBytes || evidence.ExportUUID != info.ExportUUID {
			return false, fmt.Errorf("native evidence mismatch %s", role)
		}
		for _, prior := range record.Objects {
			if prior.ExportUUID == evidence.ExportUUID || prior.SnapshotUUID == evidence.SnapshotUUID || prior.Endpoint != evidence.Endpoint || prior.Bucket != evidence.Bucket || prior.Prefix != evidence.Prefix || prior.LvsName != evidence.LvsName {
				return false, fmt.Errorf("mixed checkpoint scope or duplicate role")
			}
		}
		record.Objects[role] = evidence
	}
	body, err := json.Marshal(record)
	if err != nil {
		return false, err
	}
	reply, err := m.checkpointRPC(ctx, map[string]string{"action": "put", "key": checkpointKey(record.Binding), "body": string(body)})
	if err != nil {
		return false, err
	}
	if reply != string(body) {
		return false, fmt.Errorf("commit read-back mismatch")
	}
	// A destination must acquire before reading imported metadata. Publish an
	// immutable export-set index only after the authoritative commit exists.
	index := checkpointExportSetKey(record.Objects["rootfs"].ExportUUID, record.Objects["metadata"].ExportUUID, record.Objects["memory"].ExportUUID)
	reply, err = m.checkpointRPC(ctx, map[string]string{"action": "index-put", "key": index, "body": string(body)})
	if err != nil {
		return false, err
	}
	if reply != string(body) {
		return false, fmt.Errorf("commit index read-back mismatch")
	}
	return true, nil
}

func checkpointOrigin(o *cubecow.ImportOrigin) CheckpointExport {
	if o == nil {
		return CheckpointExport{}
	}
	return CheckpointExport{ExportUUID: o.ExportUUID, SnapshotUUID: o.SnapshotUUID, Snapshot: o.Snapshot, Endpoint: o.Endpoint, Bucket: o.Bucket, Prefix: o.Prefix, LvsName: o.LvsName, SourceBlobID: o.SourceBlobID, ManifestVersion: o.ManifestVersion, Generation: o.Generation, SizeBytes: o.SizeBytes, ClusterSize: o.ClusterSize, ChunkSize: o.ChunkSize, BlockSize: o.BlockSize}
}

func (s *SandboxImport) verifyCheckpointCommit(ctx context.Context, store *S3Cow, entry *SnapshotCatalogEntry) error {
	b := entry.DiskBinding
	if b == nil || b.NativeRootfsUUID == "" {
		return fmt.Errorf("legacy capture has no committed identity")
	}
	reply, err := store.checkpointRPC(ctx, map[string]string{"action": "get", "key": checkpointKey(*b)})
	if err != nil {
		return err
	}
	var record CheckpointCommit
	if err = json.Unmarshal([]byte(reply), &record); err != nil {
		return err
	}
	if record.Version != 1 || !reflect.DeepEqual(record.Binding, *b) || len(record.Objects) < 2 || len(record.Objects) > 3 {
		return fmt.Errorf("foreign committed capture")
	}
	roles := map[string]struct{ name, uuid string }{"rootfs": {SandboxRootfsName(s.SandboxID, 0), s.UUIDs.Rootfs}, "metadata": {S3MetadataVolumeName(s.SandboxID), s.UUIDs.Metadata}}
	if _, ok := record.Objects["memory"]; ok {
		roles["memory"] = struct{ name, uuid string }{SandboxMemoryName(s.SandboxID), s.UUIDs.Memory}
	} else if s.UUIDs.Memory != "" {
		return fmt.Errorf("unexpected memory export")
	}
	for role, target := range roles {
		e, ok := record.Objects[role]
		if !ok || !validCheckpointExport(e) {
			return fmt.Errorf("missing committed role")
		}
		info, err := store.GetVolumeInfo(ctx, target.name)
		if err != nil {
			return err
		}
		if err = requireImportedOrigin(info, target.name, target.uuid); err != nil {
			return err
		}
		if checkpointOrigin(info.ImportOrigin) != e {
			return fmt.Errorf("imported %s differs from committed capture", role)
		}
	}
	if record.Objects["rootfs"].SnapshotUUID != b.NativeRootfsUUID || record.Objects["rootfs"].Snapshot != b.RootfsObject || record.Objects["rootfs"].SizeBytes != b.RootfsSizeBytes {
		return fmt.Errorf("committed rootfs differs from capture")
	}
	return ctx.Err()
}
