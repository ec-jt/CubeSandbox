// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// CheckpointDiskBinding records the producer's capture identity. It deliberately
// does not say committed: the metadata is written BEFORE sealing/export starts.
// Neither this record nor a metadata hash authenticates rootfs contents, proves
// export completion, or prevents local/remote garbage collection.
type CheckpointDiskBinding struct {
	Version          int               `json:"version"`
	CaptureID        string            `json:"capture_id"`
	SandboxID        string            `json:"sandbox_id"`
	CheckpointID     string            `json:"checkpoint_id"`
	Backend          string            `json:"backend"`
	RootfsRole       string            `json:"rootfs_role"`
	RootfsObject     string            `json:"rootfs_object"`
	NativeRootfsUUID string            `json:"native_rootfs_uuid,omitempty"`
	RootfsGeneration uint32            `json:"rootfs_generation"`
	RootfsSizeBytes  uint64            `json:"rootfs_size_bytes"`
	MetadataRole     string            `json:"metadata_role"`
	MetadataSHA256   map[string]string `json:"metadata_sha256"`
}

const bindingMaxFile = 16 << 20

// Hash only the bounded, known restore inputs. No recursive walk, devices,
// symlinks, host paths from a catalog, or rootfs reads are allowed here.
var bindingMetadataFiles = []string{"snapshot/config.json", "snapshot/state.json", "sandbox_spec.json", "sandbox_ports.json"}

func hashBindingMetadata(dir string) (map[string]string, error) {
	out := map[string]string{}
	for _, name := range bindingMetadataFiles {
		path := filepath.Join(dir, name)
		// Reject symlinked parent directories too.
		for parent := filepath.Dir(path); parent != filepath.Dir(dir); parent = filepath.Dir(parent) {
			st, err := os.Lstat(parent)
			if err != nil {
				return nil, err
			}
			if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
				return nil, fmt.Errorf("unsafe metadata directory")
			}
			if parent == dir {
				break
			}
		}
		st, err := os.Lstat(path)
		if os.IsNotExist(err) && name != "snapshot/config.json" && name != "snapshot/state.json" {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !st.Mode().IsRegular() || st.Size() > bindingMaxFile {
			return nil, fmt.Errorf("unsafe metadata input %s", name)
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		opened, err := f.Stat()
		if err != nil || !os.SameFile(st, opened) {
			f.Close()
			return nil, fmt.Errorf("metadata input changed")
		}
		h := sha256.New()
		n, err := io.Copy(h, io.LimitReader(f, bindingMaxFile+1))
		f.Close()
		if err != nil || n != st.Size() {
			return nil, fmt.Errorf("metadata input read changed")
		}
		out[name] = hex.EncodeToString(h.Sum(nil))
	}
	return out, nil
}

// PrepareCheckpointDiskBinding is called only by the quiesced capture producer.
// It never rewrites user data. The following catalog write carries the binding
// onto the same metadata disk as the config/state that it hashes.
func PrepareCheckpointDiskBinding(ctx context.Context, entry *SnapshotCatalogEntry, sandbox string, generation uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if entry == nil || sandbox == "" || entry.SnapshotID == "" || entry.RootfsVol == "" || entry.RootfsKind != CowKindSnapshot || entry.RootfsSizeBytes == 0 {
		return fmt.Errorf("incomplete checkpoint disk binding")
	}
	if entry.Backend != cow.BackendS3 && entry.Backend != cow.BackendXFS {
		return fmt.Errorf("invalid binding backend")
	}
	files, err := hashBindingMetadata(filepath.Clean(entry.MetaDir))
	if err != nil {
		return err
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	entry.RootfsGeneration = generation
	entry.DiskBinding = &CheckpointDiskBinding{Version: 1, CaptureID: hex.EncodeToString(nonce[:]), SandboxID: sandbox,
		CheckpointID: entry.SnapshotID, Backend: entry.Backend, RootfsRole: "rootfs", RootfsObject: entry.RootfsVol,
		RootfsGeneration: generation, RootfsSizeBytes: entry.RootfsSizeBytes, MetadataRole: "metadata", MetadataSHA256: files}
	if entry.Backend == cow.BackendS3 {
		store, err := requireS3Cow()
		if err == nil && store != nil {
			info, err := store.GetVolumeInfo(ctx, entry.RootfsVol)
			if err != nil {
				return err
			}
			if info != nil {
				entry.DiskBinding.NativeRootfsUUID = info.VolumeUUID
			}
		}
	}
	return ctx.Err()
}

// DiskAdmission is an observation scoped to one Create attempt, not a reusable
// authorization token. Native origin can corroborate the source snapshot label
// and size; it cannot certify disk contents, commit, or retention. Boot stays closed.
type DiskAdmission struct {
	AttemptID           string
	CheckpointID        string
	MetadataBinding     string
	DiskIdentity        string
	Publication         string
	Retention           string
	BootAllowed         bool
	NativeOriginBinding string
}

func ObserveCheckpointDiskBinding(ctx context.Context, dir, checkpoint, sandbox, backend, attempt string) DiskAdmission {
	r := DiskAdmission{AttemptID: attempt, CheckpointID: checkpoint, MetadataBinding: "unverified", DiskIdentity: "unverified", Publication: "unverified", Retention: "unverified"}
	if ctx.Err() != nil || len(attempt) != 64 || checkpoint == "" || sandbox == "" {
		return r
	}
	if raw, err := hex.DecodeString(attempt); err != nil || len(raw) != 32 {
		return r
	}
	entry, err := readBindingCatalog(dir)
	if err != nil || entry.DiskBinding == nil {
		return r
	}
	b := entry.DiskBinding
	if b.Version != 1 || len(b.CaptureID) != 64 || b.SandboxID != sandbox || b.CheckpointID != checkpoint || entry.SnapshotID != checkpoint || b.Backend != backend || entry.Backend != backend || b.RootfsRole != "rootfs" || b.MetadataRole != "metadata" || b.RootfsObject != entry.RootfsVol || entry.RootfsKind != CowKindSnapshot || b.RootfsSizeBytes == 0 || b.RootfsSizeBytes != entry.RootfsSizeBytes || b.RootfsGeneration != entry.RootfsGeneration {
		return r
	}
	files, err := hashBindingMetadata(filepath.Clean(dir))
	if err != nil || !reflect.DeepEqual(files, b.MetadataSHA256) || ctx.Err() != nil {
		return r
	}
	r.MetadataBinding = "capture_metadata_consistent"
	return r
}

func readBindingCatalog(dir string) (*SnapshotCatalogEntry, error) {
	path := filepath.Join(dir, snapshotCatalogFileName)
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() > bindingMaxFile {
		return nil, fmt.Errorf("unsafe binding catalog")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(st, opened) {
		return nil, fmt.Errorf("binding catalog changed")
	}
	var entry SnapshotCatalogEntry
	d := json.NewDecoder(io.LimitReader(f, bindingMaxFile+1))
	if err := d.Decode(&entry); err != nil {
		return nil, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, fmt.Errorf("trailing binding catalog")
	}
	return &entry, nil
}

// ObserveDiskAdmission reads the imported metadata and live blob-local origins.
// It does not import, activate, mount, delete or pin anything. Request UUIDs are
// expected values, not evidence. Snapshot label/size agreement is explicitly not
// a full checkpoint binding: the catalog has no native source snapshot UUID.
func (s *SandboxImport) ObserveDiskAdmission(ctx context.Context, checkpoint, attempt string) DiskAdmission {
	// A checkpoint may be restored into a different destination sandbox. Source
	// identity comes from the metadata and must subsequently match the trusted
	// commit record, never the destination name or caller annotations.
	source := s.SandboxID
	if e, err := readBindingCatalog(s.MetadataDir()); err == nil && e.DiskBinding != nil {
		source = e.DiskBinding.SandboxID
	}
	r := ObserveCheckpointDiskBinding(ctx, s.MetadataDir(), checkpoint, source, cow.BackendS3, attempt)
	r.NativeOriginBinding = "unverified"
	if r.MetadataBinding != "capture_metadata_consistent" || s.UUIDs == nil {
		return r
	}
	store, err := requireS3Cow()
	if err != nil || store == nil {
		return r
	}
	metadataName := S3MetadataVolumeName(s.SandboxID)
	metadata, err := store.GetVolumeInfo(ctx, metadataName)
	if err != nil || requireImportedOrigin(metadata, metadataName, s.UUIDs.Metadata) != nil {
		return r
	}
	// Cross-node allocation creates the destination rootfs at generation zero.
	// The source rootfs generation and the export manifest generation are
	// different counters; do not compare or substitute one for the other.
	rootName := SandboxRootfsName(s.SandboxID, 0)
	root, err := store.GetVolumeInfo(ctx, rootName)
	if err != nil || requireImportedOrigin(root, rootName, s.UUIDs.Rootfs) != nil {
		return r
	}
	entry, err := readBindingCatalog(s.MetadataDir())
	if err != nil || entry.DiskBinding == nil || ctx.Err() != nil {
		return r
	}
	o := root.ImportOrigin
	if o.SnapshotUUID != "" && o.Snapshot == entry.DiskBinding.RootfsObject && o.SizeBytes == entry.DiskBinding.RootfsSizeBytes {
		r.NativeOriginBinding = "native_source_snapshot_name_size_consistent"
	}
	if s.verifyCheckpointCommit(ctx, store, entry) == nil {
		r.NativeOriginBinding = "committed_native_pair"
		r.Publication = "committed_capture"
		r.DiskIdentity = "committed_native_pair"
	}
	return r
}
