// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// Finalization requires the caller to have quiesced the package. Serialize local
// retries so they cannot race the catalog rewrite, unmount, or work deletion.
var s3FinalizeMu sync.Mutex

// FinalizeS3PackageSnapshots seals a local package, not its remote export.
// A prepared catalog is retained OUTSIDE the metadata mount before unmounting.
// Together with deterministic owned work names this allows a new process to
// resume after any partial seal or cleanup. Missing catalogs are never derived
// from object names here: names cannot prove that metadata was written.
func FinalizeS3PackageSnapshots(ctx context.Context, backend, snapshotID string) error {
	normalized, err := cow.NormalizeBackend(backend)
	if err != nil {
		return err
	}
	if normalized != cow.BackendS3 {
		return nil
	}
	id := strings.TrimSpace(snapshotID)
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, `/\\`) {
		return fmt.Errorf("invalid snapshot_id %q", snapshotID)
	}
	s3FinalizeMu.Lock()
	defer s3FinalizeMu.Unlock()
	store, err := requireCowStoreFor(cow.BackendS3)
	if err != nil {
		return err
	}
	m, ok := store.(*S3Cow)
	if !ok || m == nil {
		return fmt.Errorf("s3 cow store is not *S3Cow")
	}
	entry, prepared, err := readS3FinalizeCatalog(id)
	if err != nil {
		return err
	}
	volumes, err := m.finalizeVolumeKinds(ctx)
	if err != nil {
		return err
	}
	if entry.RootfsVol != cowTemplateRootfsName(id) || entry.RootfsKind != cowKindSnapshot {
		return fmt.Errorf("invalid package rootfs reference %q", entry.RootfsVol)
	}
	if err := m.requireFinalizationKind(ctx, volumes, entry.RootfsVol, cowKindSnapshot); err != nil {
		return err
	}
	memoryWork := cowTemplateMemoryName(id)
	memorySnap := memoryWork + "-snap"
	if entry.MemoryVol != memoryWork && entry.MemoryVol != memorySnap {
		return fmt.Errorf("invalid package memory reference %q", entry.MemoryVol)
	}
	// Older packages can contain an actual snapshot under the unsuffixed name.
	// Preserve it only when the native index proves that it is a snapshot.
	memoryKind, err := m.finalizationKind(ctx, volumes, memoryWork)
	if err != nil {
		return err
	}
	if entry.MemoryVol == memoryWork && memoryKind == cowKindSnapshot {
		memorySnap = memoryWork
	}
	metadataWork := S3MetadataVolumeName(id)
	metadataSnap := S3MetadataSnapshotName(id)
	base := IsS3MetadataBaseName(entry.MetadataVol)
	if base {
		metadataSnap = entry.MetadataVol
		kind := cowKindSnapshot
		if metadataSnap == S3MetadataBaseVolumeName {
			kind = cowKindVolume
		}
		if entry.MetadataKind != kind {
			return fmt.Errorf("invalid metadata base kind %q", entry.MetadataKind)
		}
		if err := m.requireFinalizationKind(ctx, volumes, metadataSnap, kind); err != nil {
			return err
		}
	} else if entry.MetadataVol != "" && entry.MetadataVol != metadataWork && entry.MetadataVol != metadataSnap {
		return fmt.Errorf("invalid package metadata reference %q", entry.MetadataVol)
	}
	if err := m.checkFinalizationPair(ctx, volumes, memoryWork, memorySnap); err != nil {
		return err
	}
	if !base {
		if err := m.checkFinalizationPair(ctx, volumes, metadataWork, metadataSnap); err != nil {
			return err
		}
	}
	changed := entry.MemoryVol != memorySnap || entry.MemoryKind != cowKindSnapshot ||
		(!base && (entry.MetadataVol != metadataSnap || entry.MetadataKind != cowKindSnapshot))
	metaKind, err := m.finalizationKind(ctx, volumes, metadataSnap)
	if err != nil {
		return err
	}
	if entry.DiskBinding != nil && entry.DiskBinding.NativeRootfsUUID != "" {
		// Pre-existing named snapshots are not proof of capture after a partial
		// seal crash. Retain them, but require reconciliation without a receipt.
		_, sealErr := os.Stat(filepath.Join(entry.SnapshotPath, checkpointSealFile))
		memKind, kindErr := m.finalizationKind(ctx, volumes, memorySnap)
		if kindErr != nil {
			return kindErr
		}
		if sealErr != nil && (metaKind == cowKindSnapshot || memKind == cowKindSnapshot) {
			return fmt.Errorf("ambiguous prior seal without native capture receipt")
		}
	}
	if (prepared || (!base && metaKind == cowKindSnapshot)) && changed {
		return fmt.Errorf("sealed/prepared catalog does not contain final package references")
	}
	if base && s3MetadataIsMounted(entry.MetaDir) {
		return fmt.Errorf("refusing to modify or unmount metadata base at %s", entry.MetaDir)
	}
	if !prepared {
		// Never write to an unmounted path pretending it is the metadata disk.
		// Base references are legacy host catalogs; never modify a mounted base.
		if !base && metaKind == "" && !s3MetadataIsMounted(entry.MetaDir) {
			return fmt.Errorf("unsafe metadata mount state for catalog %s", entry.MetaDir)
		}
		if changed {
			entry.MemoryVol, entry.MemoryKind = memorySnap, cowKindSnapshot
			if !base {
				entry.MetadataVol, entry.MetadataKind = metadataSnap, cowKindSnapshot
			}
			if err := WriteSnapshotCatalogFor(cow.BackendS3, entry); err != nil {
				return err
			}
		}
		if err := writeS3FinalizeCatalog(entry); err != nil {
			return err
		}
	}
	// Both known package mount locations must be detached before any seal.
	for _, dir := range []string{entry.MetaDir, filepath.Join(SnapshotHome(cow.BackendS3, SnapshotKindPause, id), SnapshotMetadataDir)} {
		if err := unmountS3FinalizeMetadata(dir); err != nil {
			return err
		}
	}
	if _, _, err := m.sealVolumeToSnapshotAs(ctx, memoryWork, memorySnap); err != nil {
		return fmt.Errorf("seal memory: %w", err)
	}
	if !base {
		if _, _, err := m.sealVolumeToSnapshotAs(ctx, metadataWork, metadataSnap); err != nil {
			return fmt.Errorf("seal metadata: %w", err)
		}
	}
	// Recheck the entire required set before any destructive work cleanup.
	volumes, err = m.finalizeVolumeKinds(ctx)
	if err != nil {
		return err
	}
	sealed := []string{entry.RootfsVol, memorySnap}
	if !base {
		sealed = append(sealed, metadataSnap)
	}
	for _, name := range sealed {
		if err := m.requireFinalizationKind(ctx, volumes, name, cowKindSnapshot); err != nil {
			return err
		}
	}
	if err := m.recordCheckpointSeal(ctx, entry); err != nil {
		return fmt.Errorf("record native sealed capture: %w", err)
	}
	for _, name := range sealed {
		if err := deactivateS3Object(ctx, m, name, cowKindSnapshot); err != nil {
			return fmt.Errorf("deactivate snapshot %s: %w", name, err)
		}
	}
	works := []string{}
	if memoryWork != memorySnap {
		works = append(works, memoryWork)
	}
	if !base {
		works = append(works, metadataWork)
	}
	for _, name := range works {
		kind, err := m.finalizationKind(ctx, volumes, name)
		if err != nil {
			return err
		}
		if kind == "" {
			continue
		}
		if kind != cowKindVolume {
			return fmt.Errorf("refusing to delete non-volume work %s", name)
		}
		if err := deactivateS3Object(ctx, m, name, cowKindVolume); err != nil {
			return fmt.Errorf("deactivate work %s: %w", name, err)
		}
		// Do not use DeleteByKind: its invalid-argument fallback can delete a
		// snapshot. Work cleanup must only ever invoke the native volume delete.
		if err := m.engine.DeleteVolume(name); err != nil && !isCowSemantic(err, cubecow.SemNotFound) {
			return fmt.Errorf("delete work %s: %w", name, err)
		}
	}
	if !base {
		if err := dropDerived(id); err != nil {
			return err
		}
	}
	snapshotCatalogMu.Lock()
	catalogNSLocked(cow.BackendS3).index[id] = cloneCatalogEntry(entry)
	snapshotCatalogMu.Unlock()
	return nil
}

// The lifecycle unmount helper permits lazy detach. Sealing must not: a lazy
// mount can retain dirty pages and open writers after disappearing from the
// mount table. Keep this stricter operation local to package finalization.
var unmountS3FinalizePath = func(path string) error {
	if _, stderr, err := utils.ExecV([]string{"umount", path}, cmdTimeout); err != nil {
		return fmt.Errorf("strict umount s3 metadata %s failed: %s: %w", path, stderr, err)
	}
	return nil
}

func unmountS3FinalizeMetadata(path string) error {
	s3MetadataMu.Lock()
	defer s3MetadataMu.Unlock()
	if !s3MetadataIsMounted(path) {
		return nil
	}
	if err := unmountS3FinalizePath(path); err != nil {
		return err
	}
	if s3MetadataIsMounted(path) {
		return fmt.Errorf("s3 metadata remains mounted at %s", path)
	}
	for id, mounted := range s3MetadataMounts {
		if filepath.Clean(mounted) == filepath.Clean(path) {
			delete(s3MetadataMounts, id)
		}
	}
	return nil
}

// The native S3 index has exactly Volume and Snapshot variants. GetVolumeInfo
// projects both to Volume, losing kind. ListVolumes filters ONLY NameKind::Volume
// (cubecow/src/engine/s3.rs). An existing object absent from a complete typed
// volume listing is therefore a snapshot. ListSnapshots is NOT sufficient: it
// returns empty when the origin work volume has already been deleted.
// Keep this optional interface local to S3 finalization; old/fake engines without
// the capability must fail closed rather than guess from a suffix.
type s3FinalizationVolumeLister interface {
	ListVolumes(uint64, string) (*cubecow.ListVolumesResult, error)
}

func (m *S3Cow) finalizeVolumeKinds(ctx context.Context) (map[string]bool, error) {
	lister, ok := m.engine.(s3FinalizationVolumeLister)
	if !ok {
		return nil, fmt.Errorf("native S3 typed volume listing unavailable")
	}
	names := map[string]bool{}
	tokens := map[string]bool{}
	token := ""
	var total uint64
	for page := 0; page < 256; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := lister.ListVolumes(128, token)
		if err != nil {
			return nil, fmt.Errorf("list S3 volume kinds: %w", err)
		}
		if result == nil {
			return nil, fmt.Errorf("nil S3 volume listing")
		}
		if page == 0 {
			total = result.TotalCount
		}
		if total != result.TotalCount {
			return nil, fmt.Errorf("S3 volume listing changed during finalization")
		}
		for _, volume := range result.Volumes {
			if volume.Name == "" || names[volume.Name] {
				return nil, fmt.Errorf("invalid/duplicate S3 volume listing entry %q", volume.Name)
			}
			names[volume.Name] = true
		}
		if result.NextPageToken == "" {
			if uint64(len(names)) != total {
				return nil, fmt.Errorf("incomplete S3 volume listing")
			}
			return names, nil
		}
		if tokens[result.NextPageToken] {
			return nil, fmt.Errorf("repeated S3 volume listing token")
		}
		token = result.NextPageToken
		tokens[token] = true
	}
	return nil, fmt.Errorf("S3 volume listing exceeds finalization limit")
}

func (m *S3Cow) finalizationKind(ctx context.Context, volumes map[string]bool, name string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	info, err := m.GetVolumeInfo(ctx, name)
	exists, err := cowObjectPresent(info, err)
	if err != nil {
		return "", fmt.Errorf("lookup S3 object %s: %w", name, err)
	}
	if !exists {
		return "", nil
	}
	if volumes[name] {
		return cowKindVolume, nil
	}
	return cowKindSnapshot, nil
}

func (m *S3Cow) requireFinalizationKind(ctx context.Context, volumes map[string]bool, name, want string) error {
	kind, err := m.finalizationKind(ctx, volumes, name)
	if err != nil {
		return err
	}
	if kind == "" {
		return fmt.Errorf("%w: %s", ErrCowObjectMissing, name)
	}
	if kind != want {
		return fmt.Errorf("S3 object %s has kind %s, want %s", name, kind, want)
	}
	return nil
}

func (m *S3Cow) checkFinalizationPair(ctx context.Context, volumes map[string]bool, work, snap string) error {
	kind, err := m.finalizationKind(ctx, volumes, snap)
	if err != nil {
		return err
	}
	if kind != "" {
		if kind != cowKindSnapshot {
			return fmt.Errorf("S3 target %s is not a snapshot", snap)
		}
		return nil
	}
	if work == snap {
		return fmt.Errorf("%w: %s", ErrCowObjectMissing, snap)
	}
	return m.requireFinalizationKind(ctx, volumes, work, cowKindVolume)
}

func (m *S3Cow) sealVolumeToSnapshot(ctx context.Context, volumeName string) (string, bool, error) {
	volumes, err := m.finalizeVolumeKinds(ctx)
	if err != nil {
		return "", false, err
	}
	kind, err := m.finalizationKind(ctx, volumes, strings.TrimSpace(volumeName))
	if err != nil {
		return "", false, err
	}
	target := strings.TrimSpace(volumeName) + "-snap"
	if kind == cowKindSnapshot {
		target = strings.TrimSpace(volumeName)
	}
	return m.sealVolumeToSnapshotAs(ctx, volumeName, target)
}

func (m *S3Cow) sealVolumeToSnapshotAs(ctx context.Context, volumeName, snapName string) (string, bool, error) {
	volumeName, snapName = strings.TrimSpace(volumeName), strings.TrimSpace(snapName)
	if volumeName == "" || snapName == "" {
		return "", false, fmt.Errorf("volume and snapshot names are required")
	}
	if IsS3MetadataBaseName(volumeName) || IsS3MetadataBaseName(snapName) {
		return "", false, fmt.Errorf("refusing to seal node-local s3 metadata base")
	}
	volumes, err := m.finalizeVolumeKinds(ctx)
	if err != nil {
		return "", false, err
	}
	if err := m.checkFinalizationPair(ctx, volumes, volumeName, snapName); err != nil {
		return "", false, err
	}
	kind, err := m.finalizationKind(ctx, volumes, snapName)
	if err != nil {
		return "", false, err
	}
	if kind == cowKindSnapshot {
		return snapName, false, nil
	}
	_, err = m.engine.CreateSnapshotFromVolume(volumeName, snapName, false)
	if err != nil && !isCowSemantic(err, cubecow.SemAlreadyExists) {
		return "", false, err
	}
	// Success and already-exists must both be followed by a typed lookup. An
	// invalid-argument error is never evidence that the source is a snapshot.
	volumes, verifyErr := m.finalizeVolumeKinds(ctx)
	if verifyErr != nil {
		return "", false, verifyErr
	}
	if verifyErr := m.requireFinalizationKind(ctx, volumes, snapName, cowKindSnapshot); verifyErr != nil {
		return "", false, verifyErr
	}
	return snapName, err == nil, nil
}
