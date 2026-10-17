// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

var errFinalizeInjected = errors.New("injected finalization failure")

// Model native persistent index behavior, including object deletion. Rebuilding
// this adapter after a failure does not retain any finalizer memory state.
type finalizeEngine struct {
	*fakeCowEngine
	lookupErrors     map[string]error
	sealErrors       map[string]error
	deactivateErrors map[string]error
	listError        error
	listResult       *cubecow.ListVolumesResult
	afterSeal        func(string)
	beforeDelete     func(string)
	calls            []string
}

func (f *finalizeEngine) GetVolumeInfo(name string) (*cubecow.Volume, error) {
	if err := f.lookupErrors[name]; err != nil {
		return nil, err
	}
	return f.fakeCowEngine.GetVolumeInfo(name)
}
func (f *finalizeEngine) ListVolumes(size uint64, token string) (*cubecow.ListVolumesResult, error) {
	if f.listError != nil {
		return nil, f.listError
	}
	if f.listResult != nil {
		return f.listResult, nil
	}
	return f.fakeCowEngine.ListVolumes(size, token)
}
func (f *finalizeEngine) CreateSnapshotFromVolume(source, snap string, activate bool) (string, error) {
	f.calls = append(f.calls, "seal:"+snap)
	if err := f.sealErrors[snap]; err != nil {
		return "", err
	}
	path, err := f.fakeCowEngine.CreateSnapshotFromVolume(source, snap, activate)
	if err == nil && f.afterSeal != nil {
		f.afterSeal(snap)
	}
	return path, err
}
func (f *finalizeEngine) DeactivateVolume(name string) error {
	f.calls = append(f.calls, "deactivate:"+name)
	if err := f.deactivateErrors[name]; err != nil {
		return err
	}
	return f.fakeCowEngine.DeactivateVolume(name)
}
func (f *finalizeEngine) DeleteVolume(name string) error {
	f.calls = append(f.calls, "delete:"+name)
	if f.beforeDelete != nil {
		f.beforeDelete(name)
	}
	if err := f.fakeCowEngine.DeleteVolume(name); err != nil {
		return err
	}
	delete(f.volumeInfos, name)
	return nil
}

type finalizeFixture struct {
	engine *finalizeEngine
	entry  *SnapshotCatalogEntry
	root   string
}

func newFinalizeFixture(t *testing.T) *finalizeFixture {
	t.Helper()
	stubS3MetadataMounts(t)
	raw := &fakeCowEngine{volumeInfos: map[string]*cubecow.Volume{}, listSnapshots: map[string][]cubecow.Snapshot{}}
	useTestCowStorageAt(t, raw)
	f := &finalizeFixture{engine: &finalizeEngine{fakeCowEngine: raw}, root: t.TempDir()}
	id := "finalize-1"
	home := filepath.Join(f.root, id)
	f.entry = &SnapshotCatalogEntry{
		SnapshotID: id, SnapshotPath: home, MetaDir: filepath.Join(home, SnapshotMetadataDir),
		RootfsVol: cowTemplateRootfsName(id), RootfsKind: cowKindSnapshot,
		MemoryVol: cowTemplateMemoryName(id), MemoryKind: cowKindVolume,
		MetadataVol: S3MetadataVolumeName(id), MetadataKind: cowKindVolume, Backend: cow.BackendS3,
	}
	for _, name := range []string{f.entry.RootfsVol, f.entry.MemoryVol, f.entry.MetadataVol} {
		raw.volumeInfos[name] = &cubecow.Volume{SizeBytes: 8 << 20}
	}
	raw.listSnapshots["root-source"] = []cubecow.Snapshot{{Name: f.entry.RootfsVol, OriginVolume: "root-source"}}
	oldRoots := snapshotCatalogRootsSnapshot(cow.BackendS3)
	SetSnapshotCatalogRootsFor(cow.BackendS3, f.root)
	t.Cleanup(func() { SetSnapshotCatalogRootsFor(cow.BackendS3, oldRoots...) })
	localStorage.s3CowManager = &S3Cow{engine: f.engine}
	require.NoError(t, WriteSnapshotCatalogFor(cow.BackendS3, f.entry))
	require.NoError(t, mountS3MetadataDevice("/dev/fake-metadata", f.entry.MetaDir))
	return f
}
func (f *finalizeFixture) run() error {
	return FinalizeS3PackageSnapshots(context.Background(), cow.BackendS3, f.entry.SnapshotID)
}
func (f *finalizeFixture) restart() {
	SetSnapshotCatalogRootsFor(cow.BackendS3, f.root)
	f.engine = &finalizeEngine{fakeCowEngine: f.engine.fakeCowEngine}
	localStorage.s3CowManager = &S3Cow{engine: f.engine}
	s3MetadataMu.Lock()
	s3MetadataMounts = map[string]string{}
	s3MetadataMu.Unlock()
}
func (f *finalizeFixture) hideMetadata(t *testing.T) {
	t.Helper()
	// A real unmount hides the disk's catalog. Keep it elsewhere to prove that
	// retry uses the prepared host record, not the old cached pointer or file.
	if _, err := os.Stat(f.entry.MetaDir); err == nil {
		require.NoError(t, os.Rename(f.entry.MetaDir, f.entry.MetaDir+"-hidden"))
	}
}
func (f *finalizeFixture) assertComplete(t *testing.T) {
	t.Helper()
	raw := f.engine.fakeCowEngine
	for _, name := range []string{cowTemplateMemoryName(f.entry.SnapshotID), S3MetadataVolumeName(f.entry.SnapshotID)} {
		require.Nil(t, raw.volumeInfos[name])
		require.NotNil(t, raw.volumeInfos[name+"-snap"])
	}
	for _, call := range raw.createSnapshots {
		require.False(t, strings.Contains(call[1], "-snap-snap"))
	}
	require.Empty(t, raw.exportSnapshots, "local sealing must not claim remote readiness")
}

func TestS3FinalizeRetryAfterCatalogWriteAndUnmountFailures(t *testing.T) {
	for _, second := range []bool{false, true} {
		t.Run(map[bool]string{false: "primary", true: "pause-mount"}[second], func(t *testing.T) {
			f := newFinalizeFixture(t)
			pauseDir := filepath.Join(SnapshotHome(cow.BackendS3, SnapshotKindPause, f.entry.SnapshotID), SnapshotMetadataDir)
			if second {
				require.NoError(t, mountS3MetadataDevice("/dev/alias", pauseDir))
			}
			original := unmountS3MetadataPath
			unmountS3MetadataPath = func(dir string) error {
				e, err := readSnapshotCatalogFile(filepath.Join(f.entry.MetaDir, snapshotCatalogFileName))
				require.NoError(t, err)
				require.Equal(t, f.entry.MemoryVol+"-snap", e.MemoryVol)
				require.Equal(t, S3MetadataSnapshotName(f.entry.SnapshotID), e.MetadataVol)
				if !second || dir == pauseDir {
					return errFinalizeInjected
				}
				return original(dir)
			}
			require.ErrorIs(t, f.run(), errFinalizeInjected)
			require.Empty(t, f.engine.createSnapshots)
			require.Empty(t, f.engine.deletedVolumes)
			unmountS3MetadataPath = original
			f.restart()
			require.NoError(t, f.run())
			f.assertComplete(t)
		})
	}
}

func TestS3FinalizeRetryPartialSeal(t *testing.T) {
	for _, role := range []string{"memory", "metadata"} {
		t.Run(role, func(t *testing.T) {
			f := newFinalizeFixture(t)
			target := f.entry.MemoryVol + "-snap"
			if role == "metadata" {
				target = S3MetadataSnapshotName(f.entry.SnapshotID)
			}
			f.engine.sealErrors = map[string]error{target: errFinalizeInjected}
			require.ErrorIs(t, f.run(), errFinalizeInjected)
			require.Empty(t, f.engine.deletedVolumes)
			require.NotNil(t, f.engine.volumeInfos[f.entry.MemoryVol])
			require.NotNil(t, f.engine.volumeInfos[f.entry.MetadataVol])
			f.hideMetadata(t)
			f.restart()
			require.NoError(t, f.run())
			f.assertComplete(t)
			// Fully sealed repeat, with no catalog on the metadata mount or cache.
			count := len(f.engine.createSnapshots)
			deleted := len(f.engine.deletedVolumes)
			f.restart()
			require.NoError(t, f.run())
			require.Len(t, f.engine.createSnapshots, count)
			require.Len(t, f.engine.deletedVolumes, deleted)
		})
	}
}

func TestS3FinalizeCatalogAndRecordWriteFailure(t *testing.T) {
	for _, record := range []bool{false, true} {
		t.Run(map[bool]string{false: "catalog", true: "prepared-record"}[record], func(t *testing.T) {
			f := newFinalizeFixture(t)
			tmp := filepath.Join(f.entry.MetaDir, snapshotCatalogFileName+".tmp")
			if record {
				tmp = filepath.Join(f.entry.SnapshotPath, s3FinalizeCatalogFile+".tmp")
			}
			require.NoError(t, os.Mkdir(tmp, 0700))
			require.Error(t, f.run())
			require.True(t, s3MetadataIsMounted(f.entry.MetaDir))
			require.Empty(t, f.engine.createSnapshots)
			require.Empty(t, f.engine.deletedVolumes)
			require.NoError(t, os.Remove(tmp))
			f.restart()
			require.NoError(t, f.run())
			f.assertComplete(t)
		})
	}
}

func TestS3FinalizeMissingAndLookupErrors(t *testing.T) {
	cases := []string{"rootfs-missing", "rootfs-volume", "memory-both-missing", "metadata-both-missing", "memory-lookup", "metadata-lookup", "target-lookup", "typed-list", "target-volume", "foreign-memory", "foreign-metadata", "catalog-missing", "catalog-corrupt", "unmounted"}
	for _, test := range cases {
		t.Run(test, func(t *testing.T) {
			f := newFinalizeFixture(t)
			switch test {
			case "rootfs-missing":
				delete(f.engine.volumeInfos, f.entry.RootfsVol)
			case "rootfs-volume":
				f.engine.listSnapshots = map[string][]cubecow.Snapshot{}
			case "memory-both-missing":
				delete(f.engine.volumeInfos, f.entry.MemoryVol)
			case "metadata-both-missing":
				delete(f.engine.volumeInfos, f.entry.MetadataVol)
			case "memory-lookup":
				f.engine.lookupErrors = map[string]error{f.entry.MemoryVol: errFinalizeInjected}
			case "metadata-lookup":
				f.engine.lookupErrors = map[string]error{f.entry.MetadataVol: errFinalizeInjected}
			case "target-lookup":
				f.engine.lookupErrors = map[string]error{f.entry.MetadataVol + "-snap": errFinalizeInjected}
			case "typed-list":
				f.engine.listError = errFinalizeInjected
			case "target-volume":
				f.engine.volumeInfos[f.entry.MemoryVol+"-snap"] = &cubecow.Volume{}
			case "foreign-memory":
				f.entry.MemoryVol = "other-package-memory"
				require.NoError(t, WriteSnapshotCatalogFor(cow.BackendS3, f.entry))
			case "foreign-metadata":
				f.entry.MetadataVol = "s3-meta-other"
				require.NoError(t, WriteSnapshotCatalogFor(cow.BackendS3, f.entry))
			case "catalog-missing":
				require.NoError(t, os.Remove(filepath.Join(f.entry.MetaDir, snapshotCatalogFileName)))
			case "catalog-corrupt":
				require.NoError(t, os.WriteFile(filepath.Join(f.entry.MetaDir, snapshotCatalogFileName), []byte("{"), 0600))
			case "unmounted":
				require.NoError(t, UnmountS3Metadata(f.entry.MetaDir))
			}
			require.Error(t, f.run())
			require.Empty(t, f.engine.createSnapshots)
			require.Empty(t, f.engine.deletedVolumes)
			require.Empty(t, f.engine.deletedSnapshots)
		})
	}
}

func TestS3FinalizeCleanupOrderingAndRetry(t *testing.T) {
	for _, failure := range []string{"snapshot-deactivate", "memory-deactivate", "metadata-deactivate", "delete"} {
		t.Run(failure, func(t *testing.T) {
			f := newFinalizeFixture(t)
			f.engine.beforeDelete = func(name string) {
				require.NotNil(t, f.engine.volumeInfos[f.entry.MemoryVol+"-snap"])
				require.NotNil(t, f.engine.volumeInfos[f.entry.MetadataVol+"-snap"])
				require.False(t, s3MetadataIsMounted(f.entry.MetaDir))
				require.Contains(t, f.engine.calls, "deactivate:"+name)
			}
			switch failure {
			case "snapshot-deactivate":
				f.engine.deactivateErrors = map[string]error{f.entry.MemoryVol + "-snap": errFinalizeInjected}
			case "memory-deactivate":
				f.engine.deactivateErrors = map[string]error{f.entry.MemoryVol: errFinalizeInjected}
			case "metadata-deactivate":
				f.engine.deactivateErrors = map[string]error{f.entry.MetadataVol: errFinalizeInjected}
			case "delete":
				f.engine.deleteVolumeErr = errFinalizeInjected
			}
			require.ErrorIs(t, f.run(), errFinalizeInjected)
			if failure == "snapshot-deactivate" || failure == "memory-deactivate" {
				require.Empty(t, f.engine.deletedVolumes)
			}
			if failure == "metadata-deactivate" {
				require.NotContains(t, f.engine.deletedVolumes, f.entry.MetadataVol)
			}
			f.engine.deleteVolumeErr = nil
			f.hideMetadata(t)
			f.restart()
			require.NoError(t, f.run())
			f.assertComplete(t)
		})
	}
}

func TestS3FinalizeDoesNotFallbackToSnapshotDelete(t *testing.T) {
	f := newFinalizeFixture(t)
	f.engine.deleteVolumeErr = &cubecow.CowError{Code: cubecow.SemInvalidArgument}
	require.Error(t, f.run())
	require.Empty(t, f.engine.deletedSnapshots)
	require.NotNil(t, f.engine.volumeInfos[f.entry.MemoryVol])
}

func TestS3FinalizeSealedSetVerifiedBeforeCleanup(t *testing.T) {
	f := newFinalizeFixture(t)
	f.engine.afterSeal = func(name string) {
		if name == f.entry.MetadataVol+"-snap" {
			delete(f.engine.volumeInfos, f.entry.MemoryVol+"-snap")
		}
	}
	require.ErrorIs(t, f.run(), ErrCowObjectMissing)
	require.Empty(t, f.engine.deletedVolumes)
}

func TestS3FinalizeBaseProtection(t *testing.T) {
	for _, base := range []string{S3MetadataBaseVolumeName, S3MetadataBaseSnapshotName} {
		t.Run(base, func(t *testing.T) {
			f := newFinalizeFixture(t)
			require.NoError(t, UnmountS3Metadata(f.entry.MetaDir))
			f.entry.MetadataVol = base
			f.entry.MetadataKind = cowKindVolume
			f.engine.volumeInfos[base] = &cubecow.Volume{}
			if base == S3MetadataBaseSnapshotName {
				f.entry.MetadataKind = cowKindSnapshot
				f.engine.listSnapshots[S3MetadataBaseVolumeName] = []cubecow.Snapshot{{Name: base}}
			}
			require.NoError(t, WriteSnapshotCatalogFor(cow.BackendS3, f.entry))
			require.NoError(t, f.run())
			require.NotContains(t, f.engine.deletedVolumes, base)
			require.NotContains(t, f.engine.deactivatedVolumes, base)
			require.NotContains(t, f.engine.activatedVolumes, base)
			for _, call := range f.engine.createSnapshots {
				require.NotEqual(t, base, call[0])
				require.NotEqual(t, base, call[1])
			}
		})
	}
}

func TestS3FinalizeBackendAndID(t *testing.T) {
	require.Error(t, FinalizeS3PackageSnapshots(context.Background(), "invalid", "x"))
	require.NoError(t, FinalizeS3PackageSnapshots(context.Background(), cow.BackendXFS, ""))
	for _, id := range []string{"", "..", "../x", "a/b"} {
		require.Error(t, FinalizeS3PackageSnapshots(context.Background(), cow.BackendS3, id))
	}
}

func TestS3SealRequiresVerifiedSnapshot(t *testing.T) {
	cases := []string{"same-name-missing", "same-name-volume", "target-volume", "source-missing", "source-snapshot", "invalid-argument", "already-exists-missing", "created-missing", "already-snapshot"}
	for _, test := range cases {
		t.Run(test, func(t *testing.T) {
			f := newFinalizeFixture(t)
			source := f.entry.MemoryVol
			target := source + "-snap"
			switch test {
			case "same-name-missing":
				target = source
				delete(f.engine.volumeInfos, source)
			case "same-name-volume":
				target = source
			case "target-volume":
				f.engine.volumeInfos[target] = &cubecow.Volume{}
			case "source-missing":
				delete(f.engine.volumeInfos, source)
			case "source-snapshot":
				f.engine.listSnapshots["other"] = []cubecow.Snapshot{{Name: source}}
			case "invalid-argument":
				f.engine.sealErrors = map[string]error{target: &cubecow.CowError{Code: cubecow.SemInvalidArgument}}
			case "already-exists-missing":
				f.engine.sealErrors = map[string]error{target: &cubecow.CowError{Code: cubecow.SemAlreadyExists}}
			case "created-missing":
				f.engine.afterSeal = func(name string) { delete(f.engine.volumeInfos, name) }
			case "already-snapshot":
				target = source
				f.engine.listSnapshots["other"] = []cubecow.Snapshot{{Name: source}}
			}
			m := &S3Cow{engine: f.engine}
			name, _, err := m.sealVolumeToSnapshotAs(context.Background(), source, target)
			if test == "already-snapshot" {
				require.NoError(t, err)
				require.Equal(t, source, name)
			} else {
				require.Error(t, err)
			}
			require.Empty(t, f.engine.deletedVolumes)
		})
	}
}

func TestS3FinalizeTypedListingFailsClosed(t *testing.T) {
	for _, test := range []string{"incomplete", "repeated-token", "bad-name"} {
		t.Run(test, func(t *testing.T) {
			f := newFinalizeFixture(t)
			switch test {
			case "incomplete":
				f.engine.listResult = &cubecow.ListVolumesResult{TotalCount: 1}
			case "repeated-token":
				f.engine.listResult = &cubecow.ListVolumesResult{NextPageToken: "same"}
			case "bad-name":
				f.engine.listResult = &cubecow.ListVolumesResult{Volumes: []cubecow.Volume{{}}, TotalCount: 1}
			}
			require.Error(t, f.run())
			require.Empty(t, f.engine.createSnapshots)
			require.Empty(t, f.engine.deletedVolumes)
		})
	}
}

func TestS3FinalizeStrictUnmountMustDetach(t *testing.T) {
	f := newFinalizeFixture(t)
	original := unmountS3FinalizePath
	unmountS3FinalizePath = func(string) error { return nil }
	require.ErrorContains(t, f.run(), "remains mounted")
	require.Empty(t, f.engine.createSnapshots)
	require.Empty(t, f.engine.deletedVolumes)
	unmountS3FinalizePath = original
	f.restart()
	require.NoError(t, f.run())
	f.assertComplete(t)
}

func TestS3FinalizeRejectsMountedBase(t *testing.T) {
	f := newFinalizeFixture(t)
	f.entry.MetadataVol, f.entry.MetadataKind = S3MetadataBaseVolumeName, cowKindVolume
	f.engine.volumeInfos[S3MetadataBaseVolumeName] = &cubecow.Volume{}
	require.NoError(t, WriteSnapshotCatalogFor(cow.BackendS3, f.entry))
	require.ErrorContains(t, f.run(), "metadata base")
	require.True(t, s3MetadataIsMounted(f.entry.MetaDir))
	require.Empty(t, f.engine.createSnapshots)
	require.Empty(t, f.engine.deactivatedVolumes)
	require.Empty(t, f.engine.deletedVolumes)
}

func TestS3FinalizePreservesVerifiedUnsuffixedMemorySnapshot(t *testing.T) {
	f := newFinalizeFixture(t)
	f.engine.listSnapshots["previous-memory"] = []cubecow.Snapshot{{Name: f.entry.MemoryVol}}
	require.NoError(t, f.run())
	require.NotContains(t, f.engine.deletedVolumes, f.entry.MemoryVol)
	require.NotNil(t, f.engine.volumeInfos[f.entry.MemoryVol])
	require.Nil(t, f.engine.volumeInfos[f.entry.MemoryVol+"-snap"])
	f.hideMetadata(t)
	f.restart()
	require.NoError(t, f.run())
}

func TestS3FinalizeCorruptPreparedRecordFailsClosed(t *testing.T) {
	f := newFinalizeFixture(t)
	f.engine.sealErrors = map[string]error{f.entry.MemoryVol + "-snap": errFinalizeInjected}
	require.ErrorIs(t, f.run(), errFinalizeInjected)
	require.NoError(t, os.WriteFile(filepath.Join(f.entry.SnapshotPath, s3FinalizeCatalogFile), []byte("{}"), 0600))
	f.restart()
	require.ErrorContains(t, f.run(), "invalid finalization record")
	require.Empty(t, f.engine.createSnapshots)
	require.Empty(t, f.engine.deletedVolumes)
}
