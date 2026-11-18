// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) add(d time.Duration) { c.t = c.t.Add(d) }
func useFakeRestoreClock(t *testing.T) *fakeClock {
	t.Helper()
	c := &fakeClock{t: time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)}
	prevNow, prevTracker, prevRoot := restoreProgressNow, restoreProgress, restorePhaseRoot
	restoreProgressNow = c.now
	restoreProgress = &restoreTracker{entries: map[string]*restoreEntry{}}
	restorePhaseRoot = filepath.Join(t.TempDir(), "phase")
	t.Cleanup(func() { restoreProgressNow, restoreProgress, restorePhaseRoot = prevNow, prevTracker, prevRoot })
	return c
}

// vmmMark does what hypervisor/vmm/src/restore_phase.rs does.
func vmmMark(t *testing.T, id, phase string) {
	t.Helper()
	dir := filepath.Join(restorePhaseRoot, id)
	if _, err := os.Stat(dir); err != nil {
		return // not requested: the VMM writes nothing
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "phase.tmp"), []byte(phase+"\n"), 0o600))
	require.NoError(t, os.Rename(filepath.Join(dir, "phase.tmp"), filepath.Join(dir, "phase")))
}

func TestRestoreProgressVMSubPhases(t *testing.T) {
	c := useFakeRestoreClock(t)
	restoreProgress.begin("sb1", threeJobs("sb1"))
	require.DirExists(t, filepath.Join(restorePhaseRoot, "sb1"), "begin asks the VMM for sub-phases")

	// A stale file must not skip the import phase.
	vmmMark(t, "sb1", RestorePhaseVMResume)
	p, _ := GetRestoreProgress("sb1")
	require.Equal(t, RestorePhaseImporting, p.Phase)
	require.NoError(t, os.Remove(filepath.Join(restorePhaseRoot, "sb1", "phase")))

	MarkRestorePhase("sb1", RestorePhaseRestoringVM, nil)
	for _, ph := range []string{RestorePhaseVMRestoreMemory, RestorePhaseVMRestoreDevices, RestorePhaseVMResume, RestorePhaseAgentReady} {
		c.add(3 * time.Second)
		vmmMark(t, "sb1", ph)
		p, _ = GetRestoreProgress("sb1")
		require.Equal(t, ph, p.Phase)
		require.Zero(t, p.StalledSeconds, "a new sub-phase is progress")
	}
	// restoring_vm reported late never moves the bar back.
	MarkRestorePhase("sb1", RestorePhaseRestoringVM, nil)
	p, _ = GetRestoreProgress("sb1")
	require.Equal(t, RestorePhaseAgentReady, p.Phase)

	MarkRestorePhase("sb1", RestorePhaseDone, nil)
	p, _ = GetRestoreProgress("sb1")
	require.Equal(t, RestorePhaseDone, p.Phase)
	require.NoDirExists(t, filepath.Join(restorePhaseRoot, "sb1"), "finished restores clean up")
}

func TestRestoreProgressFailureNamesVMStep(t *testing.T) {
	useFakeRestoreClock(t)
	restoreProgress.begin("sb2", threeJobs("sb2"))
	MarkRestorePhase("sb2", RestorePhaseRestoringVM, nil)
	vmmMark(t, "sb2", RestorePhaseVMRestoreDevices)
	MarkRestorePhase("sb2", RestorePhaseFailed, errors.New("Restore vm failed"))
	p, _ := GetRestoreProgress("sb2")
	require.Equal(t, RestorePhaseFailed, p.Phase)
	require.Equal(t, "vm_restore_devices: Restore vm failed", p.Error)
	require.NoDirExists(t, filepath.Join(restorePhaseRoot, "sb2"))
}

func TestRestoreProgressIgnoresGarbagePhaseFile(t *testing.T) {
	useFakeRestoreClock(t)
	restoreProgress.begin("sb3", threeJobs("sb3"))
	MarkRestorePhase("sb3", RestorePhaseRestoringVM, nil)
	vmmMark(t, "sb3", "done")
	p, _ := GetRestoreProgress("sb3")
	require.Equal(t, RestorePhaseRestoringVM, p.Phase)
	requestVMPhases("../evil")
	require.NoDirExists(t, filepath.Join(filepath.Dir(restorePhaseRoot), "evil"))
}

func threeJobs(id string) []importJob {
	return []importJob{
		{name: SandboxRootfsName(id, 0), role: "rootfs"},
		{name: SandboxMemoryName(id), role: "memory"},
		{name: S3MetadataVolumeName(id), role: "metadata"},
	}
}

func TestRestoreProgressSumsDisksAndHidesTotalUntilAllKnown(t *testing.T) {
	c := useFakeRestoreClock(t)
	restoreProgress.begin("sb1", threeJobs("sb1"))
	p, ok := GetRestoreProgress("sb1")
	require.True(t, ok)
	require.Equal(t, RestorePhaseImporting, p.Phase)
	require.Zero(t, p.TotalChunks)
	require.Len(t, p.Disks, 3)

	c.add(time.Second)
	restoreProgress.update("sb1", SandboxRootfsName("sb1", 0), "rootfs", &importStatus{State: "prefetching", LocalChunks: 10, PresentChunks: 100})
	restoreProgress.update("sb1", SandboxMemoryName("sb1"), "memory", &importStatus{State: "prefetching", LocalChunks: 5, PresentChunks: 50})
	p, _ = GetRestoreProgress("sb1")
	require.Zero(t, p.TotalChunks, "a disk of unknown size must not let the bar jump back later")
	require.EqualValues(t, 15, p.LocalChunks)

	restoreProgress.update("sb1", S3MetadataVolumeName("sb1"), "metadata", &importStatus{State: "done", LocalChunks: 1, PresentChunks: 1, Done: true})
	p, _ = GetRestoreProgress("sb1")
	require.EqualValues(t, 151, p.TotalChunks)
	require.EqualValues(t, 16, p.LocalChunks)
	require.Equal(t, c.t, p.LastProgressAt)
}

func TestRestoreProgressStallClockOnlyMovesWithoutProgress(t *testing.T) {
	c := useFakeRestoreClock(t)
	restoreProgress.begin("sb1", threeJobs("sb1")[:1])
	name := SandboxRootfsName("sb1", 0)
	restoreProgress.update("sb1", name, "rootfs", &importStatus{LocalChunks: 1, PresentChunks: 10})
	c.add(20 * time.Second)
	restoreProgress.update("sb1", name, "rootfs", &importStatus{LocalChunks: 1, PresentChunks: 10})
	p, _ := GetRestoreProgress("sb1")
	require.InDelta(t, 20, p.StalledSeconds, 0.001)

	restoreProgress.update("sb1", name, "rootfs", &importStatus{LocalChunks: 2, PresentChunks: 10})
	p, _ = GetRestoreProgress("sb1")
	require.Zero(t, p.StalledSeconds)
}

func TestRestoreProgressPhasesAndDoneFillsChunks(t *testing.T) {
	c := useFakeRestoreClock(t)
	restoreProgress.begin("sb1", threeJobs("sb1")[:1])
	name := SandboxRootfsName("sb1", 0)
	// done may arrive with local < present (decoupled, nothing reads through).
	restoreProgress.update("sb1", name, "rootfs", &importStatus{State: "done", LocalChunks: 7, PresentChunks: 10, Done: true})
	MarkRestorePhase("sb1", RestorePhaseRestoringVM, nil)
	p, _ := GetRestoreProgress("sb1")
	require.Equal(t, RestorePhaseRestoringVM, p.Phase)
	require.EqualValues(t, 10, p.LocalChunks)
	require.EqualValues(t, 10, p.TotalChunks)

	c.add(3 * time.Second)
	MarkRestorePhase("sb1", RestorePhaseDone, nil)
	MarkRestorePhase("sb1", RestorePhaseFailed, errors.New("late"))
	p, _ = GetRestoreProgress("sb1")
	require.Equal(t, RestorePhaseDone, p.Phase, "a finished restore keeps its final phase")
	require.Empty(t, p.Error)

	c.add(restoreProgressRetention + time.Second)
	_, ok := GetRestoreProgress("sb1")
	require.False(t, ok, "finished restores are forgotten after the retention")
}

func TestRestoreProgressFailedKeepsError(t *testing.T) {
	useFakeRestoreClock(t)
	restoreProgress.begin("sb1", threeJobs("sb1"))
	MarkRestorePhase("sb1", RestorePhaseFailed, errors.New("import deadline"))
	p, _ := GetRestoreProgress("sb1")
	require.Equal(t, RestorePhaseFailed, p.Phase)
	require.Equal(t, "import deadline", p.Error)
}

func TestWaitImportPrefetchReportsToTracker(t *testing.T) {
	useFakeRestoreClock(t)
	name := SandboxRootfsName("sb9", 0)
	restoreProgress.begin("sb9", []importJob{{name: name, role: "rootfs"}})
	st := &scriptedImportStatus{replies: []string{
		`{"state":"prefetching","present_chunks":4,"local_chunks":1}`,
		`{"state":"prefetching","present_chunks":4,"local_chunks":3}`,
		`{"state":"done","present_chunks":4,"local_chunks":4,"done":true}`,
	}}
	prevPoll := importPrefetchPoll
	importPrefetchPoll = time.Millisecond
	t.Cleanup(func() { importPrefetchPoll = prevPoll })
	ctx := withRestoreProgress(context.Background(), "sb9")
	require.NoError(t, waitImportPrefetch(ctx, st, name, "rootfs", time.Minute))
	p, _ := GetRestoreProgress("sb9")
	require.EqualValues(t, 4, p.LocalChunks)
	require.EqualValues(t, 4, p.TotalChunks)
	require.True(t, p.Disks[0].Done)
}

type scriptedImportStatus struct {
	replies []string
	i       int
}

func (s *scriptedImportStatus) ImportStatus(string) (string, error) {
	r := s.replies[s.i]
	if s.i < len(s.replies)-1 {
		s.i++
	}
	return r, nil
}

func TestRestoreProgressHTTP(t *testing.T) {
	useFakeRestoreClock(t)
	restoreProgress.begin("sb1", threeJobs("sb1"))
	h := RestoreProgressHTTPHandlers()[RestoreProgressPath]

	do := func(remote, query string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, RestoreProgressPath+query, nil)
		r.RemoteAddr = remote
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	w := do("10.0.1.20:5555", "?sandbox_id=sb1")
	require.Equal(t, http.StatusOK, w.Code)
	var p RestoreProgress
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p))
	require.Equal(t, "sb1", p.SandboxID)
	require.Equal(t, RestorePhaseImporting, p.Phase)

	require.Equal(t, http.StatusNotFound, do("127.0.0.1:1", "?sandbox_id=nope").Code)
	require.Equal(t, http.StatusBadRequest, do("127.0.0.1:1", "").Code)
	require.Equal(t, http.StatusForbidden, do("46.62.191.236:1", "?sandbox_id=sb1").Code)
}
