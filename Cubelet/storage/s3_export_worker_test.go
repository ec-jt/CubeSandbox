// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// workerEngine models native export admission: one role at a time, accepted
// exports settle to DONE after settlePolls reads, drain timeouts fail the
// role natively, and an admission error may still have been accepted.
type workerEngine struct {
	*commitEngine
	mu sync.Mutex
	// active is the role currently exporting; a second submit is refused.
	active     string
	polls      int
	settlePoll int
	submits    []string
	// drainFail[name] counts native FAILED outcomes to inject.
	drainFail map[string]int
	// ambiguous[name] counts submissions that are accepted natively but
	// report a transport error to the caller.
	ambiguous map[string]int
	// onSubmit runs before acceptance (cleanup race tests).
	onSubmit func(name string)
	// stuck[name]: once accepted the export never settles, i.e. the native
	// export died and left publication_pending behind (B16).
	stuck map[string]bool
	// abandonErr is returned by AbandonUnpublishedExport when set.
	abandonErr error
	abandoned  []string
}

func (e *workerEngine) AbandonUnpublishedExport(uuid string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.abandonErr != nil {
		return e.abandonErr
	}
	for name, info := range e.volumeInfos {
		if info.ExportUUID == uuid && info.ExportStatus == cow.ExportStatusInProgress {
			e.abandoned = append(e.abandoned, uuid)
			info.ExportUUID = ""
			info.ExportStatus = cow.ExportStatusNone
			delete(e.stuck, name)
			if e.active == name {
				e.active = ""
			}
			return nil
		}
	}
	return errors.New("not publication_pending")
}

func (e *workerEngine) ExportSnapshot(name string) (string, error) {
	if e.onSubmit != nil {
		e.onSubmit(name)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.submits = append(e.submits, name)
	if e.active != "" && e.active != name {
		return "", errors.New("registry save busy")
	}
	info := e.volumeInfos[name]
	if info == nil {
		return "", errors.New("not found")
	}
	if info.ExportStatus == cow.ExportStatusInProgress || info.ExportStatus == cow.ExportStatusDone {
		return "", errors.New("export already admitted")
	}
	info.ExportUUID = "exp-" + name
	info.ExportStatus = cow.ExportStatusInProgress
	e.active = name
	e.polls = 0
	if e.ambiguous[name] > 0 {
		e.ambiguous[name]--
		return "", errors.New("Connection timed out")
	}
	return info.ExportUUID, nil
}

func (e *workerEngine) GetVolumeInfo(name string) (*cubecow.Volume, error) {
	e.mu.Lock()
	if e.active == name && !e.stuck[name] {
		e.polls++
		info := e.volumeInfos[name]
		if e.polls > e.settlePoll {
			if e.drainFail[name] > 0 {
				e.drainFail[name]--
				info.ExportStatus = cow.ExportStatusFailed
				info.ExportUUID = ""
			} else {
				info.ExportStatus = cow.ExportStatusDone
			}
			e.active = ""
		}
	}
	e.mu.Unlock()
	return e.commitEngine.GetVolumeInfo(name)
}

func newTestWorker(t *testing.T) (*s3ExportWorker, *workerEngine, *SnapshotCatalogEntry) {
	t.Helper()
	m, ce, entry := commitFixture(t)
	for name, info := range ce.volumeInfos {
		// Native evidence follows the uuid the worker's engine will mint.
		if ev, ok := ce.evidence[info.ExportUUID]; ok {
			ev.ExportUUID = "exp-" + name
			ce.evidence["exp-"+name] = ev
		}
		info.ExportUUID = ""
		info.ExportStatus = cow.ExportStatusNone
	}
	AddSnapshotCatalogRootFor(cow.BackendS3, SnapshotKindRoot(cow.BackendS3, SnapshotKindNormal))
	e := &workerEngine{commitEngine: ce, settlePoll: 2, drainFail: map[string]int{}, ambiguous: map[string]int{},
		stuck: map[string]bool{}}
	m.engine = e
	localStorage.s3CowManager = m
	w := newS3ExportWorker(ExportWorkerConfig{
		RoleSettleTimeout: 2 * time.Second,
		PollInterval:      time.Millisecond,
		MaxAttempts:       4,
		BackoffBase:       time.Millisecond,
		BackoffMax:        2 * time.Millisecond,
	}, func() (*S3Cow, error) { return m, nil })
	t.Cleanup(setExportWorkerForTest(w))
	return w, e, entry
}

func TestExportWorkerSequentialOneRoleAtATime(t *testing.T) {
	w, e, entry := newTestWorker(t)
	require.NoError(t, w.runOne(context.Background(), entry.SnapshotID))
	// Each role submitted exactly once, in order, and never while another
	// role was active (the engine refuses that with "registry save busy").
	require.Equal(t, []string{entry.RootfsVol, entry.MemoryVol, entry.MetadataVol}, e.submits)
	for _, name := range e.submits {
		require.Equal(t, cow.ExportStatusDone, e.volumeInfos[name].ExportStatus)
	}
	st, err := (&S3Cow{engine: e}).UploadStatus(context.Background(), entry.SnapshotID)
	require.NoError(t, err)
	require.Equal(t, cow.RemoteStateReady, st.State)
	require.NotEmpty(t, st.RemoteUUIDs.Rootfs)
	require.NotEmpty(t, st.RemoteUUIDs.Metadata)
}

func TestExportWorkerRetriesDrainTimeout(t *testing.T) {
	w, e, entry := newTestWorker(t)
	e.drainFail[entry.RootfsVol] = 2
	require.NoError(t, w.runOne(context.Background(), entry.SnapshotID))
	// rootfs: two native failures then success; others once.
	require.Equal(t, []string{entry.RootfsVol, entry.RootfsVol, entry.RootfsVol, entry.MemoryVol, entry.MetadataVol}, e.submits)
}

func TestExportWorkerGivesUpAfterBudgetAndReportsFailed(t *testing.T) {
	w, e, entry := newTestWorker(t)
	e.drainFail[entry.MemoryVol] = 100
	err := w.runOne(context.Background(), entry.SnapshotID)
	require.Error(t, err)
	// Never moved past memory to metadata.
	for _, n := range e.submits {
		require.NotEqual(t, entry.MetadataVol, n)
	}
	recordExportFailed(entry.SnapshotID, err)
	w.mu.Lock()
	w.jobs[entry.SnapshotID] = &exportJobState{terminal: true, message: err.Error()}
	w.mu.Unlock()
	st, sErr := (&S3Cow{engine: e}).UploadStatus(context.Background(), entry.SnapshotID)
	require.NoError(t, sErr)
	require.Equal(t, cow.RemoteStateFailed, st.State)
}

func TestExportWorkerAmbiguousAcceptanceRereadsBeforeResubmit(t *testing.T) {
	w, e, entry := newTestWorker(t)
	e.ambiguous[entry.RootfsVol] = 1
	require.NoError(t, w.runOne(context.Background(), entry.SnapshotID))
	// The ambiguous submit was in fact accepted: the worker re-read the
	// object, saw INPROGRESS with an export uuid, and waited instead of
	// submitting rootfs a second time.
	count := 0
	for _, n := range e.submits {
		if n == entry.RootfsVol {
			count++
		}
	}
	require.Equal(t, 1, count)
	require.Equal(t, cow.ExportStatusDone, e.volumeInfos[entry.RootfsVol].ExportStatus)
}

func TestExportWorkerNeverResubmitsInProgressOrDone(t *testing.T) {
	w, e, entry := newTestWorker(t)
	e.volumeInfos[entry.RootfsVol].ExportUUID = "prior"
	e.volumeInfos[entry.RootfsVol].ExportStatus = cow.ExportStatusDone
	e.volumeInfos[entry.MemoryVol].ExportUUID = "exp-" + entry.MemoryVol
	e.volumeInfos[entry.MemoryVol].ExportStatus = cow.ExportStatusInProgress
	e.active = entry.MemoryVol
	require.NoError(t, w.runOne(context.Background(), entry.SnapshotID))
	require.Equal(t, []string{entry.MetadataVol}, e.submits)
	require.Equal(t, "prior", e.volumeInfos[entry.RootfsVol].ExportUUID)
}

func TestExportWorkerResumeFromNativeStateAfterRestart(t *testing.T) {
	w, e, entry := newTestWorker(t)
	// Previous process: rootfs DONE, memory accepted, metadata not started.
	e.volumeInfos[entry.RootfsVol].ExportUUID = "exp-" + entry.RootfsVol
	e.volumeInfos[entry.RootfsVol].ExportStatus = cow.ExportStatusDone
	e.volumeInfos[entry.MemoryVol].ExportUUID = "exp-" + entry.MemoryVol
	e.volumeInfos[entry.MemoryVol].ExportStatus = cow.ExportStatusInProgress
	e.active = entry.MemoryVol
	n, err := w.resume(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, n)
	w.mu.Lock()
	require.Equal(t, []string{entry.SnapshotID}, w.queue)
	w.mu.Unlock()
	w.start()
	require.Eventually(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		st := w.jobs[entry.SnapshotID]
		return st != nil && !st.queued && !st.active
	}, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, []string{entry.MetadataVol}, e.submits)
	// A second resume finds everything DONE and queues nothing.
	n, err = w.resume(context.Background())
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestExportWorkerResumeSkipsUnsealedAndCleanedPackages(t *testing.T) {
	w, _, entry := newTestWorker(t)
	require.NoError(t, os.WriteFile(filepath.Join(entry.SnapshotPath, exportCleanupMarkerFile), []byte("x"), 0o600))
	n, err := w.resume(context.Background())
	require.NoError(t, err)
	require.Zero(t, n)
	require.NoError(t, os.Remove(filepath.Join(entry.SnapshotPath, exportCleanupMarkerFile)))
	require.NoError(t, os.Remove(filepath.Join(entry.SnapshotPath, checkpointSealFile)))
	n, err = w.resume(context.Background())
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestExportWorkerStopsOnCleanupAndNeverResurrects(t *testing.T) {
	w, e, entry := newTestWorker(t)
	stopped := make(chan error, 1)
	e.onSubmit = func(name string) {
		if name == entry.MemoryVol {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				stopped <- StopSnapshotExport(ctx, entry.SnapshotID)
			}()
			// Give cleanup time to fence; it must wait for this native call.
			time.Sleep(20 * time.Millisecond)
		}
	}
	err := w.runOne(context.Background(), entry.SnapshotID)
	require.ErrorIs(t, err, errExportCancelled)
	require.NoError(t, <-stopped)
	require.NotContains(t, e.submits, entry.MetadataVol)
	// Re-enqueue and resume are both no-ops for a fenced package.
	w.enqueue(entry.SnapshotID)
	w.mu.Lock()
	require.Empty(t, w.queue)
	w.mu.Unlock()
	require.True(t, exportCleanupRequested(entry.SnapshotID))
	tracked, terminal, _ := w.tracked(entry.SnapshotID)
	require.False(t, tracked)
	require.False(t, terminal)
}

func TestStopSnapshotExportTimesOutWhileInNativeCall(t *testing.T) {
	w, _, entry := newTestWorker(t)
	w.mu.Lock()
	w.activeID = entry.SnapshotID
	w.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.Error(t, StopSnapshotExport(ctx, entry.SnapshotID))
	w.leave()
	require.NoError(t, StopSnapshotExport(context.Background(), entry.SnapshotID))
}

func TestUploadStatusRetryingFailedRoleStaysRunning(t *testing.T) {
	w, e, entry := newTestWorker(t)
	e.volumeInfos[entry.RootfsVol].ExportStatus = cow.ExportStatusFailed
	w.enqueue(entry.SnapshotID)
	st, err := (&S3Cow{engine: e}).UploadStatus(context.Background(), entry.SnapshotID)
	require.NoError(t, err)
	require.Equal(t, cow.RemoteStateRunning, st.State)
}

func TestUploadStatusRequeuesUntrackedSealedPackage(t *testing.T) {
	w, e, entry := newTestWorker(t)
	st, err := (&S3Cow{engine: e}).UploadStatus(context.Background(), entry.SnapshotID)
	require.NoError(t, err)
	require.Equal(t, cow.RemoteStateRunning, st.State)
	w.mu.Lock()
	require.Equal(t, []string{entry.SnapshotID}, w.queue)
	w.mu.Unlock()
}

func TestScheduleSnapshotExportDoesNotBlockOrStartLoop(t *testing.T) {
	w, e, entry := newTestWorker(t)
	pending, err := ScheduleSnapshotExport(cow.BackendS3, entry.SnapshotID)
	require.NoError(t, err)
	require.True(t, pending)
	require.Empty(t, e.submits)
	w.mu.Lock()
	require.False(t, w.started)
	w.mu.Unlock()
	pending, err = ScheduleSnapshotExport(cow.BackendXFS, entry.SnapshotID)
	require.NoError(t, err)
	require.False(t, pending)
}

func TestPublishedPackageState(t *testing.T) {
	_, _, entry := newTestWorker(t)
	catalog, sealed, err := PublishedPackageState(context.Background(), cow.BackendS3, entry.SnapshotID)
	require.NoError(t, err)
	require.True(t, catalog)
	require.True(t, sealed)
	require.NoError(t, os.Remove(filepath.Join(entry.SnapshotPath, checkpointSealFile)))
	catalog, sealed, err = PublishedPackageState(context.Background(), cow.BackendS3, entry.SnapshotID)
	require.NoError(t, err)
	require.True(t, catalog)
	require.False(t, sealed)
	catalog, sealed, err = PublishedPackageState(context.Background(), cow.BackendS3, "snap-missing")
	require.NoError(t, err)
	require.False(t, catalog)
	require.False(t, sealed)
}

// B16: an accepted export that never settles (native publication_pending left
// behind by a failed drain) is abandoned natively and resubmitted.
func stuckRole(t *testing.T) (*s3ExportWorker, *workerEngine, *SnapshotCatalogEntry) {
	t.Helper()
	w, e, entry := newTestWorker(t)
	w.cfg.OrphanAfter = 10 * time.Millisecond
	w.cfg.MaxAbandons = 2
	e.volumeInfos[entry.RootfsVol].ExportUUID = "exp-" + entry.RootfsVol
	e.volumeInfos[entry.RootfsVol].ExportStatus = cow.ExportStatusDone
	e.volumeInfos[entry.MemoryVol].ExportUUID = "orphan-" + entry.MemoryVol
	e.volumeInfos[entry.MemoryVol].ExportStatus = cow.ExportStatusInProgress
	e.active = entry.MemoryVol
	e.stuck[entry.MemoryVol] = true
	return w, e, entry
}

func TestExportWorkerAbandonsOrphanAndResubmits(t *testing.T) {
	w, e, entry := stuckRole(t)
	require.NoError(t, w.runOne(context.Background(), entry.SnapshotID))
	require.Equal(t, []string{"orphan-" + entry.MemoryVol}, e.abandoned)
	// Memory resubmitted exactly once after the abandon, then metadata.
	require.Equal(t, []string{entry.MemoryVol, entry.MetadataVol}, e.submits)
	require.Equal(t, cow.ExportStatusDone, e.volumeInfos[entry.MemoryVol].ExportStatus)
	require.Equal(t, "exp-"+entry.MemoryVol, e.volumeInfos[entry.MemoryVol].ExportUUID)
}

func TestExportWorkerDoesNotResubmitWhenAbandonRefused(t *testing.T) {
	w, e, entry := stuckRole(t)
	w.cfg.RoleSettleTimeout = 50 * time.Millisecond
	e.abandonErr = errors.New("manifest exists")
	err := w.runOne(context.Background(), entry.SnapshotID)
	require.Error(t, err)
	require.Empty(t, e.abandoned)
	// Never submitted memory again (and so never reached metadata).
	require.Empty(t, e.submits)
	require.Equal(t, "orphan-"+entry.MemoryVol, e.volumeInfos[entry.MemoryVol].ExportUUID)
	require.Equal(t, cow.ExportStatusInProgress, e.volumeInfos[entry.MemoryVol].ExportStatus)
}

func TestExportWorkerAbandonBounded(t *testing.T) {
	w, e, entry := stuckRole(t)
	w.cfg.RoleSettleTimeout = 50 * time.Millisecond
	w.cfg.MaxAbandons = 1
	// Every resubmit gets stuck again.
	e.onSubmit = func(name string) {
		if name == entry.MemoryVol {
			e.mu.Lock()
			e.stuck[name] = true
			e.mu.Unlock()
		}
	}
	require.Error(t, w.runOne(context.Background(), entry.SnapshotID))
	require.Len(t, e.abandoned, 1)
	require.Equal(t, []string{entry.MemoryVol}, e.submits)
}

func TestExportWorkerOrphanWaitRespectsCancellation(t *testing.T) {
	w, e, entry := stuckRole(t)
	w.cfg.RoleSettleTimeout = time.Minute
	e.abandonErr = errors.New("admission exists")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.runOne(ctx, entry.SnapshotID) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("runOne did not return after cancellation")
	}
	require.Empty(t, e.submits)
}
