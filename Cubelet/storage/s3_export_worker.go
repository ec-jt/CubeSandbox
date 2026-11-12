// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
	CubeLog "github.com/tencentcloud/CubeSandbox/pkgs/CubeLog"
)

// Asynchronous S3 export (B15).
//
// Pause and CommitSandbox finish once the package is captured, its catalog is
// persisted and it is sealed locally. Exporting the sealed snapshots to S3 is
// done here, off the request path, by one per-node worker:
//
//   - roles are submitted one at a time (rootfs, memory, metadata when
//     present) and each must reach export_status DONE before the next one is
//     submitted, because native registry persistence is serialized per store;
//   - a role that is already INPROGRESS or DONE is never submitted again;
//   - an admission/transport error whose acceptance is ambiguous is never
//     retried blind: the object's export_uuid/export_status is re-read first;
//   - a FAILED role (for example a flusher drain timeout) and transient errors
//     are retried with bounded backoff; only exhausting the budget is terminal;
//   - the authoritative state is native (GetVolumeInfo per object) plus two
//     small marker files in the package home, so a Cubelet restart resumes
//     from a catalog rescan instead of in-memory state;
//   - a package being cleaned up is fenced: the worker stops and never
//     resubmits it.
//
// An export failure never touches the local sandbox. It only means the package
// cannot be restored on another node.

const (
	// exportCleanupMarkerFile fences a package that cleanup has claimed. It is
	// written before any object is deleted so a restart cannot resurrect the
	// export of a half-deleted package.
	exportCleanupMarkerFile = ".export-cleanup-requested"
	// exportFailedMarkerFile records a terminal export failure so Status keeps
	// reporting failed after a restart instead of silently retrying forever.
	exportFailedMarkerFile = ".export-failed"
)

// ExportWorkerConfig bounds the worker. Zero values pick production defaults.
type ExportWorkerConfig struct {
	// RoleSettleTimeout bounds one wait for an accepted role to reach DONE.
	// Expiry does not resubmit an INPROGRESS role; it re-reads and waits again
	// until MaxAttempts is spent.
	RoleSettleTimeout time.Duration
	// PollInterval is the GetVolumeInfo poll period while a role settles.
	PollInterval time.Duration
	// MaxAttempts bounds submissions plus settle windows per role.
	MaxAttempts int
	// BackoffBase/BackoffMax bound the exponential backoff between attempts.
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

func (c ExportWorkerConfig) withDefaults() ExportWorkerConfig {
	if c.RoleSettleTimeout <= 0 {
		c.RoleSettleTimeout = 10 * time.Minute
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 8
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = 5 * time.Second
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 2 * time.Minute
	}
	return c
}

// exportJobState is the worker's in-memory view. It is a cache of native state
// and marker files, never the authority.
type exportJobState struct {
	queued    bool
	active    bool
	terminal  bool
	message   string
	cancelled bool
}

type s3ExportWorker struct {
	cfg   ExportWorkerConfig
	store func() (*S3Cow, error)
	sleep func(ctx context.Context, d time.Duration) error

	mu      sync.Mutex
	cond    *sync.Cond
	queue   []string
	jobs    map[string]*exportJobState
	started bool
	// activeID is the snapshot the worker is touching right now; cleanup waits
	// for it to leave a native call before it deletes objects.
	activeID string
}

func newS3ExportWorker(cfg ExportWorkerConfig, store func() (*S3Cow, error)) *s3ExportWorker {
	w := &s3ExportWorker{
		cfg:   cfg.withDefaults(),
		store: store,
		sleep: sleepCtx,
		jobs:  map[string]*exportJobState{},
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

var (
	exportWorkerMu sync.Mutex
	exportWorker   *s3ExportWorker
)

func defaultExportWorker() *s3ExportWorker {
	exportWorkerMu.Lock()
	defer exportWorkerMu.Unlock()
	if exportWorker == nil {
		exportWorker = newS3ExportWorker(ExportWorkerConfig{}, requireS3Cow)
	}
	return exportWorker
}

// setExportWorkerForTest swaps the singleton and returns a restore func.
func setExportWorkerForTest(w *s3ExportWorker) func() {
	exportWorkerMu.Lock()
	prev := exportWorker
	exportWorker = w
	exportWorkerMu.Unlock()
	return func() {
		exportWorkerMu.Lock()
		exportWorker = prev
		exportWorkerMu.Unlock()
	}
}

// ScheduleSnapshotExport hands a sealed S3 package to the background worker.
// It never blocks on S3. XFS is a no-op. Returns whether an export is pending.
func ScheduleSnapshotExport(backend, snapshotID string) (bool, error) {
	normalized, err := cow.NormalizeBackend(backend)
	if err != nil {
		return false, err
	}
	if normalized != cow.BackendS3 {
		return false, nil
	}
	id := strings.TrimSpace(snapshotID)
	if id == "" {
		return false, fmt.Errorf("snapshot_id is required")
	}
	defaultExportWorker().enqueue(id)
	return true, nil
}

// StopSnapshotExport fences a package before cleanup deletes it: a marker is
// persisted in the package home, the in-memory job is cancelled, and the call
// waits (bounded by ctx) until the worker is no longer inside a native call
// for this snapshot. It returns an error only when the wait is cut short; the
// caller must then report a retryable failure instead of deleting.
func StopSnapshotExport(ctx context.Context, snapshotID string) error {
	id := strings.TrimSpace(snapshotID)
	if id == "" {
		return nil
	}
	for _, home := range exportPackageHomes(id) {
		if _, err := os.Stat(home); err != nil {
			continue
		}
		_ = os.WriteFile(filepath.Join(home, exportCleanupMarkerFile), []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0o600)
	}
	return defaultExportWorker().cancel(ctx, id)
}

// ResumePendingSnapshotExports rescans local S3 catalogs after a restart and
// requeues every sealed exportable package whose roles are not all DONE.
func ResumePendingSnapshotExports(ctx context.Context) (int, error) {
	return defaultExportWorker().resume(ctx)
}

// snapshotExportTracked reports the worker's view of a package for Status.
func snapshotExportTracked(id string) (tracked bool, terminal bool, message string) {
	return defaultExportWorker().tracked(id)
}

func (w *s3ExportWorker) enqueue(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.jobs[id]
	if st == nil {
		st = &exportJobState{}
		w.jobs[id] = st
	}
	if st.cancelled || st.queued || st.active {
		return
	}
	st.queued = true
	st.terminal = false
	st.message = ""
	w.queue = append(w.queue, id)
	w.cond.Broadcast()
}

// start runs the loop once. Enqueue never starts it: the loop is started by
// the S3 init path once the handle is published, so status polls and tests
// cannot spawn a background exporter as a side effect.
func (w *s3ExportWorker) start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		return
	}
	w.started = true
	go w.loop()
}

// StartSnapshotExportWorker starts the per-node async export loop. Idempotent.
func StartSnapshotExportWorker() {
	defaultExportWorker().start()
}

func (w *s3ExportWorker) tracked(id string) (bool, bool, string) {
	w.mu.Lock()
	st := w.jobs[id]
	var tracked, terminal bool
	var msg string
	if st != nil && !st.cancelled {
		tracked = st.queued || st.active || st.terminal
		terminal = st.terminal
		msg = st.message
	}
	w.mu.Unlock()
	if !terminal {
		if failed, why := exportFailedMarker(id); failed {
			return true, true, why
		}
	}
	return tracked, terminal, msg
}

func (w *s3ExportWorker) cancel(ctx context.Context, id string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.jobs[id]
	if st == nil {
		st = &exportJobState{}
		w.jobs[id] = st
	}
	st.cancelled = true
	st.queued = false
	for i, q := range w.queue {
		if q == id {
			w.queue = append(w.queue[:i], w.queue[i+1:]...)
			break
		}
	}
	stop := context.AfterFunc(ctx, func() {
		w.mu.Lock()
		w.cond.Broadcast()
		w.mu.Unlock()
	})
	defer stop()
	for w.activeID == id {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("export of %s still in a native call: %w", id, err)
		}
		w.cond.Wait()
	}
	return nil
}

func (w *s3ExportWorker) isCancelled(id string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if st := w.jobs[id]; st != nil && st.cancelled {
		return true
	}
	return exportCleanupRequested(id)
}

func (w *s3ExportWorker) loop() {
	for {
		w.mu.Lock()
		for len(w.queue) == 0 {
			w.cond.Wait()
		}
		id := w.queue[0]
		w.queue = w.queue[1:]
		st := w.jobs[id]
		if st == nil || st.cancelled {
			w.mu.Unlock()
			continue
		}
		st.queued = false
		st.active = true
		w.mu.Unlock()

		err := w.runOne(context.Background(), id)

		w.mu.Lock()
		st.active = false
		switch {
		case err == nil:
			st.terminal = false
			st.message = ""
		case errors.Is(err, errExportCancelled):
			// Cleanup owns the package now.
		default:
			st.terminal = true
			st.message = err.Error()
		}
		w.mu.Unlock()
		if err != nil && !errors.Is(err, errExportCancelled) {
			recordExportFailed(id, err)
			CubeLog.Errorf("async s3 export %s gave up: %v", id, err)
		} else if err == nil {
			CubeLog.Infof("async s3 export %s: all roles DONE", id)
		}
	}
}

var errExportCancelled = errors.New("export cancelled by package cleanup")

// enter marks id as inside a native call; leave releases it. Cleanup waits on
// this so it never deletes an object under a concurrent export_snapshot.
func (w *s3ExportWorker) enter(id string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if st := w.jobs[id]; st != nil && st.cancelled {
		return false
	}
	if exportCleanupRequested(id) {
		return false
	}
	w.activeID = id
	return true
}

func (w *s3ExportWorker) leave() {
	w.mu.Lock()
	w.activeID = ""
	w.cond.Broadcast()
	w.mu.Unlock()
}

// runOne exports every role of one package, sequentially.
func (w *s3ExportWorker) runOne(ctx context.Context, id string) error {
	m, err := w.store()
	if err != nil {
		return fmt.Errorf("s3 store: %w", err)
	}
	refs, err := w.exportableRefs(ctx, m, id)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		if err := w.exportRole(ctx, m, id, ref); err != nil {
			return err
		}
	}
	// The checkpoint commit stays with UploadStatus (driven by the Master
	// remotestatus reconciler), which runs it once every role is DONE.
	return nil
}

func (w *s3ExportWorker) exportableRefs(ctx context.Context, m *S3Cow, id string) ([]CowObjectRef, error) {
	var out []CowObjectRef
	for _, ref := range activateObjectRefs(ctx, cow.BackendS3, id) {
		if IsS3MetadataBaseName(ref.Name) {
			continue
		}
		if ref.Role == "metadata" {
			if !w.enter(id) {
				return nil, errExportCancelled
			}
			info, infoErr := m.GetVolumeInfo(ctx, ref.Name)
			w.leave()
			exists, existsErr := cowObjectPresent(info, infoErr)
			if existsErr != nil {
				return nil, fmt.Errorf("lookup %s %s: %w", ref.Role, ref.Name, existsErr)
			}
			if !exists {
				continue
			}
		}
		out = append(out, ref)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("package %s has no exportable objects", id)
	}
	return out, nil
}

type exportRoleView struct {
	exists bool
	uuid   string
	status string
}

func (w *s3ExportWorker) readRole(ctx context.Context, m *S3Cow, id, name string) (exportRoleView, error) {
	if !w.enter(id) {
		return exportRoleView{}, errExportCancelled
	}
	info, err := m.GetVolumeInfo(ctx, name)
	w.leave()
	exists, err := cowObjectPresent(info, err)
	if err != nil {
		return exportRoleView{}, err
	}
	v := exportRoleView{exists: exists}
	if info != nil {
		v.uuid = strings.TrimSpace(info.ExportUUID)
		v.status = strings.ToUpper(strings.TrimSpace(info.ExportStatus))
	}
	return v, nil
}

// exportRole drives one role to DONE. Each loop iteration starts from a fresh
// native read, so the decision to submit is never based on a stale view.
func (w *s3ExportWorker) exportRole(ctx context.Context, m *S3Cow, id string, ref CowObjectRef) error {
	var lastErr error
	for attempt := 0; attempt < w.cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			if err := w.sleep(ctx, w.backoff(attempt)); err != nil {
				return err
			}
		}
		if w.isCancelled(id) {
			return errExportCancelled
		}
		v, err := w.readRole(ctx, m, id, ref.Name)
		if errors.Is(err, errExportCancelled) {
			return err
		}
		if err != nil {
			lastErr = fmt.Errorf("lookup %s %s: %w", ref.Role, ref.Name, err)
			continue
		}
		if !v.exists {
			return fmt.Errorf("%s %s is missing; package cannot be exported", ref.Role, ref.Name)
		}
		switch {
		case v.status == cow.ExportStatusDone && v.uuid != "":
			return nil
		case v.uuid != "" && (v.status == cow.ExportStatusInProgress || v.status == ""):
			// Accepted earlier (this run, a previous run, or before a restart).
			// Never resubmit; wait for it.
			lastErr = w.waitSettled(ctx, m, id, ref, v.uuid)
			if lastErr == nil {
				return nil
			}
			if errors.Is(lastErr, errExportCancelled) {
				return lastErr
			}
			continue
		case v.status == cow.ExportStatusNone || (v.status == "" && v.uuid == "") || cow.ExportStatusIsFailed(v.status):
			// Not exported, or a native failure (drain timeout and the like).
		default:
			lastErr = fmt.Errorf("%s %s has unknown export status %q", ref.Role, ref.Name, v.status)
			continue
		}
		if !w.enter(id) {
			return errExportCancelled
		}
		uuid, subErr := m.uploadOne(ref.Name)
		w.leave()
		if subErr != nil {
			// Acceptance may be ambiguous (timeout after admission). Do not
			// resubmit blind: the next iteration re-reads export_uuid/status
			// and only submits again if the object shows no accepted export.
			lastErr = fmt.Errorf("submit %s %s: %w", ref.Role, ref.Name, subErr)
			CubeLog.Warnf("async s3 export %s: %v (attempt %d/%d)", id, lastErr, attempt+1, w.cfg.MaxAttempts)
			continue
		}
		lastErr = w.waitSettled(ctx, m, id, ref, uuid)
		if lastErr == nil {
			return nil
		}
		if errors.Is(lastErr, errExportCancelled) {
			return lastErr
		}
		CubeLog.Warnf("async s3 export %s: %v (attempt %d/%d)", id, lastErr, attempt+1, w.cfg.MaxAttempts)
	}
	if lastErr == nil {
		lastErr = errors.New("attempt budget exhausted")
	}
	return fmt.Errorf("export %s %s: %w", ref.Role, ref.Name, lastErr)
}

// waitSettled polls until the exact accepted export is DONE. A changed or
// missing identity and a FAILED status end the wait so the caller re-reads.
func (w *s3ExportWorker) waitSettled(ctx context.Context, m *S3Cow, id string, ref CowObjectRef, uuid string) error {
	wctx, cancel := context.WithTimeout(ctx, w.cfg.RoleSettleTimeout)
	defer cancel()
	for {
		if w.isCancelled(id) {
			return errExportCancelled
		}
		v, err := w.readRole(wctx, m, id, ref.Name)
		if errors.Is(err, errExportCancelled) {
			return err
		}
		if err == nil {
			if !v.exists {
				return fmt.Errorf("%s %s disappeared while exporting", ref.Role, ref.Name)
			}
			if v.uuid != "" && v.uuid != uuid {
				return fmt.Errorf("accepted export %s identity changed to %s for %s", uuid, v.uuid, ref.Name)
			}
			switch {
			case v.status == cow.ExportStatusDone && v.uuid == uuid:
				return nil
			case cow.ExportStatusIsFailed(v.status):
				return fmt.Errorf("accepted export %s of %s ended %s", uuid, ref.Name, v.status)
			}
		}
		if err := w.sleep(wctx, w.cfg.PollInterval); err != nil {
			return fmt.Errorf("settle %s %s: %w", ref.Role, ref.Name, err)
		}
	}
}

func (w *s3ExportWorker) backoff(attempt int) time.Duration {
	d := w.cfg.BackoffBase
	for i := 1; i < attempt && d < w.cfg.BackoffMax; i++ {
		d *= 2
	}
	if d > w.cfg.BackoffMax {
		d = w.cfg.BackoffMax
	}
	return d
}

// resume requeues sealed exportable packages whose roles are not all DONE.
func (w *s3ExportWorker) resume(ctx context.Context) (int, error) {
	entries, err := ListLocalSnapshotsFor(ctx, cow.BackendS3)
	if err != nil {
		return 0, err
	}
	m, err := w.store()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range entries {
		if e == nil || !exportableCatalogKind(e.Kind) {
			continue
		}
		id := e.SnapshotID
		if exportCleanupRequested(id) {
			continue
		}
		if failed, _ := exportFailedMarker(id); failed {
			continue
		}
		if sealed, _ := snapshotPackageSealedForExport(id); !sealed {
			continue
		}
		done, err := w.allRolesDone(ctx, m, id)
		if err != nil {
			CubeLog.Warnf("async s3 export resume %s: %v; requeueing", id, err)
		}
		if done && err == nil {
			continue
		}
		w.enqueue(id)
		n++
	}
	return n, nil
}

func (w *s3ExportWorker) allRolesDone(ctx context.Context, m *S3Cow, id string) (bool, error) {
	refs, err := w.exportableRefs(ctx, m, id)
	if err != nil {
		return false, err
	}
	for _, ref := range refs {
		v, err := w.readRole(ctx, m, id, ref.Name)
		if err != nil {
			return false, err
		}
		if !(v.status == cow.ExportStatusDone && v.uuid != "") {
			return false, nil
		}
	}
	return true, nil
}

// exportableCatalogKind: templates (AppSnapshot) never export. Legacy empty
// kind is a runtime snapshot.
func exportableCatalogKind(kind string) bool {
	switch strings.TrimSpace(kind) {
	case CatalogKindPauseSnapshot, CatalogKindRuntimeSnapshot, "":
		return true
	default:
		return false
	}
}

// snapshotPackageSealedForExport is true only when local sealing provably
// finished: the finalization record exists and, for packages with a native
// capture binding, the immutable seal receipt was written.
func snapshotPackageSealedForExport(id string) (bool, error) {
	entry, prepared, err := readS3FinalizeCatalog(id)
	if err != nil {
		return false, err
	}
	if !prepared {
		return false, nil
	}
	if entry.DiskBinding != nil && entry.DiskBinding.NativeRootfsUUID != "" {
		if _, err := os.Stat(filepath.Join(entry.SnapshotPath, checkpointSealFile)); err != nil {
			return false, nil
		}
	}
	return true, nil
}

func exportPackageHomes(id string) []string {
	homes := []string{
		SnapshotHome(cow.BackendS3, SnapshotKindNormal, id),
		SnapshotHome(cow.BackendS3, SnapshotKindPause, id),
	}
	snapshotCatalogMu.RLock()
	if ns := snapshotCatalogs[cow.BackendS3]; ns != nil {
		if cached := ns.index[id]; cached != nil && cached.SnapshotPath != "" {
			homes = append(homes, cached.SnapshotPath)
		}
	}
	snapshotCatalogMu.RUnlock()
	seen := map[string]bool{}
	out := homes[:0]
	for _, h := range homes {
		if h == "" || seen[filepath.Clean(h)] {
			continue
		}
		seen[filepath.Clean(h)] = true
		out = append(out, h)
	}
	return out
}

func exportCleanupRequested(id string) bool {
	for _, home := range exportPackageHomes(id) {
		if _, err := os.Stat(filepath.Join(home, exportCleanupMarkerFile)); err == nil {
			return true
		}
	}
	return false
}

func exportFailedMarker(id string) (bool, string) {
	for _, home := range exportPackageHomes(id) {
		body, err := os.ReadFile(filepath.Join(home, exportFailedMarkerFile))
		if err == nil {
			return true, strings.TrimSpace(string(body))
		}
	}
	return false, ""
}

func recordExportFailed(id string, cause error) {
	for _, home := range exportPackageHomes(id) {
		if _, err := os.Stat(home); err != nil {
			continue
		}
		_ = os.WriteFile(filepath.Join(home, exportFailedMarkerFile), []byte(cause.Error()), 0o600)
		return
	}
}

// PublishedPackageState reports what a held publication left on disk, for the
// operator reconcile path. catalog: the package catalog was persisted. sealed:
// local sealing provably finished (S3: finalization record plus, when the
// capture is natively bound, the immutable seal receipt; XFS has no sealing
// step, so a persisted catalog is sealed). Lookup errors are returned so the
// caller can fail closed.
func PublishedPackageState(ctx context.Context, backend, snapshotID string) (catalog bool, sealed bool, err error) {
	normalized, err := cow.NormalizeBackend(backend)
	if err != nil {
		return false, false, err
	}
	id := strings.TrimSpace(snapshotID)
	if id == "" {
		return false, false, fmt.Errorf("snapshot_id is required")
	}
	if normalized != cow.BackendS3 {
		entry, gerr := GetLocalSnapshotFor(ctx, normalized, id)
		if errors.Is(gerr, ErrSnapshotCatalogNotFound) {
			return false, false, nil
		}
		if gerr != nil {
			return false, false, gerr
		}
		return entry != nil, entry != nil, nil
	}
	_, _, rerr := readS3FinalizeCatalog(id)
	if errors.Is(rerr, ErrSnapshotCatalogNotFound) {
		return false, false, nil
	}
	if rerr != nil {
		return false, false, rerr
	}
	ok, serr := snapshotPackageSealedForExport(id)
	if serr != nil {
		return true, false, serr
	}
	return true, ok, nil
}
