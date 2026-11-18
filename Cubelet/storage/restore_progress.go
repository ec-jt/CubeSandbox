// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Restore progress of cross-node restores, for the user-facing progress bar.
//
// PrefetchAll imports rootfs, memory and metadata in parallel and waits until
// all of them are local. That wait is where a cross-node resume spends its
// time (tens of seconds up to import_restore_deadline), and until now the
// caller saw nothing until the whole Create returned. The tracker records,
// per sandbox, the chunk counts rcow_import_status reports for every disk,
// the phase, and when progress last moved. Cubelet serves it read-only at
// RestoreProgressPath; Master forwards it as
// GET /internal/sandbox/{id}/restore-progress.

// Restore phases.
const (
	RestorePhaseImporting   = "importing"
	RestorePhaseRestoringVM = "restoring_vm"
	RestorePhaseDone        = "done"
	RestorePhaseFailed      = "failed"
)

// RestoreProgressPath is the Cubelet HTTP path; ?sandbox_id=<id>.
const RestoreProgressPath = "/v1/storage/restore-progress"

// restoreProgressRetention is how long a finished restore stays queryable,
// so a poller that misses the last transition still sees done or failed.
var restoreProgressRetention = 10 * time.Minute

// restoreProgressNow is swapped in tests.
var restoreProgressNow = time.Now

// DiskProgress is one disk of a restore.
type DiskProgress struct {
	Name          string `json:"name"`
	Role          string `json:"role"`
	State         string `json:"state"`
	LocalChunks   uint64 `json:"local_chunks"`
	PresentChunks uint64 `json:"present_chunks"`
	Done          bool   `json:"done"`
}

// RestoreProgress is the reply of RestoreProgressPath.
type RestoreProgress struct {
	SandboxID string `json:"sandbox_id"`
	Phase     string `json:"phase"`
	// LocalChunks and TotalChunks are summed over every disk. TotalChunks is
	// 0 until the first import status of every disk is known.
	LocalChunks    uint64         `json:"local_chunks"`
	TotalChunks    uint64         `json:"total_chunks"`
	Disks          []DiskProgress `json:"disks"`
	StartedAt      time.Time      `json:"started_at"`
	LastProgressAt time.Time      `json:"last_progress_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	// StalledSeconds is how long progress has not changed, as of UpdatedAt.
	StalledSeconds float64 `json:"stalled_seconds"`
	Error          string  `json:"error,omitempty"`
}

type restoreEntry struct {
	sandboxID    string
	phase        string
	expected     int
	disks        map[string]*DiskProgress
	started      time.Time
	lastProgress time.Time
	finished     time.Time
	err          string
}

type restoreTracker struct {
	mu      sync.Mutex
	entries map[string]*restoreEntry
}

var restoreProgress = &restoreTracker{entries: map[string]*restoreEntry{}}

// begin starts (or restarts) tracking a restore that owes the given disks.
func (t *restoreTracker) begin(sandboxID string, jobs []importJob) {
	if t == nil || sandboxID == "" {
		return
	}
	now := restoreProgressNow()
	e := &restoreEntry{
		sandboxID:    sandboxID,
		phase:        RestorePhaseImporting,
		expected:     len(jobs),
		disks:        map[string]*DiskProgress{},
		started:      now,
		lastProgress: now,
	}
	for _, j := range jobs {
		e.disks[j.name] = &DiskProgress{Name: j.name, Role: j.role, State: "pending"}
	}
	t.mu.Lock()
	t.gcLocked(now)
	t.entries[sandboxID] = e
	t.mu.Unlock()
}

// update records one rcow_import_status reading. Progress "moves" when any
// disk's local chunk count, total or done flag changes.
func (t *restoreTracker) update(sandboxID, name, role string, st *importStatus) {
	if t == nil || sandboxID == "" || st == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entries[sandboxID]
	if e == nil || !e.finished.IsZero() {
		return
	}
	d := e.disks[name]
	if d == nil {
		d = &DiskProgress{Name: name, Role: role}
		e.disks[name] = d
	}
	moved := d.LocalChunks != st.LocalChunks || d.PresentChunks != st.PresentChunks || d.Done != st.Done
	d.State = st.State
	d.LocalChunks = st.LocalChunks
	d.PresentChunks = st.PresentChunks
	d.Done = st.Done
	if st.Done && d.PresentChunks > 0 && d.LocalChunks < d.PresentChunks {
		// done means no read goes to the export any more.
		d.LocalChunks = d.PresentChunks
	}
	if moved {
		e.lastProgress = restoreProgressNow()
	}
}

// setPhase moves a restore to the next phase. A finished restore keeps its
// final phase.
func (t *restoreTracker) setPhase(sandboxID, phase string, err error) {
	if t == nil || sandboxID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entries[sandboxID]
	if e == nil || !e.finished.IsZero() {
		return
	}
	now := restoreProgressNow()
	if e.phase != phase {
		e.lastProgress = now
	}
	e.phase = phase
	if err != nil {
		e.err = err.Error()
	}
	if phase == RestorePhaseDone || phase == RestorePhaseFailed {
		e.finished = now
	}
}

func (t *restoreTracker) get(sandboxID string) (*RestoreProgress, bool) {
	if t == nil {
		return nil, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := restoreProgressNow()
	t.gcLocked(now)
	e := t.entries[sandboxID]
	if e == nil {
		return nil, false
	}
	p := &RestoreProgress{
		SandboxID:      e.sandboxID,
		Phase:          e.phase,
		StartedAt:      e.started,
		LastProgressAt: e.lastProgress,
		UpdatedAt:      now,
		Error:          e.err,
	}
	known := 0
	for _, d := range e.disks {
		p.Disks = append(p.Disks, *d)
		p.LocalChunks += d.LocalChunks
		if d.PresentChunks > 0 || d.Done {
			known++
		}
		p.TotalChunks += d.PresentChunks
	}
	if known < e.expected {
		// A disk whose size is not known yet would make the bar jump back.
		p.TotalChunks = 0
	}
	if p.TotalChunks > 0 && p.LocalChunks > p.TotalChunks {
		p.LocalChunks = p.TotalChunks
	}
	sort.Slice(p.Disks, func(i, j int) bool { return p.Disks[i].Role < p.Disks[j].Role })
	end := now
	if !e.finished.IsZero() {
		end = e.finished
	}
	if s := end.Sub(e.lastProgress).Seconds(); s > 0 {
		p.StalledSeconds = s
	}
	return p, true
}

func (t *restoreTracker) gcLocked(now time.Time) {
	for id, e := range t.entries {
		if !e.finished.IsZero() && now.Sub(e.finished) > restoreProgressRetention {
			delete(t.entries, id)
		}
	}
}

// GetRestoreProgress returns the progress of a cross-node restore of
// sandboxID on this node, if one is running or finished recently.
func GetRestoreProgress(sandboxID string) (*RestoreProgress, bool) {
	return restoreProgress.get(strings.TrimSpace(sandboxID))
}

// MarkRestorePhase records that the restore of sandboxID reached phase
// (RestorePhaseRestoringVM after the import, RestorePhaseDone or
// RestorePhaseFailed when Create returns). err is kept for failed.
func MarkRestorePhase(sandboxID, phase string, err error) {
	restoreProgress.setPhase(strings.TrimSpace(sandboxID), phase, err)
}

type restoreProgressKey struct{}

type restoreProgressReporter struct {
	sandboxID string
}

func withRestoreProgress(ctx context.Context, sandboxID string) context.Context {
	return context.WithValue(ctx, restoreProgressKey{}, &restoreProgressReporter{sandboxID: sandboxID})
}

// reportImportProgress is called by waitImportPrefetch with every reading.
func reportImportProgress(ctx context.Context, name, role string, st *importStatus) {
	if ctx == nil {
		return
	}
	r, _ := ctx.Value(restoreProgressKey{}).(*restoreProgressReporter)
	if r == nil {
		return
	}
	restoreProgress.update(r.sandboxID, name, role, st)
}

// RestoreProgressHTTPHandlers serves RestoreProgressPath. Callers outside
// loopback and private networks are refused: Cubelet HTTP binds every
// interface, and the reply names sandboxes and disks.
func RestoreProgressHTTPHandlers() map[string]http.Handler {
	return map[string]http.Handler{RestoreProgressPath: http.HandlerFunc(serveRestoreProgress)}
}

func serveRestoreProgress(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !isInternalHTTPClient(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	id := strings.TrimSpace(r.URL.Query().Get("sandbox_id"))
	if id == "" {
		http.Error(w, "sandbox_id is required", http.StatusBadRequest)
		return
	}
	p, ok := GetRestoreProgress(id)
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "no restore in progress", "sandbox_id": id})
		return
	}
	_ = json.NewEncoder(w).Encode(p)
}

func isInternalHTTPClient(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
}
