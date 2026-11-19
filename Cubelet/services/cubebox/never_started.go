// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

// B18: a Create that fails after it persisted the sandbox record (and, for a
// cross-node restore, after it imported the sandbox disks) but before any
// runtime was spawned used to leave the record and the imported volumes
// behind with no way to remove them: Create rejected the id with "sandbox
// already exists" and Destroy/CleanUp refused with "runtime ownership
// unknown" because a record with no pids carries no proof of shutdown.
//
// This file adds positive evidence that such a record never started, and the
// rollback that uses it. Every check fails closed: a query error or any trace
// of a runtime keeps the previous preserve-and-refuse behaviour.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/containerd/api/services/tasks/v1"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"github.com/containerd/errdefs/pkg/errgrpc"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/log"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
	CubeLog "github.com/tencentcloud/CubeSandbox/pkgs/CubeLog"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

// runtimeProbe answers whether any runtime artifact exists for one id. An
// error means "cannot tell" and is always treated as "may exist".
type runtimeProbe interface {
	ContainerExists(ctx context.Context, id string) (bool, error)
	TaskExists(ctx context.Context, id string) (bool, error)
	ShimExists(ctx context.Context, id string) (bool, error)
	// ProcessExists reports a live host process whose command line names
	// the id (shim "-id <id>", VMM paths under the sandbox id).
	ProcessExists(ctx context.Context, id string) (bool, error)
}

// shimStateRoots are the host directories where a shim keeps its bundle
// (shim.pid, vmm.pid, sockets). A bundle directory for the id means a shim
// was spawned and may still own the guest disks.
var shimStateRoots = []string{
	"/data/cubelet/state/io.containerd.runtime.v2.task",
	"/data/cubelet/state/io.containerd.runtime.v2/task",
}

type containerdRuntimeProbe struct{ l *local }

func (l *local) runtimeEvidenceProbe() runtimeProbe {
	if l.probe != nil {
		return l.probe
	}
	return &containerdRuntimeProbe{l: l}
}

func (p *containerdRuntimeProbe) ContainerExists(ctx context.Context, id string) (bool, error) {
	if p.l == nil || p.l.client == nil {
		return false, fmt.Errorf("containerd client unavailable")
	}
	_, err := p.l.client.ContainerService().Get(ctx, id)
	return existsFromErr(err)
}

func (p *containerdRuntimeProbe) TaskExists(ctx context.Context, id string) (bool, error) {
	if p.l == nil || p.l.localTask == nil {
		return false, fmt.Errorf("containerd task service unavailable")
	}
	_, err := p.l.localTask.Get(ctx, &tasks.GetRequest{ContainerID: id})
	return existsFromErr(errgrpc.ToNative(err))
}

func (p *containerdRuntimeProbe) ShimExists(ctx context.Context, id string) (bool, error) {
	if p.l == nil || p.l.shims == nil {
		return false, fmt.Errorf("shim manager unavailable")
	}
	if exists, err := existsFromErr(func() error { _, e := p.l.shims.Get(ctx, id); return e }()); err != nil || exists {
		return exists, err
	}
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return false, err
	}
	for _, root := range shimStateRoots {
		if _, err := os.Lstat(filepath.Join(root, ns, id)); err == nil {
			return true, nil
		} else if !os.IsNotExist(err) {
			return false, err
		}
	}
	return false, nil
}

// procRoot is a variable so tests can point the process scan at a fake tree.
var procRoot = "/proc"

func (p *containerdRuntimeProbe) ProcessExists(_ context.Context, id string) (bool, error) {
	return hostProcessMentions(procRoot, id)
}

// hostProcessMentions scans procRoot/<pid>/cmdline for id. A process that
// exits during the scan is skipped; any other read failure of the directory
// itself is an error (cannot tell).
func hostProcessMentions(root, id string) (bool, error) {
	if strings.TrimSpace(id) == "" {
		return false, fmt.Errorf("empty id")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return false, err
	}
	self := fmt.Sprint(os.Getpid())
	for _, e := range entries {
		name := e.Name()
		if name == self || name == "" || name[0] < '0' || name[0] > '9' {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, name, "cmdline"))
		if err != nil {
			continue
		}
		if strings.Contains(string(raw), id) {
			return true, nil
		}
	}
	return false, nil
}

func existsFromErr(err error) (bool, error) {
	if err == nil {
		return true, nil
	}
	if errdefs.IsNotFound(err) {
		return false, nil
	}
	return false, err
}

// provablyNeverStarted reports whether a persisted record provably has no
// runtime: it was never RUNNING or PAUSED, it records no pid, and containerd
// holds no container, task or shim for the sandbox or any of its containers.
// The reason explains the verdict for logs.
func (l *local) provablyNeverStarted(ctx context.Context, sb *cubeboxstore.CubeBox) (bool, string) {
	if sb == nil {
		return false, "no record"
	}
	if sb.GetPublicationHold() != nil {
		return false, "snapshot publication hold"
	}
	if sb.GetStatus() == nil {
		return false, "record has no status"
	}
	if e := workflow.LifecycleCleanupFrom(ctx); e != nil && e.RuntimeMayExist && !e.RuntimeGone {
		return false, "a runtime start was attempted in this process and is unresolved"
	}
	statuses := []*cubeboxstore.StatusStorage{sb.GetStatus()}
	for _, ctr := range sb.AllContainers() {
		if ctr != nil {
			statuses = append(statuses, ctr.Status)
		}
	}
	for _, s := range statuses {
		if s == nil {
			continue
		}
		st := s.Get()
		if st.StartedAt != 0 || st.PausedAt != 0 || st.PausingAt != 0 {
			return false, fmt.Sprintf("record was started or paused (state %s)", st.State())
		}
		if st.Pid != 0 {
			return false, fmt.Sprintf("record carries pid %d", st.Pid)
		}
	}
	if sb.Endpoint.Pid != 0 {
		return false, fmt.Sprintf("record carries endpoint pid %d", sb.Endpoint.Pid)
	}
	if reason, ok := l.noRuntimeArtifacts(ctx, sb, false, true); !ok {
		return false, reason
	}
	return true, "pid 0, never started, no containerd container, task or shim"
}

// noRuntimeArtifacts asks containerd and the host for any trace of a runtime
// for the sandbox and its containers. withProcess adds the host process scan.
// withContainer includes containerd container metadata; an exited record
// keeps that metadata until Destroy deletes it, and metadata alone (no task,
// no shim, no process) is not a runtime. It returns ok=false with the reason
// on any trace or query error.
func (l *local) noRuntimeArtifacts(ctx context.Context, sb *cubeboxstore.CubeBox, withProcess, withContainer bool) (string, bool) {
	ns := sb.Namespace
	if ns == "" {
		ns = namespaces.Default
	}
	ctx = namespaces.WithNamespace(ctx, ns)
	probe := l.runtimeEvidenceProbe()
	type check struct {
		what string
		fn   func(context.Context, string) (bool, error)
	}
	var checks []check
	if withContainer {
		checks = append(checks, check{"containerd container", probe.ContainerExists})
	}
	checks = append(checks,
		check{"containerd task", probe.TaskExists},
		check{"shim", probe.ShimExists},
	)
	if withProcess {
		checks = append(checks, check{"host process", probe.ProcessExists})
	}
	for _, id := range sandboxShimLookupIDs(sb) {
		for _, c := range checks {
			exists, err := c.fn(ctx, id)
			if err != nil {
				return fmt.Sprintf("cannot query %s for %s: %v", c.what, id, err), false
			}
			if exists {
				return fmt.Sprintf("%s exists for %s", c.what, id), false
			}
		}
	}
	return "", true
}

// provablyExited reports whether a persisted record that did run is now
// provably without a runtime. This covers a record recovered after a Cubelet
// restart: its status says EXITED but it carries no pid (pids are not
// persisted), so Destroy had no pid to wait on and refused forever with
// "runtime ownership unknown". It requires, failing closed on any doubt:
//   - no snapshot publication hold, not paused or pausing;
//   - the sandbox status is EXITED (FinishedAt set) and no status carries a pid;
//   - no runtime start attempted in this process that is unresolved;
//   - containerd has no task or shim and no shim bundle directory exists for
//     the sandbox or any of its containers (container metadata may remain:
//     Destroy deletes it);
//   - no live host process names the sandbox or container ids.
func (l *local) provablyExited(ctx context.Context, sb *cubeboxstore.CubeBox) (bool, string) {
	if sb == nil {
		return false, "no record"
	}
	if sb.GetPublicationHold() != nil {
		return false, "snapshot publication hold"
	}
	if sb.GetStatus() == nil {
		return false, "record has no status"
	}
	// A start attempted earlier in this process leaves RuntimeMayExist set
	// on every later lifecycle of the id, even after the attempt failed and
	// failover marked the record exited. That flag alone is not a runtime:
	// the caller holds the lifecycle lock (no start can be in flight) and the
	// checks below include the host process scan, which sees any shim or VMM
	// the attempt spawned. So it does not block the exited proof.
	main := sb.GetStatus().Get()
	if main.PausedAt != 0 || main.PausingAt != 0 {
		return false, fmt.Sprintf("record is paused (state %s)", main.State())
	}
	if main.FinishedAt == 0 {
		return false, fmt.Sprintf("record has not exited (state %s)", main.State())
	}
	statuses := []*cubeboxstore.StatusStorage{sb.GetStatus()}
	for _, ctr := range sb.AllContainers() {
		if ctr != nil {
			statuses = append(statuses, ctr.Status)
		}
	}
	for _, s := range statuses {
		if s == nil {
			continue
		}
		if pid := s.Get().Pid; pid != 0 {
			return false, fmt.Sprintf("record carries pid %d", pid)
		}
	}
	if sb.Endpoint.Pid != 0 {
		return false, fmt.Sprintf("record carries endpoint pid %d", sb.Endpoint.Pid)
	}
	if reason, ok := l.noRuntimeArtifacts(ctx, sb, true, false); !ok {
		return false, reason
	}
	return true, "exited, pid 0, no containerd task, shim or host process"
}

// recordRollbackReleasesStorage applies the Create rollback policy to a
// persisted record: storage of a restore that may reuse retained disks
// (same-node pause resume, cold recreate) stays preserved.
func recordRollbackReleasesStorage(sb *cubeboxstore.CubeBox) bool {
	if sb == nil {
		return false
	}
	req := &cubebox.RunCubeSandboxRequest{
		Annotations:  sb.Annotations,
		InstanceType: sb.InstanceType,
	}
	return (&workflow.CreateContext{ReqInfo: req}).RollbackReleasesStorage()
}

// requireRuntimeCleanupEvidenceFor is requireRuntimeCleanupEvidence for a
// record that is still present. Besides in-process lifecycle evidence it
// accepts proof that the record never started, which is the only evidence a
// failed Create from an earlier process or attempt can leave behind.
func (l *local) requireRuntimeCleanupEvidenceFor(ctx context.Context, sb *cubeboxstore.CubeBox) error {
	if workflow.RuntimeCleanupProven(ctx) || sb == nil {
		return requireRuntimeCleanupEvidence(ctx, sandboxIDOf(sb))
	}
	id := sb.ID
	ok, reason := l.provablyNeverStarted(ctx, sb)
	if !ok {
		if reaped, err := l.reapOrphanShims(ctx, sb); err != nil {
			log.G(ctx).Warnf("sandbox %s: %v", id, err)
			return requireRuntimeCleanupEvidence(ctx, id)
		} else if len(reaped) > 0 {
			log.G(ctx).Warnf("sandbox %s: orphan shim pids %v reaped", id, reaped)
		}
		// A record that ran and exited, recovered without pids after a
		// restart, can still prove its runtime is gone. Storage policy is
		// the ordinary Destroy policy: PreserveStorage is left as it is.
		if exited, why := l.provablyExited(ctx, sb); exited {
			if evidence := workflow.LifecycleCleanupFrom(ctx); evidence != nil {
				log.G(ctx).Warnf("sandbox %s: %s; runtime absence proven, releasing record", id, why)
				workflow.MarkRuntimeGone(ctx)
				return nil
			}
		} else {
			reason = reason + "; " + why
		}
		log.G(ctx).Warnf("sandbox %s: runtime absence not provable: %s", id, reason)
		return requireRuntimeCleanupEvidence(ctx, id)
	}
	if !recordRollbackReleasesStorage(sb) {
		log.G(ctx).Warnf("sandbox %s: record never started (%s) but its restore may reuse retained disks; preserve storage", id, reason)
		return requireRuntimeCleanupEvidence(ctx, id)
	}
	evidence := workflow.LifecycleCleanupFrom(ctx)
	if evidence == nil {
		return requireRuntimeCleanupEvidence(ctx, id)
	}
	log.G(ctx).Warnf("sandbox %s: record never started (%s); runtime absence proven, releasing record and storage", id, reason)
	workflow.MarkRuntimeGone(ctx)
	evidence.PreserveStorage = false
	return nil
}

// verifyStaleCleanup backs workflow.StaleCleanupVerifier: a queued cleanup
// whose evidence epoch is stale may proceed only for a record that provably
// never started and whose storage the failed attempt owns. A missing record
// is not proof of anything and keeps the refusal.
func (l *local) verifyStaleCleanup(ctx context.Context, id string) (bool, string) {
	if l == nil || l.cubeboxManger == nil {
		return false, ""
	}
	sb, err := l.cubeboxManger.Get(ctx, id)
	if (err == nil && sb == nil) || errors.Is(err, utils.ErrorKeyNotFound) || errors.Is(err, utils.ErrorBucketNotFound) {
		// A missing record alone proves nothing. It does when a cleanup in
		// this process already proved the runtime gone, removed the record
		// and then failed in a later step (storage, network): the queued
		// jobs must be able to finish those steps instead of retrying
		// forever. Any start since then clears the proof.
		if workflow.RuntimeGoneInThisProcess(id) {
			return true, "record removed after runtime absence was proven in this process"
		}
		// After a restart that in-process proof is gone. The host can
		// still prove it: no containerd container, task or shim and no
		// live process naming the id. A paused sandbox keeps its record
		// (tombstone), so a missing record is never a paused one.
		ns, ok := namespaces.Namespace(ctx)
		if !ok || ns == "" {
			ns = namespaces.Default
		}
		if why, clean := l.noRuntimeArtifactsForID(ctx, ns, id); clean {
			return true, "record removed and no containerd container, task, shim or host process remains"
		} else {
			log.G(ctx).Warnf("sandbox %s: stale cleanup for a removed record kept: %s", id, why)
		}
		return false, ""
	}
	if err != nil {
		return false, ""
	}
	ok, reason := l.provablyNeverStarted(ctx, sb)
	if ok && recordRollbackReleasesStorage(sb) {
		return true, "record provably never started: " + reason
	}
	// A record that ran and exited before this process started has only
	// stale queued cleanups; without this they retry forever. Reap an
	// orphaned shim first (it is unreachable through containerd), then
	// require the full exited proof. Paused records and publication holds
	// fail inside provablyExited, so their storage is never released here.
	// The engine releases storage on this path. A record created from a
	// template owns its clones, exactly as an explicit DELETE would release
	// them. A pause resume or cold recreate record points at retained disks
	// of a paused sandbox; those keep the refusal.
	if !exitedRecordOwnsStorage(sb) {
		return false, ""
	}
	if _, err := l.reapOrphanShims(ctx, sb); err != nil {
		log.G(ctx).Warnf("sandbox %s: %v", id, err)
		return false, ""
	}
	if exited, why := l.provablyExited(ctx, sb); exited {
		return true, "record provably exited: " + why
	}
	return false, ""
}

func sandboxIDOf(sb *cubeboxstore.CubeBox) string {
	if sb == nil {
		return ""
	}
	return sb.ID
}

// precheckExistingSandbox runs Create's pre-check against a persisted record
// under the lifecycle lock. A PAUSED record is replaced later by the cubebox
// step. Any other record is rejected unless it provably never started, in
// which case it is rolled back here and Create proceeds.
func (s *service) precheckExistingSandbox(ctx context.Context, sid string) (bool, string) {
	existing, err := s.cubeboxMgr.cubeboxManger.Get(ctx, sid)
	if err != nil {
		if errors.Is(err, utils.ErrorKeyNotFound) || errors.Is(err, utils.ErrorBucketNotFound) {
			return true, ""
		}
		return false, err.Error()
	}
	if existing == nil {
		return true, ""
	}
	if existing.GetStatus() != nil && existing.GetStatus().Get().State() == cubebox.ContainerState_CONTAINER_PAUSED {
		return true, ""
	}
	cleaned, cleanErr := s.cleanStaleNeverStartedRecord(ctx, sid, existing)
	if cleaned {
		return true, ""
	}
	if cleanErr != nil {
		return false, fmt.Sprintf("sandbox already exists: stale record cleanup failed: %v", cleanErr)
	}
	return false, "sandbox already exists"
}

// cleanStaleNeverStartedRecord rolls back a record left by an earlier failed
// Create, through the same destroy workflow failover uses. It returns false
// without side effects unless the record provably never started and its
// storage is owned by that attempt.
func (s *service) cleanStaleNeverStartedRecord(ctx context.Context, sid string, sb *cubeboxstore.CubeBox) (bool, error) {
	if err := workflow.CheckCreateRuntimeOwnership(sid); err != nil {
		return false, nil
	}
	ok, reason := s.cubeboxMgr.provablyNeverStarted(ctx, sb)
	if !ok {
		log.G(ctx).Infof("Create %s: existing record kept: %s", sid, reason)
		return false, nil
	}
	if !recordRollbackReleasesStorage(sb) {
		log.G(ctx).Warnf("Create %s: existing record never started (%s) but its restore may reuse retained disks; keep it", sid, reason)
		return false, nil
	}
	log.G(ctx).Warnf("Create %s: removing stale never-started record (%s) and its storage before proceeding", sid, reason)
	cctx := workflow.BeginSandboxLifecycle(ctx, sid)
	evidence := workflow.LifecycleCleanupFrom(cctx)
	evidence.NeverStarted = false
	evidence.RuntimeMayExist = false
	evidence.RuntimeGone = true
	evidence.PreserveStorage = false
	if err := s.destroyStaleRecord(cctx, sid, sb); err != nil {
		return false, err
	}
	if isCrossNodeRestore(sb.Annotations) {
		if err := releaseCrossNodeImports(cctx, sid); err != nil {
			log.G(ctx).Warnf("Create %s: release stale cross-node imports: %v", sid, err)
		}
	}
	log.G(ctx).Warnf("Create %s: stale never-started record removed", sid)
	return true, nil
}

func (s *service) destroyStaleRecord(ctx context.Context, sid string, sb *cubeboxstore.CubeBox) error {
	ns := sb.Namespace
	if ns == "" {
		ns = namespaces.Default
	}
	ctx = namespaces.WithNamespace(ctx, ns)
	runtimeType := ""
	if s.cubeboxMgr.config != nil {
		runtimeType = s.cubeboxMgr.config.DefaultRuntimeName
	}
	if sb.OciRuntime != nil {
		runtimeType = sb.OciRuntime.Type
	}
	ctx = constants.WithRuntimeType(ctx, runtimeType)
	ctx = constants.WithFailoverOperation(ctx)
	ctx = CubeLog.WithRequestTrace(ctx, &CubeLog.RequestTrace{
		Action:       "Destroy",
		CalleeAction: "StaleCreateCleanup",
		Caller:       constants.CubeboxServiceID.ID(),
		Callee:       s.engine.ID(),
		InstanceID:   sid,
	})
	timeout := 60 * time.Second
	if s.config != nil && s.config.destroyDeadline > 0 {
		timeout = s.config.destroyDeadline
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	opts := &workflow.DestroyContext{
		BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: sid, IsRollBack: true},
		DestroyInfo:      &cubebox.DestroyCubeSandboxRequest{SandboxID: sid},
	}
	ctx = context.WithValue(ctx, workflow.KDestroyContext, opts)
	if err := s.engine.Destroy(ctx, opts); err != nil {
		return err
	}
	if left, err := s.cubeboxMgr.cubeboxManger.Get(ctx, sid); err == nil && left != nil {
		return fmt.Errorf("record still present after rollback")
	}
	return nil
}

// releaseCrossNodeImports is storage.ReleaseCrossNodeSandboxImports, a
// variable so service tests can observe it without a cubecow engine.
var releaseCrossNodeImports = storage.ReleaseCrossNodeSandboxImports

// releaseFailedCrossNodeRestore runs when a cross-node restore Create fails
// on this node. Failover already destroyed the record and the storage it
// recorded; this also releases imports made before the storage plugin
// recorded them (the metadata disk imported at the Create entry, or a disk
// imported by a storage step that failed midway). It only acts when no
// runtime may exist and no record remains: a remaining record is left to
// Destroy/CleanUp or the next Create pre-check, which apply the same proof.
func (s *service) releaseFailedCrossNodeRestore(ctx context.Context, sid string) {
	sid = strings.TrimSpace(sid)
	if sid == "" {
		return
	}
	evidence := workflow.LifecycleCleanupFrom(ctx)
	// Strict: once a runtime start was attempted, keep the preserve
	// behaviour even if failover later proved the runtime gone.
	if evidence == nil || evidence.RuntimeMayExist {
		log.G(ctx).Warnf("cross-node restore %s failed after a runtime start was attempted; preserving imported storage", sid)
		return
	}
	if s.cubeboxMgr != nil && s.cubeboxMgr.cubeboxManger != nil {
		sb, err := s.cubeboxMgr.cubeboxManger.Get(ctx, sid)
		if err == nil && sb != nil {
			log.G(ctx).Warnf("cross-node restore %s failed; record still present, leaving its storage to Destroy/CleanUp", sid)
			return
		}
		if err != nil && !errors.Is(err, utils.ErrorKeyNotFound) && !errors.Is(err, utils.ErrorBucketNotFound) {
			log.G(ctx).Warnf("cross-node restore %s failed; cannot read record (%v), preserving imported storage", sid, err)
			return
		}
	}
	if err := releaseCrossNodeImports(ctx, sid); err != nil {
		log.G(ctx).Warnf("cross-node restore %s failed; release imported storage: %v", sid, err)
		return
	}
	log.G(ctx).Warnf("cross-node restore %s failed before any runtime started; released imported storage", sid)
}

// Orphan shim reaping (Phase 3, item 5).
//
// A create that failed after the shim spawned can leave this state behind:
// failover marked the record EXITED and containerd deleted the task and the
// shim bundle, but the shim process (which hosts the VMM) kept running with
// its working directory on the deleted bundle and the guest NVMe fds open.
// Destroy then finds no recorded pid and no bundle pid file, the process
// scan correctly sees the shim, and the record is refused forever.
//
// Such a shim is unreachable: containerd has no task for it and its bundle
// is gone, so no API can stop it. reapOrphanShims kills it and waits for it
// to exit, which turns the record into a provably exited one. It acts only
// when every condition holds, under the lifecycle lock:
//   - no snapshot publication hold; record EXITED, not paused or pausing;
//   - containerd has no task for the sandbox or any of its containers;
//   - the process is a containerd shim whose "-id" argument is exactly one
//     of those ids and whose cwd is a deleted directory.

// shimKill is a variable so tests can observe kills without signalling.
var shimKill = func(pid int) error { return syscall.Kill(pid, syscall.SIGKILL) }

// orphanShimPIDs returns pids under root of containerd shims for id whose
// working directory was deleted.
func orphanShimPIDs(root, id string) []int {
	entries, err := os.ReadDir(root)
	if err != nil || strings.TrimSpace(id) == "" {
		return nil
	}
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid <= 1 || pid == os.Getpid() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, e.Name(), "cmdline"))
		if err != nil || len(raw) == 0 {
			continue
		}
		args := strings.Split(strings.TrimRight(string(raw), "\x00"), "\x00")
		if !strings.HasPrefix(filepath.Base(args[0]), "containerd-shim") {
			continue
		}
		match := false
		for i := 1; i+1 < len(args); i++ {
			if args[i] == "-id" && args[i+1] == id {
				match = true
				break
			}
		}
		if !match {
			continue
		}
		cwd, err := os.Readlink(filepath.Join(root, e.Name(), "cwd"))
		if err != nil || !strings.HasSuffix(cwd, " (deleted)") {
			continue
		}
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	return pids
}

// reapOrphanShims kills orphaned shims of an exited record and waits for
// them to exit. It returns the reaped pids; nil means nothing qualified.
func (l *local) reapOrphanShims(ctx context.Context, sb *cubeboxstore.CubeBox) ([]int, error) {
	if sb == nil || sb.GetPublicationHold() != nil || sb.GetStatus() == nil {
		return nil, nil
	}
	st := sb.GetStatus().Get()
	if st.FinishedAt == 0 || st.PausedAt != 0 || st.PausingAt != 0 {
		return nil, nil
	}
	ns := sb.Namespace
	if ns == "" {
		ns = namespaces.Default
	}
	qctx := namespaces.WithNamespace(ctx, ns)
	probe := l.runtimeEvidenceProbe()
	ids := sandboxShimLookupIDs(sb)
	for _, id := range ids {
		exists, err := probe.TaskExists(qctx, id)
		if err != nil || exists {
			return nil, nil
		}
	}
	var pids []int
	for _, id := range ids {
		pids = append(pids, orphanShimPIDs(procRoot, id)...)
	}
	if len(pids) == 0 {
		return nil, nil
	}
	log.G(ctx).Warnf("sandbox %s: exited record, no containerd task, shim pids %v run on a deleted bundle; killing orphaned runtime", sb.ID, pids)
	for _, pid := range pids {
		if err := shimKill(pid); err != nil && !errors.Is(err, syscall.ESRCH) {
			return pids, fmt.Errorf("kill orphan shim %d: %w", pid, err)
		}
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for _, pid := range pids {
		if err := utils.WaitProcessGone(wctx, pid); err != nil {
			return pids, fmt.Errorf("orphan shim %d did not exit: %w", pid, err)
		}
	}
	return pids, nil
}

// exitedRecordOwnsStorage reports whether releasing the storage of an exited
// record can only remove disks that record created: not a same-node pause
// resume and not a cold recreate, both of which reuse retained disks.
func exitedRecordOwnsStorage(sb *cubeboxstore.CubeBox) bool {
	if sb == nil {
		return false
	}
	c := &workflow.CreateContext{ReqInfo: &cubebox.RunCubeSandboxRequest{
		Annotations:  sb.Annotations,
		InstanceType: sb.InstanceType,
	}}
	if c.IsColdRecreate() {
		return false
	}
	return !c.IsPauseResume() || c.IsCrossNodeRestore()
}

// noRuntimeArtifactsForID is noRuntimeArtifacts for an id whose record is
// gone: it checks containerd container, task and shim, and the host process
// scan, for the sandbox id only (shim and VMM are named by it).
func (l *local) noRuntimeArtifactsForID(ctx context.Context, ns, id string) (string, bool) {
	ctx = namespaces.WithNamespace(ctx, ns)
	probe := l.runtimeEvidenceProbe()
	for _, c := range []struct {
		what string
		fn   func(context.Context, string) (bool, error)
	}{
		{"containerd container", probe.ContainerExists},
		{"containerd task", probe.TaskExists},
		{"shim", probe.ShimExists},
		{"host process", probe.ProcessExists},
	} {
		exists, err := c.fn(ctx, id)
		if err != nil {
			return fmt.Sprintf("cannot query %s for %s: %v", c.what, id, err), false
		}
		if exists {
			return fmt.Sprintf("%s exists for %s", c.what, id), false
		}
	}
	return "", true
}
