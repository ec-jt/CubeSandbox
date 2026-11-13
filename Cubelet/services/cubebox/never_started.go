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
	"strings"
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
	ns := sb.Namespace
	if ns == "" {
		ns = namespaces.Default
	}
	ctx = namespaces.WithNamespace(ctx, ns)
	probe := l.runtimeEvidenceProbe()
	for _, id := range sandboxShimLookupIDs(sb) {
		for _, check := range []struct {
			what string
			fn   func(context.Context, string) (bool, error)
		}{
			{"containerd container", probe.ContainerExists},
			{"containerd task", probe.TaskExists},
			{"shim", probe.ShimExists},
		} {
			exists, err := check.fn(ctx, id)
			if err != nil {
				return false, fmt.Sprintf("cannot query %s for %s: %v", check.what, id, err)
			}
			if exists {
				return false, fmt.Sprintf("%s exists for %s", check.what, id)
			}
		}
	}
	return true, "pid 0, never started, no containerd container, task or shim"
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
		log.G(ctx).Infof("sandbox %s: runtime absence not provable: %s", id, reason)
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
	if err != nil || sb == nil {
		return false, ""
	}
	ok, reason := l.provablyNeverStarted(ctx, sb)
	if !ok || !recordRollbackReleasesStorage(sb) {
		return false, ""
	}
	return true, "record provably never started: " + reason
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
