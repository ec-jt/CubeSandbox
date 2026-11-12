// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/log"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

// Operator reconciliation of a held snapshot publication (B15 companion).
//
// A publication hold is set before capture and normally cleared once the
// package is sealed. Before B15, a remote export acceptance failure after a
// successful seal left the hold in place and the VM frozen forever. This
// private Master->Cubelet Update action resolves such a hold, never on its own
// initiative:
//
//   - sealed package (capture lineage advanced to the held snapshot and local
//     sealing provably finished): pause holds finish the pause teardown and
//     become PAUSED and resumable; the S3 export is handed to the async
//     worker. Commit holds are kept only when Master confirms its snapshot row
//     is READY; otherwise the orphaned package is discarded.
//   - unsealed package: the artifacts are discarded and the lineage is
//     invalidated (the next capture is a full one). A pause whose VM still
//     runs becomes RUNNING again; a pause whose VM is gone is left deletable.
//
// Every ambiguity refuses with PreConditionFailed and changes nothing: the
// caller must name the held snapshot exactly, lookups must succeed, and a
// persisted-but-unsealed catalog is only discarded with an explicit force.
// The action is idempotent: a sandbox whose hold is already resolved reports
// its current outcome.
const (
	publicationReconcileAction = "ReconcilePublication"

	// Request annotations.
	reconcileSnapshotIDAnnotation   = "cube.master.publication.reconcile.snapshot_id"
	reconcileForceDiscardAnnotation = "cube.master.publication.reconcile.force_discard"
	reconcileKeepCommitAnnotation   = "cube.master.publication.reconcile.keep_commit"

	// ExtInfo key carrying the outcome.
	reconcileOutcomeExtInfoKey = "publication_reconcile_outcome"

	ReconcileOutcomeSealedPaused       = "sealed_paused"
	ReconcileOutcomeSealedCommitted    = "sealed_committed"
	ReconcileOutcomeDiscardedRunning   = "discarded_running"
	ReconcileOutcomeDiscardedDeletable = "discarded_deletable"
	ReconcileOutcomeAlreadyPaused      = "already_paused"
	ReconcileOutcomeAlreadyRunning     = "already_running"
)

// Test seams.
var (
	reconcilePackageStateFn = storage.PublishedPackageState
	reconcileTaskStatusFn   = defaultReconcileTaskStatus
	reconcileScheduleExport = scheduleRemoteExportIfS3
)

// defaultReconcileTaskStatus reports the shim's task status. vmGone is true
// when containerd no longer has the task (the MicroVM is gone after
// PauseToSnapshot and the shim was reaped).
func defaultReconcileTaskStatus(ctx context.Context, sb *cubeboxstore.CubeBox) (containerd.ProcessStatus, bool, error) {
	first := sb.FirstContainer()
	if first == nil || first.Container == nil {
		return "", true, nil
	}
	ns := sb.Namespace
	if ns == "" {
		ns = namespaces.Default
	}
	ctx = namespaces.WithNamespace(ctx, ns)
	task, err := first.Container.Task(ctx, nil)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return "", true, nil
		}
		return "", false, err
	}
	qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	st, err := task.Status(qctx)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return "", true, nil
		}
		return "", false, err
	}
	return st.Status, false, nil
}

type reconcileRequest struct {
	snapshotID   string
	forceDiscard bool
	keepCommit   bool
}

func parseReconcileRequest(req *cubebox.UpdateCubeSandboxRequest) (reconcileRequest, error) {
	ann := req.GetAnnotations()
	out := reconcileRequest{
		snapshotID:   strings.TrimSpace(ann[reconcileSnapshotIDAnnotation]),
		forceDiscard: strings.EqualFold(strings.TrimSpace(ann[reconcileForceDiscardAnnotation]), "true"),
		keepCommit:   strings.EqualFold(strings.TrimSpace(ann[reconcileKeepCommitAnnotation]), "true"),
	}
	if out.snapshotID == "" {
		return out, errors.New("reconcile requires the held snapshot id (" + reconcileSnapshotIDAnnotation + ")")
	}
	return out, nil
}

// reconcilePublication runs under the sandbox lifecycle lock taken by Update.
func (s *service) reconcilePublication(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest,
	sb *cubeboxstore.CubeBox, rsp *cubebox.UpdateCubeSandboxResponse) (*cubebox.UpdateCubeSandboxResponse, error) {
	refuse := func(format string, args ...any) (*cubebox.UpdateCubeSandboxResponse, error) {
		rsp.Ret.RetCode = errorcode.ErrorCode_PreConditionFailed
		rsp.Ret.RetMsg = "publication reconcile refused: " + fmt.Sprintf(format, args...)
		log.G(ctx).Warnf("sandbox %s: %s", req.SandboxID, rsp.Ret.RetMsg)
		return rsp, nil
	}
	succeed := func(outcome string, ext map[string][]byte) (*cubebox.UpdateCubeSandboxResponse, error) {
		if ext == nil {
			ext = map[string][]byte{}
		}
		ext[reconcileOutcomeExtInfoKey] = []byte(outcome)
		rsp.ExtInfo = ext
		rsp.Ret.RetMsg = outcome
		log.G(ctx).Warnf("sandbox %s: publication reconcile outcome=%s", req.SandboxID, outcome)
		return rsp, nil
	}
	if sb == nil {
		return refuse("sandbox record missing")
	}
	// Same containerd context the pause path builds: the teardown and
	// cleanup below talk to containerd, which requires a namespace.
	ns := sb.Namespace
	if ns == "" {
		ns = namespaces.Default
	}
	ctx = namespaces.WithNamespace(ctx, ns)
	ctx = constants.WithPreStopType(ctx, constants.PreStopTypePause)
	in, err := parseReconcileRequest(req)
	if err != nil {
		return refuse("%v", err)
	}
	sync := func() error { return s.cubeboxMgr.cubeboxManger.SyncByID(ctx, sb.ID) }

	h := sb.GetPublicationHold()
	if h == nil {
		// Idempotent replay: report where the sandbox already is.
		state := sb.GetStatus()
		switch {
		case state != nil && state.Get().State() == cubebox.ContainerState_CONTAINER_PAUSED &&
			stampedPauseSnapshotID(sb) == in.snapshotID:
			return succeed(ReconcileOutcomeAlreadyPaused, remoteExportExtInfo(nil, pauseExportPending(sb, in.snapshotID)))
		case state != nil && state.Get().State() == cubebox.ContainerState_CONTAINER_RUNNING:
			return succeed(ReconcileOutcomeAlreadyRunning, nil)
		default:
			return refuse("no publication hold and sandbox is neither paused on %s nor running", in.snapshotID)
		}
	}
	if strings.TrimSpace(h.SnapshotID) != in.snapshotID {
		return refuse("held snapshot is %s, request names %s", h.SnapshotID, in.snapshotID)
	}
	if h.Operation != "pause" && h.Operation != "commit" {
		return refuse("unknown held operation %q", h.Operation)
	}
	catalog, sealed, err := reconcilePackageStateFn(ctx, h.Backend, h.SnapshotID)
	if err != nil {
		return refuse("package state lookup failed: %v", err)
	}
	captured := runtimeSnapshotLabelOf(sb) == h.SnapshotID

	if sealed && captured {
		if h.Operation == "commit" && !in.keepCommit {
			// Master has no READY row for this commit: it is an orphan.
			return s.discardHeldPublication(ctx, req, sb, h, rsp, sync, succeed, refuse)
		}
		return s.completeHeldPublication(ctx, req, sb, h, sync, succeed, refuse)
	}
	if sealed && !captured {
		// A sealed package without capture lineage is contradictory.
		return refuse("package %s is sealed but the capture lineage never advanced to it", h.SnapshotID)
	}
	if catalog && !in.forceDiscard {
		return refuse("package %s has a persisted catalog but is not sealed; rerun with force_discard after inspection", h.SnapshotID)
	}
	return s.discardHeldPublication(ctx, req, sb, h, rsp, sync, succeed, refuse)
}

type reconcileFinish func(outcome string, ext map[string][]byte) (*cubebox.UpdateCubeSandboxResponse, error)
type reconcileRefuse func(format string, args ...any) (*cubebox.UpdateCubeSandboxResponse, error)

// completeHeldPublication adopts a sealed package under the async-export model.
func (s *service) completeHeldPublication(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest,
	sb *cubeboxstore.CubeBox, h *cubeboxstore.PublicationHold, sync func() error,
	succeed reconcileFinish, refuse reconcileRefuse) (*cubebox.UpdateCubeSandboxResponse, error) {
	if h.Operation == "commit" {
		if err := completeSnapshotPublication(sb, sync); err != nil {
			return refuse("%v", err)
		}
		pending, err := reconcileScheduleExport(h.Backend, h.SnapshotID)
		if err != nil {
			log.G(ctx).Errorf("reconcile: schedule export %s: %v", h.SnapshotID, err)
		}
		return succeed(ReconcileOutcomeSealedCommitted, remoteExportExtInfo(nil, pending))
	}
	// Pause: the MicroVM is gone after a successful capture. Finish the
	// teardown exactly like the pause success path: PAUSED first, then the
	// in-process keep_tombstone destroy, then clear the hold.
	if status, gone, err := reconcileTaskStatusFn(ctx, sb); err != nil {
		return refuse("task status lookup failed: %v", err)
	} else if !gone && status == containerd.Running {
		return refuse("held pause %s is sealed but the task still reports running", h.SnapshotID)
	}
	for _, c := range sb.AllContainers() {
		if c != nil && c.Status != nil {
			_ = c.Status.Update(func(st cubeboxstore.Status) (cubeboxstore.Status, error) {
				st.PausedAt = time.Now().UnixNano()
				st.PausingAt = 0
				st.FinishedAt = 0
				st.Unknown = false
				st.Reason = ""
				st.Message = ""
				return st, nil
			})
		}
	}
	if err := sync(); err != nil {
		return refuse("persist paused state: %v", err)
	}
	ext, err := s.destroyLiveAfterPause(ctx, req, sb)
	if err != nil {
		// Hold stays; the sandbox is still fenced. A retry resumes here.
		return refuse("pause teardown: %v", err)
	}
	if err := completeSnapshotPublication(sb, sync); err != nil {
		return refuse("%v", err)
	}
	pending, err := reconcileScheduleExport(h.Backend, h.SnapshotID)
	if err != nil {
		log.G(ctx).Errorf("reconcile: schedule export %s: %v", h.SnapshotID, err)
	}
	return succeed(ReconcileOutcomeSealedPaused, remoteExportExtInfo(ext, pending))
}

// discardHeldPublication drops an unsealed (or orphaned commit) package.
func (s *service) discardHeldPublication(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest,
	sb *cubeboxstore.CubeBox, h *cubeboxstore.PublicationHold, rsp *cubebox.UpdateCubeSandboxResponse,
	sync func() error, succeed reconcileFinish, refuse reconcileRefuse) (*cubebox.UpdateCubeSandboxResponse, error) {
	_ = rsp
	status, gone, err := reconcileTaskStatusFn(ctx, sb)
	if err != nil {
		return refuse("task status lookup failed: %v", err)
	}
	running := !gone && (status == containerd.Running || status == containerd.Created)
	if !running && !gone && h.Operation == "pause" {
		// Paused/stopped/unknown shim with a live process: neither provably
		// running nor provably gone.
		return refuse("task reports %q; cannot tell whether the VM survives", status)
	}
	if h.Operation == "commit" && !running {
		return refuse("commit hold on a sandbox whose task is not running (%q, gone=%v)", status, gone)
	}
	// The capture may have reset dirty tracking: never let the discarded
	// package (or the old base) serve as an incremental base again.
	invalidateRuntimeSnapshotBindingsAfterOpaqueRestore(sb, time.Now().UTC())
	prev := *h
	sb.SetPublicationHold(nil)
	if err := sync(); err != nil {
		sb.SetPublicationHold(&prev)
		return refuse("persist hold release (hold retained): %v", err)
	}
	outcome := ReconcileOutcomeDiscardedRunning
	if h.Operation == "pause" {
		if running {
			markLocalPauseRefused(sb)
		} else {
			markLocalPauseFailed(sb, fmt.Errorf("pause publication %s discarded; VM is gone, delete the sandbox", h.SnapshotID))
			outcome = ReconcileOutcomeDiscardedDeletable
		}
		_ = sync()
	}
	cleanup, cerr := s.cleanupTemplate(ctx, &cubebox.CleanupTemplateRequest{
		RequestID:  req.RequestID,
		TemplateID: h.SnapshotID,
		Backend:    h.Backend,
	}, false)
	if cerr != nil || (cleanup != nil && cleanup.GetRet() != nil && cleanup.GetRet().GetRetCode() != errorcode.ErrorCode_Success) {
		msg := ""
		if cerr != nil {
			msg = cerr.Error()
		} else {
			msg = cleanup.GetRet().GetRetMsg()
		}
		// The hold is gone, so ordinary delete / CleanupTemplate can retry.
		log.G(ctx).Warnf("reconcile: discarded %s but artifact cleanup is incomplete (retry via CleanupTemplate): %s", h.SnapshotID, msg)
	}
	return succeed(outcome, nil)
}

func runtimeSnapshotLabelOf(sb *cubeboxstore.CubeBox) string {
	if v := cubeBoxLabel(sb, constants.MasterAnnotationRuntimeSnapshotID); v != "" {
		return v
	}
	sb.MetaLock.Lock()
	defer sb.MetaLock.Unlock()
	return strings.TrimSpace(sb.Annotations[constants.MasterAnnotationRuntimeSnapshotID])
}

// pauseExportPending is the replay answer for an already-paused sandbox: an
// S3 package reports pending so Master keeps (or re-derives) inprogress.
func pauseExportPending(sb *cubeboxstore.CubeBox, snapshotID string) bool {
	return storage.IsS3Backend(pauseCatalogBackend(sb)) && snapshotID != ""
}
