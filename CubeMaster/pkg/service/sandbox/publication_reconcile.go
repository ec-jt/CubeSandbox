// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/log"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/cubelet"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/sandboxlock"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	volrefcount "github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/volume/refcount"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

// Operator reconciliation of a held snapshot publication (B15).
//
// POST /internal/publication/reconcile drives the Cubelet ReconcilePublication
// Update action for one sandbox and applies the matching Master transition:
//
//   - Cubelet sealed_paused / already_paused: FAILED pause row -> READY with
//     remote_status=inprogress (S3) and empty export_uuids; the remotestatus
//     reconciler fills the uuids once the async export is DONE. The row is
//     only flipped after Cubelet confirmed, and only from FAILED.
//   - Cubelet discarded_running / already_running: the FAILED pause binding is
//     dropped so the sandbox is RUNNING and can pause again.
//   - Cubelet discarded_deletable: the binding stays FAILED (delete only).
//   - Commit holds: the snapshot row is not changed; Cubelet keeps the package
//     only when the row is READY (it then exports asynchronously) and
//     discards an orphan otherwise.
//
// It is operator-invoked only and runs under the sandbox lifecycle lock.
const (
	cubeletReconcileAction        = "ReconcilePublication"
	cubeletReconcileSnapshotKey   = "cube.master.publication.reconcile.snapshot_id"
	cubeletReconcileForceKey      = "cube.master.publication.reconcile.force_discard"
	cubeletReconcileKeepCommitKey = "cube.master.publication.reconcile.keep_commit"
	cubeletReconcileOutcomeKey    = "publication_reconcile_outcome"
	reconcileCubeletRPCTimeout    = 120 * time.Second
)

// ReconcilePublicationRequest is the operator request body.
type ReconcilePublicationRequest struct {
	RequestID    string `json:"request_id,omitempty"`
	SandboxID    string `json:"sandbox_id"`
	SnapshotID   string `json:"snapshot_id,omitempty"`
	HostIP       string `json:"host_ip,omitempty"`
	ForceDiscard bool   `json:"force_discard,omitempty"`
}

// ReconcilePublicationResponse reports what Cubelet did and what Master wrote.
type ReconcilePublicationResponse struct {
	Ret          *types.Ret `json:"ret"`
	SandboxID    string     `json:"sandbox_id"`
	SnapshotID   string     `json:"snapshot_id,omitempty"`
	Operation    string     `json:"operation,omitempty"`
	HostIP       string     `json:"host_ip,omitempty"`
	Outcome      string     `json:"outcome,omitempty"`
	MasterAction string     `json:"master_action,omitempty"`
}

// CommitSnapshotState is what Master knows about a commit (snapshot table) row.
type CommitSnapshotState struct {
	Found        bool
	Ready        bool
	OriginNodeIP string
	SandboxID    string
}

var (
	commitLookupMu sync.RWMutex
	commitLookup   func(ctx context.Context, snapshotID string) (CommitSnapshotState, error)
)

// SetCommitSnapshotLookup lets the template center (which imports this
// package) provide snapshot-table lookups without an import cycle.
func SetCommitSnapshotLookup(fn func(ctx context.Context, snapshotID string) (CommitSnapshotState, error)) {
	commitLookupMu.Lock()
	commitLookup = fn
	commitLookupMu.Unlock()
}

func lookupCommitSnapshot(ctx context.Context, snapshotID string) (CommitSnapshotState, error) {
	commitLookupMu.RLock()
	fn := commitLookup
	commitLookupMu.RUnlock()
	if fn == nil {
		return CommitSnapshotState{}, errors.New("snapshot table lookup is not available")
	}
	return fn(ctx, snapshotID)
}

// Test seams.
var (
	reconcileGetPauseFn  = pausesnap.GetBySandbox
	reconcileReadyFn     = pausesnap.ReconcileFailedToReady
	reconcileDeleteFn    = pausesnap.Delete
	reconcileMarkFailFn  = pausesnap.MarkFailed
	reconcileHostIPFn    = resolveSandboxHostIP
	reconcileCubeletFn   = defaultReconcileCubelet
	reconcileWithLockFn  = sandboxlock.WithLock
	reconcileApplyRefsFn = volrefcount.ApplyFromExtInfo
)

func defaultReconcileCubelet(ctx context.Context, hostIP string, req *cubebox.UpdateCubeSandboxRequest) (*cubebox.UpdateCubeSandboxResponse, error) {
	return cubelet.UpdateWithTimeout(ctx, cubelet.GetCubeletAddr(hostIP), req, reconcileCubeletRPCTimeout)
}

// ReconcilePublication is the operator entry point.
func ReconcilePublication(ctx context.Context, req *ReconcilePublicationRequest) *ReconcilePublicationResponse {
	rsp := &ReconcilePublicationResponse{Ret: &types.Ret{RetCode: int(errorcode.ErrorCode_Success), RetMsg: "ok"}}
	if req == nil || strings.TrimSpace(req.SandboxID) == "" {
		rsp.Ret.RetCode = int(errorcode.ErrorCode_MasterParamsError)
		rsp.Ret.RetMsg = "sandbox_id is required"
		return rsp
	}
	req.SandboxID = strings.TrimSpace(req.SandboxID)
	req.SnapshotID = strings.TrimSpace(req.SnapshotID)
	rsp.SandboxID = req.SandboxID
	ctx = context.WithoutCancel(ctx)
	err := reconcileWithLockFn(ctx, req.SandboxID, sandboxlock.Options{
		Value: "reconcile-publication",
		TTL:   sandboxlock.LifecycleTTL + 30*time.Second,
	}, func(ctx context.Context) error {
		reconcilePublicationLocked(ctx, req, rsp)
		return nil
	})
	if err != nil {
		res := &types.Res{Ret: rsp.Ret}
		applyLifecycleLockError(res, err)
	}
	return rsp
}

func reconcilePublicationLocked(ctx context.Context, req *ReconcilePublicationRequest, rsp *ReconcilePublicationResponse) {
	fail := func(code errorcode.ErrorCode, format string, args ...any) {
		rsp.Ret.RetCode = int(code)
		rsp.Ret.RetMsg = fmt.Sprintf(format, args...)
		log.G(ctx).Warnf("reconcile publication sandbox=%s: %s", req.SandboxID, rsp.Ret.RetMsg)
	}

	// Classify: pause binding first, then a commit snapshot row.
	var pause *pausesnap.Record
	if rec, err := reconcileGetPauseFn(ctx, req.SandboxID); err == nil && rec != nil {
		if req.SnapshotID == "" || req.SnapshotID == rec.SnapshotID {
			pause = rec
		}
	} else if err != nil && !errors.Is(err, pausesnap.ErrNotFound) {
		fail(errorcode.ErrorCode_DBError, "load pause binding: %v", err)
		return
	}
	operation := ""
	hostIP := strings.TrimSpace(req.HostIP)
	keepCommit := false
	switch {
	case pause != nil:
		operation = "pause"
		req.SnapshotID = pause.SnapshotID
		status := strings.ToUpper(strings.TrimSpace(pause.Status))
		if status != pausesnap.StatusFailed && status != "READY" {
			fail(errorcode.ErrorCode_MasterParamsError, "pause binding %s is %s; only FAILED (or an idempotent READY) can be reconciled", pause.SnapshotID, pause.Status)
			return
		}
		if ip := strings.TrimSpace(pause.NodeIP); ip != "" {
			if hostIP != "" && hostIP != ip {
				fail(errorcode.ErrorCode_MasterParamsError, "host_ip %s does not match pause binding node %s", hostIP, ip)
				return
			}
			hostIP = ip
		}
	case req.SnapshotID != "":
		st, err := lookupCommitSnapshot(ctx, req.SnapshotID)
		if err != nil {
			fail(errorcode.ErrorCode_DBError, "load snapshot row: %v", err)
			return
		}
		if !st.Found {
			fail(errorcode.ErrorCode_NotFound, "no pause binding or snapshot row for %s", req.SnapshotID)
			return
		}
		if st.SandboxID != "" && st.SandboxID != req.SandboxID {
			fail(errorcode.ErrorCode_MasterParamsError, "snapshot %s belongs to sandbox %s", req.SnapshotID, st.SandboxID)
			return
		}
		operation = "commit"
		keepCommit = st.Ready
		if hostIP == "" {
			hostIP = strings.TrimSpace(st.OriginNodeIP)
		}
	default:
		fail(errorcode.ErrorCode_MasterParamsError, "sandbox %s has no pause binding; pass snapshot_id for a commit hold", req.SandboxID)
		return
	}
	if hostIP == "" {
		if ip, ok := reconcileHostIPFn(ctx, req.SandboxID); ok {
			hostIP = ip
		}
	}
	if hostIP == "" {
		fail(errorcode.ErrorCode_NotFound, "cannot resolve the node of sandbox %s", req.SandboxID)
		return
	}
	rsp.SnapshotID, rsp.Operation, rsp.HostIP = req.SnapshotID, operation, hostIP

	requestID := strings.TrimSpace(req.RequestID)
	if requestID == "" {
		requestID = fmt.Sprintf("reconcile-%s-%d", req.SandboxID, time.Now().UnixNano())
	}
	cubeReq := &cubebox.UpdateCubeSandboxRequest{
		RequestID: requestID,
		SandboxID: req.SandboxID,
		Annotations: map[string]string{
			constants.CubeAnnotationsUpdateAction: cubeletReconcileAction,
			cubeletReconcileSnapshotKey:           req.SnapshotID,
		},
	}
	if req.ForceDiscard {
		cubeReq.Annotations[cubeletReconcileForceKey] = "true"
	}
	if keepCommit {
		cubeReq.Annotations[cubeletReconcileKeepCommitKey] = "true"
	}
	cubeRsp, err := reconcileCubeletFn(ctx, hostIP, cubeReq)
	if err != nil || cubeRsp == nil || cubeRsp.GetRet() == nil {
		msg := "cubelet response is nil"
		if err != nil {
			msg = err.Error()
		}
		// Ambiguous: Master writes nothing. Re-running is safe (idempotent).
		fail(errorcode.ErrorCode_ReqCubeAPIFailed, "cubelet reconcile: %s", msg)
		return
	}
	if code := int(cubeRsp.GetRet().GetRetCode()); code != int(errorcode.ErrorCode_Success) {
		fail(errorcode.ErrorCode(code), "cubelet reconcile: %s", cubeRsp.GetRet().GetRetMsg())
		return
	}
	ext := cubeRsp.GetExtInfo()
	outcome := strings.TrimSpace(string(ext[cubeletReconcileOutcomeKey]))
	rsp.Outcome = outcome
	reconcileApplyRefsFn(ctx, ext)

	if operation == "commit" {
		rsp.MasterAction = "none"
		return
	}
	switch outcome {
	case "sealed_paused", "already_paused":
		changed, err := reconcileReadyFn(ctx, req.SandboxID, req.SnapshotID, volrefcount.ReleasedVolumeIDs(ext))
		if err != nil {
			fail(errorcode.ErrorCode_DBError, "cubelet paused the sandbox but marking the pause binding READY failed (rerun reconcile): %v", err)
			return
		}
		rsp.MasterAction = "none"
		if changed {
			rsp.MasterAction = "pause_binding_ready"
		}
	case "discarded_running", "already_running":
		if err := reconcileDeleteFn(ctx, req.SnapshotID); err != nil {
			fail(errorcode.ErrorCode_DBError, "cubelet discarded the package but dropping the pause binding failed (rerun reconcile): %v", err)
			return
		}
		rsp.MasterAction = "pause_binding_dropped"
	case "discarded_deletable":
		reconcileMarkFailFn(ctx, req.SandboxID, req.SnapshotID, "pause publication discarded by operator reconcile; VM is gone, delete the sandbox")
		rsp.MasterAction = "pause_binding_failed_delete_only"
	default:
		fail(errorcode.ErrorCode_Unknown, "cubelet returned unknown reconcile outcome %q", outcome)
	}
}
