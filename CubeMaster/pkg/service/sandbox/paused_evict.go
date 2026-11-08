// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sandbox

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/log"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/cubelet"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/restoreplace"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	cubeleterrorcode "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

// PausedEvictionEnv gates LRU paused eviction on the Master side. Default off;
// an unparsable value is treated as off (eviction is destructive of local
// state, so it must be an explicit opt-in).
const PausedEvictionEnv = "CUBE_PAUSED_EVICTION_ENABLED"

// UpdateActionEvict is the Update action that evicts one paused sandbox. CLM
// drives it from its pressure-driven LRU loop.
const UpdateActionEvict = "evict"

const (
	cubeletEvictAction     = "EvictPausedSandbox"
	cubeletEvictDigestKey  = "paused_eviction_checkpoint_digest"
	cubeletEvictRecordKey  = "source_local_quiesced_not_global_fenced"
	evictCubeletRPCTimeout = 60 * time.Second
)

// PausedEvictionEnabled reads the flag on every call so it can be flipped
// without a restart.
func PausedEvictionEnabled() bool {
	on, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(PausedEvictionEnv)))
	return err == nil && on
}

var (
	evictLookupPauseFn = pausesnap.GetBySandbox
	evictMarkFn        = pausesnap.MarkEvicted
	evictCubeletFn     = func(ctx context.Context, hostIP string, req *cubebox.UpdateCubeSandboxRequest) (*cubebox.UpdateCubeSandboxResponse, error) {
		return cubelet.UpdateWithTimeout(ctx, cubelet.GetCubeletAddr(hostIP), req, evictCubeletRPCTimeout)
	}
	// evictProbePresentFn reports whether the origin still holds any local
	// record (PAUSED tombstone or otherwise) of the sandbox. An error means
	// unknown and never leads to marking.
	evictProbePresentFn = originHoldsSandbox
	// evictReadRecordFn reads the origin's local-quiesced record bytes.
	evictReadRecordFn = restoreplace.ReadOriginSourceQuiescedRaw
)

func originHoldsSandbox(ctx context.Context, hostIP, sandboxID string) (bool, error) {
	listRsp, err := cubelet.List(ctx, cubelet.GetCubeletAddr(hostIP), &cubebox.ListCubeSandboxRequest{Id: &sandboxID})
	if err != nil {
		return false, err
	}
	if listRsp == nil {
		return false, fmt.Errorf("origin list returned no response")
	}
	for _, item := range listRsp.GetItems() {
		if item.GetId() == sandboxID {
			return true, nil
		}
	}
	return false, nil
}

// evictionIdentity is the restore identity an eviction record must bind to.
func evictionIdentity(rec *pausesnap.Record, digest string) restoreplace.Input {
	return restoreplace.Input{
		SnapshotID:             rec.SnapshotID,
		OriginNodeID:           strings.TrimSpace(rec.NodeID),
		OriginNodeIP:           strings.TrimSpace(rec.NodeIP),
		ExcludeOrigin:          true,
		SourceSandboxID:        strings.TrimSpace(rec.SandboxID),
		SourceCheckpointID:     strings.TrimSpace(rec.SnapshotID),
		SourceCheckpointDigest: strings.TrimSpace(digest),
	}
}

// reconcilePausedEviction resolves an eviction whose outcome Master does not
// know (transport error, lost response, or an earlier attempt that dropped
// node state before Master recorded it). Caller holds the sandbox lock.
//
// It marks the binding evicted only when BOTH hold: the origin answers and no
// longer holds the sandbox, AND the origin serves an eviction record that
// strictly validates against this binding (sandbox, checkpoint, origin node,
// digest reproduced from the embedded commit). Anything else, including an
// unreachable origin, a still-present sandbox, or a missing/foreign/tampered
// record, leaves the binding unchanged. It never marks without the record.
func reconcilePausedEviction(ctx context.Context, rec *pausesnap.Record) (*pausesnap.Record, bool) {
	out, outcome := reconcilePausedEvictionOutcome(ctx, rec)
	return out, outcome == reconcileEvicted
}

// reconcileOutcome is the tri-state result of reconcilePausedEviction.
type reconcileOutcome int

const (
	// reconcileNotEvicted: definite. The origin still holds the sandbox (or
	// the binding is not a reconcile candidate); the binding is unchanged.
	reconcileNotEvicted reconcileOutcome = iota
	// reconcileEvicted: the binding was marked evicted from a valid record.
	reconcileEvicted
	// reconcileUnknown: retry later. The origin is unreachable, or it no
	// longer holds the sandbox but serves no valid record yet. The latter is
	// the eviction crash window (tombstone dropped, record still staged);
	// origin Cubelet startup recovery publishes the record, after which a
	// retry marks the binding. Never marked without a valid record.
	reconcileUnknown
)

func reconcilePausedEvictionOutcome(ctx context.Context, rec *pausesnap.Record) (*pausesnap.Record, reconcileOutcome) {
	if rec == nil || rec.Evicted() || EvictionIneligibleReason(rec) != "" {
		return rec, reconcileNotEvicted
	}
	present, err := evictProbePresentFn(ctx, rec.NodeIP, rec.SandboxID)
	if err != nil {
		log.G(ctx).Warnf("paused eviction reconcile sandbox=%s: origin probe failed, binding unchanged: %v", rec.SandboxID, err)
		return rec, reconcileUnknown
	}
	if present {
		return rec, reconcileNotEvicted
	}
	raw, err := evictReadRecordFn(ctx, rec.NodeIP, rec.SandboxID)
	if err != nil {
		log.G(ctx).Warnf("paused eviction reconcile sandbox=%s: origin holds no sandbox and no eviction record yet (outcome unknown, retry later), binding unchanged: %v", rec.SandboxID, err)
		return rec, reconcileUnknown
	}
	digest, err := restoreplace.EvictionRecordDigest(raw)
	if err != nil {
		log.G(ctx).Warnf("paused eviction reconcile sandbox=%s: invalid eviction record (outcome unknown): %v", rec.SandboxID, err)
		return rec, reconcileUnknown
	}
	if _, err := restoreplace.ValidateEvictionRecord(evictionIdentity(rec, digest), raw); err != nil {
		log.G(ctx).Warnf("paused eviction reconcile sandbox=%s: eviction record does not bind this binding (outcome unknown): %v", rec.SandboxID, err)
		return rec, reconcileUnknown
	}
	if err := evictMarkFn(ctx, rec.SandboxID, rec.SnapshotID, digest, string(raw)); err != nil {
		log.G(ctx).Warnf("paused eviction reconcile sandbox=%s: record eviction: %v", rec.SandboxID, err)
		return rec, reconcileUnknown
	}
	now := time.Now()
	out := *rec
	out.EvictedAt, out.CheckpointDigest, out.EvictionRecord = &now, digest, string(raw)
	log.G(ctx).Infof("paused eviction reconciled sandbox=%s snap=%s origin=%s digest=%s",
		rec.SandboxID, rec.SnapshotID, rec.NodeIP, digest)
	return &out, reconcileEvicted
}

// EvictionIneligibleReason returns "" when a pause binding is a valid LRU
// eviction candidate, otherwise why not. It encodes the Master half of the
// policy: READY pause, S3 backend, remote_ready (exports uploaded), not
// already evicted. The durable commit itself is verified by the origin
// Cubelet against the native store before it drops anything.
func EvictionIneligibleReason(rec *pausesnap.Record) string {
	switch {
	case rec == nil:
		return "no pause binding"
	case !strings.EqualFold(strings.TrimSpace(rec.Status), pausesnap.StatusReady):
		return "pause binding status=" + rec.Status
	case !constants.IsS3Backend(rec.Backend):
		return "backend=" + rec.Backend
	case !strings.EqualFold(strings.TrimSpace(rec.RemoteStatus), constants.RemoteStatusReady):
		return "remote_status=" + rec.RemoteStatus
	case rec.Evicted():
		return "already evicted"
	case strings.TrimSpace(rec.NodeIP) == "":
		return "origin node unknown"
	}
	return ""
}

// evictPausedSandbox runs under the per-sandbox lifecycle lock held by Update,
// so it is serialized with pause, resume and delete of the same sandbox. It
// asks the origin Cubelet to verify the committed checkpoint and drop local
// pause state, then records the eviction and checkpoint digest. It never asks
// any node to delete pause packages, native snapshots or S3 objects.
func evictPausedSandbox(ctx context.Context, req *types.UpdateRequest) *types.Res {
	rsp := &types.Res{Ret: &types.Ret{
		RetCode: int(errorcode.ErrorCode_Success),
		RetMsg:  errorcode.ErrorCode_Success.String(),
	}}
	refuse := func(format string, args ...any) *types.Res {
		rsp.Ret.RetCode = int(errorcode.ErrorCode_MasterParamsError)
		rsp.Ret.RetMsg = fmt.Sprintf(format, args...)
		return rsp
	}
	if !PausedEvictionEnabled() {
		return refuse("paused eviction is disabled (%s)", PausedEvictionEnv)
	}
	rec, err := evictLookupPauseFn(ctx, req.SandboxID)
	if err != nil {
		return refuse("load pause binding: %v", err)
	}
	if reason := EvictionIneligibleReason(rec); reason != "" {
		return refuse("sandbox %s is not evictable: %s", req.SandboxID, reason)
	}
	cubeRsp, err := evictCubeletFn(ctx, rec.NodeIP, &cubebox.UpdateCubeSandboxRequest{
		RequestID: req.RequestID,
		SandboxID: req.SandboxID,
		Annotations: map[string]string{
			constants.CubeAnnotationsUpdateAction:   cubeletEvictAction,
			constants.CubeAnnotationPauseSnapshotID: rec.SnapshotID,
			constants.CubeAnnotationStorageBackend:  constants.SnapshotBackendS3,
		},
	})
	if err != nil || cubeRsp == nil || cubeRsp.GetRet() == nil {
		// Unknown outcome. Reconcile against the origin: mark evicted only
		// if the sandbox is gone there AND its eviction record validates.
		// Otherwise the binding stays un-evicted and the next resume (or the
		// CLM retry) reconciles again.
		if _, ok := reconcilePausedEviction(ctx, rec); ok {
			return rsp
		}
		rsp.Ret.RetCode = int(errorcode.ErrorCode_ReqCubeAPIFailed)
		rsp.Ret.RetMsg = fmt.Sprintf("cubelet evict %s: outcome unknown: %v", req.SandboxID, err)
		return rsp
	}
	if code := cubeRsp.GetRet().GetRetCode(); code != cubeleterrorcode.ErrorCode_Success && code != cubeleterrorcode.ErrorCode_OK {
		// A retry after an earlier lost response lands here: the node no
		// longer holds the sandbox. Reconcile before reporting a refusal.
		switch _, outcome := reconcilePausedEvictionOutcome(ctx, rec); outcome {
		case reconcileEvicted:
			return rsp
		case reconcileUnknown:
			// Gone on the origin without a valid record (crash window), or
			// origin unreachable: retryable, never a definite refusal.
			rsp.Ret.RetCode = int(errorcode.ErrorCode_ReqCubeAPIFailed)
			rsp.Ret.RetMsg = fmt.Sprintf("cubelet evict %s: outcome unknown, retry later: %s", req.SandboxID, cubeRsp.GetRet().GetRetMsg())
			return rsp
		}
		return refuse("origin refused eviction of %s: %s", req.SandboxID, cubeRsp.GetRet().GetRetMsg())
	}
	digest := strings.TrimSpace(string(cubeRsp.GetExtInfo()[cubeletEvictDigestKey]))
	record := cubeRsp.GetExtInfo()[cubeletEvictRecordKey]
	if digest == "" || len(record) == 0 {
		rsp.Ret.RetCode = int(errorcode.ErrorCode_ReqCubeAPIFailed)
		rsp.Ret.RetMsg = "origin evicted without reporting the committed checkpoint digest and eviction record"
		return rsp
	}
	if _, err := restoreplace.ValidateEvictionRecord(evictionIdentity(rec, digest), record); err != nil {
		// Never record an eviction Master cannot later prove to the gate.
		rsp.Ret.RetCode = int(errorcode.ErrorCode_ReqCubeAPIFailed)
		rsp.Ret.RetMsg = fmt.Sprintf("origin eviction record of %s is invalid: %v", req.SandboxID, err)
		return rsp
	}
	if err := evictMarkFn(ctx, req.SandboxID, rec.SnapshotID, digest, string(record)); err != nil {
		rsp.Ret.RetCode = int(errorcode.ErrorCode_DBError)
		rsp.Ret.RetMsg = fmt.Sprintf("record eviction of %s: %v", req.SandboxID, err)
		return rsp
	}
	log.G(ctx).Infof("evicted paused sandbox=%s snap=%s origin=%s digest=%s",
		req.SandboxID, rec.SnapshotID, rec.NodeIP, digest)
	return rsp
}
