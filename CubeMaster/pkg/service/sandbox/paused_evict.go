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
)

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
		// Unknown outcome: the binding stays un-evicted. If the node did drop
		// its state, the next resume fails home placement and the operator
		// retries the eviction, which the node answers idempotently-refused.
		rsp.Ret.RetCode = int(errorcode.ErrorCode_ReqCubeAPIFailed)
		rsp.Ret.RetMsg = fmt.Sprintf("cubelet evict %s: %v", req.SandboxID, err)
		return rsp
	}
	if code := cubeRsp.GetRet().GetRetCode(); code != cubeleterrorcode.ErrorCode_Success && code != cubeleterrorcode.ErrorCode_OK {
		return refuse("origin refused eviction of %s: %s", req.SandboxID, cubeRsp.GetRet().GetRetMsg())
	}
	digest := strings.TrimSpace(string(cubeRsp.GetExtInfo()[cubeletEvictDigestKey]))
	if digest == "" {
		rsp.Ret.RetCode = int(errorcode.ErrorCode_ReqCubeAPIFailed)
		rsp.Ret.RetMsg = "origin evicted without reporting the committed checkpoint digest"
		return rsp
	}
	if err := evictMarkFn(ctx, req.SandboxID, rec.SnapshotID, digest); err != nil {
		rsp.Ret.RetCode = int(errorcode.ErrorCode_DBError)
		rsp.Ret.RetMsg = fmt.Sprintf("record eviction of %s: %v", req.SandboxID, err)
		return rsp
	}
	log.G(ctx).Infof("evicted paused sandbox=%s snap=%s origin=%s digest=%s",
		req.SandboxID, rec.SnapshotID, rec.NodeIP, digest)
	return rsp
}
