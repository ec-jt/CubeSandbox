// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/container/runc"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/log"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/cube/internals/cubes"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

// pausedEvictAction is the private Master->Cubelet Update action that evicts
// one paused, S3-committed sandbox from this node. It is the node-local half
// of LRU paused eviction; the pressure-driven selection runs in CLM and the
// durable "evicted" record lives in CubeMaster.
const pausedEvictAction = "EvictPausedSandbox"

// ExtInfo keys returned on a successful eviction. The local-quiesced record is
// returned under the same key ReadRecoverySourceQuiesced uses, so the wire
// shape has one meaning; Master persists those exact bytes.
const (
	pausedEvictDigestKey   = "paused_eviction_checkpoint_digest"
	pausedEvictReleasedKey = "paused_eviction_released"
	pausedEvictRecordKey   = sourceQuiescedExtInfoKey
)

// Test seams. Production values verify against the native store and wipe the
// paused runtime leftovers exactly like the pause-delete tombstone path.
var (
	verifyCommittedPauseCheckpointFn = storage.VerifyCommittedPauseCheckpointRecord
	cleanPausedRuncFn                = runc.Clean
)

type pausedEvictionReleased struct {
	MilliCPU  int64 `json:"milli_cpu"`
	MemoryMB  int64 `json:"memory_mb"`
	StorageMB int64 `json:"storage_disk_mb"`
}

// evictPausedSandbox runs under the sandbox lifecycle lock taken by Update,
// so it is serialized with resume Create (same lock) and with Destroy/Pause.
//
// Proof for the cross-node gate: after the PAUSED tombstone is dropped it
// publishes a durable local-quiesced record (sourcefence LocalDrainResult,
// stage local-quiesced-not-global-fenced) bound to operation=RequestID,
// sandbox, this node's fence identity, the evicted incarnation, checkpoint and
// digest, with the verified native commit as native_binding. The record is
// written and fsynced before the drop and only renamed into place after it,
// and its bytes are returned so Master can persist them.
//
// It evicts only when every condition holds:
//   - node policy enabled (paused_eviction_enabled, default off);
//   - the sandbox is fully PAUSED (not PAUSING) on its S3 backend;
//   - the Master-named pause checkpoint equals the locally stamped one;
//   - the checkpoint commit is durable and verified in the native store.
//
// Eviction drops only the PAUSED CubeBox tombstone (and leftover runc /
// containerd metadata), which removes 100% of its CPU/memory/MVM quota from
// node accounting. It never calls CleanupTemplate and never deletes native
// snapshots or S3 exports: the committed checkpoint is what a later
// cross-node restore imports, and the CubeS3lvol block cache reclaims clean
// local blocks on its own. A failed verification leaves everything in place.
func (s *service) evictPausedSandbox(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest,
	sb *cubeboxstore.CubeBox, rsp *cubebox.UpdateCubeSandboxResponse) (*cubebox.UpdateCubeSandboxResponse, error) {
	fail := func(format string, args ...any) (*cubebox.UpdateCubeSandboxResponse, error) {
		rsp.Ret.RetCode = errorcode.ErrorCode_PreConditionFailed
		rsp.Ret.RetMsg = fmt.Sprintf(format, args...)
		log.G(ctx).Warnf("evict paused sandbox %s refused: %s", req.SandboxID, rsp.Ret.RetMsg)
		return rsp, nil
	}
	if s.cubeboxMgr == nil || s.cubeboxMgr.config == nil || !s.cubeboxMgr.config.PausedEvictionEnabled {
		return fail("paused eviction is disabled on this node")
	}
	if sb == nil || sb.ID != req.SandboxID {
		return fail("sandbox record missing")
	}
	status := sb.GetStatus()
	if status == nil || status.Get().State() != cubebox.ContainerState_CONTAINER_PAUSED {
		return fail("sandbox is not fully paused")
	}
	if backend := pauseCatalogBackend(sb); backend != cow.BackendS3 {
		return fail("sandbox storage backend %q is not s3", backend)
	}
	snapID := stampedPauseSnapshotID(sb)
	want := strings.TrimSpace(req.GetAnnotations()[constants.MasterAnnotationPauseSnapshotID])
	if snapID == "" || want == "" || snapID != want {
		return fail("pause checkpoint %q does not match requested %q", snapID, want)
	}
	nodeID := strings.TrimSpace(s.cubeboxMgr.config.SourceFenceNodeID)
	if nodeID == "" {
		return fail("source_fence_node_id is not configured; eviction cannot bind its proof to this node")
	}
	digest, commit, err := verifyCommittedPauseCheckpointFn(ctx, sb.ID, snapID)
	if err != nil {
		return fail("pause checkpoint is not durably committed: %v", err)
	}
	incarnation, err := sourcefence.EvictedIncarnation(sb.ID, sb.CreatedAt, snapID, digest)
	if err != nil {
		return fail("evicted source identity: %v", err)
	}
	native, err := sourcefence.NewEvictedNativeBinding(digest, commit)
	if err != nil {
		return fail("evicted native binding: %v", err)
	}
	stage, err := workflow.EvictionRecords.StageEvicted(workflow.SourceFences, sourcefence.Binding{
		OperationID:      strings.TrimSpace(req.RequestID),
		SandboxID:        sb.ID,
		NodeID:           nodeID,
		Incarnation:      incarnation,
		CheckpointID:     snapID,
		CheckpointDigest: digest,
	}, native)
	if err != nil {
		return fail("stage eviction record: %v", err)
	}
	defer stage.Abort()

	released := pausedEvictionReleased{}
	if r := sb.ResourceWithOverHead; r != nil {
		released = pausedEvictionReleased{
			MilliCPU:  r.HostCpuQ.MilliValue(),
			MemoryMB:  bytesToMB(r.HostMemQ.Value()),
			StorageMB: r.HostStorageDiskMB,
		}
	}
	if err := s.cubeboxMgr.dropEvictedTombstone(ctx, sb); err != nil {
		// A partial drop may already have removed the tombstone. Then the
		// stage is the only proof: keep it for startup recovery.
		if _, gerr := s.cubeboxMgr.cubeboxManger.Get(ctx, sb.ID); errors.Is(gerr, utils.ErrorKeyNotFound) {
			stage.Retain()
		}
		rsp.Ret.RetCode = errorcode.ErrorCode_Unknown
		rsp.Ret.RetMsg = fmt.Sprintf("drop paused state: %v", err)
		return rsp, nil
	}
	_, record, err := stage.Publish()
	if err != nil {
		// The tombstone is gone but the record is not published. Keep the
		// staged record: startup recovery re-validates and publishes it.
		// Until then Master gets no proof (reconcile reports outcome
		// unknown, retryable) and origin restore of the sandbox is refused.
		stage.Retain()
		rsp.Ret.RetCode = errorcode.ErrorCode_Unknown
		rsp.Ret.RetMsg = fmt.Sprintf("paused state dropped but eviction record not published (kept staged for recovery): %v", err)
		log.G(ctx).Errorf("evict paused sandbox %s: %s", sb.ID, rsp.Ret.RetMsg)
		return rsp, nil
	}
	raw, _ := json.Marshal(released)
	rsp.ExtInfo = map[string][]byte{
		pausedEvictDigestKey:   []byte(digest),
		pausedEvictReleasedKey: raw,
		pausedEvictRecordKey:   record,
	}
	log.G(ctx).Infof("evicted paused sandbox %s checkpoint=%s digest=%s operation=%s released=%s",
		sb.ID, snapID, digest, req.RequestID, string(raw))
	return rsp, nil
}

// refuseEvictedCheckpointRestore keeps the eviction promise on the origin: a
// checkpoint this node evicted is never restored here again, so the record
// Master relies on stays true. Other checkpoints of the same sandbox (a later
// pause) are unaffected. Caller holds the lifecycle lock.
func refuseEvictedCheckpointRestore(sandboxID, pauseSnapshotID string) error {
	pauseSnapshotID = strings.TrimSpace(pauseSnapshotID)
	if pauseSnapshotID == "" {
		return nil
	}
	return workflow.EvictionRecords.EvictedCheckpointBlocked(sandboxID, pauseSnapshotID)
}

// dropEvictedTombstone removes the PAUSED CubeBox row and any leftover runtime
// metadata. It mirrors the pause delete-tombstone path but leaves the pause
// package catalog and native objects untouched.
func (l *local) dropEvictedTombstone(ctx context.Context, sb *cubeboxstore.CubeBox) error {
	ns := sb.Namespace
	if ns == "" {
		ns = namespaces.Default
	}
	ctx = namespaces.WithNamespace(ctx, ns)
	if sb.UserMarkDeletedTime == nil {
		now := time.Now()
		sb.UserMarkDeletedTime = &now
	}
	_ = cleanPausedRuncFn(ctx, sb.ID)
	if l.client != nil {
		for _, id := range pausedTombstoneContainerIDs(sb) {
			if err := l.client.ContainerService().Delete(ctx, id); err != nil && !errdefs.IsNotFound(err) {
				log.G(ctx).Warnf("evict paused sandbox: delete containerd %s: %v", id, err)
			}
		}
	}
	return l.cubeboxManger.Delete(ctx, &cubes.DeleteOption{CubeboxID: sb.ID})
}
