// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sandbox

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/restoreplace"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/sandboxlock"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	cubeleterr "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

type masterReconcileFixture struct {
	rec      *pausesnap.Record
	outcome  string
	cubeCode cubeleterr.ErrorCode
	cubeErr  error
	sent     *cubebox.UpdateCubeSandboxRequest
	sentHost string
	readied  int
	deleted  []string
	failed   []string
}

func installMasterReconcile(t *testing.T, f *masterReconcileFixture) {
	t.Helper()
	pg, pr, pd, pm, ph, pc, pl, pa := reconcileGetPauseFn, reconcileReadyFn, reconcileDeleteFn, reconcileMarkFailFn, reconcileHostIPFn, reconcileCubeletFn, reconcileWithLockFn, reconcileApplyRefsFn
	t.Cleanup(func() {
		reconcileGetPauseFn, reconcileReadyFn, reconcileDeleteFn, reconcileMarkFailFn, reconcileHostIPFn, reconcileCubeletFn, reconcileWithLockFn, reconcileApplyRefsFn = pg, pr, pd, pm, ph, pc, pl, pa
	})
	reconcileGetPauseFn = func(context.Context, string) (*pausesnap.Record, error) {
		if f.rec == nil {
			return nil, pausesnap.ErrNotFound
		}
		return f.rec, nil
	}
	reconcileReadyFn = func(context.Context, string, string, []string) (bool, error) { f.readied++; return true, nil }
	reconcileDeleteFn = func(_ context.Context, id string) error { f.deleted = append(f.deleted, id); return nil }
	reconcileMarkFailFn = func(_ context.Context, _, id, _ string) { f.failed = append(f.failed, id) }
	reconcileHostIPFn = func(context.Context, string) (string, bool) { return "", false }
	reconcileCubeletFn = func(_ context.Context, host string, req *cubebox.UpdateCubeSandboxRequest) (*cubebox.UpdateCubeSandboxResponse, error) {
		f.sent, f.sentHost = req, host
		if f.cubeErr != nil {
			return nil, f.cubeErr
		}
		code := f.cubeCode
		if code == 0 {
			code = cubeleterr.ErrorCode_Success
		}
		return &cubebox.UpdateCubeSandboxResponse{
			Ret:     &cubeleterr.Ret{RetCode: code, RetMsg: "x"},
			ExtInfo: map[string][]byte{cubeletReconcileOutcomeKey: []byte(f.outcome)},
		}, nil
	}
	reconcileWithLockFn = func(ctx context.Context, _ string, _ sandboxlock.Options, fn func(context.Context) error) error {
		return fn(ctx)
	}
	reconcileApplyRefsFn = func(context.Context, map[string][]byte) {}
}

func canaryRecord() *pausesnap.Record {
	return &pausesnap.Record{
		SandboxID: "5702fbf499aa4c7c8a3ee56dc1675964", SnapshotID: "snap-9537230c03614d3eaeaf4b5b",
		NodeIP: "10.0.1.21", Status: pausesnap.StatusFailed, Backend: "s3", RemoteStatus: "failed",
		LastError: "remote export acceptance: settle rootfs: context deadline exceeded",
	}
}

func TestMasterReconcileSealedPauseMarksReady(t *testing.T) {
	f := &masterReconcileFixture{rec: canaryRecord(), outcome: "sealed_paused"}
	installMasterReconcile(t, f)
	rsp := ReconcilePublication(context.Background(), &ReconcilePublicationRequest{SandboxID: "5702fbf499aa4c7c8a3ee56dc1675964"})
	require.Equal(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode, rsp.Ret.RetMsg)
	require.Equal(t, "pause_binding_ready", rsp.MasterAction)
	require.Equal(t, 1, f.readied)
	require.Equal(t, "10.0.1.21", f.sentHost)
	require.Equal(t, cubeletReconcileAction, f.sent.Annotations["cube.master.update_action"])
	require.Equal(t, "snap-9537230c03614d3eaeaf4b5b", f.sent.Annotations[cubeletReconcileSnapshotKey])
	require.Empty(t, f.sent.Annotations[cubeletReconcileForceKey])
}

func TestMasterReconcileDiscardedDropsBinding(t *testing.T) {
	f := &masterReconcileFixture{rec: canaryRecord(), outcome: "discarded_running"}
	installMasterReconcile(t, f)
	rsp := ReconcilePublication(context.Background(), &ReconcilePublicationRequest{SandboxID: "5702fbf499aa4c7c8a3ee56dc1675964", ForceDiscard: true})
	require.Equal(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode, rsp.Ret.RetMsg)
	require.Equal(t, "pause_binding_dropped", rsp.MasterAction)
	require.Equal(t, []string{"snap-9537230c03614d3eaeaf4b5b"}, f.deleted)
	require.Equal(t, "true", f.sent.Annotations[cubeletReconcileForceKey])
	require.Zero(t, f.readied)
}

func TestMasterReconcileDeletableKeepsFailedBinding(t *testing.T) {
	f := &masterReconcileFixture{rec: canaryRecord(), outcome: "discarded_deletable"}
	installMasterReconcile(t, f)
	rsp := ReconcilePublication(context.Background(), &ReconcilePublicationRequest{SandboxID: "5702fbf499aa4c7c8a3ee56dc1675964"})
	require.Equal(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode, rsp.Ret.RetMsg)
	require.Empty(t, f.deleted)
	require.Equal(t, []string{"snap-9537230c03614d3eaeaf4b5b"}, f.failed)
}

func TestMasterReconcileWritesNothingOnCubeletRefusalOrAmbiguity(t *testing.T) {
	for name, f := range map[string]*masterReconcileFixture{
		"refused":   {rec: canaryRecord(), cubeCode: cubeleterr.ErrorCode_PreConditionFailed},
		"rpc error": {rec: canaryRecord(), cubeErr: errors.New("deadline exceeded")},
		"unknown":   {rec: canaryRecord(), outcome: "weird"},
	} {
		t.Run(name, func(t *testing.T) {
			installMasterReconcile(t, f)
			rsp := ReconcilePublication(context.Background(), &ReconcilePublicationRequest{SandboxID: "5702fbf499aa4c7c8a3ee56dc1675964"})
			require.NotEqual(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode)
			require.Zero(t, f.readied)
			require.Empty(t, f.deleted)
			require.Empty(t, f.failed)
		})
	}
}

func TestMasterReconcileRejectsBadRequests(t *testing.T) {
	ready := canaryRecord()
	ready.Status = "CREATING"
	for name, tc := range map[string]struct {
		rec *pausesnap.Record
		req *ReconcilePublicationRequest
	}{
		"no sandbox":    {canaryRecord(), &ReconcilePublicationRequest{}},
		"creating row":  {ready, &ReconcilePublicationRequest{SandboxID: "5702fbf499aa4c7c8a3ee56dc1675964"}},
		"host mismatch": {canaryRecord(), &ReconcilePublicationRequest{SandboxID: "5702fbf499aa4c7c8a3ee56dc1675964", HostIP: "10.9.9.9"}},
		"no binding":    {nil, &ReconcilePublicationRequest{SandboxID: "5702fbf499aa4c7c8a3ee56dc1675964"}},
	} {
		t.Run(name, func(t *testing.T) {
			f := &masterReconcileFixture{rec: tc.rec, outcome: "sealed_paused"}
			installMasterReconcile(t, f)
			rsp := ReconcilePublication(context.Background(), tc.req)
			require.NotEqual(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode)
			require.Nil(t, f.sent)
		})
	}
}

func TestMasterReconcileCommitPassesKeepWhenRowReady(t *testing.T) {
	f := &masterReconcileFixture{outcome: "sealed_committed"}
	installMasterReconcile(t, f)
	SetCommitSnapshotLookup(func(context.Context, string) (CommitSnapshotState, error) {
		return CommitSnapshotState{Found: true, Ready: true, OriginNodeIP: "10.0.1.22", SandboxID: "sb-c"}, nil
	})
	t.Cleanup(func() { SetCommitSnapshotLookup(nil) })
	rsp := ReconcilePublication(context.Background(), &ReconcilePublicationRequest{SandboxID: "sb-c", SnapshotID: "snap-c"})
	require.Equal(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode, rsp.Ret.RetMsg)
	require.Equal(t, "commit", rsp.Operation)
	require.Equal(t, "10.0.1.22", f.sentHost)
	require.Equal(t, "true", f.sent.Annotations[cubeletReconcileKeepCommitKey])
	require.Equal(t, "none", rsp.MasterAction)
}

// Same-node resume works for an async-export row (inprogress, no uuids);
// cross-node stays refused until the reconciler marks it ready with uuids.
func TestResumePlacementAsyncExportStaysHomeUntilReadyWithUUIDs(t *testing.T) {
	orig := decidePauseResumePlacementFn
	t.Cleanup(func() { decidePauseResumePlacementFn = orig })
	decided := 0
	decidePauseResumePlacementFn = func(context.Context, restoreplace.Input) (*restoreplace.Placement, error) {
		decided++
		return &restoreplace.Placement{NodeIP: "10.0.0.2", CrossNode: true}, nil
	}
	rec := &pausesnap.Record{SnapshotID: "snap-a", Backend: "s3", RemoteStatus: "inprogress", NodeIP: "10.0.0.1", NodeID: "n1"}
	got, err := resumePlacement(context.Background(), rec, "cubebox", nil)
	require.NoError(t, err)
	require.False(t, got.CrossNode)
	require.Equal(t, "10.0.0.1", got.NodeIP)
	rec.RemoteStatus = "ready" // ready but uuids not yet filled
	got, err = resumePlacement(context.Background(), rec, "cubebox", nil)
	require.NoError(t, err)
	require.False(t, got.CrossNode)
	require.Zero(t, decided)
	rec.ExportUUIDs = `{"rootfs":"r","metadata":"m"}`
	got, err = resumePlacement(context.Background(), rec, "cubebox", nil)
	require.NoError(t, err)
	require.True(t, got.CrossNode)
	require.Equal(t, 1, decided)
}

func TestRemoteExportPendingFromExtInfo(t *testing.T) {
	require.True(t, remoteExportPendingFromExtInfo(map[string][]byte{"remote_export_pending": []byte("true")}))
	require.False(t, remoteExportPendingFromExtInfo(map[string][]byte{"remote_uuids": []byte("{}")}))
	require.False(t, remoteExportPendingFromExtInfo(nil))
}
