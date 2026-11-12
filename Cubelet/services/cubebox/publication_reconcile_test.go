// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"errors"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
	"github.com/tencentcloud/CubeSandbox/pkgs/CubeLog"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

type reconcileFixture struct {
	catalog, sealed bool
	stateErr        error
	status          containerd.ProcessStatus
	gone            bool
	statusErr       error
	scheduled       []string
	cleaned         []string
}

func installReconcileFixture(t *testing.T, f *reconcileFixture) {
	t.Helper()
	prevState, prevStatus, prevSched := reconcilePackageStateFn, reconcileTaskStatusFn, reconcileScheduleExport
	prevGet, prevCow, prevRel, prevObj, prevLocal, prevStop := getLocalSnapshotForFn, cleanupIsCowBackend, cleanupReleaseS3Metadata, cleanupObjectsFor, cleanupTemplateLocalDataFn, stopSnapshotExportFn
	reconcilePackageStateFn = func(context.Context, string, string) (bool, bool, error) { return f.catalog, f.sealed, f.stateErr }
	reconcileTaskStatusFn = func(context.Context, *cubeboxstore.CubeBox) (containerd.ProcessStatus, bool, error) {
		return f.status, f.gone, f.statusErr
	}
	reconcileScheduleExport = func(backend, id string) (bool, error) {
		f.scheduled = append(f.scheduled, id)
		return storage.IsS3Backend(backend), nil
	}
	getLocalSnapshotForFn = func(context.Context, string, string) (*storage.SnapshotCatalogEntry, error) {
		return nil, storage.ErrSnapshotCatalogNotFound
	}
	cleanupIsCowBackend = func() bool { return true }
	cleanupReleaseS3Metadata = func(context.Context, string, string) error { return nil }
	cleanupObjectsFor = func(context.Context, string, []storage.CowObjectRef) error { return nil }
	cleanupTemplateLocalDataFn = func(_ context.Context, id, _ string) error {
		f.cleaned = append(f.cleaned, id)
		return nil
	}
	stopSnapshotExportFn = func(context.Context, string) error { return nil }
	t.Cleanup(func() {
		reconcilePackageStateFn, reconcileTaskStatusFn, reconcileScheduleExport = prevState, prevStatus, prevSched
		getLocalSnapshotForFn, cleanupIsCowBackend, cleanupReleaseS3Metadata, cleanupObjectsFor, cleanupTemplateLocalDataFn, stopSnapshotExportFn = prevGet, prevCow, prevRel, prevObj, prevLocal, prevStop
	})
}

func heldCubebox(t *testing.T, op string, captured bool) *cubeboxstore.CubeBox {
	t.Helper()
	cb := newCubeboxWithStatusForTest("5702fbf499aa4c7c8a3ee56dc1675964", cubeboxstore.Status{StartedAt: 1})
	cb.Annotations = map[string]string{constants.MasterAnnotationStorageBackend: "s3"}
	if op == "pause" {
		stampPauseSnapshotID(cb, "snap-9537230c03614d3eaeaf4b5b")
	}
	setRuntimeSnapshotBindingLabels(cb, "snap-old", time.Now())
	if captured {
		setRuntimeSnapshotBindingLabels(cb, "snap-9537230c03614d3eaeaf4b5b", time.Now())
	}
	cb.SetPublicationHold(&cubeboxstore.PublicationHold{
		SnapshotID: "snap-9537230c03614d3eaeaf4b5b", Backend: "s3", Operation: op, SnapshotPath: t.TempDir(),
	})
	markLocalPauseFailed(cb, errors.New("remote export acceptance: settle rootfs: context deadline exceeded"))
	return cb
}

func reconcileReq(snap string, ann map[string]string) *cubebox.UpdateCubeSandboxRequest {
	a := map[string]string{
		constants.MasterAnnotationsUpdateAction: publicationReconcileAction,
		reconcileSnapshotIDAnnotation:           snap,
	}
	for k, v := range ann {
		a[k] = v
	}
	return &cubebox.UpdateCubeSandboxRequest{RequestID: "rq", SandboxID: "5702fbf499aa4c7c8a3ee56dc1675964", Annotations: a}
}

func runReconcile(t *testing.T, s *service, cb *cubeboxstore.CubeBox, req *cubebox.UpdateCubeSandboxRequest) *cubebox.UpdateCubeSandboxResponse {
	t.Helper()
	rsp := &cubebox.UpdateCubeSandboxResponse{RequestID: req.RequestID, Ret: &errorcode.Ret{RetCode: errorcode.ErrorCode_Success}}
	// Update installs a request trace before dispatch; the destroy workflow needs it.
	ctx := CubeLog.WithRequestTrace(context.Background(), &CubeLog.RequestTrace{Action: "Update", RequestID: req.RequestID})
	out, err := s.reconcilePublication(ctx, req, cb, rsp)
	require.NoError(t, err)
	return out
}

func TestReconcileSealedPauseBecomesPausedAndHandsOffExport(t *testing.T) {
	f := &reconcileFixture{catalog: true, sealed: true, gone: true}
	installReconcileFixture(t, f)
	cb := heldCubebox(t, "pause", true)
	s, _, flow := newDestroyLifecycleServiceForTest(cb)
	rsp := runReconcile(t, s, cb, reconcileReq("snap-9537230c03614d3eaeaf4b5b", nil))
	require.Equal(t, errorcode.ErrorCode_Success, rsp.GetRet().GetRetCode(), rsp.GetRet().GetRetMsg())
	require.Equal(t, ReconcileOutcomeSealedPaused, string(rsp.GetExtInfo()[reconcileOutcomeExtInfoKey]))
	require.Equal(t, "true", string(rsp.GetExtInfo()[storage.ExtInfoRemoteExportPending]))
	require.Nil(t, cb.GetPublicationHold())
	require.Equal(t, 1, flow.destroyCalls, "keep_tombstone teardown ran")
	require.Equal(t, cubebox.ContainerState_CONTAINER_PAUSED, cb.GetStatus().Get().State())
	require.Equal(t, []string{"snap-9537230c03614d3eaeaf4b5b"}, f.scheduled)
	require.Empty(t, f.cleaned)

	// Idempotent replay.
	rsp = runReconcile(t, s, cb, reconcileReq("snap-9537230c03614d3eaeaf4b5b", nil))
	require.Equal(t, errorcode.ErrorCode_Success, rsp.GetRet().GetRetCode())
	require.Equal(t, ReconcileOutcomeAlreadyPaused, string(rsp.GetExtInfo()[reconcileOutcomeExtInfoKey]))
	require.Equal(t, 1, flow.destroyCalls)
}

func TestReconcileUnsealedPauseDiscardsAndRunsAgain(t *testing.T) {
	f := &reconcileFixture{status: containerd.Running}
	installReconcileFixture(t, f)
	cb := heldCubebox(t, "pause", false)
	s, _, flow := newDestroyLifecycleServiceForTest(cb)
	rsp := runReconcile(t, s, cb, reconcileReq("snap-9537230c03614d3eaeaf4b5b", nil))
	require.Equal(t, errorcode.ErrorCode_Success, rsp.GetRet().GetRetCode(), rsp.GetRet().GetRetMsg())
	require.Equal(t, ReconcileOutcomeDiscardedRunning, string(rsp.GetExtInfo()[reconcileOutcomeExtInfoKey]))
	require.Nil(t, cb.GetPublicationHold())
	require.Zero(t, flow.destroyCalls)
	require.Equal(t, cubebox.ContainerState_CONTAINER_RUNNING, cb.GetStatus().Get().State())
	require.Equal(t, []string{"snap-9537230c03614d3eaeaf4b5b"}, f.cleaned)
	require.Empty(t, f.scheduled)
	// The next capture must be full: no stale incremental base.
	require.Equal(t, runtimeSnapshotBindingInvalidID, resolveBaseSnapshotID(cb))

	rsp = runReconcile(t, s, cb, reconcileReq("snap-9537230c03614d3eaeaf4b5b", nil))
	require.Equal(t, ReconcileOutcomeAlreadyRunning, string(rsp.GetExtInfo()[reconcileOutcomeExtInfoKey]))
}

func TestReconcileUnsealedPauseWithGoneVMIsLeftDeletable(t *testing.T) {
	f := &reconcileFixture{gone: true}
	installReconcileFixture(t, f)
	cb := heldCubebox(t, "pause", true)
	s, _, _ := newDestroyLifecycleServiceForTest(cb)
	rsp := runReconcile(t, s, cb, reconcileReq("snap-9537230c03614d3eaeaf4b5b", nil))
	require.Equal(t, errorcode.ErrorCode_Success, rsp.GetRet().GetRetCode(), rsp.GetRet().GetRetMsg())
	require.Equal(t, ReconcileOutcomeDiscardedDeletable, string(rsp.GetExtInfo()[reconcileOutcomeExtInfoKey]))
	require.Nil(t, cb.GetPublicationHold())
}

func TestReconcileRefusesOnAmbiguity(t *testing.T) {
	cases := map[string]struct {
		f        reconcileFixture
		op       string
		captured bool
		snap     string
	}{
		"wrong snapshot":       {f: reconcileFixture{sealed: true, catalog: true, gone: true}, op: "pause", captured: true, snap: "snap-other"},
		"state lookup error":   {f: reconcileFixture{stateErr: errors.New("io")}, op: "pause", captured: true, snap: "snap-9537230c03614d3eaeaf4b5b"},
		"catalog not sealed":   {f: reconcileFixture{catalog: true, status: containerd.Running}, op: "pause", captured: true, snap: "snap-9537230c03614d3eaeaf4b5b"},
		"sealed not captured":  {f: reconcileFixture{catalog: true, sealed: true, gone: true}, op: "pause", captured: false, snap: "snap-9537230c03614d3eaeaf4b5b"},
		"sealed but running":   {f: reconcileFixture{catalog: true, sealed: true, status: containerd.Running}, op: "pause", captured: true, snap: "snap-9537230c03614d3eaeaf4b5b"},
		"paused shim unclear":  {f: reconcileFixture{status: containerd.Paused}, op: "pause", captured: false, snap: "snap-9537230c03614d3eaeaf4b5b"},
		"task status error":    {f: reconcileFixture{statusErr: errors.New("ttrpc")}, op: "pause", captured: false, snap: "snap-9537230c03614d3eaeaf4b5b"},
		"missing snapshot arg": {f: reconcileFixture{sealed: true, catalog: true, gone: true}, op: "pause", captured: true, snap: ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := tc.f
			installReconcileFixture(t, &f)
			cb := heldCubebox(t, tc.op, tc.captured)
			s, _, flow := newDestroyLifecycleServiceForTest(cb)
			rsp := runReconcile(t, s, cb, reconcileReq(tc.snap, nil))
			require.Equal(t, errorcode.ErrorCode_PreConditionFailed, rsp.GetRet().GetRetCode())
			require.NotNil(t, cb.GetPublicationHold(), "hold must survive a refusal")
			require.Zero(t, flow.destroyCalls)
			require.Empty(t, f.cleaned)
			require.Empty(t, f.scheduled)
		})
	}
}

func TestReconcileForceDiscardOfUnsealedCatalog(t *testing.T) {
	f := &reconcileFixture{catalog: true, status: containerd.Running}
	installReconcileFixture(t, f)
	cb := heldCubebox(t, "pause", true)
	s, _, _ := newDestroyLifecycleServiceForTest(cb)
	rsp := runReconcile(t, s, cb, reconcileReq("snap-9537230c03614d3eaeaf4b5b", map[string]string{reconcileForceDiscardAnnotation: "true"}))
	require.Equal(t, errorcode.ErrorCode_Success, rsp.GetRet().GetRetCode(), rsp.GetRet().GetRetMsg())
	require.Equal(t, ReconcileOutcomeDiscardedRunning, string(rsp.GetExtInfo()[reconcileOutcomeExtInfoKey]))
}

func TestReconcileCommitHolds(t *testing.T) {
	t.Run("sealed and master ready keeps package", func(t *testing.T) {
		f := &reconcileFixture{catalog: true, sealed: true, status: containerd.Running}
		installReconcileFixture(t, f)
		cb := heldCubebox(t, "commit", true)
		s, _, _ := newDestroyLifecycleServiceForTest(cb)
		rsp := runReconcile(t, s, cb, reconcileReq("snap-9537230c03614d3eaeaf4b5b", map[string]string{reconcileKeepCommitAnnotation: "true"}))
		require.Equal(t, errorcode.ErrorCode_Success, rsp.GetRet().GetRetCode(), rsp.GetRet().GetRetMsg())
		require.Equal(t, ReconcileOutcomeSealedCommitted, string(rsp.GetExtInfo()[reconcileOutcomeExtInfoKey]))
		require.Equal(t, []string{"snap-9537230c03614d3eaeaf4b5b"}, f.scheduled)
		require.Empty(t, f.cleaned)
		require.Nil(t, cb.GetPublicationHold())
	})
	t.Run("sealed but master not ready discards orphan", func(t *testing.T) {
		f := &reconcileFixture{catalog: true, sealed: true, status: containerd.Running}
		installReconcileFixture(t, f)
		cb := heldCubebox(t, "commit", true)
		s, _, _ := newDestroyLifecycleServiceForTest(cb)
		rsp := runReconcile(t, s, cb, reconcileReq("snap-9537230c03614d3eaeaf4b5b", nil))
		require.Equal(t, errorcode.ErrorCode_Success, rsp.GetRet().GetRetCode(), rsp.GetRet().GetRetMsg())
		require.Equal(t, ReconcileOutcomeDiscardedRunning, string(rsp.GetExtInfo()[reconcileOutcomeExtInfoKey]))
		require.Equal(t, []string{"snap-9537230c03614d3eaeaf4b5b"}, f.cleaned)
	})
}

func TestUpdateRoutesReconcileToHeldSandboxOnly(t *testing.T) {
	// Every other action is still refused on a held sandbox.
	cb := heldCubebox(t, "pause", true)
	s, _, _ := newDestroyLifecycleServiceForTest(cb)
	rsp, err := s.Update(context.Background(), &cubebox.UpdateCubeSandboxRequest{
		SandboxID:   cb.ID,
		Annotations: map[string]string{constants.MasterAnnotationsUpdateAction: constants.UpdateActionPause},
	})
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, rsp.GetRet().GetRetCode())

	f := &reconcileFixture{catalog: true, sealed: true, gone: true}
	installReconcileFixture(t, f)
	rsp, err = s.Update(context.Background(), reconcileReq("snap-9537230c03614d3eaeaf4b5b", nil))
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_Success, rsp.GetRet().GetRetCode(), rsp.GetRet().GetRetMsg())
}

func TestPublishReturnsPendingWithoutWaitingForExport(t *testing.T) {
	prev := scheduleSnapshotExportFn
	t.Cleanup(func() { scheduleSnapshotExportFn = prev })
	scheduleSnapshotExportFn = func(string, string) (bool, error) { return true, nil }
	var order []string
	pending, err := publishCapturedSnapshot(
		func() error { order = append(order, "catalog"); return nil },
		func() error { order = append(order, "seal"); return nil },
		func() (bool, error) {
			order = append(order, "schedule")
			return scheduleRemoteExportIfS3("s3", "snap-1")
		},
	)
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, []string{"catalog", "seal", "schedule"}, order)
	ext := remoteExportExtInfo(map[string][]byte{"other": []byte("x")}, pending)
	require.Equal(t, "true", string(ext[storage.ExtInfoRemoteExportPending]))
	require.Equal(t, "x", string(ext["other"]))
	require.Nil(t, remoteExportExtInfo(nil, false))

	// The hold clears once the seal is durable; export is not on this path.
	cb := newCubeboxWithStatusForTest("sb-async", cubeboxstore.Status{StartedAt: 1})
	layout := snapshotWorkLayout{SnapshotID: "snap-1", Backend: "s3", Home: t.TempDir()}
	require.NoError(t, beginSnapshotPublication(cb, layout, "pause", func() error { return nil }))
	require.NoError(t, completeSnapshotPublication(cb, func() error { return nil }))
	require.Nil(t, cb.GetPublicationHold())
}
