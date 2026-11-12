// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	eventtypes "github.com/containerd/containerd/api/events"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/cube/internals/cubes"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
	"github.com/tencentcloud/CubeSandbox/pkgs/CubeLog"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

func TestSnapshotPublicationFailureBoundaries(t *testing.T) {
	for _, operation := range []string{"commit", "pause"} {
		for _, fail := range []string{"hold", "capture", "lineage", "catalog", "seal", "upload", "sync", ""} {
			t.Run(operation+"/"+fail, func(t *testing.T) {
				cb := newCubeboxWithStatusForTest("sb-publication", cubeboxstore.Status{StartedAt: 1})
				setRuntimeSnapshotBindingLabels(cb, "snap-old", time.Now())
				setRuntimeRestoreBaseLabels(cb, "snap-launch", time.Now())
				layout := snapshotWorkLayout{SnapshotID: "snap-new", Backend: "s3", Home: t.TempDir()}
				artifact := filepath.Join(layout.Home, "recoverable-memory")
				require.NoError(t, os.WriteFile(artifact, []byte("captured memory"), 0600))
				var calls []string
				var durable []byte
				step := func(name string) error {
					calls = append(calls, name)
					if name == fail {
						return errors.New("injected " + name)
					}
					return nil
				}
				persist := func(name string) func() error {
					return func() error {
						if err := step(name); err != nil {
							return err
						}
						var err error
						durable, err = json.Marshal(cb.DeepCopy())
						return err
					}
				}
				err := beginSnapshotPublication(cb, layout, operation, persist("hold"))
				if err == nil {
					err = step("capture")
				}
				if err == nil {
					err = persistCapturedLineage(cb, layout.SnapshotID, persist("lineage"))
				}
				if err == nil {
					_, err = publishCapturedSnapshot(func() error { return step("catalog") }, func() error { return step("seal") }, func() (string, error) { return "accepted-not-ready", step("upload") })
				}
				if err == nil {
					err = completeSnapshotPublication(cb, persist("sync"))
				}
				all := []string{"hold", "capture", "lineage", "catalog", "seal", "upload", "sync"}
				if fail == "" {
					require.NoError(t, err)
					require.Nil(t, cb.GetPublicationHold())
					require.Equal(t, cubebox.ContainerState_CONTAINER_RUNNING, cb.GetStatus().Get().State())
				} else {
					require.Error(t, err)
					require.NotNil(t, cb.GetPublicationHold())
					require.Equal(t, cubebox.ContainerState_CONTAINER_UNKNOWN, cb.GetStatus().Get().State())
					require.False(t, cb.GetStatus().IsTerminated())
					require.ErrorContains(t, beginSnapshotPublication(cb, layout, operation, func() error { t.Fatal("retry persisted"); return nil }), "reconciliation")
					for i, name := range all {
						if name == fail {
							all = all[:i+1]
							break
						}
					}
				}
				require.Equal(t, all, calls)
				require.FileExists(t, artifact)
				if len(durable) != 0 {
					var restarted cubeboxstore.CubeBox
					require.NoError(t, json.Unmarshal(durable, &restarted))
					// No native/runtime access is allowed during held startup recovery.
					if fail != "" {
						require.NoError(t, cubes.RecoverPod(context.Background(), nil, &restarted))
						require.ErrorContains(t, restarted.PublicationError(), "snap-new")
						require.Equal(t, cubebox.ContainerState_CONTAINER_UNKNOWN, restarted.GetStatus().Get().State())
					}
					if fail != "capture" && fail != "lineage" {
						require.Equal(t, "snap-new", resolveBaseSnapshotID(&restarted))
					}
				}
			})
		}
	}
}

func TestSnapshotPublicationHoldDeniesCleanupDestroyAndRetry(t *testing.T) {
	cb := newCubeboxWithStatusForTest("sb-held", cubeboxstore.Status{StartedAt: 1, PausedAt: 2})
	cb.SetPublicationHold(&cubeboxstore.PublicationHold{SnapshotID: "snap-held", PreviousSnapshotID: "snap-old", RestoreSnapshotID: "snap-launch", Operation: "pause", SnapshotPath: t.TempDir()})
	s, _, flow := newDestroyLifecycleServiceForTest(cb)
	for _, id := range []string{"snap-held", "snap-old", "snap-launch"} {
		rsp, err := s.cleanupTemplate(context.Background(), &cubebox.CleanupTemplateRequest{TemplateID: id}, false)
		require.NoError(t, err)
		require.Equal(t, errorcode.ErrorCode_PreConditionFailed, rsp.GetRet().GetRetCode())
	}
	rsp := destroySandboxForTest(t, s, cb.ID)
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, rsp.GetRet().GetRetCode())
	require.Zero(t, flow.destroyCalls)
	require.Zero(t, flow.cleanupCalls)
	_, err := validateCommitSandboxTarget(cb)
	require.ErrorContains(t, err, "reconciliation")
	pauseRsp, err := s.updateWithPauseCow(context.Background(), &cubebox.UpdateCubeSandboxRequest{SandboxID: cb.ID}, cb)
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, pauseRsp.GetRet().GetRetCode())
	scanDeadContainer(context.Background(), []*cubeboxstore.CubeBox{cb}, nil, 0)
	require.NotNil(t, cb.GetPublicationHold())
	require.Equal(t, cubebox.ContainerState_CONTAINER_UNKNOWN, toGRPCCubeBox(cb, nil).Containers[0].State)
}

func TestSnapshotPublicationSuccessfulPauseTeardownKeepsHoldUntilSync(t *testing.T) {
	cb := newCubeboxWithStatusForTest("sb-paused-publication", cubeboxstore.Status{StartedAt: 1, PausedAt: 2})
	cb.SetPublicationHold(&cubeboxstore.PublicationHold{SnapshotID: "snap-held", Operation: "pause"})
	s, manager, flow := newDestroyLifecycleServiceForTest(cb)
	_, err := s.destroyLiveAfterPause(CubeLog.WithRequestTrace(context.Background(), &CubeLog.RequestTrace{Action: "pause"}), &cubebox.UpdateCubeSandboxRequest{SandboxID: cb.ID}, cb)
	require.NoError(t, err)
	require.Equal(t, 1, flow.destroyCalls)
	require.NotNil(t, cb.GetPublicationHold())
	require.Equal(t, cubebox.ContainerState_CONTAINER_UNKNOWN, cb.GetStatus().Get().State())
	require.NoError(t, completeSnapshotPublication(cb, func() error { return manager.SyncByID(context.Background(), cb.ID) }))
	require.Equal(t, cubebox.ContainerState_CONTAINER_PAUSED, cb.GetStatus().Get().State())
}

func TestSnapshotPublicationTeardownSyncFailureNeverDestroys(t *testing.T) {
	cb := newCubeboxWithStatusForTest("sb-sync-failure", cubeboxstore.Status{PausedAt: 2})
	cb.SetPublicationHold(&cubeboxstore.PublicationHold{SnapshotID: "snap-held", Operation: "pause"})
	s, manager, flow := newDestroyLifecycleServiceForTest(cb)
	manager.syncErr = errors.New("DB unavailable")
	_, err := s.destroyLiveAfterPause(CubeLog.WithRequestTrace(context.Background(), &CubeLog.RequestTrace{Action: "pause"}), &cubebox.UpdateCubeSandboxRequest{SandboxID: cb.ID}, cb)
	require.ErrorContains(t, err, "persist pause teardown")
	require.Zero(t, flow.destroyCalls)
	require.NotNil(t, cb.GetPublicationHold())
}

func TestPreCaptureRuntimeRefusalReleasesHold(t *testing.T) {
	require.True(t, isPreCaptureRuntimeRefusal(errors.New("rpc error: sandbox pause forbidding, terminate exec tasks first")))
	require.False(t, isPreCaptureRuntimeRefusal(errors.New("sandbox not running")))
	require.False(t, isPreCaptureRuntimeRefusal(errors.New("ttrpc: closed")))
	require.False(t, isPreCaptureRuntimeRefusal(nil))

	cb := newCubeboxWithStatusForTest("sb-refused", cubeboxstore.Status{StartedAt: 1})
	layout := snapshotWorkLayout{SnapshotID: "snap-refused", Backend: "s3", Home: t.TempDir()}
	require.NoError(t, beginSnapshotPublication(cb, layout, "pause", func() error { return nil }))
	require.Error(t, cb.PublicationError())

	// Sync failure keeps the hold (fail closed).
	require.Error(t, releasePreCapturePublication(cb, func() error { return errors.New("db down") }))
	require.NotNil(t, cb.GetPublicationHold())

	require.NoError(t, releasePreCapturePublication(cb, func() error { return nil }))
	require.Nil(t, cb.GetPublicationHold())
	require.NoError(t, cb.PublicationError())
	markLocalPauseRefused(cb)
	require.Equal(t, cubebox.ContainerState_CONTAINER_RUNNING, cb.GetStatus().Get().State())
	// A later pause attempt is allowed again.
	require.NoError(t, beginSnapshotPublication(cb, layout, "pause", func() error { return nil }))
}

func TestAcceptedSnapshotUUIDsRequiresAllRoles(t *testing.T) {
	for _, uuids := range []*cow.RemoteUUIDs{nil, {}, {Rootfs: "root"}, {Rootfs: "root", Memory: "mem"}, {Rootfs: "root", Memory: "mem", Metadata: " "}} {
		raw, err := acceptedSnapshotUUIDs(uuids)
		require.Error(t, err)
		require.Empty(t, raw)
	}
	raw, err := acceptedSnapshotUUIDs(&cow.RemoteUUIDs{Rootfs: "root", Memory: "mem", Metadata: "meta"})
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	_, err = uploadRemoteUUIDsIfS3(context.Background(), "invalid-backend", "snap-id")
	require.Error(t, err)
}

func TestSnapshotPublicationLiveBaseCannotBeCleanedAfterRegistrationFailure(t *testing.T) {
	cb := newCubeboxWithStatusForTest("sb-live-base", cubeboxstore.Status{StartedAt: 1})
	cb.AddLabels(map[string]string{constants.MasterAnnotationRuntimeSnapshotID: "snap-base"})
	s, _, _ := newDestroyLifecycleServiceForTest(cb)
	require.ErrorContains(t, s.snapshotPackageRetentionError("snap-base"), "live memory lineage")
	require.NoError(t, s.snapshotPackageRetentionError("snap-unrelated"))
}

func TestSnapshotPublicationHoldRejectsRestoreAndCatalogUse(t *testing.T) {
	cb := newCubeboxWithStatusForTest("sb-held", cubeboxstore.Status{StartedAt: 1})
	cb.SetPublicationHold(&cubeboxstore.PublicationHold{SnapshotID: "snap-held", Operation: "commit"})
	s, _, flow := newDestroyLifecycleServiceForTest(cb)
	info, err := s.GetLocalSnapshot(context.Background(), &cubebox.GetLocalSnapshotRequest{SnapshotID: "snap-held", Backend: "s3"})
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, info.GetRet().GetRetCode())
	for _, key := range []string{constants.MasterAnnotationDesiredSandboxID, constants.MasterAnnotationRuntimeSnapshotID, constants.MasterAnnotationPauseSnapshotID, constants.MasterAnnotationAppSnapshotTemplateID} {
		value := "snap-held"
		if key == constants.MasterAnnotationDesiredSandboxID {
			value = cb.ID
		}
		rsp, err := s.Create(context.Background(), &cubebox.RunCubeSandboxRequest{Annotations: map[string]string{key: value}})
		require.NoError(t, err)
		require.Equal(t, errorcode.ErrorCode_PreConditionFailed, rsp.GetRet().GetRetCode())
	}
	for _, key := range []string{"cube.debug.cleanup", constants.AnnotationPauseKeepTombstone, constants.AnnotationPauseDeleteTombstone} {
		rsp, err := s.Destroy(context.Background(), &cubebox.DestroyCubeSandboxRequest{SandboxID: cb.ID, Annotations: map[string]string{key: "true"}})
		require.NoError(t, err)
		require.Equal(t, errorcode.ErrorCode_PreConditionFailed, rsp.GetRet().GetRetCode())
	}
	require.Zero(t, flow.cleanupCalls)
	require.Zero(t, flow.destroyCalls)
}

func TestSnapshotPublicationHoldIgnoresDelayedRuntimeEvents(t *testing.T) {
	cb := newCubeboxWithStatusForTest("sb-held-events", cubeboxstore.Status{StartedAt: 1, PausedAt: 2})
	setRuntimeSnapshotBindingLabels(cb, "snap-captured", time.Now())
	cb.SetPublicationHold(&cubeboxstore.PublicationHold{SnapshotID: "snap-captured", Operation: "pause"})
	manager := &fakeCubeboxAPI{cb: cb}
	em := &eventMonitor{c: &local{cubeboxManger: manager}}
	for _, event := range []interface{}{
		&eventtypes.TaskResumed{ContainerID: cb.FirstContainer().ID},
		&eventtypes.TaskPaused{ContainerID: cb.FirstContainer().ID},
		&eventtypes.TaskExit{ContainerID: cb.FirstContainer().ID, ID: cb.FirstContainer().ID},
	} {
		require.NoError(t, em.handleEvent(context.Background(), event))
	}
	require.Empty(t, manager.syncIDs)
	require.Equal(t, "snap-captured", resolveBaseSnapshotID(cb))
	require.Equal(t, cubebox.ContainerState_CONTAINER_UNKNOWN, cb.GetStatus().Get().State())
}
