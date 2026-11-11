// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

func legacyTestBox(t *testing.T, id string, paused bool) *cubeboxstore.CubeBox {
	t.Helper()
	status := cubeboxstore.Status{StartedAt: 1}
	if paused {
		status.PausedAt = 2
	}
	return newCubeboxWithStatusForTest(id, status)
}

func farDeadline() time.Time { return time.Now().Add(time.Minute) }

func withLegacyRoot(t *testing.T, ids ...string) {
	t.Helper()
	root := t.TempDir()
	old := legacyPauseVMRoot
	legacyPauseVMRoot = root
	t.Cleanup(func() { legacyPauseVMRoot = old })
	for _, id := range ids {
		if err := os.MkdirAll(filepath.Join(root, id), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, id, "config.json"), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLegacyUpdateRouteResumeNeedsPausevmState(t *testing.T) {
	withLegacyRoot(t, "sb-legacy")
	if !legacyUpdateRoute(constants.UpdateActionResume, legacyTestBox(t, "sb-legacy", true)) {
		t.Fatal("paused v0.6 sandbox with pausevm state must take the legacy resume path")
	}
	if legacyUpdateRoute(constants.UpdateActionResume, legacyTestBox(t, "sb-nostate", true)) {
		t.Fatal("no pausevm state: must not route legacy")
	}
	if legacyUpdateRoute(constants.UpdateActionResume, legacyTestBox(t, "sb-legacy", false)) {
		t.Fatal("running sandbox: resume must not route legacy")
	}
}

func TestLegacyUpdateRouteSkipsNativeSandboxes(t *testing.T) {
	withLegacyRoot(t, "sb-native", "sb-cow")
	native := legacyTestBox(t, "sb-native", true)
	native.ComponentVersions = map[string]string{"cube-shim": "v0.7.1"}
	if legacyUpdateRoute(constants.UpdateActionResume, native) || legacyUpdateRoute(constants.UpdateActionPause, native) {
		t.Fatal("sandbox created by this Cubelet must keep the native path")
	}
	cow := legacyTestBox(t, "sb-cow", false)
	stampPauseSnapshotID(cow, "snap-1")
	if legacyUpdateRoute(constants.UpdateActionPause, cow) {
		t.Fatal("sandbox with a native pause stamp must keep the native path")
	}
}

func TestLegacyUpdateRoutePauseOnlyRunning(t *testing.T) {
	withLegacyRoot(t)
	if !legacyUpdateRoute(constants.UpdateActionPause, legacyTestBox(t, "sb-run", false)) {
		t.Fatal("running v0.6 sandbox must re-pause in place")
	}
	if legacyUpdateRoute(constants.UpdateActionPause, legacyTestBox(t, "sb-run", true)) {
		t.Fatal("already paused: native path reports the state error")
	}
	if legacyUpdateRoute("expose-port", legacyTestBox(t, "sb-run", false)) {
		t.Fatal("only pause/resume are routed")
	}
}

func TestLegacyPauseVMStateRejectsTraversal(t *testing.T) {
	withLegacyRoot(t)
	for _, id := range []string{"", ".", "..", "../x", "a/b"} {
		if legacyPauseVMStatePresent(id) {
			t.Fatalf("accepted %q", id)
		}
	}
}

type fakePauseTask struct {
	pauseErr  error
	status    containerd.ProcessStatus
	pauseCall int
}

func (f *fakePauseTask) Pause(context.Context) error { f.pauseCall++; return f.pauseErr }
func (f *fakePauseTask) Status(context.Context) (containerd.Status, error) {
	return containerd.Status{Status: f.status}, nil
}

func TestLegacyPauseTaskLockedMarksPaused(t *testing.T) {
	cb := legacyTestBox(t, "sb-p", false)
	task := &fakePauseTask{}
	ret := legacyPauseTaskLocked(context.Background(), cb, task)
	if ret.RetCode != errorcode.ErrorCode_Success || task.pauseCall != 1 {
		t.Fatalf("ret=%v calls=%d", ret, task.pauseCall)
	}
	if !cb.GetStatus().IsPaused() {
		t.Fatal("sandbox not marked paused")
	}
}

func TestLegacyPauseTaskLockedReconcilesOnError(t *testing.T) {
	cb := legacyTestBox(t, "sb-p", false)
	task := &fakePauseTask{pauseErr: errors.New("ttrpc closed"), status: containerd.Paused}
	ret := legacyPauseTaskLocked(context.Background(), cb, task)
	if ret.RetCode != errorcode.ErrorCode_TaskPauseFailed {
		t.Fatalf("ret=%v", ret)
	}
	if !cb.GetStatus().IsPaused() {
		t.Fatal("shim reported PAUSED; state must converge to paused")
	}
}

type fakeLegacyContainer struct{ containerd.Container }

type fakeContainerLoader struct {
	ids []string
	err error
}

func (f *fakeContainerLoader) LoadContainer(_ context.Context, id string) (containerd.Container, error) {
	f.ids = append(f.ids, id)
	if f.err != nil {
		return nil, f.err
	}
	return fakeLegacyContainer{}, nil
}

// Startup recovery never attaches containers of paused sandboxes; the legacy
// path must load the handle itself (live failure: "failed to load task for
// paused sandbox" on the app1 canary).
func TestEnsureLegacyContainerLoadsMissingHandle(t *testing.T) {
	cb := legacyTestBox(t, "sb-l", true)
	if fc := cb.FirstContainer(); fc == nil || fc.Container != nil {
		t.Fatalf("precondition: first container without handle, got %+v", fc)
	}
	loader := &fakeContainerLoader{}
	if err := ensureLegacyContainer(context.Background(), loader, cb); err != nil {
		t.Fatal(err)
	}
	if len(loader.ids) != 1 || loader.ids[0] != "sb-l" || cb.FirstContainer().Container == nil {
		t.Fatalf("ids=%v handle=%v", loader.ids, cb.FirstContainer().Container)
	}
	// Already attached: no second load.
	if err := ensureLegacyContainer(context.Background(), loader, cb); err != nil || len(loader.ids) != 1 {
		t.Fatalf("err=%v ids=%v", err, loader.ids)
	}
}

func TestEnsureLegacyContainerReportsLoadError(t *testing.T) {
	cb := legacyTestBox(t, "sb-e", true)
	if err := ensureLegacyContainer(context.Background(), &fakeContainerLoader{err: errors.New("not found")}, cb); err == nil {
		t.Fatal("expected error")
	}
	if err := ensureLegacyContainer(context.Background(), nil, legacyTestBox(t, "sb-n", true)); err == nil {
		t.Fatal("nil client must error, not panic")
	}
}

// The forwarded resume reuses the native resume core on the original shim task.
func TestLegacyResumeUsesOriginalShimTask(t *testing.T) {
	cb := legacyTestBox(t, "sb-r", true)
	task := &fakeResumeTask{}
	res := resumeTaskLocked(context.Background(), cb, task, resumeOptions{
		taskDeadline: farDeadline(), reconcileDeadline: farDeadline(),
	})
	if res.ret.RetCode != errorcode.ErrorCode_Success || task.resumeCalls != 1 || cb.GetStatus().IsPaused() {
		t.Fatalf("ret=%v calls=%d paused=%v", res.ret, task.resumeCalls, cb.GetStatus().IsPaused())
	}
}
