// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/semaphore"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	CubeLog "github.com/tencentcloud/CubeSandbox/pkgs/CubeLog"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

// fakeProc writes a /proc-like entry: cmdline args and a cwd symlink.
func fakeProc(t *testing.T, root string, pid string, args []string, cwd string) {
	t.Helper()
	dir := filepath.Join(root, pid)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cmdline"), []byte(strings.Join(args, "\x00")+"\x00"), 0o644))
	require.NoError(t, os.Symlink(cwd, filepath.Join(dir, "cwd")))
}

const shimBin = "/data/cubelet/root/component_versions/cube-shim/v0.7.1/bin/containerd-shim-cube-rs"

func exitedOrphanRecord(id string) *cubeboxstore.CubeBox {
	return newCubeboxWithStatusForTest(id, cubeboxstore.Status{FinishedAt: time.Now().UnixNano()})
}

func TestOrphanShimPIDs(t *testing.T) {
	root := t.TempDir()
	id := "sb-orphan"
	bundle := "/data/cubelet/state/io.containerd.runtime.v2.task/default/"
	// Orphan: shim for id, bundle deleted.
	fakeProc(t, root, "990001", []string{shimBin, "-namespace", "default", "-id", id}, bundle+"."+id+" (deleted)")
	// Live shim for id: bundle present. Never reaped.
	fakeProc(t, root, "990002", []string{shimBin, "-namespace", "default", "-id", id}, bundle+id)
	// Another sandbox's orphan shim.
	fakeProc(t, root, "990003", []string{shimBin, "-id", "sb-other"}, bundle+".sb-other (deleted)")
	// Not a shim, mentions the id, cwd deleted.
	fakeProc(t, root, "990004", []string{"/usr/bin/bash", "-c", "echo -id " + id}, "/tmp/x (deleted)")
	// Prefix of the id must not match.
	fakeProc(t, root, "990005", []string{shimBin, "-id", id + "-extra"}, bundle+".x (deleted)")
	require.Equal(t, []int{990001}, orphanShimPIDs(root, id))
	require.Empty(t, orphanShimPIDs(root, ""))
}

func withFakeProcAndKill(t *testing.T) (string, *[]int) {
	t.Helper()
	root := t.TempDir()
	prevRoot, prevKill := procRoot, shimKill
	var killed []int
	procRoot = root
	shimKill = func(pid int) error { killed = append(killed, pid); return nil }
	t.Cleanup(func() { procRoot, shimKill = prevRoot, prevKill })
	return root, &killed
}

// An exited record whose only trace is an orphan shim (no task, deleted
// bundle) is reaped and released instead of refused forever.
func TestCleanupReapsOrphanShimOfExitedRecord(t *testing.T) {
	root, killed := withFakeProcAndKill(t)
	id := "sb-exited-orphan"
	// pid chosen beyond pid_max so WaitProcessGone sees it as gone.
	fakeProc(t, root, "99999999", []string{shimBin, "-id", id}, "/state/."+id+" (deleted)")
	l := &local{probe: &fakeRuntimeProbe{containers: map[string]bool{id: true}}}
	ctx := workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{})
	require.NoError(t, l.requireRuntimeCleanupEvidenceFor(ctx, exitedOrphanRecord(id)))
	require.Equal(t, []int{99999999}, *killed)
	require.True(t, workflow.RuntimeCleanupProven(ctx))
}

func TestOrphanShimNotReapedWhenUnsafe(t *testing.T) {
	cases := map[string]struct {
		sb    func(string) *cubeboxstore.CubeBox
		probe *fakeRuntimeProbe
	}{
		"running record": {func(id string) *cubeboxstore.CubeBox {
			return newCubeboxWithStatusForTest(id, cubeboxstore.Status{StartedAt: 1})
		}, &fakeRuntimeProbe{}},
		"paused record": {func(id string) *cubeboxstore.CubeBox {
			return newCubeboxWithStatusForTest(id, cubeboxstore.Status{FinishedAt: 1, PausedAt: 1})
		}, &fakeRuntimeProbe{}},
		"task still present": {exitedOrphanRecord, &fakeRuntimeProbe{tasks: map[string]bool{"sb-unsafe": true}}},
		"task query fails":   {exitedOrphanRecord, &fakeRuntimeProbe{err: context.DeadlineExceeded}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			root, killed := withFakeProcAndKill(t)
			id := "sb-unsafe"
			fakeProc(t, root, "99999998", []string{shimBin, "-id", id}, "/state/."+id+" (deleted)")
			l := &local{probe: tc.probe}
			ctx := workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{})
			require.ErrorContains(t, l.requireRuntimeCleanupEvidenceFor(ctx, tc.sb(id)), "ownership unknown")
			require.Empty(t, *killed)
		})
	}
}

// A failed start earlier in this process leaves RuntimeMayExist on the id's
// lifecycle. Once the record is exited and the host shows no runtime, that
// flag alone must not block the exited proof.
func TestProvablyExitedIgnoresStaleRuntimeMayExist(t *testing.T) {
	id := "sb-stale-flag"
	l := &local{probe: &fakeRuntimeProbe{containers: map[string]bool{id: true}}}
	ctx := workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{RuntimeMayExist: true})
	ok, why := l.provablyExited(ctx, exitedOrphanRecord(id))
	require.True(t, ok, why)
	// A live host process still blocks it.
	l.probe = &fakeRuntimeProbe{procs: map[string]bool{id: true}}
	ok, _ = l.provablyExited(ctx, exitedOrphanRecord(id))
	require.False(t, ok)
}

// Records that ran and exited before this Cubelet process started are only
// ever reached by queued cleanups carrying a previous-process epoch. Those
// cleanups now proceed on the exited proof (after reaping an orphan shim);
// a paused record keeps the refusal.
func TestEngineCleanUpStaleEpochUsesExitedProof(t *testing.T) {
	root, killed := withFakeProcAndKill(t)
	id := "sb-gc-exited"
	fakeProc(t, root, "99999997", []string{shimBin, "-id", id}, "/state/."+id+" (deleted)")
	api := &deletingCubeboxAPI{fakeCubeboxAPI: fakeCubeboxAPI{cb: exitedOrphanRecord(id)}}
	l := &local{cubeboxManger: api, cbriManager: noopCbri{}, probe: &fakeRuntimeProbe{containers: map[string]bool{id: true}}}
	prev := workflow.StaleCleanupVerifier
	workflow.StaleCleanupVerifier = l.verifyStaleCleanup
	t.Cleanup(func() { workflow.StaleCleanupVerifier = prev })
	st := &storageFlow{}
	e := &workflow.Engine{}
	e.AddFlow("cleanup", &workflow.Workflow{Name: "cleanup", Limiter: semaphore.NewLimiter(1),
		Steps: []*workflow.Step{{Actions: []workflow.Flow{st}}}})
	stale := &workflow.LifecycleCleanup{Epoch: "previous-process"}
	ctx := CubeLog.WithRequestTrace(context.Background(), &CubeLog.RequestTrace{RequestID: "gc-exited"})
	require.NoError(t, e.CleanUp(ctx, &workflow.CleanContext{
		BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: id}, CleanupEvidence: stale}))
	require.Equal(t, 1, st.destroyed)
	require.Equal(t, []int{99999997}, *killed)

	api.cb = newCubeboxWithStatusForTest(id, cubeboxstore.Status{FinishedAt: 1, PausedAt: 1})
	require.Error(t, e.CleanUp(ctx, &workflow.CleanContext{
		BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: id}, CleanupEvidence: stale}))
	require.Equal(t, 1, st.destroyed)
}

func TestExitedRecordOwnsStorage(t *testing.T) {
	tpl := exitedOrphanRecord("own-tpl")
	tpl.InstanceType = cubebox.InstanceType_cubebox.String()
	tpl.Annotations = map[string]string{constants.MasterAnnotationAppSnapshotTemplateID: "tpl-x"}
	require.True(t, exitedRecordOwnsStorage(tpl))

	resume := exitedOrphanRecord("own-resume")
	resume.InstanceType = cubebox.InstanceType_cubebox.String()
	resume.Annotations = map[string]string{
		constants.MasterAnnotationPauseSnapshotID:   "snap-x",
		constants.MasterAnnotationRuntimeSnapshotID: "snap-x",
	}
	require.False(t, exitedRecordOwnsStorage(resume))
	require.True(t, exitedRecordOwnsStorage(crossNodeRecord("own-xnode")))
	require.False(t, exitedRecordOwnsStorage(nil))
}

// After a cleanup removed the record and failed in a later step, the
// following stale-epoch jobs finish when this process proved the runtime
// gone, and stay refused otherwise.
func TestVerifyStaleCleanupMissingRecord(t *testing.T) {
	id := "sb-missing-record"
	// Host still shows a runtime trace: refused.
	l := &local{cubeboxManger: &deletingCubeboxAPI{}, probe: &fakeRuntimeProbe{procs: map[string]bool{id: true}}}
	ok, _ := l.verifyStaleCleanup(context.Background(), id)
	require.False(t, ok)
	// Probe failure: refused.
	l.probe = &fakeRuntimeProbe{err: context.DeadlineExceeded}
	ok, _ = l.verifyStaleCleanup(context.Background(), id)
	require.False(t, ok)
	// No trace on the host (the case after a Cubelet restart): accepted.
	l.probe = &fakeRuntimeProbe{}
	ok, why0 := l.verifyStaleCleanup(context.Background(), id)
	require.True(t, ok, why0)
	// In-process proof works even while a probe cannot answer.
	l.probe = &fakeRuntimeProbe{err: context.DeadlineExceeded}
	ctx := workflow.BeginSandboxLifecycle(context.Background(), id)
	workflow.MarkRuntimeGone(ctx)
	ok, why := l.verifyStaleCleanup(context.Background(), id)
	require.True(t, ok, why)
	workflow.MarkRuntimeStarting(ctx)
	ok, _ = l.verifyStaleCleanup(context.Background(), id)
	require.False(t, ok)
}
