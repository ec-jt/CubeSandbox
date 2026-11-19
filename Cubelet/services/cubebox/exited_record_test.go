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

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/container/runc"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

// exitedRecord mirrors the records stuck on app1/app2 on 2026-11-15: the
// sandbox ran, Cubelet restarted, the recovered record says EXITED and carries
// no pid, and no shim, task or VMM is left.
func exitedRecord(id string) *cubeboxstore.CubeBox {
	now := time.Now().UnixNano()
	sb := newCubeboxWithStatusForTest(id, cubeboxstore.Status{
		CreatedAt:  now - int64(time.Hour),
		StartedAt:  now - int64(time.Hour) + 1,
		FinishedAt: now,
	})
	sb.InstanceType = cubebox.InstanceType_cubebox.String()
	sb.Annotations = map[string]string{constants.MasterAnnotationStorageBackend: "s3"}
	return sb
}

func TestProvablyExited(t *testing.T) {
	cases := map[string]struct {
		mutate func(*cubeboxstore.CubeBox, *fakeRuntimeProbe)
		want   bool
	}{
		"exited, pid 0, nothing left": {func(*cubeboxstore.CubeBox, *fakeRuntimeProbe) {}, true},
		"still running": {func(sb *cubeboxstore.CubeBox, _ *fakeRuntimeProbe) {
			setStatus(sb, func(s *cubeboxstore.Status) { s.FinishedAt = 0 })
		}, false},
		"paused": {func(sb *cubeboxstore.CubeBox, _ *fakeRuntimeProbe) {
			setStatus(sb, func(s *cubeboxstore.Status) { s.PausedAt = 1 })
		}, false},
		"pausing": {func(sb *cubeboxstore.CubeBox, _ *fakeRuntimeProbe) {
			setStatus(sb, func(s *cubeboxstore.Status) { s.PausingAt = 1 })
		}, false},
		"carries pid": {func(sb *cubeboxstore.CubeBox, _ *fakeRuntimeProbe) {
			setStatus(sb, func(s *cubeboxstore.Status) { s.Pid = 4242 })
		}, false},
		"endpoint pid": {func(sb *cubeboxstore.CubeBox, _ *fakeRuntimeProbe) { sb.Endpoint.Pid = 7 }, false},
		"shim exists": {func(sb *cubeboxstore.CubeBox, p *fakeRuntimeProbe) {
			p.shims = map[string]bool{sb.ID: true}
		}, false},
		"task exists": {func(sb *cubeboxstore.CubeBox, p *fakeRuntimeProbe) {
			p.tasks = map[string]bool{sb.ID: true}
		}, false},
		// Container metadata alone is what an exited record keeps until
		// Destroy deletes it; it is not a runtime.
		"containerd container metadata only": {func(sb *cubeboxstore.CubeBox, p *fakeRuntimeProbe) {
			p.containers = map[string]bool{sb.ID: true}
		}, true},
		"host process names the id": {func(sb *cubeboxstore.CubeBox, p *fakeRuntimeProbe) {
			p.procs = map[string]bool{sb.ID: true}
		}, false},
		"probe error": {func(_ *cubeboxstore.CubeBox, p *fakeRuntimeProbe) {
			p.err = errors.New("containerd unavailable")
		}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			sb := exitedRecord("sb-exited")
			p := &fakeRuntimeProbe{}
			tc.mutate(sb, p)
			l := &local{probe: p}
			got, reason := l.provablyExited(context.Background(), sb)
			require.Equal(t, tc.want, got, reason)
		})
	}

	// A failed start earlier in this process leaves RuntimeMayExist set. For
	// an EXITED record that flag alone is not a runtime: the host checks
	// decide (see TestProvablyExitedIgnoresStaleRuntimeMayExist).
	l := &local{probe: &fakeRuntimeProbe{}}
	ctx := workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{RuntimeMayExist: true})
	ok, why := l.provablyExited(ctx, exitedRecord("sb-exited"))
	require.True(t, ok, why)
	l.probe = &fakeRuntimeProbe{shims: map[string]bool{"sb-exited": true}}
	ok, _ = l.provablyExited(ctx, exitedRecord("sb-exited"))
	require.False(t, ok)
}

// The 2026-11-15 incident: Destroy refused forever for an exited record that
// had lost its pids across a Cubelet restart.
func TestDestroyAcceptsProvablyExitedRecord(t *testing.T) {
	runc.Init(t.TempDir())
	id := "exited-destroy"
	api := &deletingCubeboxAPI{fakeCubeboxAPI: fakeCubeboxAPI{cb: exitedRecord(id)}}
	l := &local{cubeboxManger: api, cbriManager: noopCbri{}, probe: &fakeRuntimeProbe{}}

	ctx := workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{})
	require.NoError(t, l.requireRuntimeCleanupEvidenceFor(ctx, api.cb))
	require.True(t, workflow.RuntimeCleanupProven(ctx))

	// Storage policy is untouched: a preserve request stays a preserve.
	preserve := &workflow.LifecycleCleanup{PreserveStorage: true}
	ctx = workflow.WithLifecycleCleanup(context.Background(), preserve)
	require.NoError(t, l.requireRuntimeCleanupEvidenceFor(ctx, exitedRecord(id)))
	require.True(t, workflow.PreserveRestoreStorage(ctx), "exited proof must not drop PreserveStorage")

	// Full Destroy removes the record.
	ctx = workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{})
	ctx = constants.WithFailoverOperation(ctx)
	require.NoError(t, l.Destroy(ctx, &workflow.DestroyContext{BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: id},
		DestroyInfo: &cubebox.DestroyCubeSandboxRequest{SandboxID: id}}))
	require.Equal(t, 1, api.deletes)

	// Any trace of a runtime keeps the refusal.
	for _, p := range []*fakeRuntimeProbe{
		{shims: map[string]bool{id: true}},
		{procs: map[string]bool{id: true}},
		{err: errors.New("down")},
	} {
		l.probe = p
		ctx = workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{})
		require.ErrorContains(t, l.requireRuntimeCleanupEvidenceFor(ctx, exitedRecord(id)), "ownership unknown")
		require.False(t, workflow.RuntimeCleanupProven(ctx))
	}

	// A running record (not exited) keeps the refusal too.
	l.probe = &fakeRuntimeProbe{}
	running := exitedRecord(id)
	setStatus(running, func(s *cubeboxstore.Status) { s.FinishedAt = 0 })
	ctx = workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{})
	require.ErrorContains(t, l.requireRuntimeCleanupEvidenceFor(ctx, running), "ownership unknown")
}

func TestHostProcessMentions(t *testing.T) {
	root := t.TempDir()
	write := func(pid, cmdline string) {
		dir := filepath.Join(root, pid)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "cmdline"), []byte(cmdline), 0o644))
	}
	write("100", "containerd-shim-cube-rs\x00-namespace\x00default\x00-id\x00sb-live\x00")
	write("200", "sleep\x00100\x00")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "self"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "300"), 0o755)) // exited mid-scan: no cmdline

	got, err := hostProcessMentions(root, "sb-live")
	require.NoError(t, err)
	require.True(t, got)

	got, err = hostProcessMentions(root, "sb-gone")
	require.NoError(t, err)
	require.False(t, got)

	_, err = hostProcessMentions(filepath.Join(root, "missing"), "sb-gone")
	require.Error(t, err, "an unreadable proc root is 'cannot tell'")

	_, err = hostProcessMentions(root, " ")
	require.Error(t, err)
}
