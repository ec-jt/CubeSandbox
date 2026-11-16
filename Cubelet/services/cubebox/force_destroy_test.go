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

	"github.com/stretchr/testify/require"

	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

func fakeProcTree(t *testing.T, procs map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for pid, cmd := range procs {
		dir := filepath.Join(root, pid)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "cmdline"), []byte(cmd), 0o644))
	}
	return root
}

func TestForceDestroyRequested(t *testing.T) {
	require.False(t, forceDestroyRequested(nil))
	require.False(t, forceDestroyRequested(&cubebox.DestroyCubeSandboxRequest{}))
	require.True(t, forceDestroyRequested(&cubebox.DestroyCubeSandboxRequest{
		Annotations: map[string]string{AnnotationOperatorForceDestroy: "true"}}))
	require.False(t, forceDestroyRequested(&cubebox.DestroyCubeSandboxRequest{
		Annotations: map[string]string{AnnotationOperatorForceDestroy: "no"}}))
}

// The incident: a never-started restore record whose storage the ordinary
// policy preserves. Force destroy kills leftovers, proves absence and
// releases record and storage.
func TestForceReclaimRuntimeReleasesPreservedRecord(t *testing.T) {
	id := "force-1"
	sb := crossNodeRecord(id)
	delete(sb.Annotations, "cube.master.snapshot.cross_node")

	prevRoot, prevKill, prevAlive := procRoot, forceKill, processAlive
	t.Cleanup(func() { procRoot, forceKill, processAlive = prevRoot, prevKill, prevAlive })
	procRoot = fakeProcTree(t, map[string]string{
		"4242": "containerd-shim-cube-rs\x00-id\x00" + id,
		"4343": "unrelated",
	})
	dead := map[int]bool{}
	var killed []int
	forceKill = func(pid int) error {
		killed = append(killed, pid)
		dead[pid] = true
		require.NoError(t, os.RemoveAll(filepath.Join(procRoot, "4242")))
		return nil
	}
	processAlive = func(pid int) bool { return !dead[pid] }

	// Without force: preserved ("may reuse retained disks").
	l := &local{probe: &fakeRuntimeProbe{}}
	ctx := workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{})
	require.ErrorContains(t, l.requireRuntimeCleanupEvidenceFor(ctx, sb), "ownership unknown")

	// With force: the real process scan finds and kills the shim, then the
	// probe (fake containerd, real /proc scan through procRoot) is clean.
	l = &local{probe: &procScanProbe{}}
	ctx = workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{PreserveStorage: true})
	require.NoError(t, l.forceReclaimRuntime(ctx, sb))
	require.Equal(t, []int{4242}, killed)
	require.True(t, workflow.RuntimeCleanupProven(ctx))
	require.False(t, workflow.PreserveRestoreStorage(ctx))
	require.NoError(t, l.requireRuntimeCleanupEvidenceFor(ctx, sb))
}

// procScanProbe is a containerd-less probe whose process check scans procRoot.
type procScanProbe struct{ fakeRuntimeProbe }

func (p *procScanProbe) ProcessExists(_ context.Context, id string) (bool, error) {
	return hostProcessMentions(procRoot, id)
}

func TestForceReclaimRuntimeFailsClosed(t *testing.T) {
	prevRoot := procRoot
	t.Cleanup(func() { procRoot = prevRoot })
	procRoot = fakeProcTree(t, nil)

	cases := map[string]struct {
		probe  runtimeProbe
		mutate func(*testing.T, string)
		want   string
	}{
		"containerd task": {probe: &fakeRuntimeProbe{tasks: map[string]bool{"f": true}}, want: "containerd task"},
		"query error":     {probe: &fakeRuntimeProbe{err: errors.New("down")}, want: "cannot query"},
		"shim remains":    {probe: &fakeRuntimeProbe{shims: map[string]bool{"f": true}}, want: "runtime still present"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			sb := crossNodeRecord("f")
			l := &local{probe: tc.probe}
			ev := &workflow.LifecycleCleanup{PreserveStorage: true}
			ctx := workflow.WithLifecycleCleanup(context.Background(), ev)
			err := l.forceReclaimRuntime(ctx, sb)
			require.ErrorContains(t, err, tc.want)
			require.False(t, ev.RuntimeGone)
			require.True(t, ev.PreserveStorage)
		})
	}

	// A paused record is never force destroyed.
	sb := crossNodeRecord("p")
	setStatus(sb, func(s *cubeboxstore.Status) { s.PausedAt = 1 })
	l := &local{probe: &fakeRuntimeProbe{}}
	ctx := workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{})
	require.ErrorContains(t, l.forceReclaimRuntime(ctx, sb), "paused")
}
