// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"context"
	"errors"
	"testing"
	"time"

	CubeLog "github.com/tencentcloud/CubeSandbox/pkgs/CubeLog"

	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/internal/cbri"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/container/runc"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/semaphore"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/cube/internals/cubes"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

type fakeRuntimeProbe struct {
	containers, tasks, shims map[string]bool
	err                      error
}

func (f *fakeRuntimeProbe) check(m map[string]bool, id string) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	return m[id], nil
}
func (f *fakeRuntimeProbe) ContainerExists(_ context.Context, id string) (bool, error) {
	return f.check(f.containers, id)
}
func (f *fakeRuntimeProbe) TaskExists(_ context.Context, id string) (bool, error) {
	return f.check(f.tasks, id)
}
func (f *fakeRuntimeProbe) ShimExists(_ context.Context, id string) (bool, error) {
	return f.check(f.shims, id)
}

// crossNodeRecord mirrors the record left on the target node in the
// 2026-11-13 incident: created, pid 0, cross-node restore annotations.
func crossNodeRecord(id string) *cubeboxstore.CubeBox {
	sb := newCubeboxWithStatusForTest(id, cubeboxstore.Status{CreatedAt: time.Now().UnixNano()})
	sb.InstanceType = cubebox.InstanceType_cubebox.String()
	sb.Annotations = map[string]string{
		constants.MasterAnnotationSnapshotCrossNode:   "true",
		constants.MasterAnnotationSnapshotRemoteUUIDs: `{"rootfs":"u-r","memory":"u-m","metadata":"u-meta"}`,
		constants.MasterAnnotationPauseSnapshotID:     "snap-x",
		constants.MasterAnnotationRuntimeSnapshotID:   "snap-x",
		constants.MasterAnnotationStorageBackend:      "s3",
	}
	return sb
}

func TestProvablyNeverStarted(t *testing.T) {
	cases := map[string]struct {
		mutate func(*cubeboxstore.CubeBox, *fakeRuntimeProbe)
		want   bool
	}{
		"pid 0, no task, no shim": {func(*cubeboxstore.CubeBox, *fakeRuntimeProbe) {}, true},
		"task exists": {func(_ *cubeboxstore.CubeBox, p *fakeRuntimeProbe) {
			p.tasks = map[string]bool{"sb-ns": true}
		}, false},
		"containerd container exists": {func(_ *cubeboxstore.CubeBox, p *fakeRuntimeProbe) {
			p.containers = map[string]bool{"sb-ns": true}
		}, false},
		"shim exists": {func(_ *cubeboxstore.CubeBox, p *fakeRuntimeProbe) {
			p.shims = map[string]bool{"sb-ns": true}
		}, false},
		"nonzero pid": {func(sb *cubeboxstore.CubeBox, _ *fakeRuntimeProbe) {
			sb.GetStatus().Update(func(s cubeboxstore.Status) (cubeboxstore.Status, error) { s.Pid = 4242; return s, nil })
		}, false},
		"endpoint pid": {func(sb *cubeboxstore.CubeBox, _ *fakeRuntimeProbe) { sb.Endpoint.Pid = 7 }, false},
		"paused": {func(sb *cubeboxstore.CubeBox, _ *fakeRuntimeProbe) {
			sb.GetStatus().Update(func(s cubeboxstore.Status) (cubeboxstore.Status, error) { s.PausedAt = 1; return s, nil })
		}, false},
		"was running": {func(sb *cubeboxstore.CubeBox, _ *fakeRuntimeProbe) {
			sb.GetStatus().Update(func(s cubeboxstore.Status) (cubeboxstore.Status, error) { s.StartedAt = 1; return s, nil })
		}, false},
		"evidence query error": {func(_ *cubeboxstore.CubeBox, p *fakeRuntimeProbe) {
			p.err = errors.New("containerd unavailable")
		}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			sb := crossNodeRecord("sb-ns")
			p := &fakeRuntimeProbe{}
			tc.mutate(sb, p)
			l := &local{probe: p}
			got, reason := l.provablyNeverStarted(context.Background(), sb)
			require.Equal(t, tc.want, got, reason)
		})
	}
	// Unresolved runtime start in this process is never overridden.
	l := &local{probe: &fakeRuntimeProbe{}}
	ctx := workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{RuntimeMayExist: true})
	ok, _ := l.provablyNeverStarted(ctx, crossNodeRecord("sb-ns"))
	require.False(t, ok)
}

// deletingCubeboxAPI removes the record on Delete, like the real store.
type deletingCubeboxAPI struct {
	fakeCubeboxAPI
	deletes int
}

func (d *deletingCubeboxAPI) Get(_ context.Context, id string) (*cubeboxstore.CubeBox, error) {
	if d.cb != nil && d.cb.ID == id {
		return d.cb, nil
	}
	return nil, utils.ErrorKeyNotFound
}
func (d *deletingCubeboxAPI) Delete(_ context.Context, opt *cubes.DeleteOption) error {
	if opt.ContainerID == "" && d.cb != nil && d.cb.ID == opt.CubeboxID {
		d.cb = nil
		d.deletes++
	}
	return nil
}

type noopCbri struct{}

func (noopCbri) SetCubeRuntimeImplementation(cbri.CubeRuntimeImplementation) {}
func (noopCbri) GetPassthroughMounts(context.Context, *workflow.CreateContext) ([]specs.Mount, error) {
	return nil, nil
}
func (noopCbri) CreateSandbox(context.Context, *workflow.CreateContext) ([]oci.SpecOpts, error) {
	return nil, nil
}
func (noopCbri) CreateContainer(context.Context, *cubeboxstore.CubeBox, *cubeboxstore.Container) ([]oci.SpecOpts, error) {
	return nil, nil
}
func (noopCbri) PostCreateContainer(context.Context, *cubeboxstore.CubeBox, *cubeboxstore.Container) error {
	return nil
}
func (noopCbri) DestroySandbox(context.Context, string) error { return nil }

// storageFlow stands in for the storage plugin: it records whether storage
// Destroy ran and refuses, like storage.local.Destroy, when evidence says to
// preserve.
type storageFlow struct{ destroyed int }

func (f *storageFlow) ID() string                                            { return "storage" }
func (f *storageFlow) Init(context.Context, *workflow.InitInfo) error        { return nil }
func (f *storageFlow) Create(context.Context, *workflow.CreateContext) error { return nil }
func (f *storageFlow) CleanUp(context.Context, *workflow.CleanContext) error {
	f.destroyed++
	return nil
}
func (f *storageFlow) Destroy(ctx context.Context, _ *workflow.DestroyContext) error {
	if workflow.PreserveRestoreStorage(ctx) {
		return errors.New("restore storage retained")
	}
	f.destroyed++
	return nil
}

func newPrecheckService(t *testing.T, sb *cubeboxstore.CubeBox, probe runtimeProbe) (*service, *deletingCubeboxAPI, *storageFlow) {
	t.Helper()
	runc.Init(t.TempDir())
	api := &deletingCubeboxAPI{fakeCubeboxAPI: fakeCubeboxAPI{cb: sb}}
	l := &local{cubeboxManger: api, cbriManager: noopCbri{}, probe: probe, config: &CubeConfig{}}
	st := &storageFlow{}
	e := &workflow.Engine{}
	e.AddFlow("destroy", &workflow.Workflow{Name: "destroy", Limiter: semaphore.NewLimiter(1),
		Steps: []*workflow.Step{{Actions: []workflow.Flow{l}}, {Actions: []workflow.Flow{st}}}})
	return &service{engine: e, cubeboxMgr: l, config: &ServicesConfig{destroyDeadline: 5 * time.Second},
		sandboxLifecycleLocks: utils.NewResourceLocks()}, api, st
}

func TestCreatePrecheckCleansStaleNeverStartedRecord(t *testing.T) {
	var released []string
	prev := releaseCrossNodeImports
	releaseCrossNodeImports = func(_ context.Context, id string) error { released = append(released, id); return nil }
	t.Cleanup(func() { releaseCrossNodeImports = prev })

	id := "b18-stale-" + t.Name()
	s, api, st := newPrecheckService(t, crossNodeRecord(id), &fakeRuntimeProbe{})
	unlock := workflow.SandboxLifecycleLocks.Lock(id)
	defer unlock()
	ok, msg := s.precheckExistingSandbox(context.Background(), id)
	require.True(t, ok, msg)
	require.Equal(t, 1, api.deletes, "stale record removed")
	require.Equal(t, 1, st.destroyed, "storage destroy ran")
	require.Equal(t, []string{id}, released, "cross-node imports released")
	require.NoError(t, workflow.CheckCreateRuntimeOwnership(id))
}

func TestCreatePrecheckKeepsLiveOrPausedRecord(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*cubeboxstore.CubeBox)
		probe  *fakeRuntimeProbe
		wantOK bool
	}{
		"running": {func(sb *cubeboxstore.CubeBox) {
			setStatus(sb, func(s *cubeboxstore.Status) { s.StartedAt = 1; s.Pid = 9 })
		}, &fakeRuntimeProbe{}, false},
		"task exists":   {func(*cubeboxstore.CubeBox) {}, &fakeRuntimeProbe{tasks: map[string]bool{"__ID__": true}}, false},
		"probe error":   {func(*cubeboxstore.CubeBox) {}, &fakeRuntimeProbe{err: errors.New("down")}, false},
		"pid only":      {func(sb *cubeboxstore.CubeBox) { setStatus(sb, func(s *cubeboxstore.Status) { s.Pid = 9 }) }, &fakeRuntimeProbe{}, false},
		"paused passes": {func(sb *cubeboxstore.CubeBox) { setStatus(sb, func(s *cubeboxstore.Status) { s.PausedAt = 1 }) }, &fakeRuntimeProbe{}, true},
	} {
		t.Run(name, func(t *testing.T) {
			id := "b18-live-" + name
			sb := crossNodeRecord(id)
			tc.mutate(sb)
			if tc.probe.tasks != nil {
				tc.probe.tasks = map[string]bool{id: true}
			}
			s, api, st := newPrecheckService(t, sb, tc.probe)
			ok, msg := s.precheckExistingSandbox(context.Background(), id)
			require.Equal(t, tc.wantOK, ok, msg)
			if !tc.wantOK {
				require.Equal(t, "sandbox already exists", msg)
			}
			require.Zero(t, api.deletes, "record must be kept")
			require.Zero(t, st.destroyed, "storage must be kept")
		})
	}
}

func TestCreatePrecheckKeepsSameNodePauseResumeRecord(t *testing.T) {
	id := "b18-samenode"
	sb := crossNodeRecord(id)
	delete(sb.Annotations, constants.MasterAnnotationSnapshotCrossNode)
	s, api, st := newPrecheckService(t, sb, &fakeRuntimeProbe{})
	ok, msg := s.precheckExistingSandbox(context.Background(), id)
	require.False(t, ok)
	require.Equal(t, "sandbox already exists", msg)
	require.Zero(t, api.deletes)
	require.Zero(t, st.destroyed)
}

func setStatus(sb *cubeboxstore.CubeBox, f func(*cubeboxstore.Status)) {
	sb.GetStatus().Update(func(s cubeboxstore.Status) (cubeboxstore.Status, error) { f(&s); return s, nil })
}

// Failover after a failure injected post-import (cbri hook error before any
// container/task) must remove the record and run storage destroy; a failure
// after the runtime spawn point must keep both.
func TestCrossNodeFailoverRollback(t *testing.T) {
	for _, spawned := range []bool{false, true} {
		name := "before-runtime"
		if spawned {
			name = "after-runtime"
		}
		t.Run(name, func(t *testing.T) {
			id := "b18-rb-" + name
			sb := crossNodeRecord(id)
			s, api, st := newPrecheckService(t, sb, &fakeRuntimeProbe{})
			opts := &workflow.CreateContext{ReqInfo: &cubebox.RunCubeSandboxRequest{
				Annotations: sb.Annotations, InstanceType: sb.InstanceType}, CubeBoxCreated: true, RuntimeSpawnAttempted: spawned}
			opts.SandboxID = id
			evidence := &workflow.LifecycleCleanup{}
			workflow.ApplyCreateFailureEvidence(evidence, opts)
			ctx := workflow.WithLifecycleCleanup(context.Background(), evidence)
			ctx = constants.WithFailoverOperation(ctx)
			ctx = CubeLog.WithRequestTrace(ctx, &CubeLog.RequestTrace{RequestID: "b18"})
			dctx := &workflow.DestroyContext{BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: id, IsRollBack: true},
				DestroyInfo: &cubebox.DestroyCubeSandboxRequest{SandboxID: id}}
			err := s.engine.Destroy(ctx, dctx)
			if spawned {
				require.Error(t, err)
				require.Zero(t, api.deletes, "record kept when a runtime may exist")
				require.Zero(t, st.destroyed, "storage kept when a runtime may exist")
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, api.deletes, "record removed")
			require.Equal(t, 1, st.destroyed, "imported storage destroyed")
		})
	}
}

func TestReleaseFailedCrossNodeRestore(t *testing.T) {
	var released []string
	prev := releaseCrossNodeImports
	releaseCrossNodeImports = func(_ context.Context, id string) error { released = append(released, id); return nil }
	t.Cleanup(func() { releaseCrossNodeImports = prev })
	s := &service{cubeboxMgr: &local{cubeboxManger: &deletingCubeboxAPI{}}}

	s.releaseFailedCrossNodeRestore(workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{}), "sb-a")
	require.Equal(t, []string{"sb-a"}, released)

	released = nil
	s.releaseFailedCrossNodeRestore(workflow.WithLifecycleCleanup(context.Background(),
		&workflow.LifecycleCleanup{RuntimeMayExist: true, RuntimeGone: true}), "sb-b")
	require.Empty(t, released, "a runtime start was attempted: preserve")

	released = nil
	s.cubeboxMgr.cubeboxManger.(*deletingCubeboxAPI).cb = crossNodeRecord("sb-c")
	s.releaseFailedCrossNodeRestore(workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{}), "sb-c")
	require.Empty(t, released, "record still present: leave to Destroy/CleanUp")
}

// The incident: CleanUp refused forever with "runtime ownership unknown" for a
// record with pid 0 and no evidence from the current process.
func TestCleanUpAcceptsNeverStartedRecord(t *testing.T) {
	runc.Init(t.TempDir())
	id := "b18-cleanup"
	api := &deletingCubeboxAPI{fakeCubeboxAPI: fakeCubeboxAPI{cb: crossNodeRecord(id)}}
	l := &local{cubeboxManger: api, cbriManager: noopCbri{}, probe: &fakeRuntimeProbe{}}
	ctx := workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{})
	err := l.requireRuntimeCleanupEvidenceFor(ctx, api.cb)
	require.NoError(t, err)
	require.True(t, workflow.RuntimeCleanupProven(ctx))
	require.False(t, workflow.PreserveRestoreStorage(ctx))

	// Destroy path: same record no longer refused.
	api.cb = crossNodeRecord(id)
	ctx = workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{})
	ctx = constants.WithFailoverOperation(ctx)
	require.NoError(t, l.Destroy(ctx, &workflow.DestroyContext{BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: id},
		DestroyInfo: &cubebox.DestroyCubeSandboxRequest{SandboxID: id}}))
	require.Equal(t, 1, api.deletes)

	// Still refused when a shim exists or the probe fails.
	for _, p := range []*fakeRuntimeProbe{{shims: map[string]bool{id: true}}, {err: errors.New("down")}} {
		l.probe = p
		ctx = workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{})
		require.ErrorContains(t, l.requireRuntimeCleanupEvidenceFor(ctx, crossNodeRecord(id)), "ownership unknown")
	}
}

// A queued cleanup from an earlier process carries a stale epoch; it now
// proceeds for a never-started record instead of refusing forever.
func TestEngineCleanUpStaleEpochUsesNeverStartedProof(t *testing.T) {
	id := "b18-gc-stale"
	api := &deletingCubeboxAPI{fakeCubeboxAPI: fakeCubeboxAPI{cb: crossNodeRecord(id)}}
	l := &local{cubeboxManger: api, cbriManager: noopCbri{}, probe: &fakeRuntimeProbe{}}
	prev := workflow.StaleCleanupVerifier
	workflow.StaleCleanupVerifier = l.verifyStaleCleanup
	t.Cleanup(func() { workflow.StaleCleanupVerifier = prev })
	st := &storageFlow{}
	e := &workflow.Engine{}
	e.AddFlow("cleanup", &workflow.Workflow{Name: "cleanup", Limiter: semaphore.NewLimiter(1),
		Steps: []*workflow.Step{{Actions: []workflow.Flow{st}}}})
	stale := &workflow.LifecycleCleanup{Epoch: "previous-process"}
	ctx := CubeLog.WithRequestTrace(context.Background(), &CubeLog.RequestTrace{RequestID: "b18-gc"})
	require.NoError(t, e.CleanUp(ctx, &workflow.CleanContext{
		BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: id}, CleanupEvidence: stale}))
	require.Equal(t, 1, st.destroyed)

	// A started record keeps the refusal.
	setStatus(api.cb, func(s *cubeboxstore.Status) { s.StartedAt = 1 })
	require.Error(t, e.CleanUp(ctx, &workflow.CleanContext{
		BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: id}, CleanupEvidence: stale}))
	require.Equal(t, 1, st.destroyed)
	_ = errorcode.ErrorCode_Success
}
