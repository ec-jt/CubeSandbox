package cubebox

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/controller/runtemplate/templatetypes"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func seedSourceFence(t *testing.T, id string) {
	t.Helper()
	prior := workflow.SourceFences
	t.Cleanup(func() { workflow.SourceFences = prior })
	s, e := sourcefence.Open(filepath.Join(t.TempDir(), "fences"))
	require.NoError(t, e)
	workflow.SourceFences = s
	_, e = s.Stop(context.Background(), true, sourcefence.Binding{"op", id, "node", "epoch", "v1", strings.Repeat("a", 64)}, func() ([]sourcefence.Process, error) {
		return []sourcefence.Process{{20, "1", "boot", "shim"}, {21, "2", "boot", "vmm"}}, nil
	}, func(context.Context, []sourcefence.Process) error { return errors.New("cannot stop") })
	require.Error(t, e)
}

func TestSourceFenceRPCGuardsConfigOff(t *testing.T) {
	seedSourceFence(t, "source")
	s, _, flow := newDestroyLifecycleServiceForTest(nil)
	r := destroySandboxForTest(t, s, "source")
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, r.Ret.RetCode)
	require.Zero(t, flow.destroyCalls)
	u, e := s.Update(context.Background(), &cubebox.UpdateCubeSandboxRequest{SandboxID: "source", Annotations: map[string]string{constants.MasterAnnotationsUpdateAction: constants.UpdateActionResume}})
	require.NoError(t, e)
	require.Contains(t, u.Ret.RetMsg, "fence")
	c, e := s.Create(context.Background(), &cubebox.RunCubeSandboxRequest{Annotations: map[string]string{constants.MasterAnnotationDesiredSandboxID: "source"}})
	require.NoError(t, e)
	require.Contains(t, c.Ret.RetMsg, "fence")
	x, e := s.Exec(context.Background(), &cubebox.ExecCubeSandboxRequest{SandboxId: "source", ContainerId: "ctr"})
	require.NoError(t, e)
	require.Contains(t, x.Ret.RetMsg, "fence")
	p, e := s.CleanupTemplate(context.Background(), &cubebox.CleanupTemplateRequest{TemplateID: "selected-v1"})
	require.NoError(t, e)
	require.Contains(t, p.Ret.RetMsg, "fence")
	require.ErrorIs(t, s.engine.CleanUp(context.Background(), &workflow.CleanContext{BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: "source"}}), sourcefence.ErrFenced)
	commit, e := s.CommitSandbox(context.Background(), &cubebox.CommitSandboxRequest{SandboxID: "source", TemplateID: "selected-v1"})
	require.NoError(t, e)
	require.Contains(t, commit.Ret.RetMsg, "fence")
	rollback, e := s.RollbackSandbox(context.Background(), &cubebox.RollbackSandboxRequest{SandboxID: "source", SnapshotID: "selected-v1", NewGen: 2})
	require.NoError(t, e)
	require.Contains(t, rollback.Ret.RetMsg, "fence")
	app, e := s.AppSnapshot(context.Background(), &cubebox.AppSnapshotRequest{})
	require.NoError(t, e)
	require.Contains(t, app.Ret.RetMsg, "fence")
	require.Zero(t, flow.cleanupCalls)
	require.ErrorIs(t, s.cubeboxMgr.Init(context.Background(), nil), sourcefence.ErrFenced)
}

func TestSourceFenceIncarnationAndUnreachable(t *testing.T) {
	cb := &cubeboxstore.CubeBox{Metadata: cubeboxstore.Metadata{ID: "source", CreatedAt: 1}, LocalRunTemplate: &templatetypes.LocalRunTemplate{}}
	require.NoError(t, cb.BeginGuestMetricsEpoch(cubeboxstore.GuestMetricsEpochFreshCreate, time.Now()))
	inc, e := sourceIncarnation(context.Background(), cb)
	require.NoError(t, e)
	require.NoError(t, cb.BeginGuestMetricsEpoch(cubeboxstore.GuestMetricsEpochRollback, time.Now()))
	newInc, e := sourceIncarnation(context.Background(), cb)
	require.NoError(t, e)
	require.NotEqual(t, inc, newInc)
	s, _, _ := newDestroyLifecycleServiceForTest(cb)
	s.cubeboxMgr.config.SourceFenceEnabled = true
	s.cubeboxMgr.config.SourceFenceNodeID = "node"
	b := sourcefence.Binding{"op", "source", "node", inc, "v1", strings.Repeat("a", 64)}
	request := func() *cubebox.UpdateCubeSandboxRequest {
		raw, _ := json.Marshal(b)
		return &cubebox.UpdateCubeSandboxRequest{RequestID: "op", SandboxID: "source", Annotations: map[string]string{constants.MasterAnnotationsUpdateAction: sourceFenceAction, sourceFenceBindingAnnotation: string(raw)}}
	}
	r, e := s.Update(context.Background(), request())
	require.NoError(t, e)
	require.Contains(t, r.Ret.RetMsg, "binding mismatch")
	b.Incarnation = newInc
	prior := workflow.SourceFences
	t.Cleanup(func() { workflow.SourceFences = prior })
	workflow.SourceFences, e = sourcefence.Open(filepath.Join(t.TempDir(), "fences"))
	require.NoError(t, e)
	r, e = s.Update(context.Background(), request())
	require.NoError(t, e)
	require.Contains(t, r.Ret.RetMsg, "bundle missing")
	require.NoError(t, workflow.SourceFences.Check("source"))
	s.cubeboxMgr.cubeboxManger = &missingRuntimeAPI{}
	r, e = s.Update(context.Background(), request())
	require.NoError(t, e)
	require.Contains(t, r.Ret.RetMsg, "missing/unreachable")
}

func TestSourceFenceIncarnationEmptyNormalization(t *testing.T) {
	cb := &cubeboxstore.CubeBox{Metadata: cubeboxstore.Metadata{ID: "source", CreatedAt: 1}, LocalRunTemplate: &templatetypes.LocalRunTemplate{}}
	require.NoError(t, cb.BeginGuestMetricsEpoch(cubeboxstore.GuestMetricsEpochFreshCreate, time.Now()))
	nilHash, err := sourceIncarnation(context.Background(), cb)
	require.NoError(t, err)
	cb.LocalRunTemplate.Volumes = map[string]templatetypes.LocalBaseVolume{}
	emptyHash, err := sourceIncarnation(context.Background(), cb)
	require.NoError(t, err)
	require.Equal(t, nilHash, emptyHash)
	legacy, err := sourceIncarnationVersion(context.Background(), cb, true)
	require.NoError(t, err)
	require.NotEqual(t, legacy, emptyHash)
	cb.LocalRunTemplate.Volumes["rootfs"] = templatetypes.LocalBaseVolume{VolumeID: "native-volume"}
	nonempty, err := sourceIncarnation(context.Background(), cb)
	require.NoError(t, err)
	oldNonempty, err := sourceIncarnationVersion(context.Background(), cb, true)
	require.NoError(t, err)
	require.Equal(t, oldNonempty, nonempty)
	require.NotEqual(t, nonempty, emptyHash)
}

func rootfsMountBox(t *testing.T, mounts ...*cubebox.VolumeMounts) *cubeboxstore.CubeBox {
	t.Helper()
	cb := &cubeboxstore.CubeBox{Metadata: cubeboxstore.Metadata{ID: "source", CreatedAt: 1}}
	cb.AddContainer(&cubeboxstore.Container{
		Metadata: cubeboxstore.Metadata{ID: "ctr", Config: &cubebox.ContainerConfig{VolumeMounts: mounts}},
	})
	return cb
}

func TestSourceFenceFreshImageCohortRequiresTypedOwnership(t *testing.T) {
	// A nil LocalRunTemplate is NOT an identity. Without the persisted typed
	// ownership marker plus the live native volume, admission is refused rather
	// than fabricated from the caller's request.
	cb := rootfsMountBox(t, &cubebox.VolumeMounts{Name: "root", ContainerPath: "/"})
	require.NoError(t, cb.BeginGuestMetricsEpoch(cubeboxstore.GuestMetricsEpochFreshCreate, time.Now()))
	inc, err := sourceIncarnation(context.Background(), cb)
	require.Error(t, err)
	require.Empty(t, inc)
	// The legacy empty-map replay flag must not rescue the fresh cohort either.
	inc, err = sourceIncarnationVersion(context.Background(), cb, true)
	require.Error(t, err)
	require.Empty(t, inc)
}

func TestRootfsVolumeMountNameIsTyped(t *testing.T) {
	if _, err := rootfsVolumeMountName(nil); err == nil {
		t.Fatal("nil record must be refused")
	}
	empty := &cubeboxstore.CubeBox{Metadata: cubeboxstore.Metadata{ID: "source"}}
	if _, err := rootfsVolumeMountName(empty); err == nil {
		t.Fatal("record without a rootfs mount must be refused")
	}
	good := rootfsMountBox(t, &cubebox.VolumeMounts{Name: "root", ContainerPath: "/"})
	name, err := rootfsVolumeMountName(good)
	require.NoError(t, err)
	require.Equal(t, "root", name)
	ambiguous := &cubeboxstore.CubeBox{Metadata: cubeboxstore.Metadata{ID: "source"}}
	ambiguous.AddContainer(&cubeboxstore.Container{Metadata: cubeboxstore.Metadata{ID: "a", Config: &cubebox.ContainerConfig{VolumeMounts: []*cubebox.VolumeMounts{{Name: "root", ContainerPath: "/"}}}}})
	ambiguous.AddContainer(&cubeboxstore.Container{Metadata: cubeboxstore.Metadata{ID: "b", Config: &cubebox.ContainerConfig{VolumeMounts: []*cubebox.VolumeMounts{{Name: "other", ContainerPath: "/"}}}}})
	if _, err := rootfsVolumeMountName(ambiguous); err == nil {
		t.Fatal("ambiguous rootfs mounts must be refused")
	}
}

func TestSourceFenceWaitsForLifecycleAndHonorsCancel(t *testing.T) {
	s, _, _ := newDestroyLifecycleServiceForTest(nil)
	s.cubeboxMgr.config.SourceFenceNodeID = "node"
	b := sourcefence.Binding{"op", "source", "node", "epoch", "v1", strings.Repeat("a", 64)}
	raw, _ := json.Marshal(b)
	u := s.sandboxLifecycleLocks.Lock("source")
	defer u()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	r, e := s.Update(ctx, &cubebox.UpdateCubeSandboxRequest{RequestID: "op", SandboxID: "source", Annotations: map[string]string{constants.MasterAnnotationsUpdateAction: sourceFenceAction, sourceFenceBindingAnnotation: string(raw)}})
	require.NoError(t, e)
	require.Contains(t, r.Ret.RetMsg, "deadline")
}

func TestSourceFencePackageWaitReleasesLifecycle(t *testing.T) {
	s, _, _ := newDestroyLifecycleServiceForTest(nil)
	s.cubeboxMgr.config.SourceFenceNodeID = "node"
	b := sourcefence.Binding{"op", "source", "node", "epoch", "v1", strings.Repeat("a", 64)}
	raw, _ := json.Marshal(b)
	sourcefence.Barrier.RLock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Update(ctx, &cubebox.UpdateCubeSandboxRequest{RequestID: "op", SandboxID: "source", Annotations: map[string]string{constants.MasterAnnotationsUpdateAction: sourceFenceAction, sourceFenceBindingAnnotation: string(raw)}})
	}()
	time.Sleep(20 * time.Millisecond)
	lockCtx, lockCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	unlock, err := s.sandboxLifecycleLocks.LockContext(lockCtx, "source")
	lockCancel()
	if err == nil {
		unlock()
	}
	sourcefence.Barrier.RUnlock()
	<-done
	require.NoError(t, err, "fence waiter must not hold lifecycle lock while package writer completes")
}
