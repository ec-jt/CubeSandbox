package cubebox

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
	"testing"
	"time"
)

func TestRestoreLifecycleIdentity(t *testing.T) {
	normal := &cubebox.RunCubeSandboxRequest{}
	id, err := createLifecycleSandboxID(normal)
	require.NoError(t, err)
	require.NotEmpty(t, id)
	again, err := createLifecycleSandboxID(normal)
	require.NoError(t, err)
	require.Equal(t, id, again)
	build := &cubebox.RunCubeSandboxRequest{Annotations: map[string]string{constants.MasterAnnotationsAppSnapshotCreate: "true", constants.MasterAnnotationAppSnapshotTemplateID: "tpl-test"}}
	id, err = createLifecycleSandboxID(build)
	require.NoError(t, err)
	require.Equal(t, "tpl-test_0", id)
	_, err = createLifecycleSandboxID(&cubebox.RunCubeSandboxRequest{Annotations: map[string]string{constants.MasterAnnotationDesiredSandboxID: "../bad"}})
	require.Error(t, err)
}
func TestRestoreLifecyclePreparationWaitsForLock(t *testing.T) {
	for _, key := range []string{constants.MasterAnnotationSnapshotCrossNode, constants.MasterAnnotationPauseSnapshotID, constants.MasterAnnotationRuntimeSnapshotID} {
		t.Run(key, func(t *testing.T) {
			locks := utils.NewResourceLocks()
			u := locks.Lock("locked")
			defer u()
			// No manager is installed: reaching any metadata/runtime read would panic.
			s := &service{sandboxLifecycleLocks: locks}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			rsp, err := s.Create(ctx, &cubebox.RunCubeSandboxRequest{Annotations: map[string]string{constants.MasterAnnotationDesiredSandboxID: "locked", key: "true"}})
			require.NoError(t, err)
			require.Equal(t, errorcode.ErrorCode_TaskStateInvalid, rsp.Ret.RetCode)
		})
	}
}
func TestRestoreLifecycleDestroyMissingRecordWaitsForLock(t *testing.T) {
	s, _, f := newDestroyLifecycleServiceForTest(nil)
	u := s.sandboxLifecycleLocks.Lock("missing")
	defer u()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	rsp := destroySandboxWithContextForTest(t, s, ctx, "missing")
	require.Equal(t, errorcode.ErrorCode_TaskStateInvalid, rsp.Ret.RetCode)
	require.Zero(t, f.destroyCalls)
}
func TestRestoreLifecycleRuntimeEvidence(t *testing.T) {
	require.Error(t, requireRuntimeCleanupEvidence(context.Background(), "missing"))
	for _, e := range []*workflow.LifecycleCleanup{{NeverStarted: true}, {RuntimeGone: true}} {
		ctx := workflow.WithLifecycleCleanup(context.Background(), e)
		require.NoError(t, waitSandboxRuntimeGone(ctx, "never-started", nil))
	}
	// Known never-started rollback must not touch runtime or a prior paused row.
	l := &local{}
	ctx := workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{NeverStarted: true})
	require.NoError(t, l.Destroy(ctx, &workflow.DestroyContext{BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: "never"}}))
	require.NoError(t, l.CleanUp(ctx, &workflow.CleanContext{BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: "never"}}))
}

type missingRuntimeAPI struct{ fakeCubeboxAPI }

func (*missingRuntimeAPI) Get(context.Context, string) (*cubeboxstore.CubeBox, error) {
	return nil, utils.ErrorKeyNotFound
}
func TestRestoreLifecycleMissingRecordRefusesCleanup(t *testing.T) {
	l := &local{cubeboxManger: &missingRuntimeAPI{}}
	require.ErrorContains(t, l.Destroy(context.Background(), &workflow.DestroyContext{BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: "unknown"}}), "ownership unknown")
	require.ErrorContains(t, l.CleanUp(context.Background(), &workflow.CleanContext{BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: "unknown"}}), "ownership unknown")
}
