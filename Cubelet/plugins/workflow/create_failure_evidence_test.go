package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

func createCtx(ann map[string]string, created, spawned bool) *CreateContext {
	return &CreateContext{
		ReqInfo:               &cubebox.RunCubeSandboxRequest{Annotations: ann, InstanceType: cubebox.InstanceType_cubebox.String()},
		CubeBoxCreated:        created,
		RuntimeSpawnAttempted: spawned,
	}
}

// B18: what failover may conclude from a failed Create.
func TestApplyCreateFailureEvidence(t *testing.T) {
	crossNode := map[string]string{
		constants.MasterAnnotationSnapshotCrossNode: "true",
		constants.MasterAnnotationPauseSnapshotID:   "snap-1",
		constants.MasterAnnotationRuntimeSnapshotID: "snap-1",
	}
	sameNode := map[string]string{
		constants.MasterAnnotationPauseSnapshotID:   "snap-1",
		constants.MasterAnnotationRuntimeSnapshotID: "snap-1",
	}
	for name, tc := range map[string]struct {
		opts                                           *CreateContext
		neverStarted, mayExist, gone, preserve, proven bool
	}{
		"cross-node, record persisted, no runtime": {createCtx(crossNode, true, false), false, false, true, false, true},
		"cross-node, runtime attempted":            {createCtx(crossNode, true, true), false, true, false, true, false},
		"cross-node, cubebox step never ran":       {createCtx(crossNode, false, false), true, false, false, false, true},
		"same-node resume, no runtime":             {createCtx(sameNode, true, false), false, true, false, true, false},
		"plain create, no runtime":                 {createCtx(nil, true, false), false, false, true, false, true},
		"plain create, runtime attempted":          {createCtx(nil, true, true), false, true, false, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			e := &LifecycleCleanup{}
			ApplyCreateFailureEvidence(e, tc.opts)
			require.Equal(t, tc.neverStarted, e.NeverStarted, "NeverStarted")
			require.Equal(t, tc.mayExist, e.RuntimeMayExist, "RuntimeMayExist")
			require.Equal(t, tc.gone, e.RuntimeGone, "RuntimeGone")
			require.Equal(t, tc.preserve, e.PreserveStorage, "PreserveStorage")
			require.Equal(t, tc.proven, e.NeverStarted || e.RuntimeGone, "cleanup proven")
		})
	}
}

func TestCreateFailureEvidenceKeepsRetryGate(t *testing.T) {
	id := t.Name()
	u := SandboxLifecycleLocks.Lock(id)
	defer u()
	ctx := BeginSandboxLifecycle(t.Context(), id)
	ApplyCreateFailureEvidence(LifecycleCleanupFrom(ctx), createCtx(nil, true, true))
	require.Error(t, CheckCreateRuntimeOwnership(id), "a possibly started runtime blocks retry")
	ctx = BeginSandboxLifecycle(t.Context(), id+"-b")
	ApplyCreateFailureEvidence(LifecycleCleanupFrom(ctx), createCtx(nil, true, false))
	require.NoError(t, CheckCreateRuntimeOwnership(id+"-b"))
}
