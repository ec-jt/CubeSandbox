package cubebox

import (
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
	"testing"
)

func TestColdRecreatePolicyAdmission(t *testing.T) {
	for _, fault := range []string{"", "disabled", "policy", "identity", "missing", "remote", "override", "snapshot"} {
		t.Run(fault, func(t *testing.T) {
			t.Setenv("CUBELET_ENABLE_RETAINED_DISK_COLD_RECREATE", "true")
			r := &cubebox.RunCubeSandboxRequest{Annotations: map[string]string{
				workflow.ColdRecreatePolicy:                workflow.ColdRecreateDiscardProcesses,
				constants.MasterAnnotationDesiredSandboxID: "sb-1",
				constants.MasterAnnotationPauseSnapshotID:  "snap-1",
			}}
			switch fault {
			case "disabled":
				t.Setenv("CUBELET_ENABLE_RETAINED_DISK_COLD_RECREATE", "false")
			case "policy":
				r.Annotations[workflow.ColdRecreatePolicy] = "fallback"
			case "identity":
				r.Annotations[constants.MasterAnnotationDesiredSandboxID] = "../other"
			case "missing":
				delete(r.Annotations, constants.MasterAnnotationPauseSnapshotID)
			case "remote":
				r.Annotations[constants.MasterAnnotationSnapshotCrossNode] = "true"
			case "override":
				r.Containers = []*cubebox.ContainerConfig{{Id: "blank"}}
			case "snapshot":
				r.Annotations[constants.MasterAnnotationRuntimeSnapshotID] = "other"
			}
			err := validateColdRecreateRequest(r)
			if fault == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	require.NoError(t, validateColdRecreateRequest(&cubebox.RunCubeSandboxRequest{}))
}

func TestColdPolicyDoesNotPersistAcrossNormalResume(t *testing.T) {
	packed := &cubebox.RunCubeSandboxRequest{Annotations: map[string]string{workflow.ColdRecreatePolicy: workflow.ColdRecreateDiscardProcesses}}
	pauseRestoreBindingFrom(&cubebox.RunCubeSandboxRequest{}, "snap-1", "xfs").applyTo(packed)
	require.Empty(t, packed.Annotations[workflow.ColdRecreatePolicy])
}

func TestColdPauseSpecPreservesContainerIdentity(t *testing.T) {
	c := &cubeboxstore.Container{Metadata: cubeboxstore.Metadata{ID: "sb-1", Config: &cubebox.ContainerConfig{Name: "main"}}}
	require.Equal(t, "sb-1", containerConfigForPause(c).GetId())
	require.Empty(t, c.Config.GetId())
}

func TestColdRecreateOutcomeOnlyAfterSuccessfulCreate(t *testing.T) {
	flow := &workflow.CreateContext{ReqInfo: &cubebox.RunCubeSandboxRequest{Annotations: map[string]string{
		workflow.ColdRecreatePolicy:               workflow.ColdRecreateDiscardProcesses,
		constants.MasterAnnotationPauseSnapshotID: "snap-1",
	}}}
	rsp := &cubebox.RunCubeSandboxResponse{Ret: &errorcode.Ret{RetCode: errorcode.ErrorCode_PreConditionFailed}}
	setCubeExtKey(rsp, flow)
	require.Empty(t, rsp.ExtInfo)
	rsp.Ret.RetCode = errorcode.ErrorCode_Success
	setCubeExtKey(rsp, flow)
	require.Equal(t, "discarded", string(rsp.ExtInfo["process_state"]))
	require.Equal(t, "cold_retained_disk", string(rsp.ExtInfo["recreate_mode"]))
	require.Equal(t, "snap-1", string(rsp.ExtInfo["retained_source_snapshot"]))
}
