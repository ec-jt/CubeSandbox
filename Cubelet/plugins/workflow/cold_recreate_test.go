package workflow

import (
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"testing"
)

func TestColdRecreateSeparatesDiskAndMemory(t *testing.T) {
	c := &CreateContext{ReqInfo: &cubebox.RunCubeSandboxRequest{InstanceType: "cubebox", Annotations: map[string]string{
		constants.MasterAnnotationPauseSnapshotID:   "snap-test",
		constants.MasterAnnotationRuntimeSnapshotID: "snap-test",
	}}}
	require.True(t, c.IsRetoreSnapshot())
	require.True(t, c.IsGuestMountRestore())
	c.ReqInfo.Annotations[ColdRecreatePolicy] = ColdRecreateDiscardProcesses
	require.True(t, c.IsColdRecreate())
	require.False(t, c.IsRetoreSnapshot())
	require.False(t, c.IsGuestMountRestore())
	id, ok := c.GetSnapshotTemplateID()
	require.True(t, ok)
	require.Equal(t, "snap-test", id)
	require.True(t, c.PreserveStorageOnFailure())
}
