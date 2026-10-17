package cubebox

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

func TestTemplatePublicationRequiresBothAcknowledgements(t *testing.T) {
	for _, failure := range []string{"catalog", "seal", ""} {
		t.Run(failure, func(t *testing.T) {
			rsp := &cubebox.AppSnapshotResponse{TemplateID: "tpl-test", RootfsVol: "rootfs", MemoryVol: "memory", Ret: &errorcode.Ret{RetCode: errorcode.ErrorCode_Success}}
			var calls []string
			injected := errors.New("partial write")
			err := completeTemplatePublication(rsp, func() error {
				calls = append(calls, "catalog")
				require.NotEqual(t, errorcode.ErrorCode_Success, rsp.Ret.RetCode)
				if failure == "catalog" {
					return injected
				}
				return nil
			}, func() error {
				calls = append(calls, "seal")
				require.NotEqual(t, errorcode.ErrorCode_Success, rsp.Ret.RetCode)
				if failure == "seal" {
					return injected
				}
				return nil
			})
			if failure == "" {
				require.NoError(t, err)
				require.Equal(t, errorcode.ErrorCode_Success, rsp.Ret.RetCode)
			} else {
				require.ErrorIs(t, err, injected)
				require.NotEqual(t, errorcode.ErrorCode_Success, rsp.Ret.RetCode)
				require.Contains(t, rsp.Ret.RetMsg, "artifacts retained")
			}
			if failure == "catalog" {
				require.Equal(t, []string{"catalog"}, calls)
			} else {
				require.Equal(t, []string{"catalog", "seal"}, calls)
			}
			require.Equal(t, "rootfs", rsp.RootfsVol)
			require.Equal(t, "memory", rsp.MemoryVol)
		})
	}
}
