package sandbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/cubelet"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/legacyresume"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/sandboxspec"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

// Narrow dependency seams keep bridge routing tests independent of live DB/RPC.
var (
	legacyPauseLookup    = pausesnap.GetBySandbox
	legacySpecLookup     = sandboxspec.Get
	legacyCubeletAddr    = cubelet.GetCubeletAddr
	legacyCubeletUpdate  = cubelet.UpdateWithTimeout
	legacyAfterUpdate    = runAfterUpdateSandboxSuccessHook
	legacyPublishTimeout = publishUpdateTimeout
)

// tryLegacyPauseUpdate runs under the caller's lifecycle lock. It never creates
// synthetic native bindings or falls back after a native resume failure.
// Original nodes MUST retain their old Cubelet/shim protocol during coexistence.
func tryLegacyPauseUpdate(ctx context.Context, req *types.UpdateRequest, hostIP string) (*types.Res, bool) {
	fail := func(message string) (*types.Res, bool) {
		return &types.Res{Ret: &types.Ret{RetCode: int(errorcode.ErrorCode_MasterParamsError), RetMsg: message}}, true
	}
	cohort, err := legacyresume.LoadConfigured()
	if err != nil {
		return fail("invalid legacy manifest; refusing lifecycle update")
	}
	origin, enabled := cohort[req.SandboxID]
	if !enabled {
		return nil, false
	}
	if req.Action != "pause" && req.Action != "resume" {
		return fail("unsupported legacy lifecycle action")
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	record, err := legacyPauseLookup(lookupCtx, req.SandboxID)
	if err != nil && !errors.Is(err, pausesnap.ErrNotFound) {
		return fail("cannot verify native pause metadata; refusing legacy update")
	}
	if err == nil && record == nil {
		return fail("inconsistent native pause metadata")
	}
	spec, err := legacySpecLookup(lookupCtx, req.SandboxID)
	if err != nil {
		return fail("cannot verify legacy sandbox spec")
	}
	if spec == nil {
		return fail("legacy sandbox spec is missing")
	}
	if err := legacyresume.Validate(origin, hostIP, strings.TrimSpace(spec.Backend), record != nil); err != nil {
		return fail(err.Error())
	}
	response, err := legacyCubeletUpdate(ctx, legacyCubeletAddr(origin), &cubebox.UpdateCubeSandboxRequest{
		RequestID: req.RequestID,
		SandboxID: req.SandboxID,
		Annotations: map[string]string{
			constants.CubeAnnotationsUpdateAction: req.Action,
			constants.CubeAnnotationsInsType:      req.InstanceType,
		},
	}, 120*time.Second)
	if err != nil || response == nil || response.GetRet() == nil {
		// Leave the legacy cohort and original state intact for reconciliation.
		return &types.Res{Ret: &types.Ret{RetCode: int(errorcode.ErrorCode_ReqCubeAPIFailed),
			RetMsg: fmt.Sprintf("legacy %s RPC failed; original state retained, reconcile before retry", req.Action)}}, true
	}
	result := &types.Res{Ret: &types.Ret{RetCode: int(response.GetRet().GetRetCode()), RetMsg: response.GetRet().GetRetMsg()}}
	if result.Ret.RetCode == int(errorcode.ErrorCode_Success) {
		legacyPublishTimeout(ctx, req)
		legacyAfterUpdate(ctx, req.SandboxID, req.InstanceType, req.Action, req.RequestID)
	}
	return result, true
}
