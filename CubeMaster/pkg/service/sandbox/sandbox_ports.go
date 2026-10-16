package sandbox

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/cubelet"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/localcache"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

// Leave headroom within sandboxlock.DefaultTTL. A lost lease still prevents
// publication atomically; RPC deadlines cannot fence work already at Cubelet.
const portUpdateTimeout = 30 * time.Second

func isPortAction(action string) bool { return action == "exposePort" || action == "closePort" }

func validatePortUpdate(req *types.UpdateRequest) error {
	if req.ContainerPort < 1 || req.ContainerPort > 65535 {
		return errors.New("container_port must be in 1..65535")
	}
	switch req.ContainerPort {
	case 17300, 17301, 49983, 49999:
		return errors.New("container_port is reserved for the platform")
	}
	if req.Action == "exposePort" && req.PortClass != "" && req.PortClass != "user" && req.PortClass != "infrastructure" {
		return errors.New("port_class must be user or infrastructure")
	}
	return nil
}

type portUpdateDeps struct {
	read    func(context.Context, string) (*localcache.ProxyPortsTarget, error)
	address func(string) string
	update  func(context.Context, string, *cubebox.UpdateCubeSandboxRequest) (*cubebox.UpdateCubeSandboxResponse, error)
	list    func(context.Context, string, *cubebox.ListCubeSandboxRequest) (*cubebox.ListCubeSandboxResponse, error)
	merge   func(context.Context, *localcache.ProxyPortsTarget, map[string]string) error
}

func defaultPortUpdateDeps() portUpdateDeps {
	return portUpdateDeps{localcache.ReadSandboxProxyPortsTarget, cubelet.GetCubeletAddr, cubelet.UpdateWithContext, cubelet.ListWithContext, localcache.MergeSandboxProxyPorts}
}

// updateSandboxPorts runs synchronously under the distributed lifecycle lock.
// Every retry repeats the idempotent node operation and queries CURRENT mappings;
// no cached/predicted ports or prior failed publication payload are reused.
func updateSandboxPorts(ctx context.Context, req *types.UpdateRequest, deps portUpdateDeps) *types.Res {
	fail := func(stage string, err error) *types.Res {
		return &types.Res{Ret: &types.Ret{RetCode: int(errorcode.ErrorCode_ReqCubeAPIFailed), RetMsg: fmt.Sprintf("port update %s failed: %v; retry to reconcile routing", stage, err)}}
	}
	ctx, cancel := context.WithTimeout(ctx, portUpdateTimeout)
	defer cancel()
	target, err := deps.read(ctx, req.SandboxID)
	if err != nil {
		return fail("routing lookup", err)
	}
	if target == nil || target.HostIP == "" {
		return fail("routing lookup", errors.New("missing proxy owner"))
	}
	class := req.PortClass
	if class == "" {
		class = "user"
	}
	endpoint := deps.address(target.HostIP)
	response, err := deps.update(ctx, endpoint, &cubebox.UpdateCubeSandboxRequest{
		RequestID: req.RequestID, SandboxID: req.SandboxID,
		Annotations: map[string]string{
			constants.CubeAnnotationsUpdateAction: req.Action,
			constants.CubeAnnotationsInsType:      req.InstanceType,
			"cube.master.container_port":          strconv.Itoa(req.ContainerPort),
			"cube.master.port_class":              class,
			"cube.master.port_limit":              strconv.Itoa(req.PortLimit),
		},
	})
	if err != nil {
		return fail("Cubelet update", err)
	}
	if response == nil || response.Ret == nil {
		return fail("Cubelet update", errors.New("missing explicit Ret"))
	}
	if int(response.Ret.RetCode) != int(errorcode.ErrorCode_Success) {
		return &types.Res{Ret: &types.Ret{RetCode: int(response.Ret.RetCode), RetMsg: response.Ret.RetMsg}}
	}
	if err := ctx.Err(); err != nil {
		return fail("Cubelet update deadline", err)
	}
	info, err := deps.list(ctx, endpoint, &cubebox.ListCubeSandboxRequest{Id: &req.SandboxID})
	if err != nil {
		return fail("mapping query", err)
	}
	if info == nil || len(info.Items) != 1 || info.Items[0] == nil || info.Items[0].Id != req.SandboxID {
		return fail("mapping query", errors.New("expected exactly the requested sandbox"))
	}
	ports := make(map[string]string, len(info.Items[0].PortMappings))
	for _, mapping := range info.Items[0].PortMappings {
		if mapping == nil || mapping.ContainerPort < 1 || mapping.ContainerPort > 65535 || mapping.HostPort < 1 || mapping.HostPort > 65535 {
			return fail("mapping query", errors.New("malformed port mapping"))
		}
		container := strconv.Itoa(int(mapping.ContainerPort))
		if _, duplicate := ports[container]; duplicate {
			return fail("mapping query", errors.New("duplicate container port"))
		}
		ports[container] = strconv.Itoa(int(mapping.HostPort))
	}
	_, exposed := ports[strconv.Itoa(req.ContainerPort)]
	// Cubelet deliberately treats closing a static/template mapping as a no-op.
	// Its List response has no dynamic classification, so Master cannot infer
	// that a surviving mapping is stale. Preserve the authoritative node view.
	if req.Action == "exposePort" && !exposed {
		return fail("mapping query", errors.New("node mapping does not reflect requested action"))
	}
	if err := ctx.Err(); err != nil {
		return fail("mapping query deadline", err)
	}
	if err := deps.merge(ctx, target, ports); err != nil {
		return fail("proxy publication", err)
	}
	return &types.Res{Ret: &types.Ret{RetCode: int(errorcode.ErrorCode_Success), RetMsg: errorcode.ErrorCode_Success.String()}}
}
