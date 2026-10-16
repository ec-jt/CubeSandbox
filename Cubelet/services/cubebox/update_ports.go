// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/tencentcloud/CubeSandbox/Cubelet/network"
	networkruntime "github.com/tencentcloud/CubeSandbox/Cubelet/network/runtime"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

var reservedUserServicePorts = map[int32]struct{}{
	17300: {}, // Sandbox HTTP/WebSocket API.
	17301: {}, // Process RPC.
	49983: {}, // envd control API.
	49999: {}, // Template readiness probe.
}

// updatePort is called only while Update holds the sandbox lifecycle lock.
func (s *service) updatePort(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest, sb *cubeboxstore.CubeBox, expose bool) (*cubebox.UpdateCubeSandboxResponse, error) {
	runtime := s.dynamicPortNetwork
	if runtime == nil {
		runtime = network.DynamicPorts{}
	}
	return updateLivePort(ctx, req, sb, expose, runtime, func() error {
		return s.cubeboxMgr.cubeboxManger.SyncByID(ctx, sb.ID)
	})
}

// updateLivePort keeps the state/validation/runtime/persistence boundary testable
// without a live manager. Callers serialize the entire call, including sync.
func updateLivePort(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest, sb *cubeboxstore.CubeBox, expose bool, runtime networkruntime.DynamicPortRuntime, sync func() error) (*cubebox.UpdateCubeSandboxResponse, error) {
	rsp := &cubebox.UpdateCubeSandboxResponse{RequestID: req.RequestID, Ret: &errorcode.Ret{RetCode: errorcode.ErrorCode_Success}}
	fail := func(code errorcode.ErrorCode, message string) (*cubebox.UpdateCubeSandboxResponse, error) {
		rsp.Ret.RetCode, rsp.Ret.RetMsg = code, message
		return rsp, nil
	}
	if sb.GetStatus().Get().State() != cubebox.ContainerState_CONTAINER_RUNNING {
		return fail(errorcode.ErrorCode_TaskStateInvalid, "sandbox must be running to change a port")
	}
	port, err := strconv.ParseInt(req.Annotations["cube.master.container_port"], 10, 32)
	if err != nil || port < 1 || port > 65535 {
		return fail(errorcode.ErrorCode_InvalidParamFormat, "container_port must be between 1 and 65535")
	}
	if _, reserved := reservedUserServicePorts[int32(port)]; reserved {
		return fail(errorcode.ErrorCode_InvalidParamFormat, fmt.Sprintf("container port %d is reserved", port))
	}
	portClass := req.Annotations["cube.master.port_class"]
	if expose && portClass != "" && portClass != "user" && portClass != "infrastructure" {
		return fail(errorcode.ErrorCode_InvalidParamFormat, "port_class must be user or infrastructure")
	}
	// cube.master.port_limit is intentionally ignored, including malformed
	// values. Neither a per-plan cap nor a hard numeric count cap is enforced.
	var response *networkruntime.EnsureNetworkResponse
	if expose {
		response, err = runtime.ExposePort(ctx, sb.ID, int32(port), portClass == "infrastructure")
	} else {
		response, err = runtime.ClosePort(ctx, sb.ID, int32(port))
	}
	if err != nil {
		return fail(errorcode.ErrorCode_CreateNetworkFailed, err.Error())
	}
	if response == nil || response.SandboxID != sb.ID {
		return fail(errorcode.ErrorCode_CreateNetworkFailed, "invalid dynamic port runtime response")
	}
	var users, infrastructure []int32
	for key, target := range map[string]*[]int32{
		networkruntime.DynamicPortsMetadataKey:      &users,
		networkruntime.DynamicInfraPortsMetadataKey: &infrastructure,
	} {
		if raw := response.PersistMetadata[key]; raw != "" {
			if err := json.Unmarshal([]byte(raw), target); err != nil {
				return fail(errorcode.ErrorCode_CreateNetworkFailed, "invalid dynamic port classification: "+err.Error())
			}
		}
	}
	mappings := make([]*cubebox.PortMapping, 0, len(response.PortMappings))
	for _, mapping := range response.PortMappings {
		mappings = append(mappings, &cubebox.PortMapping{ContainerPort: mapping.ContainerPort, HostPort: mapping.HostPort})
	}
	sb.SetLivePorts(mappings, users, infrastructure)
	if err := sync(); err != nil {
		// Runtime and allocation metadata already committed. Retain the current
		// in-memory mirror; retry calls the runtime again and repairs durability.
		return fail(errorcode.ErrorCode_UpdateLocalMetaDataFailed, "persist live port mappings: "+err.Error())
	}
	return rsp, nil
}
