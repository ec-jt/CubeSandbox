// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	networkstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/network"

	networkruntime "github.com/tencentcloud/CubeSandbox/Cubelet/network/runtime"
	networktypes "github.com/tencentcloud/CubeSandbox/Cubelet/network/types"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

// The runtime lock ends before allocation persistence. Keep the entire plugin
// operation ordered against Create, Destroy, policy updates and other port calls.
// Service callers additionally hold their sandbox lifecycle lock through the
// sandbox metadata sync. Lock order: service -> plugin -> runtime -> store.
var networkOperationLocks = utils.NewResourceLocks()

// DynamicPorts adapts the optional embedded runtime capability to the service.
// The boolean means infrastructure, not the old network-agent quotaCounted flag.
type DynamicPorts struct{}

func (DynamicPorts) ExposePort(ctx context.Context, id string, port int32, infrastructure bool) (*networkruntime.EnsureNetworkResponse, error) {
	return dnm.changePort(ctx, id, port, true, infrastructure)
}

func (DynamicPorts) ClosePort(ctx context.Context, id string, port int32) (*networkruntime.EnsureNetworkResponse, error) {
	return dnm.changePort(ctx, id, port, false, false)
}

func (m *delegateNetworkManager) changePort(ctx context.Context, id string, port int32, expose, infrastructure bool) (*networkruntime.EnsureNetworkResponse, error) {
	if m == nil || m.tapPlugin == nil || m.allocationStore == nil {
		return nil, fmt.Errorf("network plugin is unavailable")
	}
	runtime, ok := m.tapPlugin.networkRuntime.(networkruntime.DynamicPortRuntime)
	if !ok {
		return nil, fmt.Errorf("network runtime does not support dynamic ports")
	}
	unlock, err := networkOperationLocks.LockContext(ctx, id)
	if err != nil {
		return nil, err
	}
	defer unlock()
	allocation, err := m.allocationStore.Get(id)
	if err != nil {
		return nil, fmt.Errorf("load network allocation: %w", err)
	}
	if allocation.NetworkType != cubebox.NetworkType_tap.String() {
		return nil, fmt.Errorf("dynamic ports require tap networking")
	}
	// Never short-circuit using allocation metadata: a previous call may have
	// committed in the runtime but failed either downstream metadata write.
	var response *networkruntime.EnsureNetworkResponse
	if expose {
		response, err = runtime.ExposePort(ctx, id, port, infrastructure)
	} else {
		response, err = runtime.ClosePort(ctx, id, port)
	}
	if err != nil {
		return nil, err
	}
	if response == nil || response.SandboxID != id {
		return nil, fmt.Errorf("invalid dynamic port runtime response for %s", id)
	}

	return m.persistPortResponse(id, allocation, response)
}

func (m *delegateNetworkManager) persistPortResponse(id string, allocation networkstore.NetworkAllocation, response *networkruntime.EnsureNetworkResponse) (*networkruntime.EnsureNetworkResponse, error) {
	// Preserve legacy and unknown allocation fields. The complete runtime result
	// retains handle, interfaces, routes and metadata (including classification
	// and DNS/QoS metadata); this is a mirror, never desired state for the runtime.
	var err error
	metadata := map[string]json.RawMessage{}
	if len(allocation.PersistentMetadata) != 0 {
		if err := json.Unmarshal(allocation.PersistentMetadata, &metadata); err != nil {
			return nil, fmt.Errorf("decode network allocation metadata: %w", err)
		}
	}
	if metadata == nil {
		metadata = map[string]json.RawMessage{}
	}
	metadata["runtime_network"], err = json.Marshal(response)
	if err != nil {
		return nil, err
	}
	allocation.PersistentMetadata, err = json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	if shim, ok := allocation.Metadata.(*networktypes.ShimNetReq); ok && shim != nil {
		// Copy on write; callers may still hold the original provider. In
		// particular, do not rebuild interfaces and lose their shim-only QoS.
		next := *shim
		next.PortMappings = buildShimPortMappings(response.PortMappings)
		next.RuntimeNetwork = response
		allocation.Metadata = &next
	}
	m.allocationStore.Add(allocation)
	if err := m.allocationStore.Sync(id); err != nil {
		// Do not roll back a committed runtime mapping or its in-memory mirror.
		return nil, fmt.Errorf("persist dynamic port allocation: %w", err)
	}
	return response, nil
}

// CaptureSnapshotPorts repairs both committed runtime changes and a stale
// allocation mirror. Caller holds the service lifecycle lock through sandbox
// sync and snapshot packing. Nil is allowed only for legacy no-allocation AND
// no-runtime state; missing allocation alone must not hide committed ports.
func (DynamicPorts) CaptureSnapshotPorts(ctx context.Context, id string) (*networkruntime.EnsureNetworkResponse, error) {
	return dnm.captureSnapshotPorts(ctx, id)
}

func (m *delegateNetworkManager) captureSnapshotPorts(ctx context.Context, id string) (*networkruntime.EnsureNetworkResponse, error) {
	if m == nil || m.tapPlugin == nil || m.allocationStore == nil {
		return nil, fmt.Errorf("network plugin is unavailable")
	}
	reader, ok := m.tapPlugin.networkRuntime.(networkruntime.PortStateRuntime)
	if !ok {
		return nil, fmt.Errorf("network runtime does not support port capture")
	}
	unlock, err := networkOperationLocks.LockContext(ctx, id)
	if err != nil {
		return nil, err
	}
	defer unlock()
	allocation, allocationErr := m.allocationStore.Get(id)
	if allocationErr != nil && !errors.Is(allocationErr, utils.ErrorKeyNotFound) {
		return nil, allocationErr
	}
	response, err := reader.ReconcileNetworkPorts(ctx, id)
	if errors.Is(allocationErr, utils.ErrorKeyNotFound) && errors.Is(err, networkruntime.ErrNetworkNotActive) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if allocationErr != nil {
		return nil, fmt.Errorf("live runtime has no network allocation: %w", allocationErr)
	}
	if allocation.NetworkType != cubebox.NetworkType_tap.String() {
		return nil, fmt.Errorf("port capture requires tap allocation")
	}
	if response == nil || response.SandboxID != id {
		return nil, fmt.Errorf("invalid port capture response")
	}
	if _, err := networkruntime.PortablePortsFromNetwork(response); err != nil {
		return nil, err
	}
	return m.persistPortResponse(id, allocation, response)
}
