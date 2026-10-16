// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"slices"

	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"google.golang.org/protobuf/proto"
)

// SetLivePorts replaces only the runtime-owned port view. The caller must hold
// the service lifecycle lock until persistence completes. Never roll this view
// back after a failed sync: the runtime may already have committed the change.
func (cb *CubeBox) SetLivePorts(mappings []*cubebox.PortMapping, users, infrastructure []int32) {
	cb.livePortsLock.Lock()
	defer cb.livePortsLock.Unlock()
	cb.PortMappings = clonePortMappings(mappings)
	cb.DynamicPorts = slices.Clone(users)
	cb.DynamicInfraPorts = slices.Clone(infrastructure)
}

func (cb *CubeBox) LivePortMappings() []*cubebox.PortMapping {
	cb.livePortsLock.RLock()
	defer cb.livePortsLock.RUnlock()
	return clonePortMappings(cb.PortMappings)
}

func clonePortMappings(mappings []*cubebox.PortMapping) []*cubebox.PortMapping {
	if mappings == nil {
		return nil
	}
	out := make([]*cubebox.PortMapping, len(mappings))
	for i, mapping := range mappings {
		if mapping != nil {
			out[i] = proto.Clone(mapping).(*cubebox.PortMapping)
		}
	}
	return out
}
