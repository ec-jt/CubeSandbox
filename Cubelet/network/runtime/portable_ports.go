// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
)

// PortablePorts contains container intent only. Host IPs, allocated host ports,
// TAP identities and runtime handles must never cross the snapshot boundary.
// A nil record is legacy; a version 1 record with an empty Ports array is an
// authoritative empty set, not a request to use template defaults.
type PortablePorts struct {
	Version int            `json:"version"`
	Ports   []PortablePort `json:"ports"`
}

type PortablePort struct {
	ContainerPort int32  `json:"container_port"`
	Class         string `json:"class"`
}

func (p *PortablePorts) Validate() error {
	if p == nil || p.Version != 1 || p.Ports == nil {
		return fmt.Errorf("invalid portable port version or missing ports array")
	}
	seen := map[int32]bool{}
	for _, port := range p.Ports {
		if port.ContainerPort < 1 || port.ContainerPort > 65535 || seen[port.ContainerPort] {
			return fmt.Errorf("invalid or duplicate portable port %d", port.ContainerPort)
		}
		seen[port.ContainerPort] = true
		switch port.Class {
		case "static":
		case "user", "infrastructure":
			switch port.ContainerPort {
			case 17300, 17301, 49983, 49999:
				return fmt.Errorf("reserved port %d cannot be dynamic", port.ContainerPort)
			}
		default:
			return fmt.Errorf("unknown portable port class %q", port.Class)
		}
	}
	return nil
}

// PortablePortsFromNetwork validates complete runtime state, including stale
// classification entries, rather than quietly promoting bad entries to static.
func PortablePortsFromNetwork(response *EnsureNetworkResponse) (*PortablePorts, error) {
	if response == nil {
		return nil, fmt.Errorf("nil network state")
	}
	classes := map[int32]string{}
	for key, class := range map[string]string{DynamicPortsMetadataKey: "user", DynamicInfraPortsMetadataKey: "infrastructure"} {
		raw, exists := response.PersistMetadata[key]
		if !exists {
			continue
		}
		var ports []int32
		if err := json.Unmarshal([]byte(raw), &ports); err != nil || ports == nil {
			return nil, fmt.Errorf("invalid dynamic classification %s", key)
		}
		for _, port := range ports {
			if classes[port] != "" {
				return nil, fmt.Errorf("duplicate/conflicting port class %d", port)
			}
			classes[port] = class
		}
	}
	p := &PortablePorts{Version: 1, Ports: []PortablePort{}}
	mapped := map[int32]bool{}
	for _, mapping := range response.PortMappings {
		if normalizePortMapping(mapping).Protocol != "tcp" {
			return nil, fmt.Errorf("portable ports require TCP")
		}
		class := classes[mapping.ContainerPort]
		if class == "" {
			class = "static"
		}
		p.Ports = append(p.Ports, PortablePort{ContainerPort: mapping.ContainerPort, Class: class})
		mapped[mapping.ContainerPort] = true
	}
	for port := range classes {
		if !mapped[port] {
			return nil, fmt.Errorf("classification for unmapped port %d", port)
		}
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	slices.SortFunc(p.Ports, func(a, b PortablePort) int { return int(a.ContainerPort - b.ContainerPort) })
	return p, nil
}

func (p *PortablePorts) ClassifiedPorts(class string) []int32 {
	result := []int32{}
	for _, port := range p.Ports {
		if port.Class == class {
			result = append(result, port.ContainerPort)
		}
	}
	return result
}

func (p *PortablePorts) ExposedPorts() []int64 {
	result := []int64{}
	for _, port := range p.Ports {
		result = append(result, int64(port.ContainerPort))
	}
	return result
}

// ApplyTo replaces port intent and allocates fresh host mappings at Ensure time.
func (p *PortablePorts) ApplyTo(req *EnsureNetworkRequest) error {
	if err := p.Validate(); err != nil {
		return err
	}
	req.PortMappings = nil
	for _, port := range p.Ports {
		req.PortMappings = append(req.PortMappings, PortMapping{Protocol: "tcp", ContainerPort: port.ContainerPort})
	}
	if req.PersistMetadata == nil {
		req.PersistMetadata = map[string]string{}
	}
	for key, class := range map[string]string{DynamicPortsMetadataKey: "user", DynamicInfraPortsMetadataKey: "infrastructure"} {
		raw, _ := json.Marshal(p.ClassifiedPorts(class))
		req.PersistMetadata[key] = string(raw)
	}
	return nil
}

type restoredPortsKey struct{}

// WithRestoredPorts is an in-process capability, never decoded from request
// annotations. Only the service's package reader supplies trusted records.
func WithRestoredPorts(ctx context.Context, p *PortablePorts) (context.Context, error) {
	if p == nil {
		return ctx, nil
	}
	if err := p.Validate(); err != nil {
		return ctx, err
	}
	clone := &PortablePorts{Version: p.Version, Ports: append([]PortablePort{}, p.Ports...)}
	return context.WithValue(ctx, restoredPortsKey{}, clone), nil
}

func RestoredPorts(ctx context.Context) *PortablePorts {
	p, _ := ctx.Value(restoredPortsKey{}).(*PortablePorts)
	return p
}

// PortStateRuntime reads an existing network after settling durable port intent.
// Unlike EnsureNetwork it never creates a sandbox when state is missing.
type PortStateRuntime interface {
	ReconcileNetworkPorts(context.Context, string) (*EnsureNetworkResponse, error)
}

func (s *NetworkController) ReconcileNetworkPorts(ctx context.Context, id string) (*EnsureNetworkResponse, error) {
	if err := validateSandboxID(id); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	unlock := s.locks.Lock(id)
	defer unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	state := s.states[id]
	s.mu.Unlock()
	if state == nil {
		return nil, fmt.Errorf("%w: sandbox %q", ErrNetworkNotActive, id)
	}
	if err := s.settlePendingPort(state); err != nil {
		return nil, err
	}
	response := state.ensureResponse()
	if _, err := PortablePortsFromNetwork(response); err != nil {
		return nil, err
	}
	return response, nil
}
