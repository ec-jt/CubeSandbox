// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

const (
	DynamicPortsMetadataKey      = "cube.dynamic_ports"
	DynamicInfraPortsMetadataKey = "cube.dynamic_infra_ports"
)

// DynamicPortRuntime is optional: existing NetworkRuntime implementations and
// external fakes need not implement dynamic ports. There is no API wiring here.
// Classification is recorded once, on first expose. Reserved ports and quotas
// are service-layer concerns. Only TCP is supported by this runtime operation.
type DynamicPortRuntime interface {
	ExposePort(context.Context, string, int32, bool) (*EnsureNetworkResponse, error)
	ClosePort(context.Context, string, int32) (*EnsureNetworkResponse, error)
}

var _ DynamicPortRuntime = (*NetworkController)(nil)

// ExposePort exposes a TCP container port, or returns an existing mapping without
// changing its classification. infrastructure selects the infrastructure set.
// A failed call has an ambiguous outcome: retry to observe the committed state.
func (s *NetworkController) ExposePort(ctx context.Context, sandboxID string, port int32, infrastructure bool) (*EnsureNetworkResponse, error) {
	return s.changePort(ctx, sandboxID, port, true, infrastructure)
}

// ClosePort removes only dynamically classified TCP mappings. Static and absent
// ports are successful no-ops. It never releases another mapping's reservation.
func (s *NetworkController) ClosePort(ctx context.Context, sandboxID string, port int32) (*EnsureNetworkResponse, error) {
	return s.changePort(ctx, sandboxID, port, false, false)
}

func (s *NetworkController) changePort(ctx context.Context, sandboxID string, port int32, expose, infrastructure bool) (*EnsureNetworkResponse, error) {
	if err := validateSandboxID(sandboxID); err != nil {
		return nil, err
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("container port must be in 1..65535")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	unlock := s.locks.Lock(sandboxID)
	defer unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	state := s.states[sandboxID]
	s.mu.Unlock()
	if state == nil {
		return nil, fmt.Errorf("%w: sandbox %q", ErrNetworkNotActive, sandboxID)
	}
	if err := s.settlePendingPort(state); err != nil {
		return nil, err
	}
	users, infra, err := dynamicPortSets(state.PersistMetadata)
	if err != nil {
		return nil, err
	}
	var mapping *PortMapping
	for _, candidate := range state.PortMappings {
		if candidate.ContainerPort == port {
			if normalizePortMapping(candidate).Protocol != "tcp" {
				return nil, fmt.Errorf("container port %d has a non-TCP mapping", port)
			}
			if mapping != nil {
				return nil, fmt.Errorf("container port %d has multiple mappings", port)
			}
			copy := candidate
			mapping = &copy
		}
	}
	if (expose && mapping != nil) || (!expose && (mapping == nil || (!users[port] && !infra[port]))) {
		return state.ensureResponse(), nil
	}
	intent := clonePersistedState(&state.persistedState)
	if expose {
		allocated, err := s.ports.Reserve(sandboxID, []PortMapping{{Protocol: "tcp", ContainerPort: port}}, s.cfg.HostPortBindIP)
		if err != nil {
			return nil, err
		}
		mapping = &allocated[0]
		intent.PortMappings = append(intent.PortMappings, *mapping)
	}
	intent.PendingPort = mapping
	// Retain the union in memory even when the write fails before rename. No
	// datapath effect occurs before intent durability. Retrying reads the actual
	// success file rather than assuming a failed rename did not commit.
	s.publishPortState(state, intent)
	if err := s.store.RewriteSuccess(intent); err != nil {
		return nil, fmt.Errorf("persist port intent: %w", err)
	}
	if !expose {
		if err := s.settlePendingPort(state); err != nil {
			return nil, err
		}
		return state.ensureResponse(), nil
	}
	if err := s.cubevsAdapter.AddPortMapping(uint32(state.TapIfIndex), uint16(port), uint16(mapping.HostPort)); err != nil {
		// Add can fail after only one direction was installed. Exact removal is
		// safe to retry; never use whole-TAP cleanup or release all ownership.
		return nil, errors.Join(fmt.Errorf("add dynamic port: %w", err), s.settlePendingPort(state))
	}
	final := clonePersistedState(intent)
	final.PendingPort = nil
	if infrastructure {
		infra[port] = true
	} else {
		users[port] = true
	}
	setDynamicPortMetadata(final, users, infra)
	if err := s.store.RewriteSuccess(final); err != nil {
		return nil, errors.Join(fmt.Errorf("commit exposed port (retry required): %w", err), s.settlePendingPort(state))
	}
	s.publishPortState(state, final)
	return state.ensureResponse(), nil
}

// settlePendingPort runs under the sandbox lock (or before startup publication).
// A pending record always means remove, never replay the provisional exposure.
// If a previous final rewrite returned an error after rename, the file instead
// contains a committed expose/close. Rewriting that observed state confirms its
// directory sync before publishing it or releasing a reservation. Thus no
// ambiguous persistence error can free a still-durable or reachable tuple.
// No background retry is needed for correctness: port operations, Ensure,
// policy update, and release retry this step; startup retries it before Active.
// Do not downgrade to a runtime unaware of PendingPort while an intent exists:
// it would mistake the provisional union for committed mappings.
func (s *NetworkController) settlePendingPort(state *managedState) error {
	if state.PendingPort == nil {
		return nil
	}
	reserved := *state.PendingPort
	if err := validatePortMappingPorts(reserved); err != nil {
		return err
	}
	if reserved.HostPort == 0 || normalizePortMapping(reserved).Protocol != "tcp" {
		return fmt.Errorf("invalid pending TCP mapping")
	}
	disk, err := s.store.Load(state.SandboxID, StateFileSuccess)
	if err != nil {
		return fmt.Errorf("read pending port state: %w", err)
	}
	if disk.SandboxID != state.SandboxID || disk.TapName != state.TapName || disk.TapIfIndex != state.TapIfIndex || disk.SandboxIP != state.SandboxIP {
		return fmt.Errorf("pending port state identity mismatch")
	}
	if disk.PendingPort != nil && *disk.PendingPort != reserved {
		return fmt.Errorf("pending port tuple mismatch")
	}
	if disk.PendingPort != nil && !slices.Contains(disk.PortMappings, reserved) {
		return fmt.Errorf("pending port absent from reserved mapping union")
	}
	keep := disk.PendingPort == nil && slices.Contains(disk.PortMappings, reserved)
	if !keep {
		if err := s.cubevsAdapter.DelPortMapping(uint32(state.TapIfIndex), uint16(reserved.ContainerPort), uint16(reserved.HostPort)); err != nil {
			return fmt.Errorf("remove pending port: %w", err)
		}
		users, infra, err := dynamicPortSets(disk.PersistMetadata)
		if err != nil {
			return err
		}
		disk.PortMappings = slices.DeleteFunc(disk.PortMappings, func(p PortMapping) bool { return p == reserved })
		delete(users, reserved.ContainerPort)
		delete(infra, reserved.ContainerPort)
		setDynamicPortMetadata(disk, users, infra)
	}
	disk.PendingPort = nil
	if err := s.store.RewriteSuccess(disk); err != nil {
		return fmt.Errorf("commit port reconciliation: %w", err)
	}
	if !keep {
		if err := s.ports.ReleasePort(state.SandboxID, uint16(reserved.HostPort)); err != nil {
			return err
		}
	}
	s.publishPortState(state, disk)
	return nil
}

func (s *NetworkController) publishPortState(state *managedState, next *persistedState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state.persistedState = *clonePersistedState(next)
	state.tap.PortMappings = append([]PortMapping(nil), next.PortMappings...)
}

// Compatible with the old plugin's sorted JSON int32 arrays. Malformed metadata
// fails closed rather than silently turning dynamic mappings into static ones.
func dynamicPortSets(metadata map[string]string) (map[int32]bool, map[int32]bool, error) {
	sets := []map[int32]bool{{}, {}}
	for i, key := range []string{DynamicPortsMetadataKey, DynamicInfraPortsMetadataKey} {
		if metadata[key] == "" {
			continue
		}
		var ports []int32
		if err := json.Unmarshal([]byte(metadata[key]), &ports); err != nil {
			return nil, nil, fmt.Errorf("invalid %s: %w", key, err)
		}
		for _, port := range ports {
			if port < 1 || port > 65535 {
				return nil, nil, fmt.Errorf("invalid port in %s", key)
			}
			sets[i][port] = true
		}
	}
	return sets[0], sets[1], nil
}

func setDynamicPortMetadata(state *persistedState, users, infra map[int32]bool) {
	if state.PersistMetadata == nil {
		state.PersistMetadata = map[string]string{}
	}
	for key, ports := range map[string]map[int32]bool{DynamicPortsMetadataKey: users, DynamicInfraPortsMetadataKey: infra} {
		values := make([]int32, 0, len(ports))
		for port := range ports {
			values = append(values, port)
		}
		slices.Sort(values)
		encoded, _ := json.Marshal(values)
		state.PersistMetadata[key] = string(encoded)
	}
}
