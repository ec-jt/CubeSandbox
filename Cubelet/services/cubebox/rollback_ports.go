// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"fmt"

	networkruntime "github.com/tencentcloud/CubeSandbox/Cubelet/network/runtime"
)

// validateRollbackSnapshotPorts is a compatibility gate, not network rollback.
// The caller holds the sandbox lifecycle lock through VM restore. Capture must
// reconcile pending runtime intent and persist both mirrors before returning.
// Legacy packages without a record retain the historical network-preserving
// behavior; absence is not proof that their port state matches the live VM.
func validateRollbackSnapshotPorts(metaDir string, capture func() (*networkruntime.PortablePorts, error)) error {
	want, err := loadSnapshotPorts(metaDir)
	if err != nil {
		return fmt.Errorf("read portable snapshot ports: %w", err)
	}
	if want == nil {
		return nil
	}
	current, err := capture()
	if err != nil {
		return fmt.Errorf("reconcile current ports: %w", err)
	}
	if err := current.Validate(); err != nil {
		return fmt.Errorf("current portable ports unavailable or invalid: %w", err)
	}
	classes := make(map[int32]string, len(current.Ports))
	for _, port := range current.Ports {
		classes[port.ContainerPort] = port.Class
	}
	if len(want.Ports) == len(current.Ports) {
		match := true
		for _, port := range want.Ports {
			if classes[port.ContainerPort] != port.Class {
				match = false
				break
			}
		}
		if match {
			return nil
		}
	}
	return fmt.Errorf("snapshot ports differ from the current network; in-place rollback cannot reconcile port changes; use snapshot recreation instead")
}
