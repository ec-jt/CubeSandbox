// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import "fmt"

const (
	recoveryModeLabel     = "cube.master.recovery.mode"
	recoveryProcessLabel  = "cube.master.recovery.process_state"
	recoveryFallbackLabel = "cube.master.recovery.disk_only_fallback"
)

// beginSnapshotRecovery finalizes server-owned mechanism labels from an
// authenticated host receipt, not from Create success or requested annotations.
// Caller holds the lifecycle/store lock and persists labels after finalization.
// Missing/invalid/old-shim receipts remain unknown. Process state is unverified.
// No outcome authorizes disk boot: disk verification, unwind, cold init and
// memory-ancestor invalidation remain separate, closed gates.
func beginSnapshotRecovery(restoring bool, record func(map[string]string), receipts ...*recoveryReceipt) func(error) error {
	if !restoring {
		return func(err error) error { return err }
	}
	record(snapshotRecoveryLabels("pending", "not_attempted"))
	return func(err error) error {
		labels := snapshotRecoveryLabels("unknown", "refused")
		labels[recoveryProtocolLabel] = "unknown"
		labels[recoveryStageLabel], labels[recoveryCauseLabel] = "unknown", "unknown"
		labels[recoveryAttemptLabel] = ""
		if len(receipts) == 1 && receipts[0] != nil {
			receipt := receipts[0]
			labels[recoveryAttemptLabel] = receipt.attempt
			if outcome, readErr := receipt.read(); readErr == nil {
				labels[recoveryProtocolLabel] = "1"
				labels[recoveryModeLabel] = outcome.Mechanism
				labels[recoveryFallbackLabel] = outcome.DiskOnlyFallback
				labels[recoveryStageLabel], labels[recoveryCauseLabel] = outcome.FailureStage, outcome.Cause
				if outcome.Mechanism == "failed" && err == nil {
					err = fmt.Errorf("shim reported failed snapshot recovery")
				}
			}
		}
		// Without an authentic receipt, a Create error proves failure, but a
		// nil error does NOT prove memory restore (old shim or cold template).
		if err != nil && labels[recoveryProtocolLabel] == "unknown" {
			labels[recoveryModeLabel] = "failed"
		}
		record(labels)
		if err != nil {
			return fmt.Errorf("snapshot recovery incomplete (process_state=unverified, disk_only_fallback=refused): %w", err)
		}
		return nil
	}
}

func snapshotRecoveryLabels(mode, fallback string) map[string]string {
	return map[string]string{
		recoveryModeLabel: mode,
		// This records the recovery mechanism, not a workload liveness claim.
		recoveryProcessLabel:  "unverified",
		recoveryFallbackLabel: fallback,
	}
}
