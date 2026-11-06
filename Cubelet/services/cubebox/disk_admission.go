// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
package cubebox

import (
	"context"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
)

// These are observations at pre-runtime admission, not status claims about a
// disk after a guest has started writing. All recovery-prefixed labels are
// server-owned. Legacy and same-node restores remain compatible but unverified.
func observeRestoreDiskAdmission(ctx context.Context, annotations map[string]string, checkpoint, attempt string) map[string]string {
	labels := map[string]string{
		"cube.master.recovery.disk_admission":         "unverified",
		"cube.master.recovery.disk_metadata":          "unverified",
		"cube.master.recovery.disk_identity":          "unverified",
		"cube.master.recovery.disk_publication":       "unverified",
		"cube.master.recovery.disk_retention":         "unverified",
		"cube.master.recovery.disk_checkpoint":        checkpoint,
		"cube.master.recovery.disk_attempt":           attempt,
		"cube.master.recovery.disk_observation_phase": "pre_runtime_only",
	}
	if imp := storage.CrossNodeSandboxImport(annotations); imp != nil {
		observation := imp.ObserveDiskAdmission(ctx, checkpoint, attempt)
		labels["cube.master.recovery.disk_metadata"] = observation.MetadataBinding
		labels["cube.master.recovery.disk_identity"] = observation.DiskIdentity
		labels["cube.master.recovery.disk_publication"] = observation.Publication
	}
	return labels
}
