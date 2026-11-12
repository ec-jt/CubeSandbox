// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
)

// beginSnapshotPublication must complete before entering the runtime. An error
// leaves the in-memory hold set too: DB errors may have ambiguous durability.
func beginSnapshotPublication(cb *cubeboxstore.CubeBox, layout snapshotWorkLayout, operation string, sync func() error) error {
	if err := cb.PublicationError(); err != nil {
		return err
	}
	cb.SetPublicationHold(&cubeboxstore.PublicationHold{
		SnapshotID: layout.SnapshotID, Backend: layout.Backend, Operation: operation,
		SnapshotPath: layout.Home, WorkPath: layout.TmpHome,
		PreviousSnapshotID: resolveBaseSnapshotID(cb),
		RestoreSnapshotID:  cubeBoxLabel(cb, constants.MasterAnnotationRuntimeRestoreSnapshotID),
	})
	if err := sync(); err != nil {
		return fmt.Errorf("persist publication hold: %w", err)
	}
	return nil
}

func persistCapturedLineage(cb *cubeboxstore.CubeBox, snapshotID string, sync func() error) error {
	// The runtime reset dirty tracking. Never leave the old base as authoritative.
	setRuntimeSnapshotBindingLabels(cb, snapshotID, time.Now().UTC())
	if err := sync(); err != nil {
		return fmt.Errorf("persist captured memory lineage: %w", err)
	}
	return nil
}

// publishCapturedSnapshot stops on the first failure. It acknowledges local
// capture persistence and sealing only. Remote export is handed to the async
// export worker afterwards and never gates the result (B15): scheduling is a
// local, in-memory hand-off whose only failure mode is an invalid backend.
func publishCapturedSnapshot(persist, seal func() error, schedule func() (bool, error)) (bool, error) {
	for _, step := range []struct {
		name string
		run  func() error
	}{
		{"catalog persistence", persist}, {"package finalization", seal},
	} {
		if err := step.run(); err != nil {
			return false, fmt.Errorf("%s: %w", step.name, err)
		}
	}
	pending, err := schedule()
	if err != nil {
		return false, fmt.Errorf("schedule remote export: %w", err)
	}
	return pending, nil
}

// isPreCaptureRuntimeRefusal reports whether the runtime rejected a capture
// request before touching the VM. The shim checks open exec tasks under its
// state lock while still Normal and returns before entering Paused, so dirty
// tracking and the destination directory are untouched. "sandbox not
// running" is deliberately excluded: it can follow a completed capture.
func isPreCaptureRuntimeRefusal(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "terminate exec tasks first")
}

// releasePreCapturePublication drops a hold that was set for a capture the
// runtime refused before starting. Nothing was captured, so the hold fences
// nothing. On sync failure the hold is retained (fail closed).
func releasePreCapturePublication(cb *cubeboxstore.CubeBox, sync func() error) error {
	h := cb.GetPublicationHold()
	if h == nil {
		return nil
	}
	cb.SetPublicationHold(nil)
	if err := sync(); err != nil {
		cb.SetPublicationHold(h)
		return fmt.Errorf("persist pre-capture hold release (hold retained): %w", err)
	}
	return nil
}

func completeSnapshotPublication(cb *cubeboxstore.CubeBox, sync func() error) error {
	h := cb.GetPublicationHold()
	cb.SetPublicationHold(nil)
	if err := sync(); err != nil {
		cb.SetPublicationHold(h)
		return fmt.Errorf("persist publication completion (hold retained): %w", err)
	}
	return nil
}

// snapshotPackageIncompleteError blocks use of the captured package, but does
// not block reads of its retained ancestors.
func (s *service) snapshotPackageIncompleteError(snapshotID string) error {
	if snapshotID == "" {
		return nil
	}
	for _, cb := range s.listCubeboxes() {
		if h := cb.GetPublicationHold(); h != nil && h.SnapshotID == snapshotID {
			return cb.PublicationError()
		}
	}
	return nil
}

func (s *service) snapshotPackageRetentionError(snapshotID string) error {
	for _, cb := range s.listCubeboxes() {
		if h := cb.GetPublicationHold(); h != nil {
			if snapshotID == h.SnapshotID || snapshotID == h.PreviousSnapshotID || snapshotID == h.RestoreSnapshotID {
				return cb.PublicationError()
			}
		}
		// A successfully captured package is still the live dirty-tracking base.
		// Master registration failure must not destroy it behind the VM's back.
		if sandboxLiveForPauseKeep(cb) && cubeBoxLabel(cb, constants.MasterAnnotationRuntimeSnapshotID) == snapshotID {
			return fmt.Errorf("snapshot %s is a live memory lineage base; cleanup denied", snapshotID)
		}
	}
	return nil
}

// publishSnapshotPackage returns whether an asynchronous S3 export is pending.
func publishSnapshotPackage(ctx context.Context, backend string, entry *storage.SnapshotCatalogEntry, sandbox string, generation uint32) (bool, error) {
	// This is pre-publication capture evidence, not a committed export receipt.
	if err := storage.PrepareCheckpointDiskBinding(ctx, entry, sandbox, generation); err != nil {
		return false, fmt.Errorf("prepare checkpoint disk binding: %w", err)
	}
	if storage.IsS3Backend(backend) && entry.DiskBinding.NativeRootfsUUID == "" {
		return false, fmt.Errorf("capture requires native rootfs snapshot identity")
	}
	return publishCapturedSnapshot(
		func() error { return storage.WriteSnapshotCatalogFor(backend, entry) },
		func() error { return storage.FinalizeS3PackageSnapshots(ctx, backend, entry.SnapshotID) },
		func() (bool, error) { return scheduleRemoteExportIfS3(backend, entry.SnapshotID) },
	)
}

// remoteExportExtInfo stamps the pending flag Master reads to record
// remote_status=inprogress with empty export_uuids.
func remoteExportExtInfo(ext map[string][]byte, pending bool) map[string][]byte {
	if !pending {
		return ext
	}
	if ext == nil {
		ext = map[string][]byte{}
	}
	ext[storage.ExtInfoRemoteExportPending] = []byte("true")
	return ext
}
