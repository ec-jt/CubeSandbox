// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"fmt"
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
// sealing and remote export acceptance only, never remote export completion.
func publishCapturedSnapshot(persist, seal func() error, upload func() (string, error)) (string, error) {
	for _, step := range []struct {
		name string
		run  func() error
	}{
		{"catalog persistence", persist}, {"package finalization", seal},
	} {
		if err := step.run(); err != nil {
			return "", fmt.Errorf("%s: %w", step.name, err)
		}
	}
	raw, err := upload()
	if err != nil {
		return "", fmt.Errorf("remote export acceptance: %w", err)
	}
	return raw, nil
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

func publishSnapshotPackage(ctx context.Context, backend string, entry *storage.SnapshotCatalogEntry) (string, error) {
	return publishCapturedSnapshot(
		func() error { return storage.WriteSnapshotCatalogFor(backend, entry) },
		func() error { return storage.FinalizeS3PackageSnapshots(ctx, backend, entry.SnapshotID) },
		func() (string, error) { return uploadRemoteUUIDsIfS3(ctx, backend, entry.SnapshotID) },
	)
}
