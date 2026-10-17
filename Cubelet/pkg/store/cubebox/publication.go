// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"fmt"
)

// PublicationHold is a bounded recovery fence, not a replay journal. It lives
// in the CubeBox DB, outside the metadata mount. Capture may have reset dirty
// tracking even if its RPC failed. Only explicit reconciliation may clear a
// failed hold: verify the package and lineage (or arrange a fresh full capture)
// before allowing another capture, restore, delete, or GC.
type PublicationHold struct {
	SnapshotID         string `json:"snapshot_id"`
	Backend            string `json:"backend"`
	Operation          string `json:"operation"`
	SnapshotPath       string `json:"snapshot_path"`
	WorkPath           string `json:"work_path"`
	PreviousSnapshotID string `json:"previous_snapshot_id,omitempty"`
	RestoreSnapshotID  string `json:"restore_snapshot_id,omitempty"`
}

func (cb *CubeBox) GetPublicationHold() *PublicationHold {
	if cb == nil {
		return nil
	}
	cb.MetaLock.Lock()
	defer cb.MetaLock.Unlock()
	if cb.PublicationHold == nil {
		return nil
	}
	h := *cb.PublicationHold
	return &h
}

func (cb *CubeBox) SetPublicationHold(h *PublicationHold) {
	cb.MetaLock.Lock()
	if h == nil {
		cb.PublicationHold = nil
	} else {
		copy := *h
		cb.PublicationHold = &copy
	}
	cb.MetaLock.Unlock()
	for _, c := range cb.AllContainers() {
		if c != nil && c.Status != nil {
			_ = c.Status.Update(func(s Status) (Status, error) { s.PublicationPending = h != nil; return s, nil })
		}
	}
}

func (cb *CubeBox) PublicationError() error {
	if h := cb.GetPublicationHold(); h != nil {
		return fmt.Errorf("snapshot publication incomplete: %s %s; artifacts retained at %s; operator reconciliation required before retry or deletion", h.Operation, h.SnapshotID, h.SnapshotPath)
	}
	return nil
}

type publicationCleanupKey struct{}

// WithPublicationCleanup permits only the in-process successful pause teardown.
// No RPC annotation can grant this exception. The recovery hold remains set.
func WithPublicationCleanup(ctx context.Context) context.Context {
	return context.WithValue(ctx, publicationCleanupKey{}, true)
}

func PublicationCleanupAllowed(ctx context.Context) bool {
	allowed, _ := ctx.Value(publicationCleanupKey{}).(bool)
	return allowed
}
