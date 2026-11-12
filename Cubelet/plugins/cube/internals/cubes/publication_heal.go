// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubes

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
)

// UncapturedPauseHold reports whether a persisted pause publication hold can
// be proven to fence nothing: the runtime never captured. All of:
//   - the hold is for a pause (commit holds are never auto-released);
//   - the memory lineage never advanced to the held snapshot (the lineage
//     label is written right after a successful runtime capture);
//   - the snapshot and work directories contain no regular files (a capture
//     always writes VM state into the metadata directory).
//
// Any doubt (stat error, unreadable directory) keeps the hold.
func UncapturedPauseHold(cb *cubeboxstore.CubeBox, h *cubeboxstore.PublicationHold) bool {
	if cb == nil || h == nil {
		return false
	}
	if h.Operation != "pause" || strings.TrimSpace(h.SnapshotID) == "" {
		return false
	}
	if runtimeSnapshotLabel(cb) == strings.TrimSpace(h.SnapshotID) {
		return false
	}
	for _, dir := range []string{h.SnapshotPath, h.WorkPath} {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		if !dirHasNoRegularFiles(dir) {
			return false
		}
	}
	return true
}

func runtimeSnapshotLabel(cb *cubeboxstore.CubeBox) string {
	cb.MetaLock.Lock()
	defer cb.MetaLock.Unlock()
	if v := strings.TrimSpace(cb.Labels[constants.MasterAnnotationRuntimeSnapshotID]); v != "" {
		return v
	}
	return strings.TrimSpace(cb.Annotations[constants.MasterAnnotationRuntimeSnapshotID])
}

// dirHasNoRegularFiles is true when dir is absent or contains only
// directories. Anything else (files, symlinks, devices, walk errors) is false.
func dirHasNoRegularFiles(dir string) bool {
	if _, err := os.Lstat(dir); err != nil {
		return os.IsNotExist(err)
	}
	clean := true
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			clean = false
			return filepath.SkipAll
		}
		if !d.IsDir() {
			clean = false
			return filepath.SkipAll
		}
		return nil
	})
	return err == nil && clean
}

// clearPauseAttemptStatus drops the pause-attempt markers so the recovered
// sandbox is classified from its live task like any other running sandbox.
func clearPauseAttemptStatus(cb *cubeboxstore.CubeBox) {
	for _, c := range cb.AllContainers() {
		if c == nil || c.Status == nil {
			continue
		}
		_ = c.Status.Update(func(s cubeboxstore.Status) (cubeboxstore.Status, error) {
			s.PausingAt = 0
			s.PublicationPending = false
			if s.Reason == "PauseFailed" {
				s.Unknown = false
				s.Reason = ""
				s.Message = ""
			}
			return s, nil
		})
	}
}
