// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

const s3FinalizeCatalogFile = ".s3-finalize.json"

type s3FinalizeCatalog struct {
	Version int                   `json:"version"`
	Catalog *SnapshotCatalogEntry `json:"catalog"`
}

// The record is outside metadata/, which disappears at unmount. It is retained
// after success for repeat verification and disappears with the package home.
// Atomic rename matches the catalog writer; neither promises power-loss
// durability (no fsync), nor does this local record prove remote export readiness.
func writeS3FinalizeCatalog(entry *SnapshotCatalogEntry) error {
	body, err := json.Marshal(s3FinalizeCatalog{Version: 1, Catalog: entry})
	if err != nil {
		return err
	}
	path := filepath.Join(entry.SnapshotPath, s3FinalizeCatalogFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func readS3FinalizeCatalog(id string) (*SnapshotCatalogEntry, bool, error) {
	homes := []string{}
	// The cache is only a path hint. It cannot substitute for a persisted catalog.
	snapshotCatalogMu.RLock()
	if ns := snapshotCatalogs[cow.BackendS3]; ns != nil {
		if cached := ns.index[id]; cached != nil {
			homes = append(homes, cached.SnapshotPath)
		}
	}
	snapshotCatalogMu.RUnlock()
	for _, root := range snapshotCatalogRootsSnapshot(cow.BackendS3) {
		homes = append(homes, filepath.Join(root, id))
	}
	homes = append(homes, SnapshotHome(cow.BackendS3, SnapshotKindNormal, id), SnapshotHome(cow.BackendS3, SnapshotKindPause, id))
	seen := map[string]bool{}
	for _, home := range homes {
		if home == "" || seen[home] {
			continue
		}
		seen[home] = true
		path := filepath.Join(home, s3FinalizeCatalogFile)
		body, err := os.ReadFile(path)
		var entry *SnapshotCatalogEntry
		prepared := false
		switch {
		case err == nil:
			var record s3FinalizeCatalog
			if err := json.Unmarshal(body, &record); err != nil {
				return nil, false, fmt.Errorf("decode %s: %w", path, err)
			}
			if record.Version != 1 || record.Catalog == nil {
				return nil, false, fmt.Errorf("invalid finalization record %s", path)
			}
			entry, prepared = record.Catalog, true
		case !errors.Is(err, os.ErrNotExist):
			return nil, false, err
		default:
			entry, err = readSnapshotCatalogFile(filepath.Join(home, SnapshotMetadataDir, snapshotCatalogFileName))
			if errors.Is(err, ErrSnapshotCatalogNotFound) {
				continue
			}
			if err != nil {
				return nil, false, err
			}
		}
		if entry.SnapshotID != id || filepath.Clean(entry.SnapshotPath) != filepath.Clean(home) ||
			filepath.Clean(entry.MetaDir) != filepath.Join(filepath.Clean(home), SnapshotMetadataDir) {
			return nil, false, fmt.Errorf("inconsistent S3 finalization catalog for %s", id)
		}
		return entry, prepared, nil
	}
	return nil, false, fmt.Errorf("s3 package %s: %w", id, ErrSnapshotCatalogNotFound)
}
