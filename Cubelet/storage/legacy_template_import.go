// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/pathutil"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// legacyTemplateObjectPrefix marks cubecow objects owned by a template build
// (tpl-<id>-rootfs, tpl-<id>-memory, tpl-<id>-build-rootfs). Sandbox-owned
// objects (sb-*) are never imported: they belong to sandboxes that still run
// or are paused under the pre-upgrade layout and must stay where their shim
// opened them.
const legacyTemplateObjectPrefix = "tpl-"

// LegacyTemplateImportResult summarises one import pass.
type LegacyTemplateImportResult struct {
	Imported []string
	Skipped  []string
}

// cloneFileFn is swapped in tests on filesystems without FICLONE.
var cloneFileFn = ficloneFile

// importLegacyReflinkTemplates makes template objects created by a pre-v0.7
// cubelet visible to the current cubecow reflink pool.
//
// Older cubelets rooted the reflink pool at <work>/cubecow-reflink; the
// current one roots it at <work>/xfs/objects. Without this import every
// template built before the upgrade fails to create with "snapshot catalog
// not found" / object NotFound even though its data is on the node.
//
// Each tpl-* file is FICLONE'd (metadata-only on xfs/btrfs, no data copy)
// into the same <volumes>/<vol>/<file> position under the new root. The
// legacy pool is left untouched so a rollback to the old cubelet keeps
// working, and existing destinations are never overwritten. Must run before
// the cubecow engine scans its root.
func importLegacyReflinkTemplates(work string) (LegacyTemplateImportResult, error) {
	var res LegacyTemplateImportResult
	work = strings.TrimSpace(work)
	if work == "" || work == "." {
		return res, nil
	}
	legacyVolumes := filepath.Join(work, legacyReflinkDirName, reflinkVolumesDirName)
	currentVolumes := filepath.Join(work, cow.BackendXFS, SnapshotObjectsDir, reflinkVolumesDirName)

	st, err := os.Lstat(legacyVolumes)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return res, nil
		}
		return res, err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return res, nil
	}
	entries, err := os.ReadDir(legacyVolumes)
	if err != nil {
		return res, err
	}
	var errs []error
	for _, vol := range entries {
		volName := vol.Name()
		if !vol.IsDir() || !strings.HasPrefix(volName, legacyTemplateObjectPrefix) {
			continue
		}
		if pathutil.ValidateSafeID(volName) != nil {
			continue
		}
		srcDir := filepath.Join(legacyVolumes, volName)
		files, err := os.ReadDir(srcDir)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, f := range files {
			name := f.Name()
			if !strings.HasPrefix(name, legacyTemplateObjectPrefix) || strings.HasPrefix(name, ".") {
				continue
			}
			if pathutil.ValidateSafeID(name) != nil {
				continue
			}
			info, err := f.Info()
			if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
				continue
			}
			dstDir := filepath.Join(currentVolumes, volName)
			dst := filepath.Join(dstDir, name)
			if _, err := os.Lstat(dst); err == nil {
				res.Skipped = append(res.Skipped, volName+"/"+name)
				continue
			}
			if err := os.MkdirAll(dstDir, 0o755); err != nil {
				errs = append(errs, err)
				continue
			}
			if err := cloneFileFn(filepath.Join(srcDir, name), dst, info.Mode().Perm()); err != nil {
				errs = append(errs, fmt.Errorf("import %s/%s: %w", volName, name, err))
				continue
			}
			res.Imported = append(res.Imported, volName+"/"+name)
		}
	}
	return res, errors.Join(errs...)
}

// ficloneFile reflinks src to dst through a temporary name so a crash never
// leaves a partial file under the final name (cubecow's startup scan would
// otherwise register it).
func ficloneFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := filepath.Join(filepath.Dir(dst), ".import-"+filepath.Base(dst))
	_ = os.Remove(tmp)
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return err
	}
	if err := unix.IoctlFileClone(int(out.Fd()), int(in.Fd())); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("FICLONE: %w", err)
	}
	if err := out.Sync(); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return fsyncDirPath(filepath.Dir(dst))
}

func fsyncDirPath(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
