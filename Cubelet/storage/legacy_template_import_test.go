// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// copyClone stands in for FICLONE on test filesystems (tmpfs/ext4).
func copyClone(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func useCopyClone(t *testing.T) {
	t.Helper()
	prev := cloneFileFn
	cloneFileFn = copyClone
	t.Cleanup(func() { cloneFileFn = prev })
}

func TestImportLegacyReflinkTemplatesImportsTemplateObjectsOnly(t *testing.T) {
	useCopyClone(t)
	work := t.TempDir()
	legacy := filepath.Join(work, legacyReflinkDirName)
	// v0.6 layout: memory volume, plus a build-rootfs dir whose main file was
	// deleted but which still holds the template snapshot and sandbox clones.
	writeLegacyVolume(t, legacy, "tpl-tpl-a-memory")
	buildDir := filepath.Join(legacy, reflinkVolumesDirName, "tpl-tpl-a-build-rootfs")
	require.NoError(t, os.MkdirAll(buildDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(buildDir, "tpl-tpl-a-rootfs"), []byte("rootfs"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(buildDir, "sb-1-rootfs-gen0"), []byte("sandbox"), 0o644))
	writeLegacyVolume(t, legacy, "sb-2-memory")

	res, err := importLegacyReflinkTemplates(work)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		"tpl-tpl-a-memory/tpl-tpl-a-memory",
		"tpl-tpl-a-build-rootfs/tpl-tpl-a-rootfs",
	}, res.Imported)

	cur := filepath.Join(work, cow.BackendXFS, SnapshotObjectsDir, reflinkVolumesDirName)
	b, err := os.ReadFile(filepath.Join(cur, "tpl-tpl-a-build-rootfs", "tpl-tpl-a-rootfs"))
	require.NoError(t, err)
	require.Equal(t, "rootfs", string(b))
	_, err = os.Stat(filepath.Join(cur, "tpl-tpl-a-build-rootfs", "sb-1-rootfs-gen0"))
	require.True(t, os.IsNotExist(err), "sandbox clones must stay in the legacy pool")
	_, err = os.Stat(filepath.Join(cur, "sb-2-memory"))
	require.True(t, os.IsNotExist(err))
	// Legacy pool untouched so a rollback keeps working.
	_, err = os.Stat(filepath.Join(buildDir, "tpl-tpl-a-rootfs"))
	require.NoError(t, err)

	// Idempotent and never overwrites.
	require.NoError(t, os.WriteFile(filepath.Join(cur, "tpl-tpl-a-memory", "tpl-tpl-a-memory"), []byte("newer"), 0o644))
	res, err = importLegacyReflinkTemplates(work)
	require.NoError(t, err)
	require.Empty(t, res.Imported)
	require.Len(t, res.Skipped, 2)
	b, err = os.ReadFile(filepath.Join(cur, "tpl-tpl-a-memory", "tpl-tpl-a-memory"))
	require.NoError(t, err)
	require.Equal(t, "newer", string(b))
}

func TestImportLegacyReflinkTemplatesNoLegacyPool(t *testing.T) {
	res, err := importLegacyReflinkTemplates(t.TempDir())
	require.NoError(t, err)
	require.Empty(t, res.Imported)
}

func TestLegacyTemplateCatalogResolvesFromDefaultSnapshotDirLayout(t *testing.T) {
	root := t.TempDir()
	specDir := filepath.Join(root, "cubebox", "tpl-legacy1", "2C4096M")
	require.NoError(t, os.MkdirAll(specDir, 0o755))
	body := `{"snapshot_id":"tpl-legacy1","instance_type":"cubebox","spec_dir":"2C4096M",` +
		`"snapshot_path":"` + specDir + `","meta_dir":"` + specDir + `",` +
		`"rootfs_vol":"tpl-tpl-legacy1-rootfs","rootfs_kind":"snapshot",` +
		`"memory_vol":"tpl-tpl-legacy1-memory","memory_kind":"volume","kind":"template"}`
	require.NoError(t, os.WriteFile(filepath.Join(specDir, snapshotCatalogFileName), []byte(body), 0o644))

	SetSnapshotCatalogRootsFor(cow.BackendXFS, filepath.Join(t.TempDir(), "xfs", "snapshots"), root)
	t.Cleanup(func() { SetSnapshotCatalogRootsFor(cow.BackendXFS) })

	entry, err := GetLocalSnapshotFor(context.Background(), cow.BackendXFS, "tpl-legacy1")
	require.NoError(t, err)
	require.Equal(t, "tpl-tpl-legacy1-memory", entry.MemoryVol)
	require.Equal(t, specDir, entry.SnapshotPath)
}
