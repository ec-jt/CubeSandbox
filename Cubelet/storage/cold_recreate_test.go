package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
	"os"
	"path/filepath"
	"testing"
)

func TestColdRecreateBinding(t *testing.T) {
	for _, fault := range []string{"", "sandbox", "missing", "corrupt", "symlink", "unbound", "remote", "catalog"} {
		t.Run(fault, func(t *testing.T) {
			dir := t.TempDir()
			e := bindingFixture(t, dir)
			e.Kind = CatalogKindPauseSnapshot
			e.Backend = cow.BackendXFS
			require.NoError(t, PrepareCheckpointDiskBinding(context.Background(), e, "sb-1", 7))
			putBinding(t, dir, e)
			sandbox := "sb-1"
			switch fault {
			case "sandbox":
				sandbox = "sb-2"
			case "missing":
				require.NoError(t, os.Remove(filepath.Join(dir, "sandbox_spec.json")))
			case "corrupt":
				require.NoError(t, os.WriteFile(filepath.Join(dir, "sandbox_spec.json"), []byte("corrupt"), 0600))
			case "symlink":
				require.NoError(t, os.Remove(filepath.Join(dir, "sandbox_spec.json")))
				require.NoError(t, os.Symlink(filepath.Join(dir, "snapshot/config.json"), filepath.Join(dir, "sandbox_spec.json")))
			case "unbound":
				e.DiskBinding = nil
			case "remote":
				e.Backend = cow.BackendS3
			case "catalog":
				e.RootfsVol = "foreign"
			}
			err := ValidateColdRecreateBinding(context.Background(), dir, e, sandbox)
			if fault == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
