package cubeboxcbri

import (
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/images/v1"
	"os"
	"path/filepath"
	"testing"
)

func TestColdRecreateLowerBinding(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "app"), 0700))
	path := filepath.Join(dir, "app", "app.ext4")
	require.NoError(t, os.WriteFile(path, []byte("immutable lower"), 0600))
	e := &cubeboxInstancePlugin{config: &cubeboxInstancePluginConfig{ImageBasePath: dir}}
	c := &cubebox.ContainerConfig{Image: &images.ImageSpec{Image: "app"}}
	require.Error(t, e.bindRetainedLower(c, true))
	require.NoError(t, e.bindRetainedLower(c, false))
	require.NoError(t, e.bindRetainedLower(c, true))
	require.NoError(t, os.WriteFile(path, []byte("changed"), 0600))
	require.Error(t, e.bindRetainedLower(c, true))
	c.Image.Image = "../outside"
	require.Error(t, e.bindRetainedLower(c, false))
}
