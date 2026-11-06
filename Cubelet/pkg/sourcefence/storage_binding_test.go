package sourcefence

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceStorageSidecarImmutableRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fences")
	s, err := Open(root)
	require.NoError(t, err)
	b := Binding{"op", "source", "node", "epoch", "selected", strings.Repeat("a", 64)}
	raw := []byte(`{"live_uuid":"native","generation":7}`)
	require.NoError(t, s.BindStorage(b, raw))
	require.ErrorIs(t, s.Check("source"), ErrFenced)
	require.ErrorIs(t, s.Any(), ErrFenced)
	s, err = Open(root)
	require.NoError(t, err)
	require.NoError(t, s.BindStorage(b, raw))
	require.ErrorIs(t, s.BindStorage(b, []byte(`{"live_uuid":"other"}`)), ErrBinding)
	other := b
	other.OperationID = "different"
	require.ErrorIs(t, s.BindStorage(other, raw), ErrBinding)
	require.False(t, s.MatchesBinding(b), "sidecar alone is not runtime admission")
	path := s.filename(b.SandboxID) + ".storage-v1"
	require.NoError(t, os.WriteFile(path, []byte("corrupt"), 0600))
	require.ErrorIs(t, s.BindStorage(b, raw), ErrBinding)
	require.ErrorIs(t, s.Check("source"), ErrFenced)
}

func TestSourceStorageCompatibilityNeverMigratesRecord(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "fences"))
	require.NoError(t, err)
	b := Binding{"op", "source", "node", "legacy-empty-map", "selected", strings.Repeat("a", 64)}
	r := &Record{Version: 1, Binding: b, Stage: RuntimeStopped, Processes: []Process{{PID: 99, StartTime: "1", BootID: "boot", Executable: "shim"}}}
	require.NoError(t, s.persist(r, true))
	before, err := os.ReadFile(s.filename(b.SandboxID))
	require.NoError(t, err)
	require.True(t, s.MatchesBinding(b))
	changed := b
	changed.Incarnation = "canonical-nil"
	require.False(t, s.MatchesBinding(changed))
	after, err := os.ReadFile(s.filename(b.SandboxID))
	require.NoError(t, err)
	require.Equal(t, before, after)
}
