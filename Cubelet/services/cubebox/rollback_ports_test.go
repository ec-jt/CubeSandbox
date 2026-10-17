package cubebox

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	networkruntime "github.com/tencentcloud/CubeSandbox/Cubelet/network/runtime"
)

func TestRollbackPortsMatchingAndChangedIntent(t *testing.T) {
	want := &networkruntime.PortablePorts{Version: 1, Ports: []networkruntime.PortablePort{
		{ContainerPort: 80, Class: "static"}, {ContainerPort: 3000, Class: "user"}, {ContainerPort: 4000, Class: "infrastructure"},
	}}
	dir := t.TempDir()
	require.NoError(t, writeSnapshotPorts(dir, want))
	for _, tc := range []struct {
		name    string
		ports   []networkruntime.PortablePort
		allowed bool
	}{
		{"reordered", []networkruntime.PortablePort{want.Ports[2], want.Ports[0], want.Ports[1]}, true},
		{"closed", want.Ports[:2], false},
		{"added", append(append([]networkruntime.PortablePort{}, want.Ports...), networkruntime.PortablePort{ContainerPort: 5000, Class: "user"}), false},
		{"reclassified", []networkruntime.PortablePort{want.Ports[0], {ContainerPort: 3000, Class: "static"}, want.Ports[2]}, false},
		{"empty", []networkruntime.PortablePort{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := validateRollbackSnapshotPorts(dir, func() (*networkruntime.PortablePorts, error) {
				calls++
				return &networkruntime.PortablePorts{Version: 1, Ports: tc.ports}, nil
			})
			require.Equal(t, 1, calls)
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "snapshot recreation")
			}
		})
	}
}

func TestRollbackPortsEmptyAndLegacy(t *testing.T) {
	dir := t.TempDir()
	calls := 0
	capture := func() (*networkruntime.PortablePorts, error) {
		calls++
		return &networkruntime.PortablePorts{Version: 1, Ports: []networkruntime.PortablePort{}}, nil
	}
	require.NoError(t, validateRollbackSnapshotPorts(dir, capture))
	require.Zero(t, calls, "legacy package retains historical behavior")
	require.NoError(t, writeSnapshotPorts(dir, &networkruntime.PortablePorts{Version: 1, Ports: []networkruntime.PortablePort{}}))
	require.NoError(t, validateRollbackSnapshotPorts(dir, capture))
	require.Equal(t, 1, calls, "explicit empty requires live reconciliation")
	require.Error(t, validateRollbackSnapshotPorts(dir, func() (*networkruntime.PortablePorts, error) {
		return &networkruntime.PortablePorts{Version: 1, Ports: []networkruntime.PortablePort{{ContainerPort: 80, Class: "static"}}}, nil
	}))
}

func TestRollbackPortsCaptureFailureAndInvalidState(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeSnapshotPorts(dir, &networkruntime.PortablePorts{Version: 1, Ports: []networkruntime.PortablePort{}}))
	for _, message := range []string{"pending runtime", "allocation sync", "sandbox sync"} {
		failure := errors.New(message)
		require.ErrorIs(t, validateRollbackSnapshotPorts(dir, func() (*networkruntime.PortablePorts, error) { return nil, failure }), failure)
	}
	for _, current := range []*networkruntime.PortablePorts{nil, {Version: 2, Ports: []networkruntime.PortablePort{}}, {Version: 1}} {
		require.ErrorContains(t, validateRollbackSnapshotPorts(dir, func() (*networkruntime.PortablePorts, error) { return current, nil }), "unavailable or invalid")
	}
}

func TestRollbackPortsMalformedRecordStopsBeforeCapture(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, snapshotPortsFileName), []byte(`{"version":999,"ports":[]}`), 0600))
	require.ErrorContains(t, validateRollbackSnapshotPorts(dir, func() (*networkruntime.PortablePorts, error) {
		t.Fatal("capture called for invalid snapshot")
		return nil, nil
	}), "read portable snapshot ports")
}
