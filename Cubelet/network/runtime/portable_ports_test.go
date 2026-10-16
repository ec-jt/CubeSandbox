package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPortablePortsRoundTripFreshAllocation(t *testing.T) {
	c, _ := newDynamicTestController(t)
	ctx := context.Background()
	_, err := c.ExposePort(ctx, "dynamic", 3000, false)
	require.NoError(t, err)
	_, err = c.ExposePort(ctx, "dynamic", 4000, true)
	require.NoError(t, err)
	_, err = c.ExposePort(ctx, "dynamic", 5000, false)
	require.NoError(t, err)
	_, err = c.ClosePort(ctx, "dynamic", 5000)
	require.NoError(t, err)
	before, err := c.ReconcileNetworkPorts(ctx, "dynamic")
	require.NoError(t, err)
	ports, err := PortablePortsFromNetwork(before)
	require.NoError(t, err)
	raw, err := json.Marshal(ports)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "host")
	var packed PortablePorts
	require.NoError(t, json.Unmarshal(raw, &packed))
	require.Equal(t, []int64{80, 3000, 4000}, packed.ExposedPorts())
	request := &EnsureNetworkRequest{SandboxID: "restored"}
	require.NoError(t, packed.ApplyTo(request))
	for _, p := range request.PortMappings {
		require.Zero(t, p.HostPort)
		require.Empty(t, p.HostIP)
	}
	after, err := c.EnsureNetwork(ctx, request)
	require.NoError(t, err)
	t.Cleanup(func() { c.tapAdapter.Close(c.states["restored"].tap.File) })
	// Source reservations are still held, so restore must choose different ports.
	for _, a := range before.PortMappings {
		for _, b := range after.PortMappings {
			require.NotEqual(t, a.HostPort, b.HostPort)
		}
	}
	restored, err := PortablePortsFromNetwork(after)
	require.NoError(t, err)
	require.Equal(t, ports, restored)
	for _, port := range []int32{3000, 4000} {
		_, err := c.ClosePort(ctx, "restored", port)
		require.NoError(t, err)
	}
	after, err = c.ClosePort(ctx, "restored", 80)
	require.NoError(t, err)
	require.Len(t, after.PortMappings, 1)
	require.Equal(t, int32(80), after.PortMappings[0].ContainerPort)
}

func TestPortablePortsValidation(t *testing.T) {
	for _, p := range []*PortablePorts{
		nil, {Version: 2, Ports: []PortablePort{}}, {Version: 1},
		{Version: 1, Ports: []PortablePort{{0, "static"}}},
		{Version: 1, Ports: []PortablePort{{65536, "user"}}},
		{Version: 1, Ports: []PortablePort{{80, "static"}, {80, "user"}}},
		{Version: 1, Ports: []PortablePort{{80, "invalid"}}},
		{Version: 1, Ports: []PortablePort{{17300, "infrastructure"}}},
		{Version: 1, Ports: []PortablePort{{17301, "user"}}},
		{Version: 1, Ports: []PortablePort{{49983, "user"}}},
		{Version: 1, Ports: []PortablePort{{49999, "infrastructure"}}},
	} {
		require.Error(t, p.Validate())
	}
	require.NoError(t, (&PortablePorts{Version: 1, Ports: []PortablePort{}}).Validate())
	for _, metadata := range []map[string]string{
		{DynamicPortsMetadataKey: "[80,80]"},
		{DynamicPortsMetadataKey: "[80]", DynamicInfraPortsMetadataKey: "[80]"},
		{DynamicPortsMetadataKey: "[3000]"},
		{DynamicPortsMetadataKey: "null"},
		{DynamicPortsMetadataKey: "garbage"},
	} {
		_, err := PortablePortsFromNetwork(&EnsureNetworkResponse{PortMappings: []PortMapping{{ContainerPort: 80}}, PersistMetadata: metadata})
		require.Error(t, err)
	}
}

func TestPortablePortsCapturePendingBarrier(t *testing.T) {
	c, a := newDynamicTestController(t)
	ctx := context.Background()
	_, err := c.ExposePort(ctx, "dynamic", 3000, false)
	require.NoError(t, err)
	a.failDelete = true
	_, err = c.ClosePort(ctx, "dynamic", 3000)
	require.Error(t, err)
	_, err = c.ReconcileNetworkPorts(ctx, "dynamic")
	require.Error(t, err)
	a.failDelete = false
	response, err := c.ReconcileNetworkPorts(ctx, "dynamic")
	require.NoError(t, err)
	ports, err := PortablePortsFromNetwork(response)
	require.NoError(t, err)
	require.Equal(t, []int64{80}, ports.ExposedPorts())
	assertDynamicState(t, c)
	count := len(c.states)
	_, err = c.ReconcileNetworkPorts(ctx, "absent")
	require.True(t, errors.Is(err, ErrNetworkNotActive))
	require.Equal(t, count, len(c.states))
	require.False(t, c.store.Exists("absent", StateFileSuccess))
}

func TestPortablePortsCaptureFailedExposeAndConcurrentChanges(t *testing.T) {
	c, a := newDynamicTestController(t)
	ctx := context.Background()
	a.failAdd, a.failDelete = true, true
	_, err := c.ExposePort(ctx, "dynamic", 3000, false)
	require.Error(t, err)
	_, err = c.ReconcileNetworkPorts(ctx, "dynamic")
	require.Error(t, err)
	a.failAdd, a.failDelete = false, false
	response, err := c.ReconcileNetworkPorts(ctx, "dynamic")
	require.NoError(t, err)
	ports, err := PortablePortsFromNetwork(response)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual([]int64{80}, ports.ExposedPorts()))
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 20; i++ {
			if _, err := c.ExposePort(ctx, "dynamic", 3000, false); err != nil {
				done <- err
				return
			}
			if _, err := c.ClosePort(ctx, "dynamic", 3000); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for i := 0; i < 20; i++ {
		_, err := c.ReconcileNetworkPorts(ctx, "dynamic")
		require.NoError(t, err)
	}
	require.NoError(t, <-done)
}
