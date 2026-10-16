// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	networkruntime "github.com/tencentcloud/CubeSandbox/Cubelet/network/runtime"
	networktypes "github.com/tencentcloud/CubeSandbox/Cubelet/network/types"
	networkstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/network"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
)

type portPluginRuntime struct {
	*fakeNetworkRuntime
	response       *networkruntime.EnsureNetworkResponse
	err            error
	calls          int
	port           int32
	infrastructure bool
	expose         bool
}

func (r *portPluginRuntime) ExposePort(_ context.Context, _ string, port int32, infrastructure bool) (*networkruntime.EnsureNetworkResponse, error) {
	r.calls++
	r.port, r.infrastructure, r.expose = port, infrastructure, true
	return r.response, r.err
}

func (r *portPluginRuntime) ClosePort(_ context.Context, _ string, port int32) (*networkruntime.EnsureNetworkResponse, error) {
	r.calls++
	r.port, r.expose = port, false
	return r.response, r.err
}

func newPortPlugin(t *testing.T) (*delegateNetworkManager, *portPluginRuntime, *utils.CubeStore) {
	t.Helper()
	db, err := utils.NewCubeStoreExt(t.TempDir(), "ports.db", 10, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	runtime := &portPluginRuntime{fakeNetworkRuntime: &fakeNetworkRuntime{}, response: &networkruntime.EnsureNetworkResponse{
		SandboxID: "ports", NetworkHandle: "handle",
		Interfaces:      []networkruntime.Interface{{Name: "tap0", MTU: 1500}},
		PersistMetadata: map[string]string{"sandbox_ip": "192.0.2.1", "dns": "1.1.1.1", "qos_enabled": "true", networkruntime.DynamicPortsMetadataKey: "[3000]"},
		PortMappings:    []networkruntime.PortMapping{{ContainerPort: 3000, HostPort: 20000, Protocol: "tcp"}},
	}}
	store := networkstore.NewStore(db)
	store.Add(networkstore.NetworkAllocation{SandboxID: "ports", NetworkType: "tap", AppID: 42, Timestamp: 123,
		Metadata:           &networktypes.ShimNetReq{Interfaces: []*networktypes.Interface{{Name: "tap0", IPAddr: net.ParseIP("192.0.2.1"), Qos: &networktypes.QosConfig{BwSize: 42}}}},
		PersistentMetadata: []byte(`{"sandbox_ip":"192.0.2.1","unknown":{"keep":true}}`),
	})
	require.NoError(t, store.Sync("ports"))
	return &delegateNetworkManager{tapPlugin: &local{networkRuntime: runtime}, allocationStore: store}, runtime, db
}

func readPortAllocation(t *testing.T, store *networkstore.Store) networkruntime.EnsureNetworkResponse {
	t.Helper()
	allocation, err := store.Get("ports")
	require.NoError(t, err)
	require.Equal(t, int64(42), allocation.AppID)
	require.Equal(t, int64(123), allocation.Timestamp)
	var metadata struct {
		SandboxIP string                               `json:"sandbox_ip"`
		Unknown   map[string]bool                      `json:"unknown"`
		Runtime   networkruntime.EnsureNetworkResponse `json:"runtime_network"`
	}
	require.NoError(t, json.Unmarshal(allocation.PersistentMetadata, &metadata))
	require.Equal(t, "192.0.2.1", metadata.SandboxIP)
	require.True(t, metadata.Unknown["keep"])
	return metadata.Runtime
}

func TestPortPluginCapabilityUnavailable(t *testing.T) {
	var absent *delegateNetworkManager
	_, err := absent.changePort(context.Background(), "ports", 3000, true, false)
	require.ErrorContains(t, err, "unavailable")
	m, _, _ := newPortPlugin(t)
	m.tapPlugin.networkRuntime = &fakeNetworkRuntime{}
	_, err = m.changePort(context.Background(), "ports", 3000, true, false)
	require.ErrorContains(t, err, "does not support")
}

func TestPortPluginDelegationAndReload(t *testing.T) {
	m, runtime, db := newPortPlugin(t)
	for _, infrastructure := range []bool{false, true} {
		response, err := m.changePort(context.Background(), "ports", 3000, true, infrastructure)
		require.NoError(t, err)
		require.Equal(t, infrastructure, runtime.infrastructure)
		require.Equal(t, runtime.response, response)
	}
	// Even a static/absent close must reach the runtime. The plugin does not
	// infer membership or reclassify a static/idempotent expose from annotations.
	_, err := m.changePort(context.Background(), "ports", 8080, false, false)
	require.NoError(t, err)
	require.Equal(t, 3, runtime.calls)
	require.False(t, runtime.expose)
	require.Equal(t, int32(8080), runtime.port)
	allocation, err := m.allocationStore.Get("ports")
	require.NoError(t, err)
	shim := allocation.Metadata.(*networktypes.ShimNetReq)
	require.Equal(t, 42, shim.Interfaces[0].Qos.BwSize)
	require.Equal(t, uint16(3000), shim.PortMappings[0].ContainerPort)
	recovered, err := networkstore.RecoverFromDB(db)
	require.NoError(t, err)
	require.Equal(t, *runtime.response, readPortAllocation(t, recovered))
}

func TestPortPluginPersistenceFailureRetry(t *testing.T) {
	m, runtime, db := newPortPlugin(t)
	require.NoError(t, db.Close())
	_, err := m.changePort(context.Background(), "ports", 3000, true, false)
	require.ErrorContains(t, err, "persist dynamic port allocation")
	require.Equal(t, *runtime.response, readPortAllocation(t, m.allocationStore))
	// Simulate restart with a stale allocation record but newer runtime state.
	replacement, _, replacementDB := newPortPlugin(t)
	m.allocationStore = replacement.allocationStore
	runtime.response.PortMappings = append(runtime.response.PortMappings, networkruntime.PortMapping{ContainerPort: 4000, HostPort: 20001})
	runtime.response.PersistMetadata[networkruntime.DynamicInfraPortsMetadataKey] = "[4000]"
	_, err = m.changePort(context.Background(), "ports", 3000, true, false)
	require.NoError(t, err)
	require.Equal(t, 2, runtime.calls)
	recovered, err := networkstore.RecoverFromDB(replacementDB)
	require.NoError(t, err)
	require.Equal(t, *runtime.response, readPortAllocation(t, recovered))
	// An already-closed port still refreshes both stores on retry.
	runtime.response.PortMappings = nil
	runtime.response.PersistMetadata[networkruntime.DynamicPortsMetadataKey] = "[]"
	runtime.response.PersistMetadata[networkruntime.DynamicInfraPortsMetadataKey] = "[]"
	_, err = m.changePort(context.Background(), "ports", 3000, false, false)
	require.NoError(t, err)
	recovered, err = networkstore.RecoverFromDB(replacementDB)
	require.NoError(t, err)
	require.Empty(t, readPortAllocation(t, recovered).PortMappings)
}

func TestPortPluginRuntimeFailureDoesNotPublish(t *testing.T) {
	m, runtime, db := newPortPlugin(t)
	runtime.err = errors.New("runtime failed")
	_, err := m.changePort(context.Background(), "ports", 3000, true, false)
	require.ErrorIs(t, err, runtime.err)
	recovered, err := networkstore.RecoverFromDB(db)
	require.NoError(t, err)
	require.Empty(t, readPortAllocation(t, recovered).PortMappings)
}

type sequentialPortRuntime struct{ *portPluginRuntime }

func (r *sequentialPortRuntime) ExposePort(_ context.Context, _ string, port int32, _ bool) (*networkruntime.EnsureNetworkResponse, error) {
	// Deliberately not synchronized: the plugin must serialize runtime call AND
	// metadata publication. Running this under -race also checks that boundary.
	r.calls++
	r.response.PortMappings = append(r.response.PortMappings, networkruntime.PortMapping{ContainerPort: port, HostPort: port + 10000})
	copy := *r.response
	copy.PortMappings = append([]networkruntime.PortMapping(nil), r.response.PortMappings...)
	return &copy, nil
}

func TestPortPluginConcurrentUpdatesRetainAllMappings(t *testing.T) {
	m, runtime, db := newPortPlugin(t)
	m.tapPlugin.networkRuntime = &sequentialPortRuntime{runtime}
	var wg sync.WaitGroup
	errors := make(chan error, 16)
	for i := int32(0); i < 16; i++ {
		wg.Add(1)
		go func(port int32) {
			defer wg.Done()
			_, err := m.changePort(context.Background(), "ports", port, true, false)
			if err != nil {
				errors <- fmt.Errorf("port %d: %w", port, err)
			}
		}(4000 + i)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	recovered, err := networkstore.RecoverFromDB(db)
	require.NoError(t, err)
	require.Len(t, readPortAllocation(t, recovered).PortMappings, 17)
}
