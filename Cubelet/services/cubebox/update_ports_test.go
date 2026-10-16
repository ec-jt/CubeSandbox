// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	networkruntime "github.com/tencentcloud/CubeSandbox/Cubelet/network/runtime"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

type livePortRuntime struct {
	response       *networkruntime.EnsureNetworkResponse
	err            error
	calls          int
	port           int32
	infrastructure bool
	expose         bool
	entered        chan struct{}
	proceed        chan struct{}
}

func (r *livePortRuntime) ExposePort(_ context.Context, _ string, port int32, infrastructure bool) (*networkruntime.EnsureNetworkResponse, error) {
	if r.entered != nil {
		r.entered <- struct{}{}
		<-r.proceed
	}
	r.calls++
	r.port, r.infrastructure, r.expose = port, infrastructure, true
	return r.response, r.err
}

func (r *livePortRuntime) ClosePort(_ context.Context, _ string, port int32) (*networkruntime.EnsureNetworkResponse, error) {
	r.calls++
	r.port, r.expose = port, false
	return r.response, r.err
}

func portRequest(action, port, class string) *cubebox.UpdateCubeSandboxRequest {
	return &cubebox.UpdateCubeSandboxRequest{SandboxID: "ports", RequestID: "port-request", Annotations: map[string]string{
		constants.MasterAnnotationsUpdateAction: action,
		"cube.master.container_port":            port, "cube.master.port_class": class,
	}}
}

func newPortService(t *testing.T) (*service, *fakeCubeboxAPI, *livePortRuntime) {
	t.Helper()
	sb := newCubeboxWithStatusForTest("ports", cubeboxstore.Status{StartedAt: time.Now().UnixNano()})
	s, manager, _ := newDestroyLifecycleServiceForTest(sb)
	runtime := &livePortRuntime{response: &networkruntime.EnsureNetworkResponse{
		SandboxID:       sb.ID,
		PortMappings:    []networkruntime.PortMapping{{ContainerPort: 3000, HostPort: 20000}},
		PersistMetadata: map[string]string{networkruntime.DynamicPortsMetadataKey: "[3000]", networkruntime.DynamicInfraPortsMetadataKey: "[]"},
	}}
	s.dynamicPortNetwork = runtime
	return s, manager, runtime
}

func TestLivePortValidation(t *testing.T) {
	for _, action := range []string{constants.UpdateActionExposePort, constants.UpdateActionClosePort} {
		for _, port := range []string{"", "abc", "0", "-1", "65536", "2147483648", "17300", "17301", "49983", "49999"} {
			t.Run(action+"/"+port, func(t *testing.T) {
				s, manager, runtime := newPortService(t)
				rsp, err := s.Update(context.Background(), portRequest(action, port, "infrastructure"))
				require.NoError(t, err)
				require.Equal(t, errorcode.ErrorCode_InvalidParamFormat, rsp.Ret.RetCode)
				require.Zero(t, runtime.calls)
				require.Empty(t, manager.syncIDs)
			})
		}
	}
	s, _, runtime := newPortService(t)
	rsp, err := s.Update(context.Background(), portRequest(constants.UpdateActionExposePort, "3000", "invalid"))
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_InvalidParamFormat, rsp.Ret.RetCode)
	require.Zero(t, runtime.calls)
}

func TestLivePortStateGate(t *testing.T) {
	for _, status := range []cubeboxstore.Status{{}, {CreatedAt: 1}, {StartedAt: 1, PausingAt: 2}, {StartedAt: 1, PausedAt: 2}, {StartedAt: 1, FinishedAt: 2}, {Unknown: true}} {
		for _, action := range []string{constants.UpdateActionExposePort, constants.UpdateActionClosePort} {
			s, manager, runtime := newPortService(t)
			manager.cb.FirstContainer().Status = cubeboxstore.StoreStatus(status)
			rsp, err := s.Update(context.Background(), portRequest(action, "3000", "user"))
			require.NoError(t, err)
			require.Equal(t, errorcode.ErrorCode_TaskStateInvalid, rsp.Ret.RetCode)
			require.Zero(t, runtime.calls)
		}
	}
}

func TestLivePortClassificationAndUncappedCompatibility(t *testing.T) {
	for _, class := range []string{"", "user", "infrastructure"} {
		for _, port := range []string{"1", "65535"} {
			for _, limit := range []string{"0", "1", "-1", "bad", "999999999999999999"} {
				s, manager, runtime := newPortService(t)
				req := portRequest(constants.UpdateActionExposePort, port, class)
				req.Annotations["cube.master.port_limit"] = limit
				rsp, err := s.Update(context.Background(), req)
				require.NoError(t, err)
				require.Equal(t, errorcode.ErrorCode_Success, rsp.Ret.RetCode)
				require.Equal(t, class == "infrastructure", runtime.infrastructure)
				// Response classification wins over the requested classification,
				// including an idempotent expose of a previously classified port.
				require.Equal(t, []int32{3000}, manager.cb.DynamicPorts)
				require.Empty(t, manager.cb.DynamicInfraPorts)
			}
		}
	}
}

func TestLivePortRuntimeAndSandboxPersistenceFailures(t *testing.T) {
	for _, action := range []string{constants.UpdateActionExposePort, constants.UpdateActionClosePort} {
		t.Run(action, func(t *testing.T) {
			s, manager, runtime := newPortService(t)
			runtime.err = errors.New("runtime failed")
			req := portRequest(action, "3000", "")
			rsp, err := s.Update(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, errorcode.ErrorCode_CreateNetworkFailed, rsp.Ret.RetCode)
			require.Empty(t, manager.syncIDs)
			runtime.err = nil
			manager.syncErr = errors.New("database failed")
			rsp, err = s.Update(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, errorcode.ErrorCode_UpdateLocalMetaDataFailed, rsp.Ret.RetCode)
			require.Len(t, manager.cb.PortMappings, 1, "do not roll back committed runtime view")
			// Another runtime mapping appeared before retry. A stale local
			// existence check must not skip runtime delegation or the next sync.
			runtime.response.PortMappings = append(runtime.response.PortMappings, networkruntime.PortMapping{ContainerPort: 4000, HostPort: 20001})
			runtime.response.PersistMetadata[networkruntime.DynamicInfraPortsMetadataKey] = "[4000]"
			manager.syncErr = nil
			rsp, err = s.Update(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, errorcode.ErrorCode_Success, rsp.Ret.RetCode)
			require.Equal(t, 3, runtime.calls)
			require.Len(t, manager.cb.PortMappings, 2)
			require.Equal(t, []int32{4000}, manager.cb.DynamicInfraPorts)
		})
	}
}

func TestLivePortSandboxDiskRoundTrip(t *testing.T) {
	_, manager, runtime := newPortService(t)
	sb := manager.cb
	sb.NetworkType = "tap"
	sb.ExposedPorts = []int64{8080}
	sb.Labels = map[string]string{"keep": "label"}
	sb.CubeNetworkConfig = &cubebox.CubeNetworkConfig{AllowOut: []string{"example.com"}}
	db, err := utils.NewCubeStoreExt(t.TempDir(), "sandbox.db", 10, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store := cubeboxstore.NewStore(db)
	store.Add(sb)
	for _, expose := range []bool{true, false, false} {
		if !expose {
			runtime.response.PortMappings = nil
			runtime.response.PersistMetadata[networkruntime.DynamicPortsMetadataKey] = "[]"
		}
		rsp, err := updateLivePort(context.Background(), portRequest("", "3000", ""), sb, expose, runtime, func() error { return store.Sync(sb.ID) })
		require.NoError(t, err)
		require.Equal(t, errorcode.ErrorCode_Success, rsp.Ret.RetCode)
		rows, err := db.ReadAll(cubeboxstore.DBBucketSandbox)
		require.NoError(t, err)
		var recovered cubeboxstore.CubeBox
		require.NoError(t, json.Unmarshal(rows[sb.ID], &recovered))
		require.ElementsMatch(t, sb.DynamicPorts, recovered.DynamicPorts)
		require.Len(t, recovered.PortMappings, len(runtime.response.PortMappings))
		require.Equal(t, []int64{8080}, recovered.ExposedPorts, "template port intent must not be rewritten")
		require.Equal(t, "label", recovered.Labels["keep"])
		require.Equal(t, "tap", recovered.NetworkType)
		require.Equal(t, []string{"example.com"}, recovered.CubeNetworkConfig.AllowOut)
	}
}

func TestLivePortLifecycleLockIncludesPersistence(t *testing.T) {
	s, _, runtime := newPortService(t)
	runtime.entered, runtime.proceed = make(chan struct{}, 2), make(chan struct{})
	done := make(chan error, 2)
	call := func() {
		rsp, err := s.Update(context.Background(), portRequest(constants.UpdateActionExposePort, "3000", ""))
		if err == nil && rsp.Ret.RetCode != errorcode.ErrorCode_Success {
			err = fmt.Errorf("%v", rsp.Ret)
		}
		done <- err
	}
	go call()
	<-runtime.entered
	go call()
	select {
	case <-runtime.entered:
		t.Fatal("second update bypassed lifecycle lock")
	case <-time.After(30 * time.Millisecond):
	}
	close(runtime.proceed)
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	require.Equal(t, 2, runtime.calls)
}
