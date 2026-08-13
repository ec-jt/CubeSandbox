// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	cubeboxv1 "github.com/tencentcloud/CubeSandbox/Cubelet/api/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/networkagentclient"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
)

func TestPersistLivePortMappingsTracksExposeCloseAndResumeState(t *testing.T) {
	ctx := context.Background()
	sb := newCubeboxWithStatusForTest("sb-dynamic-ports", cubeboxstore.Status{})
	manager := &fakeCubeboxAPI{cb: sb}
	svc := &service{cubeboxMgr: &local{cubeboxManger: manager}}

	exposed := []networkagentclient.PortMapping{
		{ContainerPort: 17300, HostPort: 49901},
		{ContainerPort: 3000, HostPort: 49902},
	}
	require.NoError(t, svc.persistLivePortMappings(ctx, sb, exposed))
	require.Equal(t, map[int32]int32{17300: 49901, 3000: 49902}, storedPortMappings(sb.PortMappings))
	require.Equal(t, []string{sb.ID}, manager.syncIDs)

	// The persisted CubeBox is the record returned by Cubelet List and retained
	// across pause/resume, so the dynamic mapping remains re-advertisable.
	recovered := sb.DeepCopy()
	require.Equal(t, int32(49902), storedPortMappings(recovered.PortMappings)[3000])

	closed := []networkagentclient.PortMapping{{ContainerPort: 17300, HostPort: 49901}}
	require.NoError(t, svc.persistLivePortMappings(ctx, sb, closed))
	require.Equal(t, map[int32]int32{17300: 49901}, storedPortMappings(sb.PortMappings))
	_, exists := storedPortMappings(sb.PortMappings)[3000]
	require.False(t, exists)
	require.Equal(t, []string{sb.ID, sb.ID}, manager.syncIDs)
}

func storedPortMappings(mappings []*cubeboxv1.PortMapping) map[int32]int32 {
	result := make(map[int32]int32, len(mappings))
	for _, mapping := range mappings {
		result[mapping.GetContainerPort()] = mapping.GetHostPort()
	}
	return result
}
