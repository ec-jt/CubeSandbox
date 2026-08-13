// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizePortMappingsDeduplicatesAndSorts(t *testing.T) {
	s := &localService{cfg: Config{HostProxyBindIP: "127.0.0.1"}}
	got := s.normalizePortMappings([]PortMapping{
		{ContainerPort: 4000},
		{ContainerPort: 3000, Protocol: "tcp"},
		{ContainerPort: 4000, HostIP: "0.0.0.0"},
		{ContainerPort: 0},
	})
	require.Len(t, got, 2)
	require.Equal(t, int32(3000), got[0].ContainerPort)
	require.Equal(t, int32(4000), got[1].ContainerPort)
	require.Equal(t, "0.0.0.0", got[1].HostIP)
}
