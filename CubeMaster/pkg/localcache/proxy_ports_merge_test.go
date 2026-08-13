// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package localcache

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/config"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/rediskey"
	proxytypes "github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/types"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/wrapredis"
)

// TestMergeSandboxProxyPortsAddsDynamicPort is a regression test for the
// dynamic-port proxy-map gap: after exposePort, the Redis proxy hash must
// contain the dynamic container->host port mapping. The previous refresh path
// rewrote the whole hash via SetSandboxProxyMap and was observed to drop the
// dynamic port; MergeSandboxProxyPorts writes the port fields directly.
func TestMergeSandboxProxyPortsAddsDynamicPort(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := config.GetConfig()
	cfg.RedisConf = &config.RedisConf{
		Nodes:       server.Addr(),
		MaxActive:   4,
		MaxIdle:     1,
		MaxRetry:    1,
		DbNo:        0,
		IdleTimeout: 30,
	}
	// The proxy-map read/write helpers resolve the process-global Redis pool
	// from config on first use; reset it so this test's miniredis is the one
	// actually dialed regardless of test execution order.
	wrapredis.ResetPoolForTest()

	cache := &local{}
	sandboxID := "sandbox-merge-1"
	key := rediskey.SandboxProxy(sandboxID)

	// Seed the proxy hash as sandbox creation does: template ports only.
	seed := &proxytypes.SandboxProxyMap{
		HostIP:             "10.0.1.2",
		SandboxIP:          "192.168.0.10",
		CreatedAt:          "111",
		AllowPublicTraffic: true,
		ContainerToHostPorts: map[string]string{
			"17300": "20027",
			"49983": "20028",
			"49999": "20029",
		},
	}
	if err := cache.setByPassProsyToRedis(context.Background(), key, seed); err != nil {
		t.Fatalf("seed setByPassProsyToRedis failed: %v", err)
	}

	// Expose 3000 -> 20030 (template ports + the new dynamic port).
	merged := map[string]string{
		"17300": "20027",
		"49983": "20028",
		"49999": "20029",
		"3000":  "20030",
	}
	if err := MergeSandboxProxyPorts(context.Background(), sandboxID, merged); err != nil {
		t.Fatalf("MergeSandboxProxyPorts failed: %v", err)
	}

	got, err := cache.getByPassProsyFromRedis(context.Background(), key)
	if err != nil {
		t.Fatalf("getByPassProsyFromRedis failed: %v", err)
	}
	if got.ContainerToHostPorts["3000"] != "20030" {
		t.Fatalf("dynamic port 3000 missing from proxy map: %#v", got.ContainerToHostPorts)
	}
	// Metadata fields must be preserved by the merge (not cleared).
	if got.HostIP != "10.0.1.2" || got.SandboxIP != "192.168.0.10" || !got.AllowPublicTraffic {
		t.Fatalf("metadata fields clobbered by merge: %#v", got)
	}

	// Closing 3000 must remove only that field, leaving template ports intact.
	afterClose := map[string]string{
		"17300": "20027",
		"49983": "20028",
		"49999": "20029",
	}
	if err := MergeSandboxProxyPorts(context.Background(), sandboxID, afterClose); err != nil {
		t.Fatalf("MergeSandboxProxyPorts (close) failed: %v", err)
	}
	got2, err := cache.getByPassProsyFromRedis(context.Background(), key)
	if err != nil {
		t.Fatalf("getByPassProsyFromRedis after close failed: %v", err)
	}
	if _, present := got2.ContainerToHostPorts["3000"]; present {
		t.Fatalf("dynamic port 3000 not removed after close: %#v", got2.ContainerToHostPorts)
	}
	if got2.ContainerToHostPorts["17300"] != "20027" {
		t.Fatalf("template port lost after close: %#v", got2.ContainerToHostPorts)
	}
}
