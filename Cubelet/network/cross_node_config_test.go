package network

import "testing"

func TestCrossNodeConfigForwarded(t *testing.T) {
	cfg := networkRuntimeConfigFromPluginConfig(&Config{EthName: "eth0", CrossNodeEthName: "vlan4000"})
	if cfg.EthName != "eth0" || cfg.CrossNodeEthName != "vlan4000" {
		t.Fatalf("configuration lost: %+v", cfg)
	}
	cfg = networkRuntimeConfigFromPluginConfig(&Config{EthName: "eth0"})
	if cfg.CrossNodeEthName != "" {
		t.Fatal("cross-node mode enabled implicitly")
	}
}
