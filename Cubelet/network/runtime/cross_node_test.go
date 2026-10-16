package runtime

import (
	"errors"
	"github.com/tencentcloud/CubeSandbox/CubeNet/cubevs"
	"github.com/vishvananda/netlink"
	"net"
	"testing"
)

func TestConfigureCrossNode(t *testing.T) {
	oldLink, oldAddr := crossNodeLinkByName, crossNodeAddrList
	t.Cleanup(func() { crossNodeLinkByName, crossNodeAddrList = oldLink, oldAddr })
	for _, name := range []string{"disabled", "valid", "lookup-error", "missing-ip", "multiple-ip", "bad-mac", "same-nic", "router-conflict"} {
		t.Run(name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.EthName = "eth0"
			cfg.CrossNodeEthName = "vlan4000"
			if name == "disabled" {
				cfg.CrossNodeEthName = ""
			}
			if name == "same-nic" {
				cfg.CrossNodeEthName = "eth0"
			}
			calls := 0
			crossNodeLinkByName = func(string) (netlink.Link, error) {
				calls++
				if name == "lookup-error" {
					return nil, errors.New("missing")
				}
				mac := net.HardwareAddr{2, 3, 4, 5, 6, 7}
				if name == "bad-mac" {
					mac = nil
				}
				return &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Index: 2, HardwareAddr: mac}}, nil
			}
			crossNodeAddrList = func(netlink.Link, int) ([]netlink.Addr, error) {
				a := netlink.Addr{IPNet: &net.IPNet{IP: net.ParseIP("10.0.1.1"), Mask: net.CIDRMask(24, 32)}}
				if name == "missing-ip" {
					return nil, nil
				}
				if name == "multiple-ip" {
					return []netlink.Addr{a, a}, nil
				}
				return []netlink.Addr{a}, nil
			}
			params := cubevs.Params{NodeIfindex: 1, CubeRouterIfindex: 3}
			if name == "router-conflict" {
				params.CubeRouterIfindex = 2
			}
			err := configureCrossNode(cfg, &params)
			if name == "disabled" {
				if err != nil || calls != 0 || params.CrossNodeIngressIfindex != 0 {
					t.Fatal("disabled touched network")
				}
				return
			}
			if name == "valid" {
				if err != nil || params.CrossNodeIngressIfindex != 2 || !params.CrossNodeIP.Equal(net.ParseIP("10.0.1.1")) {
					t.Fatalf("invalid result: %v", err)
				}
			} else if err == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
}
