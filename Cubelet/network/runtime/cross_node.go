package runtime

import (
	"fmt"
	"github.com/tencentcloud/CubeSandbox/CubeNet/cubevs"
	"github.com/vishvananda/netlink"
	"net"
)

// Private VLANs need no default gateway. Resolve only their L2/IPv4 identity.
var crossNodeLinkByName = netlink.LinkByName
var crossNodeAddrList = netlink.AddrList

func configureCrossNode(cfg Config, params *cubevs.Params) error {
	if cfg.CrossNodeEthName == "" {
		return nil
	}
	if cfg.CrossNodeEthName == cfg.EthName {
		return fmt.Errorf("cross-node interface must differ from primary interface")
	}
	link, err := crossNodeLinkByName(cfg.CrossNodeEthName)
	if err != nil {
		return fmt.Errorf("resolve cross-node interface: %w", err)
	}
	if link == nil || link.Attrs() == nil || link.Attrs().Index <= 0 || len(link.Attrs().HardwareAddr) != 6 {
		return fmt.Errorf("cross-node interface requires valid ifindex and Ethernet MAC")
	}
	attrs := link.Attrs()
	if uint32(attrs.Index) == params.NodeIfindex || uint32(attrs.Index) == params.CubeRouterIfindex {
		return fmt.Errorf("cross-node interface conflicts with primary/router device")
	}
	addresses, err := crossNodeAddrList(link, netlink.FAMILY_V4)
	if err != nil {
		return fmt.Errorf("list cross-node IPv4 addresses: %w", err)
	}
	if len(addresses) != 1 || addresses[0].IP.To4() == nil || !addresses[0].IP.IsGlobalUnicast() {
		return fmt.Errorf("cross-node interface requires exactly one unicast IPv4 address")
	}
	params.CrossNodeIngressIfindex = uint32(attrs.Index)
	params.CrossNodeIP = append(net.IP(nil), addresses[0].IP.To4()...)
	params.CrossNodeMacAddr = append(net.HardwareAddr(nil), attrs.HardwareAddr...)
	return nil
}
