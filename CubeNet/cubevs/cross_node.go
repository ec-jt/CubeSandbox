package cubevs

import "fmt"

func crossNodeConstants(params Params) (map[string]interface{}, error) {
	values := map[string]interface{}{
		globalNameCrossNodeIP: uint32(0), globalNameCrossNodeIfindex: uint32(0),
		globalNameCrossNodeMacaddrP1: uint32(0), globalNameCrossNodeMacaddrP2: uint16(0),
	}
	if params.CrossNodeIngressIfindex == 0 {
		return values, nil
	}
	if params.CrossNodeIngressIfindex == params.NodeIfindex || params.CrossNodeIngressIfindex == params.CubeRouterIfindex {
		return nil, fmt.Errorf("cross-node interface conflicts with primary/router")
	}
	if params.CrossNodeIP.To4() == nil || len(params.CrossNodeMacAddr) != 6 {
		return nil, fmt.Errorf("cross-node mode requires IPv4 and Ethernet MAC")
	}
	values[globalNameCrossNodeIP] = ipToUint32(params.CrossNodeIP.To4())
	values[globalNameCrossNodeIfindex] = params.CrossNodeIngressIfindex
	values[globalNameCrossNodeMacaddrP1] = hardwareAddrToUint32(params.CrossNodeMacAddr)
	values[globalNameCrossNodeMacaddrP2] = hardwareAddrToUint16(params.CrossNodeMacAddr)
	return values, nil
}
