package cubevs

import (
	"github.com/cilium/ebpf"
	"net"
	"testing"
)

func TestCrossNodeConstants(t *testing.T) {
	values, err := crossNodeConstants(Params{})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range values {
		if v != uint32(0) && v != uint16(0) {
			t.Fatal("disabled mode not zero")
		}
	}
	p := Params{NodeIfindex: 1, CubeRouterIfindex: 3, CrossNodeIngressIfindex: 2, CrossNodeIP: net.ParseIP("10.0.1.1"), CrossNodeMacAddr: net.HardwareAddr{2, 3, 4, 5, 6, 7}}
	values, err = crossNodeConstants(p)
	if err != nil || values[globalNameCrossNodeIfindex] != uint32(2) {
		t.Fatalf("valid configuration: %v %v", values, err)
	}
	for _, index := range []uint32{1, 3} {
		bad := p
		bad.CrossNodeIngressIfindex = index
		if _, err := crossNodeConstants(bad); err == nil {
			t.Fatal("accepted conflicting NIC")
		}
	}
	bad := p
	bad.CrossNodeIP = nil
	if _, err := crossNodeConstants(bad); err == nil {
		t.Fatal("accepted nil IP")
	}
	bad = p
	bad.CrossNodeMacAddr = nil
	if _, err := crossNodeConstants(bad); err == nil {
		t.Fatal("accepted nil MAC")
	}
}

// CollectionSpec parsing and constant writes do not load any kernel programs.
func TestCrossNodeObjectABI(t *testing.T) {
	p := Params{NodeIfindex: 1, CrossNodeIngressIfindex: 2, CrossNodeIP: net.ParseIP("10.0.1.1"), CrossNodeMacAddr: net.HardwareAddr{2, 3, 4, 5, 6, 7}}
	values, err := crossNodeConstants(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, load := range []func() (*ebpf.CollectionSpec, error){loadLocalgw, loadNodenic, loadMvmtap} {
		spec, err := load()
		if err != nil {
			t.Fatal(err)
		}
		for name, value := range values {
			v := spec.Variables[name]
			if v == nil {
				t.Fatalf("missing global %s", name)
			}
			if err := v.Set(value); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		}
		m := spec.Maps["cross_egress_v2"]
		if m == nil || m.Type != ebpf.LRUHash || m.KeySize != 20 {
			t.Fatalf("wrong cross-node map ABI: %+v", m)
		}
		if spec.Maps["host_port_egress"] != nil {
			t.Fatal("legacy map ABI reused")
		}
	}
}
