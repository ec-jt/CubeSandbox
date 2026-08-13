package network

import "testing"

func TestDynamicPortSetRoundTrip(t *testing.T) {
	metadata := map[string]string{DynamicPortsMetadataKey: `[4000,3000,4000,0,65536]`}
	ports := DynamicPortSet(metadata)
	if len(ports) != 2 {
		t.Fatalf("dynamic ports=%v, want exactly 3000 and 4000", ports)
	}
	if got := encodeDynamicPortSet(ports); got != `[3000,4000]` {
		t.Fatalf("encoded dynamic ports=%s, want sorted stable JSON", got)
	}
}

func TestDynamicPortSetInvalidMetadataIsEmpty(t *testing.T) {
	if ports := DynamicPortSet(map[string]string{DynamicPortsMetadataKey: `invalid`}); len(ports) != 0 {
		t.Fatalf("invalid metadata produced ports=%v", ports)
	}
}

func TestDynamicUserAndInfraPortSetsAreIndependent(t *testing.T) {
	metadata := map[string]string{
		DynamicPortsMetadataKey:      `[3000,4000]`,
		DynamicInfraPortsMetadataKey: `[9000,2999,4001]`,
	}
	if got := len(DynamicPortSet(metadata)); got != 2 {
		t.Fatalf("quota-counted dynamic ports=%d, want 2", got)
	}
	infra := DynamicInfraPortSet(metadata)
	if got := len(infra); got != 3 {
		t.Fatalf("infrastructure dynamic ports=%d, want 3", got)
	}
	if _, ok := infra[9000]; !ok {
		t.Fatal("infrastructure port 9000 was not retained")
	}
}
