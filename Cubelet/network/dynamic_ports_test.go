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
