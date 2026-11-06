package sourcefence

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// This is a serialization compatibility check, NOT executable qualification or
// permission to call the candidate retained stop from the production callback.
func TestSourceFenceRetainedShimWire(t *testing.T) {
	r := Record{Version: 1, Binding: Binding{"operation-1", "source-1", "node-1", "incarnation-1", "checkpoint-1", strings.Repeat("a", 64)}, Stage: Admitted,
		Processes: []Process{{20, "1234", "boot-1", "/usr/bin/containerd-shim-cube-rs"}, {21, "1235", "boot-1", "/usr/bin/cube-runtime"}}}
	want, err := os.ReadFile("../../../CubeShim/shim/src/common/retained-wire.json")
	if err != nil { t.Fatal(err) }
	got, err := json.Marshal(r)
	if err != nil { t.Fatal(err) }
	if !bytes.Equal(got, bytes.TrimSpace(want)) { t.Fatalf("retained shim wire mismatch: %s", got) }
}
