package legacyresume

import (
	"strings"
	"testing"
)

func TestParseManifest(t *testing.T) {
	valid := `{"version":1,"sessions":[{"sandbox_id":"sb-existing","host_ip":"10.0.1.1"}]}`
	got, err := Parse(strings.NewReader(valid))
	if err != nil || got["sb-existing"] != "10.0.1.1" {
		t.Fatalf("valid manifest: %v %v", got, err)
	}
	for _, input := range []string{
		`{}`, `{"version":2}`, `{"version":1,"unexpected":true}`,
		valid + `{}`, strings.Repeat(" ", MaxManifestBytes+1),
		`{"version":1,"sessions":[{"sandbox_id":"","host_ip":"10.0.1.1"}]}`,
		`{"version":1,"sessions":[{"sandbox_id":"sb-x","host_ip":"example.com"}]}`,
		`{"version":1,"sessions":[{"sandbox_id":"sb-x","host_ip":"8.8.8.8"}]}`,
		`{"version":1,"sessions":[{"sandbox_id":"sb-x","host_ip":"10.0.1.1"},{"sandbox_id":"sb-x","host_ip":"10.0.1.2"}]}`,
	} {
		if _, err := Parse(strings.NewReader(input)); err == nil {
			t.Errorf("accepted invalid input %.100q", input)
		}
	}
}

func TestValidateCohort(t *testing.T) {
	for _, backend := range []string{"", "xfs"} {
		if err := Validate("10.0.1.1", "10.0.1.1", backend, false); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		origin, host, backend string
		native                bool
	}{
		{"10.0.1.1", "10.0.1.2", "xfs", false},
		{"10.0.1.1", "", "xfs", false},
		{"10.0.1.1", "10.0.1.1", "s3", false},
		{"10.0.1.1", "10.0.1.1", "unknown", false},
		{"10.0.1.1", "10.0.1.1", "xfs", true},
	} {
		if err := Validate(tc.origin, tc.host, tc.backend, tc.native); err == nil {
			t.Errorf("accepted conflicting cohort: %+v", tc)
		}
	}
}

func TestIsLegacyCubeletVersion(t *testing.T) {
	for v, want := range map[string]bool{
		"v0.6.0": true, "0.5.1": true, "v0.6.0-dirty": true, "v0.6": true,
		"v0.7.1": false, "v0.7.0": false, "v1.0.0": false, "": false, "unknown": false, "v0.x": false,
	} {
		if got := IsLegacyCubeletVersion(v); got != want {
			t.Errorf("%q: got %v want %v", v, got, want)
		}
	}
}
