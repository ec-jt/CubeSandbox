// Package legacyresume implements the opt-in policy for legacy pausevm cohorts.
// It does not migrate snapshots or infer legacy ownership from missing records.
package legacyresume

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

const MaxManifestBytes = 1 << 20

// LoadConfigured returns an empty cohort when disabled. A configured manifest
// that cannot be read or validated is an error, never an implicit opt-out.
func LoadConfigured() (map[string]string, error) {
	path := os.Getenv("CUBE_LEGACY_PAUSE_MANIFEST")
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("legacy manifest unavailable")
	}
	defer f.Close()
	cohort, err := Parse(f)
	if err != nil {
		return nil, fmt.Errorf("invalid legacy manifest")
	}
	return cohort, nil
}

type Entry struct {
	SandboxID string `json:"sandbox_id"`
	HostIP    string `json:"host_ip"`
}

type Manifest struct {
	Version  int     `json:"version"`
	Sessions []Entry `json:"sessions"`
}

// Parse rejects ambiguous or incomplete manifests rather than changing routing.
func Parse(r io.Reader) (map[string]string, error) {
	data, err := io.ReadAll(io.LimitReader(r, MaxManifestBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxManifestBytes {
		return nil, fmt.Errorf("legacy manifest exceeds size limit")
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("legacy manifest has trailing content")
	}
	if m.Version != 1 {
		return nil, fmt.Errorf("unsupported legacy manifest version")
	}
	result := make(map[string]string, len(m.Sessions))
	for _, entry := range m.Sessions {
		if entry.SandboxID == "" || strings.TrimSpace(entry.SandboxID) != entry.SandboxID {
			return nil, fmt.Errorf("invalid legacy sandbox ID")
		}
		ip := net.ParseIP(entry.HostIP)
		// Current Cubelet endpoint construction is IPv4-only. Do not accept
		// hostnames or arbitrary endpoints from this administrative manifest.
		if ip == nil || ip.To4() == nil || !ip.IsPrivate() {
			return nil, fmt.Errorf("legacy origin must be a private IPv4 address")
		}
		if _, duplicate := result[entry.SandboxID]; duplicate {
			return nil, fmt.Errorf("duplicate legacy sandbox ID")
		}
		result[entry.SandboxID] = ip.String()
	}
	return result, nil
}

// Validate is called only after an explicit manifest membership match and
// successful authoritative metadata reads. No broad fallback on DB errors.
func Validate(origin, observedHost, backend string, nativeBinding bool) error {
	if nativeBinding {
		return fmt.Errorf("legacy session has a native pause binding; refusing conflicting route")
	}
	if backend != "" && backend != "xfs" {
		return fmt.Errorf("legacy session is not XFS-backed")
	}
	if net.ParseIP(origin) == nil || net.ParseIP(observedHost) == nil || !net.ParseIP(origin).Equal(net.ParseIP(observedHost)) {
		return fmt.Errorf("legacy session origin differs from observed owner")
	}
	return nil
}

// IsLegacyCubeletVersion reports whether a node's reported cubelet version
// predates v0.7 (in-place pausevm protocol, no CubeCow pause snapshots).
// Accepts "v0.6.0", "0.6.0", "v0.6.0-dirty" style values. Unknown or
// unparsable versions are NOT legacy: the native path is the default.
func IsLegacyCubeletVersion(v string) bool {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	if v == "" {
		return false
	}
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 2 {
		return false
	}
	major, err1 := strconv.Atoi(parts[0])
	minorStr := parts[1]
	for i, r := range minorStr {
		if r < '0' || r > '9' {
			minorStr = minorStr[:i]
			break
		}
	}
	minor, err2 := strconv.Atoi(minorStr)
	if err1 != nil || err2 != nil {
		return false
	}
	return major == 0 && minor < 7
}
