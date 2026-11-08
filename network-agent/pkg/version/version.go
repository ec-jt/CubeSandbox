// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

// Package version provides version information for network-agent.
package version

import (
	"fmt"
	"runtime/debug"
)

var (
	// Version is the semantic release version, injected at build time via ldflags.
	Version = "0.0.0-dev"

	// Commit is the full git commit SHA, injected at build time via ldflags.
	Commit = "unknown"

	// BuildTime is the UTC ISO 8601 build timestamp, injected at build time via ldflags.
	BuildTime = "unknown"
)

func init() {
	// Fallback: recover VCS commit/time from the binary's embedded build info
	// when not injected via ldflags (default -buildvcs=true builds).
	if Commit != "unknown" && Commit != "" {
		return
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	var rev, t string
	dirty := false
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.time":
			t = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev != "" {
		Commit = rev
		if dirty {
			Commit += "-dirty"
		}
	}
	if BuildTime == "unknown" && t != "" {
		BuildTime = t
	}
}

// VersionString returns the unified version string for the given binary name.
func VersionString(binaryName string) string {
	return fmt.Sprintf("%s %s (%s) built at %s", binaryName, Version, Commit, BuildTime)
}

// String returns the version string for the network-agent binary.
func String() string {
	return VersionString("network-agent")
}
