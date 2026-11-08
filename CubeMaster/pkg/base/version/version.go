// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

// Package version provides version information
package version

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
)

var (
	// Version is the semantic release version, injected at build time via ldflags.
	// Falls back to "0.0.0-dev" when not set (e.g. local development builds).
	Version = "0.0.0-dev"

	// Commit is the full git commit SHA, injected at build time via ldflags.
	Commit = "unknown"

	// BuildTime is the UTC ISO 8601 build timestamp, injected at build time via ldflags.
	BuildTime = "unknown"

	Package = "github.com/tencentcloud/CubeSandbox/CubeMaster"

	Revision = "v1"

	GoVersion = runtime.Version()
)

func init() {
	// Fallback: when the binary was built from a git checkout without ldflags
	// (the default -buildvcs=true embeds VCS metadata), recover the commit and
	// commit time so --version is still meaningful instead of "unknown".
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

func ShowAndExit(show bool) {
	if show {
		fmt.Println(VersionString("cubemaster"))
		os.Exit(0)
	}
}

func ShowVersion() string {
	return Version
}
