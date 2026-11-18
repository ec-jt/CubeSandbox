// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"bytes"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// s3lvol's rcow_start.sh writes its own pid to this file before it starts the
// target and removes it at "==== up", once the lvstore is attached, every
// recorded namespace is back and the initiator is connected. While the file
// names a live rcow_start.sh, s3lvol answers RPCs but is not ready: the active
// registry is being replayed, and a Cubelet call in that window (the init
// probe, a create) reads a registry that describes namespaces which do not
// exist yet. Live 2026-11-17 22:50 (app2) and 2026-11-18 01:45 (app1): the
// probe loaded the registry before the replay moved it aside, every replay
// call was then answered "already active", and creates got an empty device
// path.
//
// So Cubelet treats s3lvol as not ready while the marker is held, both in the
// init loop and for every backend=s3 request. That is also what lets s3lvol be
// restarted under a running Cubelet: requests fail fast with ErrS3NotReady for
// the minutes the start takes, then are served again, with nothing to restart.
const defaultS3lvolStartingFile = "/var/tmp/rcow/s3lvol.starting"

var s3lvolStartingFile = func() string {
	if p := strings.TrimSpace(os.Getenv("S3LVOL_STARTING_FILE")); p != "" {
		return p
	}
	return defaultS3lvolStartingFile
}()

// processAlive is swapped in tests.
var processAlive = func(pid int) bool {
	if pid <= 0 {
		return false
	}
	if err := syscall.Kill(pid, 0); err != nil && err != syscall.EPERM {
		return false
	}
	// A recycled pid is not the start script. The cmdline check keeps a
	// file left by a killed start from blocking s3 forever.
	cmd, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
	if err != nil {
		return false
	}
	return bytes.Contains(cmd, []byte("rcow_start"))
}

// s3lvolStarting reports whether an s3lvol start is in progress, and since
// when (the marker's mtime).
func s3lvolStarting() (bool, time.Time) {
	st, err := os.Stat(s3lvolStartingFile)
	if err != nil {
		return false, time.Time{}
	}
	raw, err := os.ReadFile(s3lvolStartingFile)
	if err != nil {
		return false, time.Time{}
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || !processAlive(pid) {
		return false, time.Time{}
	}
	return true, st.ModTime()
}

// s3InitBackoff is the wait before init attempt n+1 after n failures:
// s3InitRetryInterval doubling up to s3InitRetryMax. An s3lvol start takes
// minutes; a fixed 5 s retry filled the log with 30+ identical failures.
var s3InitRetryMax = 30 * time.Second

func s3InitBackoff(failures int) time.Duration {
	d := s3InitRetryInterval
	for i := 1; i < failures && d < s3InitRetryMax; i++ {
		d *= 2
	}
	if d > s3InitRetryMax {
		d = s3InitRetryMax
	}
	return d
}
