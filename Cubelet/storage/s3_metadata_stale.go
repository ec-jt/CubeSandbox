// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
	CubeLog "github.com/tencentcloud/CubeSandbox/pkgs/CubeLog"
)

// Stale metadata mounts.
//
// Cubelet keeps its ext4 metadata mounts in a pinned mount namespace, so they
// outlive both Cubelet and s3lvol restarts. When s3lvol is stopped with the
// NVMe/TCP initiator disconnected (a plain `systemctl restart` before the
// unit learned to keep it connected), or the host renumbers a namespace on
// reconnect (nvme7n2 -> nvme7n1, seen 2026-11-14), the mount stays on a block
// device that no longer exists and every read of it fails with EIO. Live
// 2026-11-18 on app2: 9 such mounts, among them the template metadata of
// tpl-afe6ba5a247c49c5bd2cd2ac, made every Create fail with
// "stat .../metadata/metadata: input/output error" until someone unmounted
// them by hand and restarted Cubelet.
//
// Two defences, both here:
//   - mountS3MetadataLockedTimeout never reuses a stale mount; it lazily
//     unmounts it and mounts again.
//   - sweepStaleS3Mounts, run when S3 init succeeds and then every
//     staleMountSweepInterval, finds stale mounts under the s3 storage root,
//     lazily unmounts them and remounts those whose owner Cubelet knows.
//
// Cubelet itself runs in that pinned namespace, so /proc/self/mountinfo and
// the umount it spawns see and act on the right mounts.

// staleMountSweepInterval is how often the background sweep runs.
var staleMountSweepInterval = 30 * time.Second

// staleMountProbeTimeout bounds the read probe of one mount. A probe that
// does not return in time is "unknown", never "stale": a slow device is not a
// dead one, and lazily unmounting a live mount under a running sandbox would
// cost more than waiting for the next sweep.
var staleMountProbeTimeout = 5 * time.Second

// sysBlockRoot is /sys/block; swapped in tests.
var sysBlockRoot = "/sys/block"

// mountInfoPath is the mountinfo of Cubelet's own mount namespace; swapped in
// tests.
var mountInfoPath = "/proc/self/mountinfo"

// probeMountFn reads the mount root and returns the error, if any. Swapped
// in tests.
var probeMountFn = probeMountImpl

// lazyUnmountFn detaches a mount even when its device is gone. Swapped in
// tests.
var lazyUnmountFn = func(mountPath string) error {
	_, stderr, err := utils.ExecV([]string{"umount", "-l", mountPath}, metadataMountTimeout)
	if err != nil {
		return errors.New(strings.TrimSpace(stderr) + ": " + err.Error())
	}
	return nil
}

// remountStaleFn mounts the metadata disk of snapshotID again at mountPath.
// Swapped in tests.
var remountStaleFn = func(ctx context.Context, snapshotID, mountPath string) error {
	return mountS3MetadataAt(ctx, cow.BackendS3, snapshotID, mountPath, 0)
}

var nvmeNamespaceRe = regexp.MustCompile(`^nvme[0-9]+n[0-9]+$`)

// mountEntry is one line of mountinfo.
type mountEntry struct {
	Target string
	Source string
	MajMin string
}

func readMountInfo() ([]mountEntry, error) {
	f, err := os.Open(mountInfoPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []mountEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 5 {
			continue
		}
		e := mountEntry{Target: filepath.Clean(fields[4]), MajMin: fields[2]}
		for i, f := range fields {
			if f == "-" && i+2 < len(fields) {
				e.Source = fields[i+2]
				break
			}
		}
		out = append(out, e)
	}
	return out, sc.Err()
}

// mountEntryFor returns the topmost mount at path, or nil.
func mountEntryFor(path string) *mountEntry {
	entries, err := readMountInfo()
	if err != nil {
		return nil
	}
	path = filepath.Clean(path)
	var hit *mountEntry
	for i := range entries {
		if entries[i].Target == path {
			hit = &entries[i]
		}
	}
	return hit
}

// staleDeviceReason reports why the block device behind e is gone, or "".
// Only NVMe namespace sources are checked against /sys/block: other sources
// (device-mapper, loop) are not listed there under their /dev name.
func staleDeviceReason(e mountEntry) string {
	src := strings.TrimSpace(e.Source)
	if !strings.HasPrefix(src, "/dev/") {
		return ""
	}
	name := filepath.Base(src)
	if !nvmeNamespaceRe.MatchString(name) {
		return ""
	}
	raw, err := os.ReadFile(filepath.Join(sysBlockRoot, name, "dev"))
	if err != nil {
		if os.IsNotExist(err) {
			return "device " + src + " is gone"
		}
		return ""
	}
	// Same name, different device: the namespace was renumbered and another
	// one now carries the name this mount was made on.
	if have := strings.TrimSpace(string(raw)); e.MajMin != "" && have != "" && have != e.MajMin {
		return "device " + src + " is now " + have + ", the mount was made on " + e.MajMin
	}
	return ""
}

func probeMountImpl(mountPath string) error {
	f, err := os.Open(mountPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Readdirnames(-1)
	return err
}

// isDeadDeviceErr is an error a read of a mount on a vanished device gives.
func isDeadDeviceErr(err error) bool {
	return errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ENXIO) ||
		errors.Is(err, syscall.ENODEV) || errors.Is(err, syscall.ESHUTDOWN)
}

// probeStaleReason runs probeMountFn under staleMountProbeTimeout and returns
// a reason when the mount answers with a dead-device error. A timeout or any
// other error is not proof and returns "".
func probeStaleReason(mountPath string) string {
	done := make(chan error, 1)
	go func() { done <- probeMountFn(mountPath) }()
	select {
	case err := <-done:
		if err != nil && isDeadDeviceErr(err) {
			return "read of " + mountPath + " failed: " + err.Error()
		}
		return ""
	case <-time.After(staleMountProbeTimeout):
		CubeLog.Warnf("stale mount probe of %s did not return in %s; leaving it", mountPath, staleMountProbeTimeout)
		return ""
	}
}

// s3MountStaleReason reports why the mount e is stale, or "".
func s3MountStaleReason(e mountEntry) string {
	if r := staleDeviceReason(e); r != "" {
		return r
	}
	return probeStaleReason(e.Target)
}

// s3MetadataMountStale is the check mountS3MetadataLockedTimeout runs before
// it reuses an existing mount. Swapped in tests.
var s3MetadataMountStale = func(mountPath string) string {
	e := mountEntryFor(mountPath)
	if e == nil {
		return ""
	}
	return s3MountStaleReason(*e)
}

// dropStaleMountLocked lazily unmounts a stale mount and forgets it. Caller
// holds s3MetadataMu.
func dropStaleMountLocked(mountPath, reason string) error {
	if err := lazyUnmountFn(mountPath); err != nil {
		return err
	}
	clean := filepath.Clean(mountPath)
	for id, p := range s3MetadataMounts {
		if filepath.Clean(p) == clean {
			delete(s3MetadataMounts, id)
		}
	}
	CubeLog.Warnf("dropped stale s3 metadata mount %s (%s)", mountPath, reason)
	return nil
}

// staleSweepResult is what one sweep did, for logs and tests.
type staleSweepResult struct {
	Dropped   []string
	Remounted []string
	Failed    []string
}

// sweepStaleS3Mounts finds stale mounts under the s3 storage root, lazily
// unmounts them and remounts the ones whose owner is known (the in-memory
// mount table or the persisted derived records). A mount with no known owner
// stays unmounted; the next Create or restore that needs it mounts it again.
// Nothing runs while s3lvol is starting: its namespaces are coming back.
func sweepStaleS3Mounts(ctx context.Context) staleSweepResult {
	var res staleSweepResult
	if starting, _ := s3lvolStarting(); starting {
		return res
	}
	root := filepath.Clean(BackendStorageRoot(cow.BackendS3))
	entries, err := readMountInfo()
	if err != nil {
		CubeLog.Warnf("stale mount sweep: read mountinfo: %v", err)
		return res
	}
	type stale struct{ path, reason string }
	var found []stale
	seen := map[string]bool{}
	for _, e := range entries {
		if e.Target != root && !strings.HasPrefix(e.Target, root+string(filepath.Separator)) {
			continue
		}
		if seen[e.Target] {
			continue
		}
		if r := s3MountStaleReason(e); r != "" {
			seen[e.Target] = true
			found = append(found, stale{e.Target, r})
		}
	}
	if len(found) == 0 {
		return res
	}

	owners := map[string]string{}
	s3MetadataMu.Lock()
	for id, p := range s3MetadataMounts {
		owners[filepath.Clean(p)] = id
	}
	if state, err := loadS3MetadataState(); err == nil && state != nil {
		for id, rec := range state.Derived {
			if p := strings.TrimSpace(rec.MountPath); p != "" {
				if _, ok := owners[filepath.Clean(p)]; !ok {
					owners[filepath.Clean(p)] = id
				}
			}
		}
	}
	var toRemount []stale
	for _, s := range found {
		if s3MetadataMountsInFlight[s.path] {
			continue
		}
		// A stacked stale mount needs one lazy unmount per layer.
		var uerr error
		for i := 0; i < 8; i++ {
			if uerr = dropStaleMountLocked(s.path, s.reason); uerr != nil {
				break
			}
			e := mountEntryFor(s.path)
			if e == nil || s3MountStaleReason(*e) == "" {
				break
			}
		}
		if uerr != nil {
			CubeLog.Errorf("stale mount sweep: could not unmount %s (%s): %v", s.path, s.reason, uerr)
			res.Failed = append(res.Failed, s.path)
			continue
		}
		res.Dropped = append(res.Dropped, s.path)
		toRemount = append(toRemount, s)
	}
	s3MetadataMu.Unlock()

	for _, s := range toRemount {
		id := owners[s.path]
		if id == "" {
			continue
		}
		if err := remountStaleFn(ctx, id, s.path); err != nil {
			CubeLog.Errorf("stale mount sweep: remount %s for %s failed: %v", s.path, id, err)
			res.Failed = append(res.Failed, s.path)
			continue
		}
		res.Remounted = append(res.Remounted, s.path)
	}
	CubeLog.Warnf("stale mount sweep: %d stale mount(s) dropped, %d remounted, %d failed",
		len(res.Dropped), len(res.Remounted), len(res.Failed))
	return res
}

var staleSweeperStarted sync.Once

// startStaleS3MountSweeperOnce starts the sweeper at most once per process:
// the S3 init loop can be restarted, the sweeper must not multiply.
func startStaleS3MountSweeperOnce(ctx context.Context) {
	staleSweeperStarted.Do(func() { startStaleS3MountSweeper(context.WithoutCancel(ctx)) })
}

// startStaleS3MountSweeper sweeps now and then every staleMountSweepInterval
// until ctx ends.
func startStaleS3MountSweeper(ctx context.Context) {
	go func() {
		sweepStaleS3Mounts(ctx)
		t := time.NewTicker(staleMountSweepInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				sweepStaleS3Mounts(ctx)
			}
		}
	}()
}
