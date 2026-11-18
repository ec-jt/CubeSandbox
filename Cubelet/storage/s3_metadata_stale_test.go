// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// staleFixture fakes mountinfo, /sys/block and the umount/remount hooks.
type staleFixture struct {
	t         *testing.T
	sys       string
	info      string
	mounts    []mountEntry
	probeErr  map[string]error
	unmounted []string
	remounted map[string]string
}

func newStaleFixture(t *testing.T) *staleFixture {
	t.Helper()
	f := &staleFixture{
		t:         t,
		sys:       t.TempDir(),
		info:      filepath.Join(t.TempDir(), "mountinfo"),
		probeErr:  map[string]error{},
		remounted: map[string]string{},
	}
	prevSys, prevInfo := sysBlockRoot, mountInfoPath
	prevProbe, prevUmount, prevRemount := probeMountFn, lazyUnmountFn, remountStaleFn
	prevStarting := s3lvolStartingFile
	sysBlockRoot, mountInfoPath = f.sys, f.info
	s3lvolStartingFile = filepath.Join(t.TempDir(), "absent")
	probeMountFn = func(p string) error { return f.probeErr[filepath.Clean(p)] }
	lazyUnmountFn = func(p string) error {
		f.unmounted = append(f.unmounted, p)
		// Drop the topmost layer at p, like umount -l.
		for i := len(f.mounts) - 1; i >= 0; i-- {
			if f.mounts[i].Target == filepath.Clean(p) {
				f.mounts = append(f.mounts[:i], f.mounts[i+1:]...)
				break
			}
		}
		f.write()
		return nil
	}
	remountStaleFn = func(_ context.Context, id, p string) error {
		f.remounted[p] = id
		return nil
	}
	t.Cleanup(func() {
		sysBlockRoot, mountInfoPath = prevSys, prevInfo
		probeMountFn, lazyUnmountFn, remountStaleFn = prevProbe, prevUmount, prevRemount
		s3lvolStartingFile = prevStarting
		s3MetadataMu.Lock()
		s3MetadataMounts = map[string]string{}
		s3MetadataMountsInFlight = map[string]bool{}
		s3MetadataMu.Unlock()
	})
	return f
}

func (f *staleFixture) device(name, majmin string) {
	require.NoError(f.t, os.MkdirAll(filepath.Join(f.sys, name), 0o755))
	require.NoError(f.t, os.WriteFile(filepath.Join(f.sys, name, "dev"), []byte(majmin+"\n"), 0o644))
}

func (f *staleFixture) mount(target, source, majmin string) {
	f.mounts = append(f.mounts, mountEntry{Target: filepath.Clean(target), Source: source, MajMin: majmin})
	f.write()
}

func (f *staleFixture) write() {
	var b strings.Builder
	for i, m := range f.mounts {
		fmt.Fprintf(&b, "%d 1 %s / %s rw,relatime - ext4 %s rw\n", 100+i, m.MajMin, m.Target, m.Source)
	}
	require.NoError(f.t, os.WriteFile(f.info, []byte(b.String()), 0o644))
}

func TestStaleDeviceReasonDeviceGone(t *testing.T) {
	f := newStaleFixture(t)
	require.Contains(t, staleDeviceReason(mountEntry{Target: "/m", Source: "/dev/nvme32n3", MajMin: "259:121"}), "is gone")
	f.device("nvme32n3", "259:121")
	require.Empty(t, staleDeviceReason(mountEntry{Target: "/m", Source: "/dev/nvme32n3", MajMin: "259:121"}))
}

func TestStaleDeviceReasonRenumberedNamespace(t *testing.T) {
	f := newStaleFixture(t)
	f.device("nvme7n1", "259:7")
	r := staleDeviceReason(mountEntry{Target: "/m", Source: "/dev/nvme7n1", MajMin: "259:9"})
	require.Contains(t, r, "is now 259:7")
}

func TestStaleDeviceReasonIgnoresNonNVMe(t *testing.T) {
	newStaleFixture(t)
	require.Empty(t, staleDeviceReason(mountEntry{Target: "/m", Source: "/dev/mapper/x", MajMin: "253:1"}))
	require.Empty(t, staleDeviceReason(mountEntry{Target: "/m", Source: "tmpfs", MajMin: "0:5"}))
}

func TestProbeStaleReasonOnlyDeadDeviceErrors(t *testing.T) {
	f := newStaleFixture(t)
	f.probeErr["/a"] = &os.PathError{Op: "readdirent", Path: "/a", Err: syscall.EIO}
	f.probeErr["/b"] = &os.PathError{Op: "open", Path: "/b", Err: syscall.EACCES}
	require.NotEmpty(t, probeStaleReason("/a"))
	require.Empty(t, probeStaleReason("/b"))
	require.Empty(t, probeStaleReason("/c"))
}

func TestProbeStaleReasonTimeoutIsNotStale(t *testing.T) {
	newStaleFixture(t)
	prev := staleMountProbeTimeout
	staleMountProbeTimeout = 20 * time.Millisecond
	t.Cleanup(func() { staleMountProbeTimeout = prev })
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	probeMountFn = func(string) error { <-block; return syscall.EIO }
	require.Empty(t, probeStaleReason("/slow"))
}

// The reported incident: a mount on a vanished device was reused and every
// Create failed with EIO. The mount path must be dropped and remounted on the
// device the volume has now.
func TestMountS3MetadataReplacesMountOnVanishedDevice(t *testing.T) {
	stubS3MetadataMounts(t)
	f := newStaleFixture(t)
	mount := t.TempDir()
	f.device("nvme5n2", "259:50")
	f.mount(mount, "/dev/nvme32n3", "259:121") // nvme32n3 no longer exists

	mountedOn := map[string]string{mount: "/dev/nvme32n3"}
	prevIs, prevSrc, prevMount := s3MetadataIsMounted, s3MetadataMountSourceFn, mountS3MetadataDeviceTimeout
	s3MetadataIsMounted = func(p string) bool { return mountEntryFor(p) != nil }
	s3MetadataMountSourceFn = func(p string) string {
		if e := mountEntryFor(p); e != nil {
			return e.Source
		}
		return ""
	}
	mountS3MetadataDeviceTimeout = func(dev, p string, _ time.Duration) error {
		mountedOn[p] = dev
		f.mount(p, dev, "259:50")
		return nil
	}
	t.Cleanup(func() {
		s3MetadataIsMounted, s3MetadataMountSourceFn, mountS3MetadataDeviceTimeout = prevIs, prevSrc, prevMount
	})

	s3MetadataMu.Lock()
	err := mountS3MetadataLockedTimeout("tpl-1", "/dev/nvme32n3", mount, 0)
	s3MetadataMu.Unlock()
	require.NoError(t, err)
	require.Equal(t, []string{mount}, f.unmounted, "the stale mount must be dropped, not reused")
	require.Equal(t, "/dev/nvme32n3", mountedOn[mount])
	require.Len(t, f.mounts, 1)
}

func TestMountS3MetadataReusesHealthyMount(t *testing.T) {
	stubS3MetadataMounts(t)
	f := newStaleFixture(t)
	mount := t.TempDir()
	f.device("nvme5n2", "259:50")
	f.mount(mount, "/dev/nvme5n2", "259:50")
	prevIs, prevSrc, prevMount := s3MetadataIsMounted, s3MetadataMountSourceFn, mountS3MetadataDeviceTimeout
	s3MetadataIsMounted = func(p string) bool { return mountEntryFor(p) != nil }
	s3MetadataMountSourceFn = func(p string) string { return mountEntryFor(p).Source }
	calls := 0
	mountS3MetadataDeviceTimeout = func(string, string, time.Duration) error { calls++; return nil }
	t.Cleanup(func() {
		s3MetadataIsMounted, s3MetadataMountSourceFn, mountS3MetadataDeviceTimeout = prevIs, prevSrc, prevMount
	})
	s3MetadataMu.Lock()
	err := mountS3MetadataLockedTimeout("tpl-1", "/dev/nvme5n2", mount, 0)
	s3MetadataMu.Unlock()
	require.NoError(t, err)
	require.Empty(t, f.unmounted)
	require.Zero(t, calls)
}

func TestMountS3MetadataReplacesMountThatReadsEIO(t *testing.T) {
	stubS3MetadataMounts(t)
	f := newStaleFixture(t)
	mount := t.TempDir()
	f.device("nvme5n2", "259:50") // device present, I/O dead
	f.mount(mount, "/dev/nvme5n2", "259:50")
	f.probeErr[filepath.Clean(mount)] = syscall.EIO
	prevIs, prevSrc, prevMount := s3MetadataIsMounted, s3MetadataMountSourceFn, mountS3MetadataDeviceTimeout
	s3MetadataIsMounted = func(p string) bool { return mountEntryFor(p) != nil }
	s3MetadataMountSourceFn = func(p string) string {
		if e := mountEntryFor(p); e != nil {
			return e.Source
		}
		return ""
	}
	calls := 0
	mountS3MetadataDeviceTimeout = func(dev, p string, _ time.Duration) error {
		calls++
		delete(f.probeErr, filepath.Clean(p))
		f.mount(p, dev, "259:50")
		return nil
	}
	t.Cleanup(func() {
		s3MetadataIsMounted, s3MetadataMountSourceFn, mountS3MetadataDeviceTimeout = prevIs, prevSrc, prevMount
	})
	s3MetadataMu.Lock()
	err := mountS3MetadataLockedTimeout("tpl-1", "/dev/nvme5n2", mount, 0)
	s3MetadataMu.Unlock()
	require.NoError(t, err)
	require.Equal(t, []string{mount}, f.unmounted)
	require.Equal(t, 1, calls)
}

func TestSweepDropsStaleMountsAndRemountsKnownOwners(t *testing.T) {
	stubS3MetadataMounts(t)
	f := newStaleFixture(t)
	root := BackendStorageRoot(cow.BackendS3)
	tplMeta := filepath.Join(root, "snapshots", "tpl-afe6", "metadata")
	sbMeta := filepath.Join(root, "snapshots", "sb-1", "metadata")
	orphan := filepath.Join(root, "snapshots", "gone", "metadata")
	healthy := filepath.Join(root, "snapshots", "ok", "metadata")
	outside := "/data/elsewhere"
	f.device("nvme5n2", "259:50")
	f.mount(tplMeta, "/dev/nvme32n3", "259:121")
	f.mount(sbMeta, "/dev/nvme30n1", "259:100")
	f.mount(orphan, "/dev/nvme31n1", "259:110")
	f.mount(healthy, "/dev/nvme5n2", "259:50")
	f.mount(outside, "/dev/nvme29n1", "259:90")

	s3MetadataMu.Lock()
	s3MetadataMounts["tpl-afe6"] = tplMeta
	require.NoError(t, saveS3MetadataState(&s3MetadataState{Derived: map[string]s3MetadataDerivedRecord{
		"sb-1": {SnapshotID: "sb-1", VolName: "s3-meta-sb-1", MountPath: sbMeta},
	}}))
	s3MetadataMu.Unlock()

	res := sweepStaleS3Mounts(context.Background())
	require.ElementsMatch(t, []string{tplMeta, sbMeta, orphan}, res.Dropped)
	require.ElementsMatch(t, []string{tplMeta, sbMeta}, res.Remounted)
	require.Empty(t, res.Failed)
	require.Equal(t, "tpl-afe6", f.remounted[tplMeta])
	require.Equal(t, "sb-1", f.remounted[sbMeta])
	require.NotContains(t, f.unmounted, healthy)
	require.NotContains(t, f.unmounted, outside, "mounts outside the s3 root are not Cubelet's")
	s3MetadataMu.Lock()
	_, stillTracked := s3MetadataMounts["tpl-afe6"]
	s3MetadataMu.Unlock()
	require.False(t, stillTracked, "the dropped mount must be forgotten until remounted")
}

func TestSweepUnmountsEveryStackedStaleLayer(t *testing.T) {
	stubS3MetadataMounts(t)
	f := newStaleFixture(t)
	p := filepath.Join(BackendStorageRoot(cow.BackendS3), "snapshots", "tpl-x", "metadata")
	f.mount(p, "/dev/nvme32n3", "259:121")
	f.mount(p, "/dev/nvme33n3", "259:122")
	res := sweepStaleS3Mounts(context.Background())
	require.Equal(t, []string{p}, res.Dropped)
	require.Len(t, f.unmounted, 2)
	require.Empty(t, f.mounts)
}

func TestSweepSkipsWhileS3lvolStarting(t *testing.T) {
	stubS3MetadataMounts(t)
	f := newStaleFixture(t)
	p := filepath.Join(BackendStorageRoot(cow.BackendS3), "snapshots", "tpl-x", "metadata")
	f.mount(p, "/dev/nvme32n3", "259:121")
	marker := filepath.Join(t.TempDir(), "starting")
	require.NoError(t, os.WriteFile(marker, []byte(fmt.Sprint(os.Getpid())), 0o644))
	s3lvolStartingFile = marker
	prevAlive := processAlive
	processAlive = func(int) bool { return true }
	t.Cleanup(func() { processAlive = prevAlive })
	res := sweepStaleS3Mounts(context.Background())
	require.Empty(t, res.Dropped)
	require.Empty(t, f.unmounted)
}

func TestSweepLeavesInFlightMountAlone(t *testing.T) {
	stubS3MetadataMounts(t)
	f := newStaleFixture(t)
	p := filepath.Join(BackendStorageRoot(cow.BackendS3), "snapshots", "tpl-x", "metadata")
	f.mount(p, "/dev/nvme32n3", "259:121")
	s3MetadataMu.Lock()
	s3MetadataMountsInFlight[filepath.Clean(p)] = true
	s3MetadataMu.Unlock()
	res := sweepStaleS3Mounts(context.Background())
	require.Empty(t, res.Dropped)
	require.Empty(t, f.unmounted)
}
