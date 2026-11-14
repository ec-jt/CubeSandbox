// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/internal/tomlext"
)

type scriptedStatus struct {
	replies []string
	err     error
	calls   int
}

func (s *scriptedStatus) ImportStatus(name string) (string, error) {
	s.calls++
	if s.err != nil {
		return "", s.err
	}
	i := s.calls - 1
	if i >= len(s.replies) {
		i = len(s.replies) - 1
	}
	return s.replies[i], nil
}

func fastImportPoll(t *testing.T) {
	t.Helper()
	prevPoll, prevEvery := importPrefetchPoll, importProgressEvery
	importPrefetchPoll = time.Millisecond
	importProgressEvery = time.Millisecond
	t.Cleanup(func() {
		importPrefetchPoll, importProgressEvery = prevPoll, prevEvery
	})
}

func TestWaitImportPrefetchReturnsOnDone(t *testing.T) {
	fastImportPoll(t)
	st := &scriptedStatus{replies: []string{
		`{"state":"queued","present_chunks":17,"local_chunks":0}`,
		`{"state":"running","present_chunks":17,"local_chunks":5,"chunks_fetched":9,"fetch_inflight":8}`,
		`{"state":"done","present_chunks":17,"local_chunks":17,"done":true}`,
	}}
	require.NoError(t, waitImportPrefetch(context.Background(), st, "s3-meta-sb", "metadata", time.Minute))
	require.Equal(t, 3, st.calls)
}

func TestWaitImportPrefetchTimesOutWithClearError(t *testing.T) {
	fastImportPoll(t)
	st := &scriptedStatus{replies: []string{
		`{"state":"running","present_chunks":17,"local_chunks":3}`,
	}}
	start := time.Now()
	err := waitImportPrefetch(context.Background(), st, "sb-x-rootfs-gen0", "rootfs", 30*time.Millisecond)
	require.Error(t, err)
	require.True(t, errors.Is(err, errImportPrefetchTimeout))
	require.Contains(t, err.Error(), "sb-x-rootfs-gen0")
	require.Contains(t, err.Error(), "3/17")
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestWaitImportPrefetchPropagatesPrefetchError(t *testing.T) {
	fastImportPoll(t)
	for _, reply := range []string{
		`{"state":"failed","present_chunks":17,"local_chunks":4,"error":-5,"error_msg":"Input/output error"}`,
		`{"state":"running","present_chunks":17,"local_chunks":4,"error":-5,"error_msg":"Input/output error"}`,
		`{"state":"cancelled","present_chunks":17,"local_chunks":4,"error":-125}`,
	} {
		st := &scriptedStatus{replies: []string{reply}}
		err := waitImportPrefetch(context.Background(), st, "sb-x-memory", "memory", time.Minute)
		require.Error(t, err, reply)
		require.False(t, errors.Is(err, errImportPrefetchTimeout), reply)
		require.Contains(t, err.Error(), "sb-x-memory")
		require.Equal(t, 1, st.calls, "fails at once, does not poll until the deadline")
	}
}

func TestWaitImportPrefetchPropagatesStatusRPCError(t *testing.T) {
	fastImportPoll(t)
	st := &scriptedStatus{err: fmt.Errorf("s3lvol rpc: no such lvol")}
	err := waitImportPrefetch(context.Background(), st, "s3-meta-sb", "metadata", time.Minute)
	require.ErrorContains(t, err, "no such lvol")
}

func TestWaitImportPrefetchHonoursContext(t *testing.T) {
	fastImportPoll(t)
	st := &scriptedStatus{replies: []string{`{"state":"running"}`}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := waitImportPrefetch(ctx, st, "s3-meta-sb", "metadata", time.Minute)
	require.ErrorIs(t, err, context.Canceled)
}

// The imported metadata disk is mounted with the import mount timeout, never
// with cmdTimeout; and nothing is mounted until the import is local.
func TestImportedMetadataMountUsesItsOwnTimeoutAfterPrefetch(t *testing.T) {
	stubS3MetadataMounts(t)
	fastImportPoll(t)
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)

	prevCmd, prevMount := cmdTimeout, importedMountTimeout
	cmdTimeout = 3 * time.Second
	importedMountTimeout = 120 * time.Second
	t.Cleanup(func() { cmdTimeout, importedMountTimeout = prevCmd, prevMount })

	var mountTimeouts []time.Duration
	prevTimeoutMount := mountS3MetadataDeviceTimeout
	mountS3MetadataDeviceTimeout = func(dev, path string, timeout time.Duration) error {
		mountTimeouts = append(mountTimeouts, timeout)
		// Must not be reachable before the import reported done.
		require.GreaterOrEqual(t, len(engine.importStatusCalls), 3)
		return mountS3MetadataDevice(dev, path)
	}
	t.Cleanup(func() { mountS3MetadataDeviceTimeout = prevTimeoutMount })

	engine.importStatusFn = func(name string, call int) (string, error) {
		if call < 3 {
			return `{"state":"running","present_chunks":17,"local_chunks":` +
				fmt.Sprint(call*5) + `}`, nil
		}
		return `{"state":"done","present_chunks":17,"local_chunks":17,"done":true}`, nil
	}

	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	require.NotNil(t, imp)
	_, err := imp.EnsureMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, []time.Duration{120 * time.Second}, mountTimeouts)
	for _, name := range engine.importStatusCalls {
		require.Equal(t, S3MetadataVolumeName("sb-1"), name)
	}
}

// A prefetch failure fails the import before the volume is activated or
// mounted, so the Create fails and the B18 rollback has imports to release.
func TestImportFailsClosedOnPrefetchError(t *testing.T) {
	stubS3MetadataMounts(t)
	fastImportPoll(t)
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)
	engine.importStatusFn = func(string, int) (string, error) {
		return `{"state":"failed","present_chunks":17,"local_chunks":2,"error":-5,"error_msg":"Input/output error"}`, nil
	}

	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	_, err := imp.EnsureMetadata(context.Background())
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "not local"), err.Error())
	require.NotContains(t, engine.activatedVolumes, S3MetadataVolumeName("sb-1"))
	require.Len(t, engine.importedLvols, 1, "the import happened and is left for B18 to release")

	_, _, err = imp.Rootfs(context.Background(), 0)
	require.Error(t, err)
	require.NotContains(t, engine.activatedVolumes, SandboxRootfsName("sb-1", 0))
}

func TestImportTimeoutsFromConfig(t *testing.T) {
	prevWait, prevMount := importPrefetchTimeout, importedMountTimeout
	t.Cleanup(func() { importPrefetchTimeout, importedMountTimeout = prevWait, prevMount })

	importPrefetchTimeout, importedMountTimeout = defaultImportPrefetchTimeout, defaultImportedMountTimeout
	require.NoError(t, applyImportTimeouts(&Config{}))
	require.Equal(t, 300*time.Second, importPrefetchTimeout)
	require.Equal(t, 120*time.Second, importedMountTimeout)

	cfg := &Config{}
	cfg.ImportPrefetchTimeout = tomlext.FromStdTime(42 * time.Second)
	cfg.ImportedMetadataMountTimeout = tomlext.FromStdTime(7 * time.Second)
	require.NoError(t, applyImportTimeouts(cfg))
	require.Equal(t, 42*time.Second, importPrefetchTimeout)
	require.Equal(t, 7*time.Second, importedMountTimeout)

	bad := &Config{}
	bad.ImportPrefetchTimeout = tomlext.FromStdTime(-time.Second)
	require.Error(t, applyImportTimeouts(bad))
}

// B18 still releases what a prefetch failure left behind: the metadata volume
// was imported but never activated or mounted, and the rollback path
// (ReleaseCrossNodeSandboxImports, called from releaseFailedCrossNodeRestore)
// must delete it.
func TestPrefetchFailureLeavesImportsForB18Release(t *testing.T) {
	stubS3MetadataMounts(t)
	fastImportPoll(t)
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)
	engine.importStatusFn = func(string, int) (string, error) {
		return `{"state":"failed","error":-5,"error_msg":"Input/output error"}`, nil
	}
	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	_, err := imp.EnsureMetadata(context.Background())
	require.Error(t, err)
	_, _, err = imp.Rootfs(context.Background(), 0)
	require.Error(t, err)

	require.NoError(t, ReleaseCrossNodeSandboxImports(context.Background(), "sb-1"))
	require.Contains(t, engine.deletedVolumes, S3MetadataVolumeName("sb-1"))
	require.Contains(t, engine.deletedVolumes, SandboxRootfsName("sb-1", 0))
}
