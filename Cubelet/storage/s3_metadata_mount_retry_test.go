// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/internal/tomlext"
)

func withMetadataMountKnobs(t *testing.T, timeout time.Duration, attempts int) *[]time.Duration {
	t.Helper()
	prevT, prevA, prevB, prevS := metadataMountTimeout, metadataMountAttempts,
		metadataMountRetryBackoff, metadataMountRetrySleep
	prevCmd := cmdTimeout
	metadataMountTimeout, metadataMountAttempts = timeout, attempts
	metadataMountRetryBackoff = time.Second
	cmdTimeout = 3 * time.Second
	var sleeps []time.Duration
	metadataMountRetrySleep = func(d time.Duration) { sleeps = append(sleeps, d) }
	t.Cleanup(func() {
		metadataMountTimeout, metadataMountAttempts = prevT, prevA
		metadataMountRetryBackoff, metadataMountRetrySleep = prevB, prevS
		cmdTimeout = prevCmd
	})
	return &sleeps
}

// A pause / create metadata mount never uses cmd_timeout (3s): it uses
// metadata_mount_timeout (default 60s).
func TestMetadataMountUsesMetadataTimeoutNotCmdTimeout(t *testing.T) {
	stubS3MetadataMounts(t)
	withMetadataMountKnobs(t, defaultMetadataMountTimeout, defaultMetadataMountAttempts)
	var got []time.Duration
	prev := mountS3MetadataDeviceTimeout
	mountS3MetadataDeviceTimeout = func(dev, path string, timeout time.Duration) error {
		got = append(got, timeout)
		return prev(dev, path, timeout)
	}
	t.Cleanup(func() { mountS3MetadataDeviceTimeout = prev })

	s3MetadataMu.Lock()
	err := mountS3MetadataLocked("snap-1", "/dev/nvme9n1", t.TempDir()+"/metadata")
	s3MetadataMu.Unlock()
	require.NoError(t, err)
	require.Equal(t, []time.Duration{60 * time.Second}, got)
}

// A failed mount is retried with backoff; success on a later attempt wins.
func TestMetadataMountRetriesThenSucceeds(t *testing.T) {
	stubS3MetadataMounts(t)
	sleeps := withMetadataMountKnobs(t, 60*time.Second, 3)
	calls := 0
	prev := mountS3MetadataDeviceTimeout
	mountS3MetadataDeviceTimeout = func(dev, path string, timeout time.Duration) error {
		calls++
		if calls < 3 {
			return errors.New("mount: timed out")
		}
		return prev(dev, path, timeout)
	}
	t.Cleanup(func() { mountS3MetadataDeviceTimeout = prev })

	path := t.TempDir() + "/metadata"
	s3MetadataMu.Lock()
	err := mountS3MetadataLocked("snap-2", "/dev/nvme9n2", path)
	s3MetadataMu.Unlock()
	require.NoError(t, err)
	require.Equal(t, 3, calls)
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second}, *sleeps)
	s3MetadataMu.Lock()
	require.NotEmpty(t, s3MetadataMounts["snap-2"])
	s3MetadataMu.Unlock()
}

// The retry is bounded: after metadata_mount_attempts the error is returned.
func TestMetadataMountRetryIsBounded(t *testing.T) {
	stubS3MetadataMounts(t)
	withMetadataMountKnobs(t, 60*time.Second, 2)
	calls := 0
	prev := mountS3MetadataDeviceTimeout
	mountS3MetadataDeviceTimeout = func(string, string, time.Duration) error {
		calls++
		return errors.New("mount: timed out")
	}
	t.Cleanup(func() { mountS3MetadataDeviceTimeout = prev })

	s3MetadataMu.Lock()
	err := mountS3MetadataLocked("snap-3", "/dev/nvme9n3", t.TempDir()+"/metadata")
	s3MetadataMu.Unlock()
	require.Error(t, err)
	require.Contains(t, err.Error(), "after 2 attempt(s)")
	require.Equal(t, 2, calls)
}

// A mount that timed out but still completed in the kernel is accepted.
func TestMetadataMountAcceptsLateKernelMount(t *testing.T) {
	stubS3MetadataMounts(t)
	sleeps := withMetadataMountKnobs(t, 60*time.Second, 3)
	calls := 0
	prev := mountS3MetadataDeviceTimeout
	prevSrc := s3MetadataMountSourceFn
	path := t.TempDir() + "/metadata"
	mountS3MetadataDeviceTimeout = func(dev, p string, timeout time.Duration) error {
		calls++
		_ = prev(dev, p, timeout)
		return errors.New("mount: timed out")
	}
	s3MetadataMountSourceFn = func(string) string { return "/dev/nvme9n4" }
	t.Cleanup(func() {
		mountS3MetadataDeviceTimeout = prev
		s3MetadataMountSourceFn = prevSrc
	})
	s3MetadataMu.Lock()
	err := mountS3MetadataLocked("snap-4", "/dev/nvme9n4", path)
	s3MetadataMu.Unlock()
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Empty(t, *sleeps)
}

func TestMetadataMountKnobsFromConfig(t *testing.T) {
	withMetadataMountKnobs(t, defaultMetadataMountTimeout, defaultMetadataMountAttempts)
	require.NoError(t, applyImportTimeouts(&Config{}))
	require.Equal(t, 60*time.Second, metadataMountTimeout)
	require.Equal(t, 3, metadataMountAttempts)

	cfg := &Config{MetadataMountTimeout: tomlext.FromStdTime(90 * time.Second), MetadataMountAttempts: 5}
	require.NoError(t, applyImportTimeouts(cfg))
	require.Equal(t, 90*time.Second, metadataMountTimeout)
	require.Equal(t, 5, metadataMountAttempts)
	require.Equal(t, 120*time.Second, effectiveMetadataMountTimeout(120*time.Second))
	require.Equal(t, 90*time.Second, effectiveMetadataMountTimeout(0))

	require.Error(t, applyImportTimeouts(&Config{MetadataMountTimeout: tomlext.FromStdTime(-time.Second)}))
	require.Error(t, applyImportTimeouts(&Config{MetadataMountAttempts: -1}))
}
