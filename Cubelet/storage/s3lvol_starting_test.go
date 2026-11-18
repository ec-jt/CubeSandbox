// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

func withStartingFile(t *testing.T, alive func(int) bool) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "s3lvol.starting")
	prevF, prevA := s3lvolStartingFile, processAlive
	s3lvolStartingFile, processAlive = p, alive
	t.Cleanup(func() { s3lvolStartingFile, processAlive = prevF, prevA })
	return p
}

func TestS3lvolStartingNeedsALiveStarter(t *testing.T) {
	p := withStartingFile(t, func(pid int) bool { return pid == 4242 })

	got, _ := s3lvolStarting()
	require.False(t, got, "no marker: ready")

	require.NoError(t, os.WriteFile(p, []byte("4242\n"), 0o644))
	got, since := s3lvolStarting()
	require.True(t, got)
	require.False(t, since.IsZero())

	// A marker left by a killed start names a dead pid and is ignored.
	require.NoError(t, os.WriteFile(p, []byte("17\n"), 0o644))
	got, _ = s3lvolStarting()
	require.False(t, got)

	require.NoError(t, os.WriteFile(p, []byte("garbage"), 0o644))
	got, _ = s3lvolStarting()
	require.False(t, got)
}

func TestProcessAliveRejectsOtherProcesses(t *testing.T) {
	// This test binary is alive but is not rcow_start.sh.
	require.False(t, processAlive(os.Getpid()))
	require.False(t, processAlive(0))
	require.False(t, processAlive(-1))
}

// While s3lvol is starting, backend=s3 requests fail fast with ErrS3NotReady
// even though the handle is published; afterwards they are served again with
// nothing restarted.
func TestStoreForBackendWaitsForS3lvolStart(t *testing.T) {
	p := withStartingFile(t, func(int) bool { return true })
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)

	store, err := localStorage.storeForBackend(cow.BackendS3)
	require.NoError(t, err)
	require.NotNil(t, store)

	require.NoError(t, os.WriteFile(p, []byte("99"), 0o644))
	_, err = localStorage.storeForBackend(cow.BackendS3)
	require.ErrorIs(t, err, ErrS3NotReady)
	_, err = requireS3Cow()
	require.ErrorIs(t, err, ErrS3NotReady)

	require.NoError(t, os.Remove(p))
	store, err = localStorage.storeForBackend(cow.BackendS3)
	require.NoError(t, err)
	require.NotNil(t, store)
}

// The init loop never calls s3lvol while a start is in progress, and never
// gives up.
func TestS3InitLoopWaitsWhileStarting(t *testing.T) {
	p := withStartingFile(t, func(int) bool { return true })
	require.NoError(t, os.WriteFile(p, []byte("99"), 0o644))
	prevPoll := s3InitStartingPoll
	s3InitStartingPoll = 5 * time.Millisecond
	t.Cleanup(func() { s3InitStartingPoll = prevPoll })

	l := &local{config: &Config{}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { l.runS3CowInitLoop(ctx); close(done) }()
	time.Sleep(60 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("init loop exited while s3lvol was starting")
	default:
	}
	cancel()
	<-done
	require.Nil(t, l.s3CowManager)
}

func TestS3InitBackoffIsCapped(t *testing.T) {
	prevI, prevM := s3InitRetryInterval, s3InitRetryMax
	s3InitRetryInterval, s3InitRetryMax = 5*time.Second, 30*time.Second
	t.Cleanup(func() { s3InitRetryInterval, s3InitRetryMax = prevI, prevM })
	require.Equal(t, 5*time.Second, s3InitBackoff(1))
	require.Equal(t, 10*time.Second, s3InitBackoff(2))
	require.Equal(t, 20*time.Second, s3InitBackoff(3))
	require.Equal(t, 30*time.Second, s3InitBackoff(4))
	require.Equal(t, 30*time.Second, s3InitBackoff(400))
}
