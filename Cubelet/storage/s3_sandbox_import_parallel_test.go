// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/internal/tomlext"
)

func withImportTimings(t *testing.T, restore, perDisk, poll time.Duration) {
	t.Helper()
	prevR, prevP, prevPoll := importRestoreDeadline, importPrefetchTimeout, importPrefetchPoll
	importRestoreDeadline, importPrefetchTimeout, importPrefetchPoll = restore, perDisk, poll
	t.Cleanup(func() {
		importRestoreDeadline, importPrefetchTimeout, importPrefetchPoll = prevR, prevP, prevPoll
	})
}

// All three disks are imported before any is waited on: the prefetch of the
// first must not hold back the import of the next. Nothing is activated.
func TestPrefetchAllImportsEveryDiskInParallel(t *testing.T) {
	withImportTimings(t, 5*time.Second, 5*time.Second, time.Millisecond)
	var (
		mu       sync.Mutex
		release  = make(chan struct{})
		started  = map[string]bool{}
		released bool
	)
	engine := &fakeCowEngine{}
	engine.importStatusFn = func(name string, _ int) (string, error) {
		mu.Lock()
		started[name] = true
		if len(started) == 3 && !released {
			released = true
			close(release)
		}
		mu.Unlock()
		select {
		case <-release:
			return `{"state":"done","done":true,"present_chunks":4,"local_chunks":4}`, nil
		default:
			return `{"state":"prefetching","present_chunks":4,"local_chunks":1}`, nil
		}
	}
	useTestCowStorageAt(t, engine)

	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	require.NotNil(t, imp)
	require.NoError(t, imp.PrefetchAll(context.Background()))

	got := []string{}
	for _, l := range engine.importedLvols {
		got = append(got, l[0]+"="+l[1])
	}
	sort.Strings(got)
	require.Equal(t, []string{
		"s3-meta-sb-1=u-meta",
		"sb-sb-1-memory=u-mem",
		"sb-sb-1-rootfs-gen0=u-root",
	}, got)
	require.Empty(t, engine.activatedVolumes, "the import phase must not activate anything")

	// The later per-disk steps adopt the imports: no second import.
	_, _, err := imp.Rootfs(context.Background(), 0)
	require.NoError(t, err)
	_, _, err = imp.Memory(context.Background())
	require.NoError(t, err)
	require.Len(t, engine.importedLvols, 3)
}

// One deadline bounds the whole phase, not one per disk: with a per-disk
// timeout far above it, a stuck prefetch still fails at the phase deadline.
func TestPrefetchAllHonoursOneOverallDeadline(t *testing.T) {
	withImportTimings(t, 150*time.Millisecond, time.Hour, 5*time.Millisecond)
	engine := &fakeCowEngine{}
	engine.importStatusFn = func(name string, _ int) (string, error) {
		if strings.Contains(name, "memory") {
			return `{"state":"prefetching","present_chunks":4,"local_chunks":1}`, nil
		}
		return `{"state":"done","done":true,"present_chunks":4,"local_chunks":4}`, nil
	}
	useTestCowStorageAt(t, engine)

	start := time.Now()
	err := CrossNodeSandboxImport(crossNodeAnnotations()).PrefetchAll(context.Background())
	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second)
	require.ErrorContains(t, err, "memory")
	require.ErrorContains(t, err, "import phase failed")
}

// The first failure cancels the other waits, and the error reported is the
// cause rather than the cancellation it triggered.
func TestPrefetchAllFailsFastOnFirstError(t *testing.T) {
	withImportTimings(t, time.Hour, time.Hour, 5*time.Millisecond)
	engine := &fakeCowEngine{}
	engine.importStatusFn = func(name string, _ int) (string, error) {
		if strings.Contains(name, "rootfs") {
			return `{"state":"failed","error":-5,"error_msg":"Input/output error","present_chunks":4}`, nil
		}
		return `{"state":"prefetching","present_chunks":4,"local_chunks":1}`, nil
	}
	useTestCowStorageAt(t, engine)

	start := time.Now()
	err := CrossNodeSandboxImport(crossNodeAnnotations()).PrefetchAll(context.Background())
	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second)
	require.ErrorContains(t, err, "Input/output error")
	require.Empty(t, engine.activatedVolumes)
}

func TestPrefetchAllNeedsRootfsAndMetadata(t *testing.T) {
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)
	imp := &SandboxImport{SandboxID: "sb-1", UUIDs: nil}
	require.Error(t, imp.PrefetchAll(context.Background()))
	require.Empty(t, engine.importedLvols)
}

// A rootfs-only package imports two disks.
func TestPrefetchAllWithoutMemory(t *testing.T) {
	withImportTimings(t, time.Second, time.Second, time.Millisecond)
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)
	ann := crossNodeAnnotations()
	for k, v := range ann {
		if strings.Contains(v, `"memory"`) {
			ann[k] = `{"rootfs":"u-root","metadata":"u-meta"}`
		}
	}
	require.NoError(t, CrossNodeSandboxImport(ann).PrefetchAll(context.Background()))
	require.Len(t, engine.importedLvols, 2)
}

func TestFirstImportErrorPrefersCause(t *testing.T) {
	cause := errors.New("boom")
	require.Nil(t, firstImportError([]error{nil, nil}))
	require.Equal(t, cause, firstImportError([]error{context.Canceled, nil, cause}))
	require.ErrorIs(t, firstImportError([]error{nil, context.Canceled}), context.Canceled)
}

func TestImportRestoreDeadlineDefaultAndConfig(t *testing.T) {
	require.Equal(t, 330*time.Second, defaultImportRestoreDeadline)
	require.Less(t, defaultImportRestoreDeadline, 360*time.Second, "must end before cube-api gives up on a resume")
	prev := importRestoreDeadline
	t.Cleanup(func() { importRestoreDeadline = prev })
	cfg := &Config{}
	cfg.ImportRestoreDeadline = tomlext.FromStdTime(200 * time.Second)
	require.NoError(t, applyImportTimeouts(cfg))
	require.Equal(t, 200*time.Second, importRestoreDeadline)
	cfg.ImportRestoreDeadline = tomlext.FromStdTime(-time.Second)
	require.Error(t, applyImportTimeouts(cfg))
}
