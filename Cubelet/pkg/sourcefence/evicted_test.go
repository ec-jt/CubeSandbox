// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sourcefence

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func evictedFixture(t *testing.T) (fences, ev *Store, b Binding, native []byte) {
	t.Helper()
	root := t.TempDir()
	var err error
	fences, err = Open(filepath.Join(root, "fences"))
	require.NoError(t, err)
	ev, err = Open(filepath.Join(root, "evictions"))
	require.NoError(t, err)
	commit := []byte(`{"version":1,"objects":{}}`)
	sum := sha256.Sum256(commit)
	digest := hex.EncodeToString(sum[:])
	inc, err := EvictedIncarnation("sb", 42, "snap-a", digest)
	require.NoError(t, err)
	b = Binding{"evict-op", "sb", "node-a", inc, "snap-a", digest}
	native, err = NewEvictedNativeBinding(digest, commit)
	require.NoError(t, err)
	return fences, ev, b, native
}

func TestEvictedRecordRoundTripsThroughLoadLocalDrain(t *testing.T) {
	fences, ev, b, native := evictedFixture(t)
	st, err := ev.StageEvicted(fences, b, native)
	require.NoError(t, err)
	// Unpublished: not readable yet.
	_, err = ev.LoadLocalDrain("sb")
	require.ErrorIs(t, err, os.ErrNotExist)
	res, raw, err := st.Publish()
	require.NoError(t, err)
	got, err := ev.LoadLocalDrain("sb")
	require.NoError(t, err)
	require.Equal(t, res, got)
	require.Equal(t, b, got.Binding)
	require.Equal(t, LocalQuiesced, got.Stage)
	require.NotEmpty(t, raw)
	// The eviction root is not a fence: the fence store stays empty.
	require.NoError(t, fences.Any())
	require.NoError(t, fences.Check("sb"))
}

func TestStageEvictedRefusesBadInput(t *testing.T) {
	fences, ev, b, native := evictedFixture(t)

	_, err := ev.StageEvicted(ev, b, native)
	require.ErrorIs(t, err, ErrBinding, "must not share the fence root")

	other := b
	other.CheckpointDigest = "00" + b.CheckpointDigest[2:]
	_, err = ev.StageEvicted(fences, other, native)
	require.ErrorIs(t, err, ErrBinding, "native commit must hash to the binding digest")

	_, err = ev.StageEvicted(fences, b, []byte(`{"kind":"other","checkpoint_commit":{}}`))
	require.ErrorIs(t, err, ErrBinding)
	_, err = ev.StageEvicted(fences, b, append([]byte(nil), native[:len(native)-1]...))
	require.ErrorIs(t, err, ErrBinding, "truncated native binding")

	_, err = NewEvictedNativeBinding(b.CheckpointDigest, []byte(`{ "version":1, "objects":{} }`))
	require.ErrorIs(t, err, ErrBinding, "non-canonical commit")

	// A recovery fence on the sandbox blocks eviction records entirely.
	require.NoError(t, fences.BindStorage(Binding{"op", "sb", "node-a", "inc", "snap-a", b.CheckpointDigest}, []byte(`{}`)))
	_, err = ev.StageEvicted(fences, b, native)
	require.ErrorIs(t, err, ErrFenced)
}

func TestEvictedRecordAbortLeavesNothing(t *testing.T) {
	fences, ev, b, native := evictedFixture(t)
	st, err := ev.StageEvicted(fences, b, native)
	require.NoError(t, err)
	st.Abort()
	_, _, err = st.Publish()
	require.Error(t, err)
	es, err := os.ReadDir(filepath.Dir(ev.filename("sb")))
	require.NoError(t, err)
	require.Empty(t, es)
}

func TestEvictedCheckpointBlocked(t *testing.T) {
	fences, ev, b, native := evictedFixture(t)
	require.NoError(t, ev.EvictedCheckpointBlocked("sb", "snap-a"), "no record yet")
	st, err := ev.StageEvicted(fences, b, native)
	require.NoError(t, err)
	_, _, err = st.Publish()
	require.NoError(t, err)
	require.ErrorIs(t, ev.EvictedCheckpointBlocked("sb", "snap-a"), ErrFenced)
	require.NoError(t, ev.EvictedCheckpointBlocked("sb", "snap-b"))
	// A corrupt record blocks every checkpoint of the sandbox.
	require.NoError(t, os.WriteFile(ev.filename("sb")+".local-drain-v1", []byte(`{"version":1`), 0600))
	require.ErrorIs(t, ev.EvictedCheckpointBlocked("sb", "snap-b"), ErrFenced)
}
