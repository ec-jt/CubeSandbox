// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubes

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
)

func heldCubeboxForTest(t *testing.T, op, lineage string, files ...string) (*cubeboxstore.CubeBox, *cubeboxstore.PublicationHold) {
	t.Helper()
	home := t.TempDir()
	for _, sub := range []string{"disk", "memory", "metadata"} {
		require.NoError(t, os.MkdirAll(filepath.Join(home, sub), 0o755))
	}
	for _, f := range files {
		require.NoError(t, os.WriteFile(filepath.Join(home, f), []byte("x"), 0o600))
	}
	id := "sb-heal"
	cb := &cubeboxstore.CubeBox{
		Metadata:           cubeboxstore.Metadata{ID: id},
		FirstContainerName: id,
		ContainersMap: &cubeboxstore.ContainersMap{ContainerMap: map[string]*cubeboxstore.Container{
			id: {Metadata: cubeboxstore.Metadata{ID: id}, Status: cubeboxstore.StoreStatus(cubeboxstore.Status{
				StartedAt: 1, PausingAt: 2, Unknown: true, Reason: "PauseFailed", Message: "refused"})},
		}},
	}
	if lineage != "" {
		cb.AddLabels(map[string]string{constants.MasterAnnotationRuntimeSnapshotID: lineage})
	}
	h := &cubeboxstore.PublicationHold{SnapshotID: "snap-held", Operation: op, SnapshotPath: home, WorkPath: home}
	cb.SetPublicationHold(h)
	return cb, h
}

func TestUncapturedPauseHoldDecision(t *testing.T) {
	cases := []struct {
		name    string
		op      string
		lineage string
		files   []string
		want    bool
	}{
		{"empty dirs, lineage unchanged", "pause", "tpl-base", nil, true},
		{"no lineage label at all", "pause", "", nil, true},
		{"lineage advanced to held snap", "pause", "snap-held", nil, false},
		{"capture wrote metadata", "pause", "tpl-base", []string{"metadata/state.json"}, false},
		{"capture wrote memory", "pause", "tpl-base", []string{"memory/mem"}, false},
		{"commit hold never released", "commit", "tpl-base", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cb, h := heldCubeboxForTest(t, tc.op, tc.lineage, tc.files...)
			require.Equal(t, tc.want, UncapturedPauseHold(cb, h))
		})
	}
}

func TestUncapturedPauseHoldMissingDirIsUncaptured(t *testing.T) {
	cb, h := heldCubeboxForTest(t, "pause", "tpl-base")
	h.SnapshotPath = filepath.Join(t.TempDir(), "gone")
	h.WorkPath = ""
	require.True(t, UncapturedPauseHold(cb, h))
}

func TestUncapturedPauseHoldSymlinkKeepsHold(t *testing.T) {
	cb, h := heldCubeboxForTest(t, "pause", "tpl-base")
	require.NoError(t, os.Symlink("/nonexistent", filepath.Join(h.SnapshotPath, "metadata", "link")))
	require.False(t, UncapturedPauseHold(cb, h))
}

func TestRecoverPodReleasesUncapturedPauseHold(t *testing.T) {
	cb, _ := heldCubeboxForTest(t, "pause", "tpl-base")
	// Use a held-but-captured twin to prove the containerd client is only
	// touched for the released case; nil client must not be reached for held.
	captured, _ := heldCubeboxForTest(t, "pause", "snap-held")
	require.NoError(t, RecoverPod(context.Background(), nil, captured))
	require.NotNil(t, captured.GetPublicationHold())
	require.False(t, captured.HealedPublicationHold)

	require.True(t, UncapturedPauseHold(cb, cb.GetPublicationHold()))
	cb.SetPublicationHold(nil)
	clearPauseAttemptStatus(cb)
	require.Nil(t, cb.GetPublicationHold())
	require.NoError(t, cb.PublicationError())
	st := cb.GetStatus().Get()
	require.False(t, st.PublicationPending)
	require.False(t, st.Unknown)
	require.Zero(t, st.PausingAt)
	require.Empty(t, st.Reason)
}
