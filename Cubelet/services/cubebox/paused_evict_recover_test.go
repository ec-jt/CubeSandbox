// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

// stageCrash reproduces steps 1-2 of evictPausedSandbox for sb on node, and
// then "crashes": the stage stays on disk, neither published nor aborted.
func stageCrash(t *testing.T, sb *cubeboxstore.CubeBox, node, snap string) {
	t.Helper()
	inc, err := sourcefence.EvictedIncarnation(sb.ID, sb.CreatedAt, snap, testDigest)
	require.NoError(t, err)
	native, err := sourcefence.NewEvictedNativeBinding(testDigest, testCommit)
	require.NoError(t, err)
	_, err = workflow.EvictionRecords.StageEvicted(workflow.SourceFences, sourcefence.Binding{
		OperationID: "req-" + sb.ID, SandboxID: sb.ID, NodeID: node, Incarnation: inc,
		CheckpointID: snap, CheckpointDigest: testDigest,
	}, native)
	require.NoError(t, err)
}

func stagedNames(t *testing.T) []string {
	t.Helper()
	st, err := workflow.EvictionRecords.StagedEvictions()
	require.NoError(t, err)
	var out []string
	for _, s := range st {
		out = append(out, s.Name)
	}
	return out
}

func recoverNow(t *testing.T, s *service, api *evictFakeAPI) evictionRecoveryReport {
	t.Helper()
	rep, err := recoverStagedEvictions(context.Background(), api, s.sandboxLifecycleLocks, s.cubeboxMgr.config.SourceFenceNodeID)
	require.NoError(t, err)
	return rep
}

func readRecord(t *testing.T, s *service, id string) *cubebox.UpdateCubeSandboxResponse {
	t.Helper()
	rsp, err := s.Update(context.Background(), &cubebox.UpdateCubeSandboxRequest{
		RequestID: "read", SandboxID: id,
		Annotations: map[string]string{constants.MasterAnnotationsUpdateAction: sourceQuiescedReadAction},
	})
	require.NoError(t, err)
	return rsp
}

// Crash after the tombstone drop, before publish: startup recovery publishes
// the exact staged record; the read path serves it (Master reconcile then has
// its proof) and origin restore of that checkpoint is refused.
func TestEvictionRecoveryPublishesAfterTombstoneDrop(t *testing.T) {
	sb := pausedS3Sandbox("sb-ev", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, api := newEvictService(t, sb, true, committed)
	stageCrash(t, sb, "node-a", "snap-a")
	staged, err := workflow.EvictionRecords.StagedEvictions()
	require.NoError(t, err)
	require.Len(t, staged, 1)
	api.cb = nil // step 3 ran: tombstone gone

	// Before recovery: no proof, and the sandbox is fail-closed for origin
	// restore only after recovery runs (or Retain marks it at runtime).
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, readRecord(t, s, "sb-ev").Ret.RetCode)

	rep := recoverNow(t, s, api)
	assert.Equal(t, []string{"sb-ev"}, rep.Published)
	assert.Empty(t, rep.Unresolved)
	assert.Empty(t, stagedNames(t))

	read := readRecord(t, s, "sb-ev")
	require.Equal(t, errorcode.ErrorCode_Success, read.Ret.RetCode, read.Ret.RetMsg)
	assert.Equal(t, string(staged[0].Raw), string(read.ExtInfo[sourceQuiescedExtInfoKey]))
	require.ErrorIs(t, refuseEvictedCheckpointRestore("sb-ev", "snap-a"), sourcefence.ErrFenced)
	require.NoError(t, workflow.SourceFences.Any(), "eviction recovery must not create a recovery fence")

	// Idempotent: a second run finds nothing to do and changes nothing.
	rep = recoverNow(t, s, api)
	assert.Empty(t, rep.Published)
	assert.Empty(t, rep.Unresolved)
	assert.Equal(t, string(staged[0].Raw), string(readRecord(t, s, "sb-ev").ExtInfo[sourceQuiescedExtInfoKey]))
}

// Crash after staging, before the drop: recovery aborts the stage. The sandbox
// stays a normal paused sandbox, resumable on this node, and a later eviction
// works.
func TestEvictionRecoveryAbortsWhileTombstonePresent(t *testing.T) {
	sb := pausedS3Sandbox("sb-p", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, api := newEvictService(t, sb, true, committed)
	stageCrash(t, sb, "node-a", "snap-a")

	rep := recoverNow(t, s, api)
	assert.Equal(t, []string{"sb-p"}, rep.Aborted)
	assert.Empty(t, rep.Published)
	assert.Empty(t, rep.Unresolved)
	assert.Empty(t, stagedNames(t))
	assert.NotNil(t, api.cb)
	assert.Empty(t, api.deleted)
	require.NoError(t, refuseEvictedCheckpointRestore("sb-p", "snap-a"), "origin resume stays allowed")
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, readRecord(t, s, "sb-p").Ret.RetCode)

	rsp, err := s.Update(context.Background(), evictReq("sb-p", "snap-a"))
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_Success, rsp.Ret.RetCode, rsp.Ret.RetMsg)
}

func corruptStage(t *testing.T, sb *cubeboxstore.CubeBox) string {
	t.Helper()
	stageCrash(t, sb, "node-a", "snap-a")
	names := stagedNames(t)
	require.Len(t, names, 1)
	p := filepath.Join(evictionRoot(t), names[0])
	raw, err := os.ReadFile(p)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(p, raw[:len(raw)/2], 0600)) // truncated
	return names[0]
}

func evictionRoot(t *testing.T) string {
	t.Helper()
	require.NotEmpty(t, lastEvictionRoot)
	return lastEvictionRoot
}

// A corrupt/truncated stage next to a live tombstone is deleted: the eviction
// never finished staging, so nothing was dropped.
func TestEvictionRecoveryDiscardsCorruptStageWithTombstone(t *testing.T) {
	sb := pausedS3Sandbox("sb-c", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, api := newEvictService(t, sb, true, committed)
	name := corruptStage(t, sb)

	rep := recoverNow(t, s, api)
	assert.Equal(t, []string{name}, rep.Discarded)
	assert.Empty(t, rep.Unresolved)
	assert.Empty(t, stagedNames(t))
	require.NoError(t, refuseEvictedCheckpointRestore("sb-c", "snap-a"))
}

// A corrupt stage with the tombstone gone is never published: it is kept,
// surfaced as unresolved (status + error log) and blocks origin restore.
func TestEvictionRecoveryKeepsCorruptStageWithoutTombstone(t *testing.T) {
	sb := pausedS3Sandbox("sb-c", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, api := newEvictService(t, sb, true, committed)
	name := corruptStage(t, sb)
	api.cb = nil

	for i := 0; i < 2; i++ { // idempotent
		rep := recoverNow(t, s, api)
		assert.Empty(t, rep.Published)
		assert.False(t, rep.Retry, "a corrupt stage is not a transient failure")
		require.Contains(t, rep.Unresolved, sourcefence.SandboxKey("sb-c"))
		assert.Equal(t, []string{name}, stagedNames(t))
	}
	assert.Contains(t, workflow.EvictionRecords.UnresolvedEvictions(), sourcefence.SandboxKey("sb-c"))
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, readRecord(t, s, "sb-c").Ret.RetCode)
	require.ErrorIs(t, refuseEvictedCheckpointRestore("sb-c", "snap-a"), sourcefence.ErrFenced)
}

// A stage bound to another node, or one whose commit no longer verifies, is
// never published when the tombstone is gone.
func TestEvictionRecoveryRefusesForeignOrStaleStage(t *testing.T) {
	cases := map[string]struct {
		node   string
		verify func(context.Context, string, string) (string, error)
		retry  bool
	}{
		"other node":   {"node-b", committed, false},
		"commit moved": {"node-a", func(context.Context, string, string) (string, error) { return "ab" + testDigest[2:], nil }, false},
		"not verifiable now": {"node-a", func(context.Context, string, string) (string, error) {
			return "", errors.New("storage is not initialized")
		}, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			sb := pausedS3Sandbox("sb-f", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
			s, api := newEvictService(t, sb, true, tc.verify)
			stageCrash(t, sb, tc.node, "snap-a")
			api.cb = nil
			rep := recoverNow(t, s, api)
			assert.Empty(t, rep.Published)
			assert.Equal(t, tc.retry, rep.Retry)
			require.Contains(t, rep.Unresolved, sourcefence.SandboxKey("sb-f"))
			assert.Len(t, stagedNames(t), 1)
			require.Equal(t, errorcode.ErrorCode_PreConditionFailed, readRecord(t, s, "sb-f").Ret.RetCode)
			require.ErrorIs(t, refuseEvictedCheckpointRestore("sb-f", "snap-a"), sourcefence.ErrFenced)
		})
	}
}

// A staged record whose binding names another sandbox than its file key is
// never published (a renamed/foreign stage).
func TestEvictionRecoveryRefusesStageForOtherSandbox(t *testing.T) {
	sb := pausedS3Sandbox("sb-1", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, api := newEvictService(t, sb, true, committed)
	stageCrash(t, sb, "node-a", "snap-a")
	names := stagedNames(t)
	require.Len(t, names, 1)
	root := evictionRoot(t)
	foreign := ".evicted-" + sourcefence.SandboxKey("sb-2") + "-x"
	require.NoError(t, os.Rename(filepath.Join(root, names[0]), filepath.Join(root, foreign)))
	api.cb = nil

	rep := recoverNow(t, s, api)
	assert.Empty(t, rep.Published)
	require.Contains(t, rep.Unresolved, sourcefence.SandboxKey("sb-2"))
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, readRecord(t, s, "sb-1").Ret.RetCode)
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, readRecord(t, s, "sb-2").Ret.RetCode)
}

// A transient verification failure resolves on a later pass (the background
// retry) without operator action, and clears the unresolved status.
func TestEvictionRecoveryRetryPublishesLater(t *testing.T) {
	sb := pausedS3Sandbox("sb-r", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	ready := false
	s, api := newEvictService(t, sb, true, func(context.Context, string, string) (string, error) {
		if !ready {
			return "", errors.New("native s3 store unavailable")
		}
		return testDigest, nil
	})
	stageCrash(t, sb, "node-a", "snap-a")
	api.cb = nil
	rep := recoverNow(t, s, api)
	require.True(t, rep.Retry)
	require.NotEmpty(t, workflow.EvictionRecords.UnresolvedEvictions())

	ready = true
	rep = recoverNow(t, s, api)
	assert.Equal(t, []string{"sb-r"}, rep.Published)
	assert.Empty(t, workflow.EvictionRecords.UnresolvedEvictions())
	require.Equal(t, errorcode.ErrorCode_Success, readRecord(t, s, "sb-r").Ret.RetCode)
}

// Recovery that crashed after its publish rename (stage gone, record present)
// re-runs cleanly; a crash before the rename simply publishes on re-run.
func TestEvictionRecoveryCrashDuringRecovery(t *testing.T) {
	sb := pausedS3Sandbox("sb-x", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, api := newEvictService(t, sb, true, committed)
	stageCrash(t, sb, "node-a", "snap-a")
	api.cb = nil
	staged, err := workflow.EvictionRecords.StagedEvictions()
	require.NoError(t, err)
	// Simulate: recovery published (rename done), then crashed.
	_, err = workflow.EvictionRecords.PublishStaged(staged[0])
	require.NoError(t, err)
	rep := recoverNow(t, s, api)
	assert.Empty(t, rep.Published)
	assert.Empty(t, rep.Unresolved)
	require.Equal(t, errorcode.ErrorCode_Success, readRecord(t, s, "sb-x").Ret.RetCode)
	// Publishing a listed stage twice fails without side effects.
	_, err = workflow.EvictionRecords.PublishStaged(staged[0])
	require.Error(t, err)
}

// At runtime, a publish failure after the drop keeps the stage (Retain) and
// blocks origin restore until recovery publishes it.
func TestEvictionPublishFailureRetainsStage(t *testing.T) {
	sb := pausedS3Sandbox("sb-pf", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, api := newEvictService(t, sb, true, committed)
	// Make the record path a directory so the rename fails.
	path := filepath.Join(evictionRoot(t), sourcefence.SandboxKey("sb-pf")+".json.local-drain-v1")
	require.NoError(t, os.MkdirAll(filepath.Join(path, "x"), 0700))

	rsp, err := s.Update(context.Background(), evictReq("sb-pf", "snap-a"))
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_Unknown, rsp.Ret.RetCode)
	require.Nil(t, api.cb)
	require.Len(t, stagedNames(t), 1, "the stage is the only proof and must survive")
	require.ErrorIs(t, refuseEvictedCheckpointRestore("sb-pf", "snap-a"), sourcefence.ErrFenced)

	require.NoError(t, os.RemoveAll(path))
	rep := recoverNow(t, s, api)
	assert.Equal(t, []string{"sb-pf"}, rep.Published)
	require.Equal(t, errorcode.ErrorCode_Success, readRecord(t, s, "sb-pf").Ret.RetCode)
}
