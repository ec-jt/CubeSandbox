// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"bytes"
	"context"
	"encoding/json"
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

// Eviction produces a durable local-quiesced record bound to the eviction
// operation, sandbox, this node, checkpoint and digest; returns its exact
// bytes; and ReadRecoverySourceQuiesced serves the same bytes afterwards.
func TestPausedEvictionPublishesLocalQuiescedRecord(t *testing.T) {
	sb := pausedS3Sandbox("sb-ev", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, api := newEvictService(t, sb, true, committed)

	rsp, err := s.Update(context.Background(), evictReq("sb-ev", "snap-a"))
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_Success, rsp.Ret.RetCode, rsp.Ret.RetMsg)
	require.Equal(t, []string{"sb-ev"}, api.deleted)

	raw := rsp.ExtInfo[pausedEvictRecordKey]
	require.NotEmpty(t, raw)
	var rec sourcefence.LocalDrainResult
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	require.NoError(t, d.Decode(&rec))
	assert.Equal(t, 1, rec.Version)
	assert.Equal(t, sourcefence.LocalQuiesced, rec.Stage)
	assert.Equal(t, "req-sb-ev", rec.Binding.OperationID)
	assert.Equal(t, "sb-ev", rec.Binding.SandboxID)
	assert.Equal(t, "node-a", rec.Binding.NodeID)
	assert.Equal(t, "snap-a", rec.Binding.CheckpointID)
	assert.Equal(t, testDigest, rec.Binding.CheckpointDigest)
	inc, err := sourcefence.EvictedIncarnation("sb-ev", sb.CreatedAt, "snap-a", testDigest)
	require.NoError(t, err)
	assert.Equal(t, inc, rec.Binding.Incarnation)
	var nb sourcefence.EvictedNativeBinding
	require.NoError(t, json.Unmarshal(rec.NativeBinding, &nb))
	assert.Equal(t, sourcefence.EvictedNativeKind, nb.Kind)
	assert.Equal(t, string(testCommit), string(nb.CheckpointCommit))

	// Read path (sandbox is gone from the node) serves the same bytes.
	read, err := s.Update(context.Background(), &cubebox.UpdateCubeSandboxRequest{
		RequestID: "read", SandboxID: "sb-ev",
		Annotations: map[string]string{constants.MasterAnnotationsUpdateAction: sourceQuiescedReadAction},
	})
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_Success, read.Ret.RetCode, read.Ret.RetMsg)
	assert.Equal(t, string(raw), string(read.ExtInfo[sourceQuiescedExtInfoKey]))

	// The eviction record is not a recovery fence: no node-wide deny.
	require.NoError(t, workflow.SourceFences.Any())
	require.NoError(t, workflow.SourceFences.Check("sb-ev"))
}

// A failed drop leaves no published record, so ReadRecoverySourceQuiesced
// keeps refusing and Master never sees proof for a still-present sandbox.
func TestPausedEvictionRefusalPublishesNoRecord(t *testing.T) {
	sb := pausedS3Sandbox("sb-x", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, _ := newEvictService(t, sb, true, committed)
	// Checkpoint mismatch: refused before any state change.
	rsp, err := s.Update(context.Background(), evictReq("sb-x", "snap-b"))
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, rsp.Ret.RetCode)
	require.Empty(t, rsp.ExtInfo[pausedEvictRecordKey])
	_, err = workflow.EvictionRecords.LoadLocalDrain("sb-x")
	require.Error(t, err)
}

// No node fence identity configured: eviction cannot bind proof, so refuse.
func TestPausedEvictionRequiresNodeIdentity(t *testing.T) {
	sb := pausedS3Sandbox("sb-n", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, api := newEvictService(t, sb, true, committed)
	s.cubeboxMgr.config.SourceFenceNodeID = ""
	rsp, err := s.Update(context.Background(), evictReq("sb-n", "snap-a"))
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, rsp.Ret.RetCode)
	require.Empty(t, api.deleted)
}

// An active recovery fence on the sandbox blocks eviction at the Update gate.
func TestPausedEvictionRefusedWhileFenced(t *testing.T) {
	sb := pausedS3Sandbox("sb-f", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, api := newEvictService(t, sb, true, committed)
	b := sourcefence.Binding{OperationID: "op", SandboxID: "sb-f", NodeID: "node-a", Incarnation: "inc",
		CheckpointID: "snap-a", CheckpointDigest: testDigest}
	require.NoError(t, workflow.SourceFences.BindStorage(b, []byte(`{}`)))
	rsp, err := s.Update(context.Background(), evictReq("sb-f", "snap-a"))
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_PreConditionFailed, rsp.Ret.RetCode)
	require.Empty(t, api.deleted)
	_, err = workflow.EvictionRecords.LoadLocalDrain("sb-f")
	require.Error(t, err)
}

// The origin keeps the eviction promise: it refuses to restore the evicted
// checkpoint, but a later, different pause checkpoint is unaffected.
func TestEvictedCheckpointRestoreRefusedOnOrigin(t *testing.T) {
	sb := pausedS3Sandbox("sb-ev", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, _ := newEvictService(t, sb, true, committed)
	rsp, err := s.Update(context.Background(), evictReq("sb-ev", "snap-a"))
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_Success, rsp.Ret.RetCode, rsp.Ret.RetMsg)

	require.ErrorIs(t, refuseEvictedCheckpointRestore("sb-ev", "snap-a"), sourcefence.ErrFenced)
	require.NoError(t, refuseEvictedCheckpointRestore("sb-ev", "snap-later"))
	require.NoError(t, refuseEvictedCheckpointRestore("sb-ev", ""))
	require.NoError(t, refuseEvictedCheckpointRestore("sb-other", "snap-a"))
}
