// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sandbox

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/restoreplace"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	cubeleterrorcode "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

// Tombstone gone on the origin + valid eviction record served there: the
// binding is marked evicted and the origin's exact record is persisted.
func TestReconcileTombstoneGoneRecordPresentMarksEvicted(t *testing.T) {
	raw := evictRecord(t, "sb-1", "snap-1", "node-a", evictDigest)
	s := &evictSeams{rec: readyS3Binding(), present: false, originRaw: raw}
	installEvictSeams(t, s)

	got, ok := reconcilePausedEviction(context.Background(), s.rec)
	require.True(t, ok)
	require.True(t, got.Evicted())
	require.Equal(t, evictDigest, got.CheckpointDigest)
	require.Equal(t, []string{"sb-1/snap-1/" + evictDigest}, s.marked)
	require.Equal(t, string(raw), s.records[0])
}

// The origin still holds the sandbox (still PAUSED): never mark, and do not
// even read the record.
func TestReconcileStillPausedNoChange(t *testing.T) {
	s := &evictSeams{rec: readyS3Binding(), present: true,
		originRaw: evictRecord(t, "sb-1", "snap-1", "node-a", evictDigest)}
	installEvictSeams(t, s)
	got, ok := reconcilePausedEviction(context.Background(), s.rec)
	require.False(t, ok)
	require.False(t, got.Evicted())
	require.Empty(t, s.marked)
	require.Zero(t, s.recordReads)
}

// Tombstone gone but no (or a bad) record: never mark evicted.
func TestReconcileMissingOrBadRecordNeverMarks(t *testing.T) {
	cases := map[string]*evictSeams{
		"origin unreachable":          {presentErr: errors.New("conn refused")},
		"record missing":              {originErr: errors.New("origin refused: no such file")},
		"record empty":                {originRaw: []byte{}},
		"truncated":                   {originRaw: evictRecord(t, "sb-1", "snap-1", "node-a", evictDigest)[:40]},
		"foreign sandbox":             {originRaw: evictRecord(t, "sb-2", "snap-1", "node-a", evictDigest)},
		"other checkpoint":            {originRaw: evictRecord(t, "sb-1", "snap-2", "node-a", evictDigest)},
		"other origin node":           {originRaw: evictRecord(t, "sb-1", "snap-1", "node-b", evictDigest)},
		"digest not of proof":         {originRaw: evictRecord(t, "sb-1", "snap-1", "node-a", "0000000000000000000000000000000000000000000000000000000000000001")},
		"fence record (not eviction)": {originRaw: fenceRecordJSON(t)},
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			s.rec = readyS3Binding()
			installEvictSeams(t, s)
			got, ok := reconcilePausedEviction(context.Background(), s.rec)
			require.False(t, ok)
			require.False(t, got.Evicted())
			require.Empty(t, s.marked)
		})
	}
}

func okRefusal(msg string) *cubebox.UpdateCubeSandboxResponse {
	return &cubebox.UpdateCubeSandboxResponse{
		Ret: &cubeleterrorcode.Ret{RetCode: cubeleterrorcode.ErrorCode_InvalidParamFormat, RetMsg: msg}}
}

func fenceRecordJSON(t *testing.T) []byte {
	t.Helper()
	// A FenceRecoverySource drain record: valid local-quiesced shape, but
	// its native_binding is a membership, not a paused-eviction binding.
	return []byte(`{"version":1,"binding":{"operation_id":"op","sandbox_id":"sb-1","node_id":"node-a","incarnation":"inc","checkpoint_id":"snap-1","checkpoint_digest":"` +
		evictDigest + `"},"stage":"local-quiesced-not-global-fenced","native_binding":{"token":"native"}}`)
}

// Transport error on evict, but the origin had in fact dropped the sandbox
// and published its record: evict reconciles and reports success.
func TestEvictTransportErrorReconciles(t *testing.T) {
	t.Setenv(PausedEvictionEnv, "true")
	raw := evictRecord(t, "sb-1", "snap-1", "node-a", evictDigest)
	s := &evictSeams{rec: readyS3Binding(), cubeletErr: errors.New("context deadline exceeded"), originRaw: raw}
	installEvictSeams(t, s)
	rsp := evictPausedSandbox(context.Background(), &types.UpdateRequest{RequestID: "r", SandboxID: "sb-1"})
	require.Equal(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode, rsp.Ret.RetMsg)
	require.True(t, s.rec.Evicted())
	require.Equal(t, string(raw), s.rec.EvictionRecord)
}

// A CLM retry after a lost response: the origin refuses (sandbox gone), and
// reconcile turns that into a recorded eviction instead of a refusal.
func TestEvictRetryAfterLostResponseReconciles(t *testing.T) {
	t.Setenv(PausedEvictionEnv, "true")
	raw := evictRecord(t, "sb-1", "snap-1", "node-a", evictDigest)
	s := &evictSeams{rec: readyS3Binding(), originRaw: raw, cubeletRsp: okRefusal("sandbox record missing")}
	installEvictSeams(t, s)
	rsp := evictPausedSandbox(context.Background(), &types.UpdateRequest{RequestID: "r2", SandboxID: "sb-1"})
	require.Equal(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode, rsp.Ret.RetMsg)
	require.True(t, s.rec.Evicted())
}

// Unknown outcome that reconcile cannot prove keeps the outcome-unknown code
// so CLM retries.
func TestEvictTransportErrorUnprovenKeepsUnknownCode(t *testing.T) {
	t.Setenv(PausedEvictionEnv, "true")
	s := &evictSeams{rec: readyS3Binding(), cubeletErr: errors.New("timeout"), presentErr: errors.New("down")}
	installEvictSeams(t, s)
	rsp := evictPausedSandbox(context.Background(), &types.UpdateRequest{RequestID: "r", SandboxID: "sb-1"})
	require.Equal(t, int(errorcode.ErrorCode_ReqCubeAPIFailed), rsp.Ret.RetCode)
	require.False(t, s.rec.Evicted())
}

// The origin returns success with a record that does not bind this binding:
// Master refuses to record the eviction.
func TestEvictInvalidRecordNotPersisted(t *testing.T) {
	t.Setenv(PausedEvictionEnv, "true")
	s := &evictSeams{rec: readyS3Binding(), cubeletRsp: okEvictRspWith(evictRecord(t, "sb-1", "snap-1", "node-b", evictDigest))}
	installEvictSeams(t, s)
	rsp := evictPausedSandbox(context.Background(), &types.UpdateRequest{RequestID: "r", SandboxID: "sb-1"})
	require.NotEqual(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode)
	require.Empty(t, s.marked)
}

// Resume reconciles an ambiguous earlier eviction before placement, so the
// binding goes cross-node (with the persisted record) instead of home.
func TestResumeReconcilesAmbiguousEvictionFirst(t *testing.T) {
	t.Setenv(PausedEvictionEnv, "true")
	raw := evictRecord(t, "sb-1", "snap-1", "node-a", evictDigest)
	s := &evictSeams{rec: readyS3Binding(), originRaw: raw}
	installEvictSeams(t, s)
	got, ok := resumeReconcileEvictionFn(context.Background(), s.rec)
	require.True(t, ok)

	orig := decidePauseResumePlacementFn
	t.Cleanup(func() { decidePauseResumePlacementFn = orig })
	var in restoreplace.Input
	decidePauseResumePlacementFn = func(_ context.Context, i restoreplace.Input) (*restoreplace.Placement, error) {
		in = i
		return &restoreplace.Placement{NodeID: "node-b", NodeIP: "10.0.0.2", CrossNode: true}, nil
	}
	_, err := resumePlacement(context.Background(), got, "cubebox", nil)
	require.NoError(t, err)
	require.True(t, in.ExcludeOrigin)
	require.Equal(t, string(raw), string(in.PersistedSourceQuiescedRecord))
	require.Equal(t, evictDigest, in.SourceCheckpointDigest)
}

// A non-evicted binding never carries a persisted record into placement.
func TestResumeNonEvictedCarriesNoPersistedRecord(t *testing.T) {
	rec := readyS3Binding()
	rec.EvictionRecord = string(evictRecord(t, "sb-1", "snap-1", "node-a", evictDigest))
	in := pauseResumePlacementInput(rec, "cubebox", nil)
	require.Empty(t, in.PersistedSourceQuiescedRecord)
	require.False(t, in.ExcludeOrigin)
	_ = pausesnap.StatusReady
}
