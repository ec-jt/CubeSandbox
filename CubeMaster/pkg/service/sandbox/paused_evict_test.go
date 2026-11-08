// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/restoreplace"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	cubeleterrorcode "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

// evictCommit stands in for the origin's canonical native commit record;
// evictDigest is its sha256, as the origin Cubelet computes it.
var evictCommit = []byte(`{"version":1,"binding":{"sandbox_id":"sb-1"},"objects":{}}`)

var evictDigest = func() string { s := sha256.Sum256(evictCommit); return hex.EncodeToString(s[:]) }()

// evictRecord builds the origin local-quiesced eviction record bytes exactly
// as Cubelet publishes them.
func evictRecord(t *testing.T, sandbox, checkpoint, node, digest string) []byte {
	t.Helper()
	native, err := json.Marshal(map[string]any{"kind": restoreplace.EvictedNativeKind, "checkpoint_commit": json.RawMessage(evictCommit)})
	require.NoError(t, err)
	raw, err := json.Marshal(restoreplace.SourceQuiescedRecord{
		Version: 1,
		Binding: restoreplace.SourceQuiescedBinding{
			OperationID: "evict-op", SandboxID: sandbox, NodeID: node, Incarnation: "inc",
			CheckpointID: checkpoint, CheckpointDigest: digest,
		},
		Stage:         restoreplace.LocalQuiesced,
		NativeBinding: native,
	})
	require.NoError(t, err)
	return raw
}

func readyS3Binding() *pausesnap.Record {
	return &pausesnap.Record{
		SandboxID: "sb-1", SnapshotID: "snap-1", NodeID: "node-a", NodeIP: "10.0.0.1",
		Status: pausesnap.StatusReady, Backend: "s3", RemoteStatus: "ready",
	}
}

type evictSeams struct {
	rec          *pausesnap.Record
	cubeletCalls []*cubebox.UpdateCubeSandboxRequest
	cubeletRsp   *cubebox.UpdateCubeSandboxResponse
	cubeletErr   error
	marked       []string
	records      []string
	// Reconcile probes of the origin.
	present     bool
	presentErr  error
	originRaw   []byte
	originErr   error
	probeCalls  int
	recordReads int
}

func installEvictSeams(t *testing.T, s *evictSeams) {
	t.Helper()
	origLookup, origMark, origCubelet := evictLookupPauseFn, evictMarkFn, evictCubeletFn
	origProbe, origRead := evictProbePresentFn, evictReadRecordFn
	t.Cleanup(func() {
		evictLookupPauseFn, evictMarkFn, evictCubeletFn = origLookup, origMark, origCubelet
		evictProbePresentFn, evictReadRecordFn = origProbe, origRead
	})
	evictLookupPauseFn = func(context.Context, string) (*pausesnap.Record, error) {
		if s.rec == nil {
			return nil, pausesnap.ErrNotFound
		}
		return s.rec, nil
	}
	evictMarkFn = func(_ context.Context, sid, snap, digest, record string) error {
		if record == "" {
			t.Fatal("MarkEvicted must never be called without an eviction record")
		}
		s.marked = append(s.marked, sid+"/"+snap+"/"+digest)
		s.records = append(s.records, record)
		now := time.Now()
		s.rec.EvictedAt, s.rec.CheckpointDigest, s.rec.EvictionRecord = &now, digest, record
		return nil
	}
	evictCubeletFn = func(_ context.Context, _ string, req *cubebox.UpdateCubeSandboxRequest) (*cubebox.UpdateCubeSandboxResponse, error) {
		s.cubeletCalls = append(s.cubeletCalls, req)
		return s.cubeletRsp, s.cubeletErr
	}
	evictProbePresentFn = func(context.Context, string, string) (bool, error) {
		s.probeCalls++
		return s.present, s.presentErr
	}
	evictReadRecordFn = func(context.Context, string, string) ([]byte, error) {
		s.recordReads++
		return s.originRaw, s.originErr
	}
}

func okEvictRspWith(record []byte) *cubebox.UpdateCubeSandboxResponse {
	return &cubebox.UpdateCubeSandboxResponse{
		Ret: &cubeleterrorcode.Ret{RetCode: cubeleterrorcode.ErrorCode_Success},
		ExtInfo: map[string][]byte{
			cubeletEvictDigestKey: []byte(evictDigest),
			cubeletEvictRecordKey: record,
		},
	}
}

func okEvictRsp() *cubebox.UpdateCubeSandboxResponse {
	// Record built without *testing.T: helpers below assert on it.
	native, _ := json.Marshal(map[string]any{"kind": restoreplace.EvictedNativeKind, "checkpoint_commit": json.RawMessage(evictCommit)})
	raw, _ := json.Marshal(restoreplace.SourceQuiescedRecord{
		Version: 1,
		Binding: restoreplace.SourceQuiescedBinding{OperationID: "evict-op", SandboxID: "sb-1", NodeID: "node-a",
			Incarnation: "inc", CheckpointID: "snap-1", CheckpointDigest: evictDigest},
		Stage: restoreplace.LocalQuiesced, NativeBinding: native,
	})
	return okEvictRspWith(raw)
}

func TestPausedEvictionMasterFlagOffByDefault(t *testing.T) {
	t.Setenv(PausedEvictionEnv, "")
	require.False(t, PausedEvictionEnabled())
	t.Setenv(PausedEvictionEnv, "garbage")
	require.False(t, PausedEvictionEnabled(), "unparsable must stay off")
	s := &evictSeams{rec: readyS3Binding(), cubeletRsp: okEvictRsp()}
	installEvictSeams(t, s)
	t.Setenv(PausedEvictionEnv, "")
	rsp := evictPausedSandbox(context.Background(), &types.UpdateRequest{RequestID: "r", SandboxID: "sb-1"})
	require.NotEqual(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode)
	require.Empty(t, s.cubeletCalls)
	require.Empty(t, s.marked)
}

func TestEvictionIneligibleReason(t *testing.T) {
	require.Empty(t, EvictionIneligibleReason(readyS3Binding()))
	for name, mutate := range map[string]func(*pausesnap.Record){
		"cubecow":    func(r *pausesnap.Record) { r.Backend = "xfs" },
		"not ready":  func(r *pausesnap.Record) { r.Status = "CREATING" },
		"not remote": func(r *pausesnap.Record) { r.RemoteStatus = "inprogress" },
		"evicted":    func(r *pausesnap.Record) { now := time.Now(); r.EvictedAt = &now },
		"no origin":  func(r *pausesnap.Record) { r.NodeIP = "" },
	} {
		r := readyS3Binding()
		mutate(r)
		require.NotEmpty(t, EvictionIneligibleReason(r), name)
	}
	require.NotEmpty(t, EvictionIneligibleReason(nil))
}

func TestPausedEvictionMasterRecordsDigestAndNeverCleans(t *testing.T) {
	t.Setenv(PausedEvictionEnv, "true")
	s := &evictSeams{rec: readyS3Binding(), cubeletRsp: okEvictRsp()}
	installEvictSeams(t, s)

	rsp := evictPausedSandbox(context.Background(), &types.UpdateRequest{RequestID: "r", SandboxID: "sb-1"})
	require.Equal(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode, rsp.Ret.RetMsg)
	require.Len(t, s.cubeletCalls, 1)
	ann := s.cubeletCalls[0].GetAnnotations()
	require.Equal(t, cubeletEvictAction, ann[constants.CubeAnnotationsUpdateAction])
	require.Equal(t, "snap-1", ann[constants.CubeAnnotationPauseSnapshotID])
	require.Equal(t, []string{"sb-1/snap-1/" + evictDigest}, s.marked)
	require.Equal(t, string(okEvictRsp().GetExtInfo()[cubeletEvictRecordKey]), s.records[0],
		"Master persists the exact origin record bytes")

	// Second attempt: already evicted, refused without touching the node.
	rsp = evictPausedSandbox(context.Background(), &types.UpdateRequest{RequestID: "r2", SandboxID: "sb-1"})
	require.NotEqual(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode)
	require.Len(t, s.cubeletCalls, 1)
}

func TestPausedEvictionMasterOriginRefusalLeavesBinding(t *testing.T) {
	t.Setenv(PausedEvictionEnv, "true")
	for name, s := range map[string]*evictSeams{
		"refused": {rec: readyS3Binding(), present: true, cubeletRsp: &cubebox.UpdateCubeSandboxResponse{
			Ret: &cubeleterrorcode.Ret{RetCode: cubeleterrorcode.ErrorCode_PreConditionFailed, RetMsg: "not committed"}}},
		"transport, origin unreachable": {rec: readyS3Binding(), cubeletErr: errors.New("timeout"), presentErr: errors.New("down")},
		"transport, still paused":       {rec: readyS3Binding(), cubeletErr: errors.New("timeout"), present: true},
		"no digest": {rec: readyS3Binding(), cubeletRsp: &cubebox.UpdateCubeSandboxResponse{
			Ret: &cubeleterrorcode.Ret{RetCode: cubeleterrorcode.ErrorCode_Success}}},
		"no record": {rec: readyS3Binding(), cubeletRsp: &cubebox.UpdateCubeSandboxResponse{
			Ret:     &cubeleterrorcode.Ret{RetCode: cubeleterrorcode.ErrorCode_Success},
			ExtInfo: map[string][]byte{cubeletEvictDigestKey: []byte(evictDigest)}}},
	} {
		installEvictSeams(t, s)
		rsp := evictPausedSandbox(context.Background(), &types.UpdateRequest{RequestID: "r", SandboxID: "sb-1"})
		require.NotEqual(t, int(errorcode.ErrorCode_Success), rsp.Ret.RetCode, name)
		require.Empty(t, s.marked, name)
		require.False(t, s.rec.Evicted(), name)
	}
}

func TestPausedEvictionViewReportsPressureAndEligibility(t *testing.T) {
	origNodes, origBindings := pausedEvictionNodesFn, pausedEvictionBindingsFn
	t.Cleanup(func() { pausedEvictionNodesFn, pausedEvictionBindingsFn = origNodes, origBindings })
	t.Setenv(PausedEvictionEnv, "")
	pausedEvictionNodesFn = func() node.NodeList {
		return node.NodeList{&node.Node{InsID: "node-a", IP: "10.0.0.1", Healthy: true,
			QuotaMem: 1000, QuotaMemUsage: 950, StorageDiskUsagePer: 40}}
	}
	now := time.Now()
	pausedEvictionBindingsFn = func(context.Context) ([]*pausesnap.Record, error) {
		return []*pausesnap.Record{
			{SandboxID: "ok", SnapshotID: "s1", NodeIP: "10.0.0.1", Status: "READY", Backend: "s3", RemoteStatus: "ready", UpdatedAt: now},
			{SandboxID: "cow", SnapshotID: "s2", NodeIP: "10.0.0.1", Status: "READY", Backend: "xfs", UpdatedAt: now},
			{SandboxID: "gone", SnapshotID: "s3", NodeIP: "10.0.0.1", Status: "READY", Backend: "s3", RemoteStatus: "ready", EvictedAt: &now},
		}, nil
	}
	v := BuildPausedEvictionView(context.Background())
	require.False(t, v.Enabled, "master flag is off by default")
	require.Len(t, v.Nodes, 1)
	require.Equal(t, int64(950), v.Nodes[0].QuotaMemUsage)
	want := map[string]bool{"ok": true, "cow": false, "gone": false}
	for _, b := range v.Bindings {
		require.Equal(t, want[b.SandboxID], b.Eligible, "%s reason=%q", b.SandboxID, b.Reason)
	}
}

// Resume of an evicted binding never places home: it calls Decide with
// ExcludeOrigin and the eviction digest, so the cross-node gate binds to the
// exact sandbox, checkpoint and committed digest.
func TestResumePlacementEvictedGoesCrossNodeWithIdentity(t *testing.T) {
	orig := decidePauseResumePlacementFn
	t.Cleanup(func() { decidePauseResumePlacementFn = orig })
	var got restoreplace.Input
	decidePauseResumePlacementFn = func(_ context.Context, in restoreplace.Input) (*restoreplace.Placement, error) {
		got = in
		return &restoreplace.Placement{NodeID: "node-b", NodeIP: "10.0.0.2", CrossNode: true}, nil
	}
	rec := readyS3Binding()
	now := time.Now()
	rec.EvictedAt, rec.CheckpointDigest = &now, evictDigest

	p, err := resumePlacement(context.Background(), rec, "cubebox", nil)
	require.NoError(t, err)
	require.True(t, p.CrossNode)
	require.True(t, got.ExcludeOrigin)
	require.Equal(t, "sb-1", got.SourceSandboxID)
	require.Equal(t, "snap-1", got.SourceCheckpointID)
	require.Equal(t, evictDigest, got.SourceCheckpointDigest)
	require.Equal(t, "node-a", got.OriginNodeID)

	// An evicted binding whose checkpoint cannot cross nodes is unrestorable,
	// never silently sent home where no local state exists.
	rec.RemoteStatus = "inprogress"
	_, err = resumePlacement(context.Background(), rec, "cubebox", nil)
	require.Error(t, err)
}
