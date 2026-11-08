// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/cube/internals/cubes"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

type evictFakeAPI struct {
	fakeCubeboxAPI
	deleted []string
}

func (f *evictFakeAPI) Delete(_ context.Context, opt *cubes.DeleteOption) error {
	f.deleted = append(f.deleted, opt.CubeboxID)
	if f.cb != nil && f.cb.ID == opt.CubeboxID {
		f.cb = nil
	}
	return nil
}

func (f *evictFakeAPI) List() []*cubeboxstore.CubeBox {
	if f.cb == nil {
		return nil
	}
	return []*cubeboxstore.CubeBox{f.cb}
}

func (f *evictFakeAPI) Get(ctx context.Context, id string) (*cubeboxstore.CubeBox, error) {
	if f.cb == nil || f.cb.ID != id {
		return nil, utils.ErrorKeyNotFound
	}
	return f.cb, nil
}

func pausedS3Sandbox(id, snap, backend string, status cubeboxstore.Status) *cubeboxstore.CubeBox {
	sb := sandboxWithResourceForTest(id, status, "2000m", "4Gi", 2, 0, 100)
	sb.AddAnnotations(map[string]string{constants.MasterAnnotationStorageBackend: backend})
	if snap != "" {
		stampPauseSnapshotID(sb, snap)
	}
	return sb
}

func newEvictService(t *testing.T, sb *cubeboxstore.CubeBox, enabled bool, verify func(context.Context, string, string) (string, error)) (*service, *evictFakeAPI) {
	t.Helper()
	api := &evictFakeAPI{fakeCubeboxAPI: fakeCubeboxAPI{cb: sb}}
	origVerify, origClean := verifyCommittedPauseCheckpointFn, cleanPausedRuncFn
	verifyCommittedPauseCheckpointFn = verify
	cleanPausedRuncFn = func(context.Context, string) error { return nil }
	t.Cleanup(func() { verifyCommittedPauseCheckpointFn, cleanPausedRuncFn = origVerify, origClean })
	s := &service{
		sandboxLifecycleLocks: utils.NewResourceLocks(),
		cubeboxMgr: &local{
			cubeboxManger: api,
			config:        &CubeConfig{PausedEvictionEnabled: enabled},
		},
	}
	return s, api
}

func evictReq(id, snap string) *cubebox.UpdateCubeSandboxRequest {
	return &cubebox.UpdateCubeSandboxRequest{
		RequestID: "req-" + id,
		SandboxID: id,
		Annotations: map[string]string{
			constants.MasterAnnotationsUpdateAction:   pausedEvictAction,
			constants.MasterAnnotationPauseSnapshotID: snap,
		},
	}
}

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func committed(context.Context, string, string) (string, error) { return testDigest, nil }

func TestPausedEvictionDisabledByDefault(t *testing.T) {
	require.False(t, (&CubeConfig{}).PausedEvictionEnabled, "zero-value config must not evict")
	sb := pausedS3Sandbox("sb-off", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	verified := false
	s, api := newEvictService(t, sb, false, func(context.Context, string, string) (string, error) {
		verified = true
		return testDigest, nil
	})
	rsp, err := s.Update(context.Background(), evictReq("sb-off", "snap-a"))
	require.NoError(t, err)
	assert.Equal(t, errorcode.ErrorCode_PreConditionFailed, rsp.Ret.RetCode)
	assert.False(t, verified)
	assert.Empty(t, api.deleted)
}

func TestPausedEvictionRefusesIneligibleSandboxes(t *testing.T) {
	now := time.Now().UnixNano()
	cases := map[string]struct {
		sb     *cubeboxstore.CubeBox
		snap   string
		verify func(context.Context, string, string) (string, error)
	}{
		"cubecow backend":       {pausedS3Sandbox("sb", "snap-a", "xfs", cubeboxstore.Status{PausedAt: now}), "snap-a", committed},
		"running":               {pausedS3Sandbox("sb", "snap-a", "s3", cubeboxstore.Status{StartedAt: now}), "snap-a", committed},
		"pausing":               {pausedS3Sandbox("sb", "snap-a", "s3", cubeboxstore.Status{PausingAt: now}), "snap-a", committed},
		"checkpoint mismatch":   {pausedS3Sandbox("sb", "snap-a", "s3", cubeboxstore.Status{PausedAt: now}), "snap-b", committed},
		"no stamped checkpoint": {pausedS3Sandbox("sb", "", "s3", cubeboxstore.Status{PausedAt: now}), "snap-a", committed},
		"uncommitted": {pausedS3Sandbox("sb", "snap-a", "s3", cubeboxstore.Status{PausedAt: now}), "snap-a",
			func(context.Context, string, string) (string, error) { return "", errors.New("checkpoint not sealed") }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, api := newEvictService(t, tc.sb, true, tc.verify)
			rsp, err := s.Update(context.Background(), evictReq("sb", tc.snap))
			require.NoError(t, err)
			assert.Equal(t, errorcode.ErrorCode_PreConditionFailed, rsp.Ret.RetCode, rsp.Ret.RetMsg)
			assert.Empty(t, api.deleted, "ineligible sandbox must keep its local pause state")
			assert.NotNil(t, api.cb)
		})
	}
}

// Eviction removes the PAUSED tombstone, so the node stops accounting any of
// its CPU/memory quota (100% release, independent of the release ratio), and
// reports the committed checkpoint digest for the cross-node gate.
func TestPausedEvictionReleasesFullQuotaAndKeepsCheckpoint(t *testing.T) {
	sb := pausedS3Sandbox("sb-ev", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	var gotSandbox, gotSnap string
	s, api := newEvictService(t, sb, true, func(_ context.Context, sandbox, snap string) (string, error) {
		gotSandbox, gotSnap = sandbox, snap
		return testDigest, nil
	})
	before := aggregateSandboxResources(api.List(), 0)
	require.Equal(t, int64(2000), before.MilliCPU)
	require.Equal(t, int64(4096), before.MemoryMB)

	rsp, err := s.Update(context.Background(), evictReq("sb-ev", "snap-a"))
	require.NoError(t, err)
	require.Equal(t, errorcode.ErrorCode_Success, rsp.Ret.RetCode, rsp.Ret.RetMsg)
	assert.Equal(t, "sb-ev", gotSandbox)
	assert.Equal(t, "snap-a", gotSnap)
	assert.Equal(t, []string{"sb-ev"}, api.deleted)
	assert.Equal(t, testDigest, string(rsp.ExtInfo[pausedEvictDigestKey]))
	var released pausedEvictionReleased
	require.NoError(t, json.Unmarshal(rsp.ExtInfo[pausedEvictReleasedKey], &released))
	assert.Equal(t, pausedEvictionReleased{MilliCPU: 2000, MemoryMB: 4096, StorageMB: 100}, released)

	after := aggregateSandboxResources(api.List(), 0)
	assert.Zero(t, after.MilliCPU)
	assert.Zero(t, after.MemoryMB)
	assert.Zero(t, after.MvmNum)
	// The checkpoint path is read-only by construction: eviction never calls
	// CleanupTemplate or the native object cleanup (no seam was invoked).
}

// Resume Create and eviction take the same per-sandbox lifecycle lock, so an
// eviction that arrives while a resume holds the sandbox waits, then observes
// the post-resume state and refuses.
func TestPausedEvictionSerializedWithResume(t *testing.T) {
	sb := pausedS3Sandbox("sb-race", "snap-a", "s3", cubeboxstore.Status{PausedAt: time.Now().UnixNano()})
	s, api := newEvictService(t, sb, true, committed)

	unlock := s.sandboxLifecycleLocks.Lock("sb-race") // resume Create in flight
	done := make(chan *cubebox.UpdateCubeSandboxResponse, 1)
	go func() {
		rsp, _ := s.Update(context.Background(), evictReq("sb-race", "snap-a"))
		done <- rsp
	}()
	select {
	case <-done:
		t.Fatal("eviction must wait for the in-flight resume")
	case <-time.After(100 * time.Millisecond):
	}
	// The resume completes: the sandbox is running again on this node.
	sb.GetStatus().Update(func(st cubeboxstore.Status) (cubeboxstore.Status, error) {
		st.PausedAt = 0
		st.StartedAt = time.Now().UnixNano()
		return st, nil
	})
	unlock()
	rsp := <-done
	assert.Equal(t, errorcode.ErrorCode_PreConditionFailed, rsp.Ret.RetCode)
	assert.Empty(t, api.deleted)
}
