// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"bytes"
	"context"
	"errors"
	"expvar"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/log"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/cube/internals/cubes"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
)

// Startup recovery of the paused-eviction crash window.
//
// evictPausedSandbox runs, under the sandbox lifecycle lock:
//
//  1. verify the committed checkpoint;
//  2. stage the eviction record (written + fsynced, unpublished);
//  3. drop the PAUSED tombstone;
//  4. publish the record (rename + directory fsync).
//
// A crash between 3 and 4 leaves no tombstone and no record: Master cannot
// mark the binding evicted and the origin restore is impossible. A crash
// between 2 and 3 leaves a stage next to a still-paused sandbox. Recovery
// resolves every stage left behind, once at startup before lifecycle RPCs are
// served, under the same per-sandbox lifecycle lock:
//
//   - tombstone present: the drop never happened. Abort (delete the stage);
//     the sandbox stays a normal paused sandbox, resumable on this node.
//   - tombstone gone, stage valid, bound to this node, and the checkpoint
//     commit still verifies byte-for-byte: publish exactly like step 4.
//   - tombstone gone, anything else: keep the stage, never publish, log at
//     error, expose it (unresolved status + expvar) and block origin restore
//     of the sandbox. A failed commit verification (storage not ready) is
//     retried in the background; validation failures are permanent.
//
// Every action is idempotent and crash-safe: publish is a rename (a crash
// before it re-runs, after it there is no stage), abort/discard is an unlink.
//
// Last resort (design note, not implemented): when the record cannot be
// recovered (corrupt stage, commit mismatch, origin node lost) the committed
// S3 checkpoint is still intact, so the session is restorable on any node.
// There is no per-session admin override of the source-quiesced gate today;
// the only bypass is the global CUBE_CROSSNODE_REQUIRE_SOURCE_QUIESCED=false
// (default off). A future override must be explicit and admin-only: an
// operator-signed receipt naming sandbox, checkpoint and digest, persisted by
// Master with the eviction binding and validated by the gate in place of the
// origin record, never inferred from a missing record. The origin keeps
// refusing restore of the unresolved sandbox until an operator removes the
// kept stage under <root>-paused-evictions.

// evictionRecoveryRetryInterval and evictionRecoveryRetryMax bound the
// background retry of stages whose commit verification failed transiently.
var (
	evictionRecoveryRetryInterval = 30 * time.Second
	evictionRecoveryRetryMax      = 120
)

type evictionRecoveryReport struct {
	Published  []string          // sandbox ids
	Aborted    []string          // sandbox ids (tombstone still present)
	Discarded  []string          // stage names (corrupt, tombstone present)
	Unresolved map[string]string // sandbox key -> reason
	// Retry is true when at least one unresolved stage failed only because
	// the committed checkpoint could not be verified right now.
	Retry bool
}

// recoverStagedEvictions resolves the staged eviction records of the
// workflow.EvictionRecords store. nodeID is this node's source fence identity.
func recoverStagedEvictions(ctx context.Context, api cubes.CubeboxAPI, locks *utils.ResourceLocks, nodeID string) (evictionRecoveryReport, error) {
	rep := evictionRecoveryReport{Unresolved: map[string]string{}}
	store := workflow.EvictionRecords
	if store == nil {
		return rep, nil
	}
	staged, err := store.StagedEvictions()
	if err != nil {
		return rep, fmt.Errorf("list staged eviction records: %w", err)
	}
	if len(staged) == 0 {
		store.SetUnresolvedEvictions(nil)
		return rep, nil
	}
	tombstones := map[string]*cubeboxstore.CubeBox{}
	for _, cb := range api.List() {
		if cb != nil && cb.ID != "" {
			tombstones[sourcefence.SandboxKey(cb.ID)] = cb
		}
	}
	nodeID = strings.TrimSpace(nodeID)
	for _, st := range staged {
		key := st.SandboxKey
		if key == "" && st.Result != nil {
			key = sourcefence.SandboxKey(st.Result.Binding.SandboxID)
		}
		if key == "" {
			// Legacy unkeyed and unreadable: the sandbox is unknown, so
			// neither the tombstone check nor publish is possible.
			rep.unresolved(ctx, st.Name, "unkeyed staged record is unreadable: "+errString(st.Err), false)
			continue
		}
		sandboxID := ""
		if cb := tombstones[key]; cb != nil {
			sandboxID = cb.ID
		} else if st.Result != nil {
			sandboxID = st.Result.Binding.SandboxID
		}
		if sandboxID == "" {
			// Corrupt or truncated stage and no tombstone: the drop may have
			// happened, so the proof is lost. Never publish; surface it.
			rep.unresolved(ctx, key, "staged record unreadable and paused tombstone gone: "+errString(st.Err), false)
			continue
		}
		rep.resolveOne(ctx, api, locks, store, st, key, sandboxID, nodeID)
	}
	store.SetUnresolvedEvictions(rep.Unresolved)
	return rep, nil
}

func (rep *evictionRecoveryReport) resolveOne(ctx context.Context, api cubes.CubeboxAPI, locks *utils.ResourceLocks,
	store *sourcefence.Store, st sourcefence.StagedEviction, key, sandboxID, nodeID string) {
	unlock := locks.Lock(sandboxID)
	defer unlock()
	cb, err := api.Get(ctx, sandboxID)
	switch {
	case err == nil && cb != nil:
		// Step 3 never ran (it runs under this lock): abort the eviction.
		if err := store.DiscardStaged(st.Name); err != nil {
			rep.unresolved(ctx, key, "abort staged record: "+err.Error(), true)
			return
		}
		if st.Result == nil {
			rep.Discarded = append(rep.Discarded, st.Name)
			log.G(ctx).Warnf("paused eviction recovery: discarded unreadable staged record %s of still-present sandbox %s: %v",
				st.Name, sandboxID, st.Err)
		} else {
			rep.Aborted = append(rep.Aborted, sandboxID)
			log.G(ctx).Infof("paused eviction recovery: aborted staged eviction of %s (tombstone still present, checkpoint %s)",
				sandboxID, st.Result.Binding.CheckpointID)
		}
		return
	case err != nil && !errors.Is(err, utils.ErrorKeyNotFound):
		rep.unresolved(ctx, key, "tombstone lookup: "+err.Error(), true)
		return
	}
	// Tombstone gone: the eviction got past the drop.
	if st.Result == nil {
		rep.unresolved(ctx, key, "staged record invalid and paused tombstone gone: "+errString(st.Err), false)
		return
	}
	b := st.Result.Binding
	if nodeID == "" || b.NodeID != nodeID {
		rep.unresolved(ctx, key, fmt.Sprintf("staged record binds node %q, this node is %q", b.NodeID, nodeID), false)
		return
	}
	if b.SandboxID != sandboxID || sourcefence.SandboxKey(b.SandboxID) != key {
		rep.unresolved(ctx, key, "staged record binds another sandbox", false)
		return
	}
	digest, commit, err := verifyCommittedPauseCheckpointFn(ctx, b.SandboxID, b.CheckpointID)
	if err != nil {
		rep.unresolved(ctx, key, "committed checkpoint not verifiable now: "+err.Error(), true)
		return
	}
	if digest != b.CheckpointDigest || !bytes.Equal(commit, sourcefence.EvictedCommit(st.Result)) {
		rep.unresolved(ctx, key, "committed checkpoint no longer matches the staged record", false)
		return
	}
	if _, err := store.PublishStaged(st); err != nil {
		rep.unresolved(ctx, key, "publish staged record: "+err.Error(), true)
		return
	}
	rep.Published = append(rep.Published, b.SandboxID)
	log.G(ctx).Infof("paused eviction recovery: published eviction record of %s checkpoint=%s digest=%s operation=%s",
		b.SandboxID, b.CheckpointID, b.CheckpointDigest, b.OperationID)
}

func (rep *evictionRecoveryReport) unresolved(ctx context.Context, key, reason string, retry bool) {
	if prev, ok := rep.Unresolved[key]; ok {
		reason = prev + "; " + reason
	}
	rep.Unresolved[key] = reason
	rep.Retry = rep.Retry || retry
	log.G(ctx).Errorf("paused eviction recovery: staged eviction record %s left unpublished: %s", key, reason)
}

func errString(err error) string {
	if err == nil {
		return "unknown"
	}
	return err.Error()
}

var publishEvictionStatusOnce sync.Once

// publishEvictionRecoveryStatus exposes the unresolved staged evictions at
// /debug/vars as "cubelet_paused_eviction_unresolved" (sandbox key -> reason).
func publishEvictionRecoveryStatus() {
	publishEvictionStatusOnce.Do(func() {
		expvar.Publish("cubelet_paused_eviction_unresolved", expvar.Func(func() any {
			m := workflow.EvictionRecords.UnresolvedEvictions()
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			out := make([]map[string]string, 0, len(keys))
			for _, k := range keys {
				out = append(out, map[string]string{"sandbox_key": k, "reason": m[k]})
			}
			return out
		}))
	})
}

// startEvictionRecovery runs the synchronous startup pass and, when a stage
// failed only transiently, a bounded background retry under the same locks.
func startEvictionRecovery(ctx context.Context, api cubes.CubeboxAPI, nodeID string) error {
	publishEvictionRecoveryStatus()
	rep, err := recoverStagedEvictions(ctx, api, workflow.SandboxLifecycleLocks, nodeID)
	if err != nil {
		return err
	}
	if rep.Retry {
		go func() {
			for i := 0; i < evictionRecoveryRetryMax; i++ {
				time.Sleep(evictionRecoveryRetryInterval)
				r, err := recoverStagedEvictions(ctx, api, workflow.SandboxLifecycleLocks, nodeID)
				if err != nil {
					log.G(ctx).Errorf("paused eviction recovery retry: %v", err)
					continue
				}
				if !r.Retry {
					return
				}
			}
		}()
	}
	return nil
}
