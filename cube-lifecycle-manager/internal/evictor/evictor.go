// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

// Package evictor is the pressure-driven, least-recently-used eviction loop
// for paused sandboxes whose state is durably committed to S3.
//
// Placement of responsibilities:
//   - CLM (this package) decides WHEN and WHICH: it alone tracks last activity
//     (CubeProxy last_active, resume) and pin state (timeout=-1), and already
//     runs the leader-gated sweeper. It evicts only on node pressure, never on
//     age alone, oldest last-use first, a bounded number per node per tick.
//   - CubeMaster (Update action "evict") holds the per-sandbox lifecycle lock,
//     re-checks binding eligibility (READY, s3, remote_ready, not evicted) and
//     records the eviction plus the committed checkpoint digest.
//   - Cubelet (EvictPausedSandbox) verifies the commit against the native
//     store and drops only local pause state, releasing 100% of the quota.
//
// Resume of an evicted sandbox needs nothing here: CubeMaster's resume path
// sees the evicted binding and restores cross-node from S3 through the
// source-quiesced gate.
package evictor

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/cubemasterclient"
	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/leader"
	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/registry"
)

// Thresholds are fractions (quota) and percentages (disk). A node is under
// pressure when any dimension is at or above its high-water mark. Defaults
// follow plan section 5 (disk ~80%).
type Thresholds struct {
	MemQuotaHigh float64 // used/quota, e.g. 0.85
	CPUQuotaHigh float64 // used/quota, e.g. 0.90
	DiskHighPct  float64 // storage disk usage percent, e.g. 80
}

// DefaultThresholds are conservative starting values; tune from canary data.
func DefaultThresholds() Thresholds {
	return Thresholds{MemQuotaHigh: 0.85, CPUQuotaHigh: 0.90, DiskHighPct: 80}
}

type Options struct {
	Enabled    bool
	Registry   *registry.Registry
	Redis      stateStore
	CubeMaster master
	Thresholds Thresholds
	// MaxPerNodePerTick bounds evictions per node per sweep (throttle; the
	// next tick re-reads node metrics, so no size estimate is needed).
	MaxPerNodePerTick int
	// MinPausedAge protects freshly paused sandboxes (also bounds churn).
	MinPausedAge  time.Duration
	Interval      time.Duration
	StateLockTTL  time.Duration
	ActionTimeout time.Duration
	Now           func() time.Time
	Log           *zap.Logger
	Leader        leader.Status
}

type Evictor struct {
	o       Options
	evicted atomic.Int64
	failed  atomic.Int64
}

func New(o Options) *Evictor {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.MaxPerNodePerTick <= 0 {
		o.MaxPerNodePerTick = 1
	}
	if o.Log == nil {
		o.Log = zap.NewNop()
	}
	return &Evictor{o: o}
}

// Run blocks until ctx is cancelled. When the feature is disabled it only
// waits, so wiring it unconditionally is safe.
func (e *Evictor) Run(ctx context.Context) error {
	if !e.o.Enabled || e.o.Interval <= 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	t := time.NewTicker(e.o.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			e.sweepOnce(ctx)
		}
	}
}

// Stats returns (evicted, failed) counters.
func (e *Evictor) Stats() (int64, int64) { return e.evicted.Load(), e.failed.Load() }

// UnderPressure reports whether a node needs paused capacity back.
func UnderPressure(n cubemasterclient.PausedEvictionNode, th Thresholds) bool {
	if n.QuotaMem > 0 && th.MemQuotaHigh > 0 && float64(n.QuotaMemUsage)/float64(n.QuotaMem) >= th.MemQuotaHigh {
		return true
	}
	if n.QuotaCpu > 0 && th.CPUQuotaHigh > 0 && float64(n.QuotaCpuUsage)/float64(n.QuotaCpu) >= th.CPUQuotaHigh {
		return true
	}
	return th.DiskHighPct > 0 && n.StorageDiskUsagePer >= th.DiskHighPct
}

// Candidate is one eviction choice with its LRU key.
type Candidate struct {
	SandboxID    string
	InstanceType string
	LastUsedMs   int64
}

// pinned is CLM's pin signal: timeout -1 (never idle-expire). dc-danus pins
// a session by setting an infinite timeout, and CubeMaster resume with
// timeout=-1 does the same.
func pinned(e *registry.Entry) bool {
	return e != nil && e.Meta.TimeoutSeconds != nil && *e.Meta.TimeoutSeconds < 0
}

// lastUsedMs is the activity baseline: max(proxy last_active, creation).
// Resume and user traffic bump LastActiveMs, so both count as activity. The
// pause binding's updated_at is only a fallback when no activity is known: it
// also moves on remote_status reconciliation, so it must not mask real order.
func lastUsedMs(e *registry.Entry, pausedAtMs int64) int64 {
	var v int64
	if e != nil {
		v = e.LastActiveMs
		if e.Meta.CreatedAt > v {
			v = e.Meta.CreatedAt
		}
	}
	if v == 0 {
		v = pausedAtMs
	}
	return v
}

// SelectCandidates returns, per node under pressure, eligible paused sandboxes
// ordered oldest last-use first. Eligibility = Master-eligible binding (READY,
// s3, remote_ready, not evicted), known to CLM, not pinned, paused long enough.
// Nodes not under pressure contribute nothing (no age-only eviction).
func SelectCandidates(view *cubemasterclient.PausedEvictionView, reg *registry.Registry, th Thresholds,
	minPausedAge time.Duration, now time.Time) map[string][]Candidate {
	out := map[string][]Candidate{}
	if view == nil {
		return out
	}
	pressured := map[string]bool{}
	for _, n := range view.Nodes {
		if UnderPressure(n, th) {
			if n.NodeIP != "" {
				pressured[n.NodeIP] = true
			}
			if n.NodeID != "" {
				pressured[n.NodeID] = true
			}
		}
	}
	for _, b := range view.Bindings {
		if !b.Eligible || b.Evicted {
			continue
		}
		key := b.NodeIP
		if !pressured[key] {
			key = b.NodeID
			if !pressured[key] {
				continue
			}
		}
		if minPausedAge > 0 && b.PausedAtMilli > 0 && now.Sub(time.UnixMilli(b.PausedAtMilli)) < minPausedAge {
			continue
		}
		var entry *registry.Entry
		if reg != nil {
			entry = reg.Get(b.SandboxID)
		}
		if entry == nil || pinned(entry) {
			// Unknown to CLM means unknown activity and pin state: keep it.
			continue
		}
		it := b.InstanceType
		if it == "" {
			it = entry.Meta.InstanceType
		}
		out[key] = append(out[key], Candidate{SandboxID: b.SandboxID, InstanceType: it, LastUsedMs: lastUsedMs(entry, b.PausedAtMilli)})
	}
	for node := range out {
		c := out[node]
		sort.SliceStable(c, func(i, j int) bool {
			if c[i].LastUsedMs != c[j].LastUsedMs {
				return c[i].LastUsedMs < c[j].LastUsedMs
			}
			return c[i].SandboxID < c[j].SandboxID
		})
	}
	return out
}

func (e *Evictor) sweepOnce(ctx context.Context) {
	if !e.o.Enabled {
		return
	}
	if e.o.Leader != nil && !e.o.Leader.IsLeader() {
		return
	}
	view, err := e.o.CubeMaster.PausedEvictionView(ctx)
	if err != nil {
		e.o.Log.Warn("paused eviction view failed", zap.Error(err))
		return
	}
	if !view.Enabled {
		// Master flag off: never evict, whatever CLM is configured to do.
		return
	}
	nodes := make([]string, 0)
	cands := SelectCandidates(view, e.o.Registry, e.o.Thresholds, e.o.MinPausedAge, e.o.Now())
	for n := range cands {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	for _, n := range nodes {
		done := 0
		for _, c := range cands[n] {
			if done >= e.o.MaxPerNodePerTick {
				break
			}
			if e.tryEvict(ctx, c) {
				done++
			}
		}
	}
}

// tryEvict claims the sandbox's lifecycle state (paused -> evicting) so a
// concurrent CLM resume waits instead of racing, then asks CubeMaster to
// evict (which also takes the Master per-sandbox lock shared with resume).
// The marker always returns to paused: an evicted sandbox is still paused
// to the dataplane, and its next resume goes cross-node.
func (e *Evictor) tryEvict(ctx context.Context, c Candidate) bool {
	ctx, cancel := context.WithTimeout(ctx, e.o.ActionTimeout)
	defer cancel()
	_, got, err := e.o.Redis.AcquireEvict(ctx, c.SandboxID, e.o.StateLockTTL)
	if err != nil || !got {
		return false
	}
	evictErr := e.o.CubeMaster.Evict(ctx, c.SandboxID, c.InstanceType)
	release, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancelRelease()
	if _, err := e.o.Redis.ReleaseEvict(release, c.SandboxID); err != nil {
		e.o.Log.Warn("release evicting marker failed", zap.String("sandbox_id", c.SandboxID), zap.Error(err))
	}
	if evictErr != nil {
		e.failed.Add(1)
		var apiErr *cubemasterclient.APIError
		if errors.As(evictErr, &apiErr) {
			e.o.Log.Info("paused eviction refused", zap.String("sandbox_id", c.SandboxID),
				zap.Int("ret_code", apiErr.RetCode), zap.String("ret_msg", apiErr.RetMsg))
		} else {
			e.o.Log.Warn("paused eviction failed", zap.String("sandbox_id", c.SandboxID), zap.Error(evictErr))
		}
		return false
	}
	e.evicted.Add(1)
	e.o.Log.Info("evicted paused sandbox", zap.String("sandbox_id", c.SandboxID), zap.Int64("last_used_ms", c.LastUsedMs))
	return true
}
