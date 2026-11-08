// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package evictor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/config"
	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/cubemasterclient"
	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/lifecycle"
	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/registry"
)

var now = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

func pressured() cubemasterclient.PausedEvictionNode {
	return cubemasterclient.PausedEvictionNode{NodeID: "node-a", NodeIP: "10.0.0.1", Healthy: true,
		QuotaMem: 1000, QuotaMemUsage: 950, QuotaCpu: 1000, QuotaCpuUsage: 100, StorageDiskUsagePer: 30}
}

func calm() cubemasterclient.PausedEvictionNode {
	return cubemasterclient.PausedEvictionNode{NodeID: "node-b", NodeIP: "10.0.0.2", Healthy: true,
		QuotaMem: 1000, QuotaMemUsage: 100, QuotaCpu: 1000, QuotaCpuUsage: 100, StorageDiskUsagePer: 30}
}

func binding(id, ip string, eligible bool) cubemasterclient.PausedEvictionBinding {
	return cubemasterclient.PausedEvictionBinding{SandboxID: id, SnapshotID: "snap-" + id, NodeIP: ip,
		Backend: "s3", RemoteStatus: "ready", Status: "READY", Eligible: eligible,
		PausedAtMilli: now.Add(-time.Hour).UnixMilli()}
}

func regWith(entries map[string]int64, pinnedIDs ...string) *registry.Registry {
	reg := registry.New()
	for id, last := range entries {
		meta := lifecycle.SandboxLifecycleMeta{SandboxID: id, InstanceType: "cubebox", AutoPause: true}
		for _, p := range pinnedIDs {
			if p == id {
				meta.TimeoutSeconds = lifecycle.TimeoutSecondsPtr(-1)
			}
		}
		reg.Upsert(meta)
		reg.MergeLastActive(id, last)
	}
	return reg
}

func TestUnderPressureThresholds(t *testing.T) {
	th := DefaultThresholds()
	if UnderPressure(calm(), th) {
		t.Fatal("calm node must not be under pressure")
	}
	if !UnderPressure(pressured(), th) {
		t.Fatal("memory quota at 95% must be pressure")
	}
	cpu := calm()
	cpu.QuotaCpuUsage = 950
	disk := calm()
	disk.StorageDiskUsagePer = 85
	if !UnderPressure(cpu, th) || !UnderPressure(disk, th) {
		t.Fatal("cpu quota and disk thresholds must count as pressure")
	}
}

// LRU among eligible only: pinned, non-s3 / uncommitted (Master-ineligible)
// and CLM-unknown sandboxes are never selected; oldest last-use is first.
func TestSelectCandidatesLRUEligibleOnly(t *testing.T) {
	t0 := now.Add(-48 * time.Hour).UnixMilli()
	reg := regWith(map[string]int64{
		"old":     t0,
		"mid":     t0 + 1000,
		"new":     t0 + 2000,
		"pinned":  t0 - 5000,
		"cubecow": t0 - 4000,
	}, "pinned")
	view := &cubemasterclient.PausedEvictionView{
		Enabled: true,
		Nodes:   []cubemasterclient.PausedEvictionNode{pressured()},
		Bindings: []cubemasterclient.PausedEvictionBinding{
			binding("new", "10.0.0.1", true),
			binding("old", "10.0.0.1", true),
			binding("mid", "10.0.0.1", true),
			binding("pinned", "10.0.0.1", true),
			binding("cubecow", "10.0.0.1", false), // Master: backend/commit ineligible
			binding("unknown", "10.0.0.1", true),  // not in CLM registry
		},
	}
	got := SelectCandidates(view, reg, DefaultThresholds(), 0, now)["10.0.0.1"]
	var ids []string
	for _, c := range got {
		ids = append(ids, c.SandboxID)
	}
	want := []string{"old", "mid", "new"}
	if len(ids) != len(want) {
		t.Fatalf("candidates %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("LRU order %v, want %v", ids, want)
		}
	}
}

func TestSelectCandidatesNoPressureNoEviction(t *testing.T) {
	reg := regWith(map[string]int64{"ancient": now.Add(-365 * 24 * time.Hour).UnixMilli()})
	view := &cubemasterclient.PausedEvictionView{
		Enabled:  true,
		Nodes:    []cubemasterclient.PausedEvictionNode{calm()},
		Bindings: []cubemasterclient.PausedEvictionBinding{binding("ancient", "10.0.0.2", true)},
	}
	if got := SelectCandidates(view, reg, DefaultThresholds(), 0, now); len(got) != 0 {
		t.Fatalf("age alone must never evict, got %v", got)
	}
}

func TestSelectCandidatesRespectsMinPausedAge(t *testing.T) {
	reg := regWith(map[string]int64{"fresh": 1})
	b := binding("fresh", "10.0.0.1", true)
	b.PausedAtMilli = now.Add(-time.Minute).UnixMilli()
	view := &cubemasterclient.PausedEvictionView{Enabled: true,
		Nodes: []cubemasterclient.PausedEvictionNode{pressured()}, Bindings: []cubemasterclient.PausedEvictionBinding{b}}
	if got := SelectCandidates(view, reg, DefaultThresholds(), 10*time.Minute, now); len(got) != 0 {
		t.Fatalf("freshly paused sandbox must be kept, got %v", got)
	}
}

type fakeStore struct {
	mu     sync.Mutex
	states map[string]string
}

func (f *fakeStore) AcquireEvict(_ context.Context, sid string, _ time.Duration) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur := f.states[sid]
	if cur != lifecycle.StatePaused {
		return cur, false, nil
	}
	f.states[sid] = "evicting"
	return cur, true, nil
}

func (f *fakeStore) ReleaseEvict(_ context.Context, sid string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.states[sid] != "evicting" {
		return false, nil
	}
	f.states[sid] = lifecycle.StatePaused
	return true, nil
}

type fakeMaster struct {
	view    *cubemasterclient.PausedEvictionView
	evicted []string
	during  func(sid string)
}

func (f *fakeMaster) PausedEvictionView(context.Context) (*cubemasterclient.PausedEvictionView, error) {
	return f.view, nil
}

func (f *fakeMaster) Evict(_ context.Context, sid, _ string) error {
	if f.during != nil {
		f.during(sid)
	}
	f.evicted = append(f.evicted, sid)
	return nil
}

func newEvictor(store *fakeStore, m *fakeMaster, reg *registry.Registry, enabled bool) *Evictor {
	return New(Options{Enabled: enabled, Registry: reg, Redis: store, CubeMaster: m,
		Thresholds: DefaultThresholds(), MaxPerNodePerTick: 1, StateLockTTL: time.Minute,
		ActionTimeout: time.Second, Now: func() time.Time { return now }})
}

func TestEvictorFlagOffByDefault(t *testing.T) {
	if config.Default().PausedEvictionEnabled {
		t.Fatal("CLM paused eviction must be off by default")
	}
	store := &fakeStore{states: map[string]string{"old": "paused"}}
	m := &fakeMaster{view: &cubemasterclient.PausedEvictionView{Enabled: true,
		Nodes: []cubemasterclient.PausedEvictionNode{pressured()}, Bindings: []cubemasterclient.PausedEvictionBinding{binding("old", "10.0.0.1", true)}}}
	newEvictor(store, m, regWith(map[string]int64{"old": 1}), false).sweepOnce(context.Background())
	if len(m.evicted) != 0 {
		t.Fatal("disabled evictor must not evict")
	}
	// CLM enabled but Master flag off: still nothing.
	m.view.Enabled = false
	newEvictor(store, m, regWith(map[string]int64{"old": 1}), true).sweepOnce(context.Background())
	if len(m.evicted) != 0 {
		t.Fatal("master flag off must veto eviction")
	}
}

func TestEvictorEvictsOldestUnderPressureThrottled(t *testing.T) {
	store := &fakeStore{states: map[string]string{"old": "paused", "new": "paused"}}
	m := &fakeMaster{view: &cubemasterclient.PausedEvictionView{Enabled: true,
		Nodes:    []cubemasterclient.PausedEvictionNode{pressured()},
		Bindings: []cubemasterclient.PausedEvictionBinding{binding("new", "10.0.0.1", true), binding("old", "10.0.0.1", true)}}}
	var during string
	m.during = func(sid string) { during = store.states[sid] }
	e := newEvictor(store, m, regWith(map[string]int64{"old": 100, "new": 200}), true)
	e.sweepOnce(context.Background())
	if len(m.evicted) != 1 || m.evicted[0] != "old" {
		t.Fatalf("want one eviction of the LRU sandbox, got %v", m.evicted)
	}
	if during != "evicting" {
		t.Fatalf("eviction must hold the evicting marker, saw %q", during)
	}
	if store.states["old"] != lifecycle.StatePaused {
		t.Fatalf("marker must resolve back to paused, got %q", store.states["old"])
	}
}

// Concurrent resume and eviction are serialized on the shared state key: a
// sandbox already claimed by a resume (or running) is skipped.
func TestEvictorSkipsSandboxOwnedByResume(t *testing.T) {
	store := &fakeStore{states: map[string]string{"old": "resuming"}}
	m := &fakeMaster{view: &cubemasterclient.PausedEvictionView{Enabled: true,
		Nodes: []cubemasterclient.PausedEvictionNode{pressured()}, Bindings: []cubemasterclient.PausedEvictionBinding{binding("old", "10.0.0.1", true)}}}
	newEvictor(store, m, regWith(map[string]int64{"old": 1}), true).sweepOnce(context.Background())
	if len(m.evicted) != 0 {
		t.Fatalf("eviction must not race an in-flight resume, got %v", m.evicted)
	}
}
