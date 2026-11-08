// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package resumer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/lifecycle"
	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/registry"
)

// A resume that arrives while LRU eviction holds the sandbox waits for the
// marker to clear and then drives exactly one resume RPC (which CubeMaster
// places cross-node when the sandbox was evicted). It never resumes mid-evict.
func TestResumeWaitsForInFlightEviction(t *testing.T) {
	reg := registry.New()
	reg.Upsert(lifecycle.SandboxLifecycleMeta{SandboxID: "sbx", InstanceType: "cubebox", AutoResume: true})
	store := newFakeStore()
	store.states["sbx"] = "evicting"
	master := &fakeMaster{}
	r := newTestResumer(reg, store, master, &fakePush{})

	done := make(chan error, 1)
	go func() { done <- r.Resume(context.Background(), "sbx") }()
	select {
	case err := <-done:
		t.Fatalf("resume must wait for the eviction, returned %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	if got := atomic.LoadInt32(&master.calls); got != 0 {
		t.Fatalf("no resume RPC while evicting, got %d", got)
	}
	// Eviction completes: marker returns to paused.
	store.mu.Lock()
	store.states["sbx"] = lifecycle.StatePaused
	store.mu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("resume after eviction: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resume did not proceed after eviction finished")
	}
	if got := atomic.LoadInt32(&master.calls); got != 1 {
		t.Fatalf("want exactly one resume RPC, got %d", got)
	}
	if got := store.state("sbx"); got != "running" {
		t.Fatalf("want running, got %q", got)
	}
}
