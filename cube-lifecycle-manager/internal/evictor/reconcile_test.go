// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package evictor

import (
	"context"
	"errors"
	"testing"

	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/cubemasterclient"
)

// An unknown-outcome eviction is retried on the next tick even after node
// pressure clears, so Master's evict action can reconcile it. A definite
// refusal or success stops the retry; so does the binding becoming evicted.
func TestEvictorRetriesUnknownOutcomeUntilReconciled(t *testing.T) {
	store := &fakeStore{states: map[string]string{"old": "paused"}}
	m := &fakeMaster{view: &cubemasterclient.PausedEvictionView{Enabled: true,
		Nodes:    []cubemasterclient.PausedEvictionNode{pressured()},
		Bindings: []cubemasterclient.PausedEvictionBinding{binding("old", "10.0.0.1", true)}},
		errs: []error{&cubemasterclient.APIError{RetCode: RetCodeOutcomeUnknown, RetMsg: "outcome unknown"}}}
	e := newEvictor(store, m, regWith(map[string]int64{"old": 1}), true)

	e.sweepOnce(context.Background())
	if len(m.evicted) != 1 || len(e.uncertain) != 1 {
		t.Fatalf("first attempt: evicted=%v uncertain=%v", m.evicted, e.uncertain)
	}
	// Pressure cleared: no new candidates, but the uncertain one is retried.
	m.view.Nodes = []cubemasterclient.PausedEvictionNode{calm()}
	e.sweepOnce(context.Background())
	if len(m.evicted) != 2 || len(e.uncertain) != 0 {
		t.Fatalf("retry must reconcile: evicted=%v uncertain=%v", m.evicted, e.uncertain)
	}
	// Reconciled: no further calls.
	e.sweepOnce(context.Background())
	if len(m.evicted) != 2 {
		t.Fatalf("no more calls after reconcile, got %v", m.evicted)
	}
}

func TestEvictorDropsUncertainOnceBindingEvictedOrDefiniteRefusal(t *testing.T) {
	store := &fakeStore{states: map[string]string{"a": "paused", "b": "paused"}}
	m := &fakeMaster{view: &cubemasterclient.PausedEvictionView{Enabled: true,
		Nodes:    []cubemasterclient.PausedEvictionNode{calm()},
		Bindings: []cubemasterclient.PausedEvictionBinding{binding("a", "10.0.0.2", true), binding("b", "10.0.0.2", true)}},
		errs: []error{&cubemasterclient.APIError{RetCode: 130001, RetMsg: "not evictable"}}}
	e := newEvictor(store, m, regWith(map[string]int64{"a": 1, "b": 2}), true)
	e.uncertain["a"] = Candidate{SandboxID: "a", InstanceType: "cubebox"}
	e.uncertain["b"] = Candidate{SandboxID: "b", InstanceType: "cubebox"}
	// "b" is already evicted per Master: dropped without a call.
	m.view.Bindings[1].Evicted = true
	m.view.Bindings[1].Eligible = false
	e.sweepOnce(context.Background())
	if len(m.evicted) != 1 || m.evicted[0] != "a" {
		t.Fatalf("only a must be retried, got %v", m.evicted)
	}
	if len(e.uncertain) != 0 {
		t.Fatalf("definite refusal and evicted binding must leave no uncertain entries: %v", e.uncertain)
	}
	if !outcomeUnknown(errors.New("connection reset")) {
		t.Fatal("transport errors are unknown outcomes")
	}
}
