// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package restoreprogress

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// fakeCubelet serves the Cubelet endpoint on 127.0.0.<n> so several "nodes"
// can share one port.
func fakeCubelet(t *testing.T, ip string, port int, has map[string]bool, hits *int32) {
	t.Helper()
	l, err := net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(port)))
	if err != nil {
		t.Skipf("cannot listen on %s: %v", ip, err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(hits, 1)
		if r.URL.Path != cubeletPath {
			http.NotFound(w, r)
			return
		}
		id := r.URL.Query().Get("sandbox_id")
		if !has[id] {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"sandbox_id": id, "phase": "importing", "local_chunks": 3, "total_chunks": 9})
	})}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func reset(t *testing.T) {
	mu.Lock()
	targets = map[string]target{}
	mu.Unlock()
	prev := now
	t.Cleanup(func() { now = prev })
}

func TestFindUsesRecordedTargetFirst(t *testing.T) {
	reset(t)
	port := freePort(t)
	var hitsA, hitsB int32
	fakeCubelet(t, "127.0.0.2", port, map[string]bool{"sb1": true}, &hitsA)
	fakeCubelet(t, "127.0.0.3", port, map[string]bool{}, &hitsB)
	Record("sb1", "127.0.0.2")
	f := &Finder{Port: port, Nodes: func() []string { return []string{"127.0.0.3", "127.0.0.2"} }}
	doc, err := f.Find(context.Background(), "sb1")
	if err != nil {
		t.Fatal(err)
	}
	if doc["node_ip"] != "127.0.0.2" || doc["phase"] != "importing" {
		t.Fatalf("unexpected doc %v", doc)
	}
	if atomic.LoadInt32(&hitsB) != 0 {
		t.Fatalf("fan-out ran although the recorded target answered")
	}
}

func TestFindFansOutWithoutRecord(t *testing.T) {
	reset(t)
	port := freePort(t)
	var hitsA, hitsB int32
	fakeCubelet(t, "127.0.0.2", port, map[string]bool{}, &hitsA)
	fakeCubelet(t, "127.0.0.3", port, map[string]bool{"sb2": true}, &hitsB)
	f := &Finder{Port: port, Nodes: func() []string { return []string{"127.0.0.2", "127.0.0.3"} }}
	doc, err := f.Find(context.Background(), "sb2")
	if err != nil {
		t.Fatal(err)
	}
	if doc["node_ip"] != "127.0.0.3" {
		t.Fatalf("unexpected node %v", doc["node_ip"])
	}
}

func TestFindNotFoundAnywhere(t *testing.T) {
	reset(t)
	port := freePort(t)
	var hits int32
	fakeCubelet(t, "127.0.0.2", port, map[string]bool{}, &hits)
	f := &Finder{Port: port, Nodes: func() []string { return []string{"127.0.0.2", "127.0.0.9"} }}
	if _, err := f.Find(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if _, err := f.Find(context.Background(), ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty id: want ErrNotFound, got %v", err)
	}
}

func TestRecordExpires(t *testing.T) {
	reset(t)
	base := time.Now()
	now = func() time.Time { return base }
	Record("sb1", "10.0.1.2")
	if recorded("sb1") != "10.0.1.2" {
		t.Fatal("record missing")
	}
	now = func() time.Time { return base.Add(recordTTL + time.Second) }
	if recorded("sb1") != "" {
		t.Fatal("record must expire")
	}
}

func TestCubeletHTTPPortEnv(t *testing.T) {
	t.Setenv(CubeletHTTPPortEnv, "")
	if cubeletHTTPPort() != defaultCubeletHTTPPort {
		t.Fatal("default port")
	}
	t.Setenv(CubeletHTTPPortEnv, "19998")
	if cubeletHTTPPort() != 19998 {
		t.Fatal("env port")
	}
	t.Setenv(CubeletHTTPPortEnv, "x")
	if cubeletHTTPPort() != defaultCubeletHTTPPort {
		t.Fatal("bad env falls back")
	}
	_ = httptest.NewRecorder
}
