// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

// Package restoreprogress finds the node running a cross-node restore and
// reads its progress from that node's Cubelet
// (GET :<cubelet http port>/v1/storage/restore-progress?sandbox_id=<id>).
//
// Master records the target of every cross-node resume when it sends the
// Create (Record). A lookup asks that node first; when Master has no record
// (restarted mid-resume, or a create from snapshot that went through another
// path) it asks every known node and returns the first that has one.
package restoreprogress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CubeletHTTPPortEnv overrides the Cubelet HTTP port ([http] address).
const CubeletHTTPPortEnv = "CUBE_MASTER_CUBELET_HTTP_PORT"

const defaultCubeletHTTPPort = 9998

// cubeletPath is the Cubelet endpoint (Cubelet storage.RestoreProgressPath).
const cubeletPath = "/v1/storage/restore-progress"

// ErrNotFound means no node reports a restore of the sandbox.
var ErrNotFound = errors.New("no restore in progress")

// recordTTL bounds how long a recorded target is trusted. It covers the
// resume budget (cube-api 360 s, Master resume lock 420 s) plus the time a
// finished restore stays readable on Cubelet.
var recordTTL = 20 * time.Minute

var (
	mu      sync.Mutex
	targets = map[string]target{}
	now     = time.Now
)

type target struct {
	ip string
	at time.Time
}

// Record notes that sandboxID is being restored on nodeIP.
func Record(sandboxID, nodeIP string) {
	sandboxID, nodeIP = strings.TrimSpace(sandboxID), strings.TrimSpace(nodeIP)
	if sandboxID == "" || nodeIP == "" {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	t := now()
	for id, v := range targets {
		if t.Sub(v.at) > recordTTL {
			delete(targets, id)
		}
	}
	targets[sandboxID] = target{ip: nodeIP, at: t}
}

func recorded(sandboxID string) string {
	mu.Lock()
	defer mu.Unlock()
	v, ok := targets[sandboxID]
	if !ok || now().Sub(v.at) > recordTTL {
		return ""
	}
	return v.ip
}

// Finder looks the progress up. Nodes lists candidate node IPs for the
// fan-out; Client and Port are overridable for tests.
type Finder struct {
	Nodes  func() []string
	Client *http.Client
	Port   int
}

func cubeletHTTPPort() int {
	if v := strings.TrimSpace(os.Getenv(CubeletHTTPPortEnv)); v != "" {
		if p, err := strconv.Atoi(v); err == nil && p > 0 && p < 65536 {
			return p
		}
	}
	return defaultCubeletHTTPPort
}

// Find returns the progress document of sandboxID with "node_ip" added.
func (f *Finder) Find(ctx context.Context, sandboxID string) (map[string]any, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return nil, ErrNotFound
	}
	port := f.Port
	if port <= 0 {
		port = cubeletHTTPPort()
	}
	client := f.Client
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	tried := map[string]bool{}
	if ip := recorded(sandboxID); ip != "" {
		tried[ip] = true
		doc, err := query(ctx, client, ip, port, sandboxID)
		if err == nil {
			return doc, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	var nodes []string
	if f.Nodes != nil {
		nodes = f.Nodes()
	}
	type result struct {
		doc map[string]any
		err error
	}
	ch := make(chan result, len(nodes))
	n := 0
	for _, ip := range nodes {
		ip = strings.TrimSpace(ip)
		if ip == "" || tried[ip] {
			continue
		}
		tried[ip] = true
		n++
		go func(ip string) {
			doc, err := query(ctx, client, ip, port, sandboxID)
			ch <- result{doc, err}
		}(ip)
	}
	var lastErr error
	for i := 0; i < n; i++ {
		r := <-ch
		if r.err == nil {
			return r.doc, nil
		}
		if !errors.Is(r.err, ErrNotFound) {
			lastErr = r.err
		}
	}
	if lastErr != nil && n > 0 && len(tried) == 1 {
		return nil, lastErr
	}
	return nil, ErrNotFound
}

func query(ctx context.Context, client *http.Client, ip string, port int, sandboxID string) (map[string]any, error) {
	u := url.URL{
		Scheme:   "http",
		Host:     net.JoinHostPort(ip, strconv.Itoa(port)),
		Path:     cubeletPath,
		RawQuery: url.Values{"sandbox_id": {sandboxID}}.Encode(),
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	rsp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("restore progress from %s: %w", ip, err)
	}
	defer rsp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(rsp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("restore progress from %s: %w", ip, err)
	}
	switch rsp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("restore progress from %s: HTTP %d", ip, rsp.StatusCode)
	}
	doc := map[string]any{}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("restore progress from %s: %w", ip, err)
	}
	doc["node_ip"] = ip
	return doc, nil
}
