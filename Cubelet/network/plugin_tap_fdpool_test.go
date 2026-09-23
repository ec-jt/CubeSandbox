// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package network

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/tencentcloud/CubeSandbox/Cubelet/internal/tomlext"
	"github.com/tencentcloud/CubeSandbox/Cubelet/network/proto"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/networkagentclient"
)

// fakeTapFdServer emulates network-agent's tap fd socket: it answers each
// request with a JSON envelope and (on success) one SCM_RIGHTS fd. It records
// every request so tests can assert exactly which sandboxes were resolved.
type fakeTapFdServer struct {
	t        *testing.T
	path     string
	ln       *net.UnixListener
	known    map[string]string // tapName -> sandboxID
	ifindex  map[string]int
	requests []networkAgentTapFDRequest
	done     chan struct{}
}

func newFakeTapFdServer(t *testing.T, known map[string]string, ifindex map[string]int) *fakeTapFdServer {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "na-tap.sock")
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeTapFdServer{t: t, path: path, ln: ln, known: known, ifindex: ifindex, done: make(chan struct{})}
	go s.serve()
	t.Cleanup(func() { _ = ln.Close(); <-s.done })
	return s
}

func (s *fakeTapFdServer) serve() {
	defer close(s.done)
	for {
		conn, err := s.ln.AcceptUnix()
		if err != nil {
			return
		}
		func() {
			defer conn.Close()
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			var req networkAgentTapFDRequest
			if err := json.Unmarshal(buf[:n], &req); err != nil {
				_, _, _ = conn.WriteMsgUnix([]byte(`{"errCode":"1001","errMsg":"Parse json failed"}`), nil, nil)
				return
			}
			s.requests = append(s.requests, req)
			owner, ok := s.known[req.Name]
			if !ok || owner != req.SandboxID {
				_, _, _ = conn.WriteMsgUnix([]byte(`{"errCode":"1002","errMsg":"sandbox not found"}`), nil, nil)
				return
			}
			// Any fd works as a stand-in for the tap fd; a pipe end is cheap.
			r, w, err := os.Pipe()
			if err != nil {
				return
			}
			defer r.Close()
			defer w.Close()
			payload := []byte(`{"errCode":"0","errMsg":"Success","ifindex":` + itoa(s.ifindex[req.Name]) + `}`)
			_, _, _ = conn.WriteMsgUnix(payload, syscall.UnixRights(int(r.Fd())), nil)
		}()
	}
}

func itoa(i int) string {
	b, _ := json.Marshal(i)
	return string(b)
}

// listingNetworkAgentClient returns a fixed ListNetworks / GetNetwork view.
type listingNetworkAgentClient struct {
	fakeNetworkAgentClient
	networks []networkagentclient.NetworkState
}

func (c *listingNetworkAgentClient) ListNetworks(context.Context, *networkagentclient.ListNetworksRequest) (*networkagentclient.ListNetworksResponse, error) {
	return &networkagentclient.ListNetworksResponse{Networks: c.networks}, nil
}

func (c *listingNetworkAgentClient) GetNetwork(_ context.Context, req *networkagentclient.GetNetworkRequest) (*networkagentclient.GetNetworkResponse, error) {
	for _, n := range c.networks {
		if n.SandboxID == req.SandboxID {
			return &networkagentclient.GetNetworkResponse{
				SandboxID:       n.SandboxID,
				NetworkHandle:   n.NetworkHandle,
				Interfaces:      []networkagentclient.Interface{{Name: n.TapName}},
				PortMappings:    n.PortMappings,
				PersistMetadata: map[string]string{"sandbox_ip": n.SandboxIP},
			}, nil
		}
	}
	return nil, nil
}

func resetFdPool() {
	Name2MvmNet.Range(func(k, _ any) bool { Name2MvmNet.Delete(k); return true })
}

func newFdPoolLocal(sockPath string, client networkagentclient.Client) *local {
	return &local{
		Config: &Config{
			EnableNetworkAgent:       true,
			NetworkAgentTapSocket:    sockPath,
			NetworkAgentTapFDTimeout: tomlext.FromStdTime(2 * time.Second),
		},
		networkAgentClient: client,
	}
}

func TestLookupTapFileRepairsPoolMissFromNetworkAgent(t *testing.T) {
	resetFdPool()
	t.Cleanup(resetFdPool)

	srv := newFakeTapFdServer(t,
		map[string]string{"z192.168.1.14": "sb-paused"},
		map[string]int{"z192.168.1.14": 351})
	client := &listingNetworkAgentClient{networks: []networkagentclient.NetworkState{
		{SandboxID: "sb-paused", NetworkHandle: "sb-paused", TapName: "z192.168.1.14", TapIfIndex: 351, SandboxIP: "192.168.1.14"},
	}}
	l := newFdPoolLocal(srv.path, client)

	// Simulate the post-restart state: the pool is empty even though the
	// sandbox (and its tap) still exist. This is exactly what made every
	// Cloud Hypervisor vm.restore EBUSY on the tap.
	if _, exist := Name2MvmNet.Load("z192.168.1.14"); exist {
		t.Fatal("precondition: pool must be empty")
	}

	file, ok, mismatch := l.LookupTapFile("z192.168.1.14", "sb-paused")
	if mismatch {
		t.Fatal("unexpected id mismatch")
	}
	if !ok || file == nil {
		t.Fatal("LookupTapFile did not repair the pool miss from network-agent")
	}
	if len(srv.requests) != 1 || srv.requests[0].SandboxID != "sb-paused" || srv.requests[0].Name != "z192.168.1.14" {
		t.Fatalf("network-agent requests = %+v, want exactly one for sb-paused/z192.168.1.14", srv.requests)
	}

	// The repaired entry is now cached: a second lookup is served from memory
	// (no further network-agent round trip) and carries the ifindex.
	file2, ok2, _ := l.LookupTapFile("z192.168.1.14", "sb-paused")
	if !ok2 || file2 != file {
		t.Fatal("second lookup should be served from the cached pool entry")
	}
	if len(srv.requests) != 1 {
		t.Fatalf("second lookup must not hit network-agent again, requests=%d", len(srv.requests))
	}
	m := l.loadNet("sb-paused")
	if m == nil || m.Tap == nil || m.Tap.Index != 351 || m.Tap.IP.String() != "192.168.1.14" {
		t.Fatalf("cached MvmNet incomplete: %+v", m)
	}
}

func TestLookupTapFileRejectsForeignSandbox(t *testing.T) {
	resetFdPool()
	t.Cleanup(resetFdPool)

	srv := newFakeTapFdServer(t, map[string]string{"z10.0.0.2": "owner"}, nil)
	client := &listingNetworkAgentClient{networks: []networkagentclient.NetworkState{
		{SandboxID: "owner", TapName: "z10.0.0.2", SandboxIP: "10.0.0.2"},
	}}
	l := newFdPoolLocal(srv.path, client)

	// Cached entry belongs to a different sandbox: must be rejected without
	// asking network-agent (never hand a foreign tap to a VM).
	l.storeNet(&proto.MvmNet{ID: "owner", Tap: &proto.Tap{Name: "z10.0.0.2"}})
	_, ok, mismatch := l.LookupTapFile("z10.0.0.2", "intruder")
	if ok || !mismatch {
		t.Fatalf("cached foreign tap: ok=%v mismatch=%v, want ok=false mismatch=true", ok, mismatch)
	}
	if len(srv.requests) != 0 {
		t.Fatalf("mismatch on a cached entry must not consult network-agent, requests=%+v", srv.requests)
	}

	// Pool miss and network-agent refuses (unknown sandbox): plain not-found.
	resetFdPool()
	_, ok, mismatch = l.LookupTapFile("z10.0.0.9", "nobody")
	if ok || mismatch {
		t.Fatalf("unknown sandbox: ok=%v mismatch=%v, want both false", ok, mismatch)
	}
}

func TestRecoverTapFdPoolRebuildsFromListNetworks(t *testing.T) {
	resetFdPool()
	t.Cleanup(resetFdPool)

	srv := newFakeTapFdServer(t,
		map[string]string{"z192.168.1.14": "sb-a", "z192.168.1.15": "sb-b"},
		map[string]int{"z192.168.1.14": 351, "z192.168.1.15": 352})
	client := &listingNetworkAgentClient{networks: []networkagentclient.NetworkState{
		{SandboxID: "sb-a", TapName: "z192.168.1.14", TapIfIndex: 351, SandboxIP: "192.168.1.14",
			PortMappings: []networkagentclient.PortMapping{{HostPort: 30001, ContainerPort: 17300}}},
		{SandboxID: "sb-b", TapName: "z192.168.1.15", TapIfIndex: 352, SandboxIP: "192.168.1.15"},
		// Known to network-agent's list but its fd request is refused: must be
		// counted as failed and must not abort recovery of the others.
		{SandboxID: "sb-gone", TapName: "z192.168.1.16", TapIfIndex: 353, SandboxIP: "192.168.1.16"},
		// Malformed entries are skipped silently.
		{SandboxID: "", TapName: "z192.168.1.17"},
	}}
	l := newFdPoolLocal(srv.path, client)

	recovered, failed := l.RecoverTapFdPool(context.Background())
	if recovered != 2 || failed != 1 {
		t.Fatalf("recovered=%d failed=%d, want 2/1", recovered, failed)
	}
	for _, tc := range []struct {
		id, tap string
		idx     int
	}{{"sb-a", "z192.168.1.14", 351}, {"sb-b", "z192.168.1.15", 352}} {
		m := l.loadNet(tc.id)
		if m == nil || m.Tap == nil || m.Tap.File == nil || m.Tap.Index != tc.idx || m.Tap.Name != tc.tap {
			t.Fatalf("%s not recovered into pool: %+v", tc.id, m)
		}
		v, exist := Name2MvmNet.Load(tc.tap)
		if !exist || v.(*proto.MvmNet).ID != tc.id {
			t.Fatalf("Name2MvmNet missing %s -> %s", tc.tap, tc.id)
		}
	}
	if m := l.loadNet("sb-a"); m.Tap.PortMappings[17300] != 30001 {
		t.Fatalf("port mappings not carried over: %+v", m.Tap.PortMappings)
	}
	if l.loadNet("sb-gone") != nil {
		t.Fatal("refused sandbox must not be registered")
	}

	// Idempotent: a second pass finds every entry already cached with an fd.
	before := len(srv.requests)
	recovered, failed = l.RecoverTapFdPool(context.Background())
	if recovered != 0 {
		t.Fatalf("second pass recovered=%d, want 0 (already cached)", recovered)
	}
	// Only the still-failing entry is retried.
	if len(srv.requests)-before != 1 || failed != 1 {
		t.Fatalf("second pass should retry only sb-gone: new requests=%d failed=%d", len(srv.requests)-before, failed)
	}
}

func TestRecoverTapFdPoolNoopWhenNetworkAgentDisabled(t *testing.T) {
	resetFdPool()
	l := &local{Config: &Config{EnableNetworkAgent: false}}
	if r, f := l.RecoverTapFdPool(context.Background()); r != 0 || f != 0 {
		t.Fatalf("disabled agent: recovered=%d failed=%d, want 0/0", r, f)
	}
	var nilLocal *local
	if r, f := nilLocal.RecoverTapFdPool(context.Background()); r != 0 || f != 0 {
		t.Fatalf("nil receiver: recovered=%d failed=%d, want 0/0", r, f)
	}
}
