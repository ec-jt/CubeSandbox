// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"testing"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
)

func peerAnn(peer string) map[string]string {
	return map[string]string{
		constants.MasterAnnotationSnapshotCrossNode:   "true",
		constants.MasterAnnotationStorageBackend:      "s3",
		constants.MasterAnnotationDesiredSandboxID:    "sb-peer",
		constants.MasterAnnotationSnapshotRemoteUUIDs: `{"rootfs":"r","metadata":"m","memory":"x"}`,
		constants.MasterAnnotationSnapshotSourcePeer:  peer,
	}
}

func TestCrossNodeSandboxImportCarriesSourcePeer(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"10.0.1.1", "10.0.1.1"},
		{" 10.0.1.2:17310 ", "10.0.1.2:17310"},
		{"", ""},
		{"0.0.0.0", ""},
		{"127.0.0.1", ""},
		{"10.0.1.1:", ""},
		{"10.0.1.1:abc", ""},
		{"app1", ""},
		{"::1", ""},
	} {
		imp := CrossNodeSandboxImport(peerAnn(tc.in))
		if imp == nil {
			t.Fatalf("%q: no import", tc.in)
		}
		if imp.Peer != tc.want {
			t.Errorf("%q: peer %q, want %q", tc.in, imp.Peer, tc.want)
		}
	}
}

type recImporter struct {
	plain, peer []string
}

func (r *recImporter) ImportLvol(name, uuid string) (string, error) {
	r.plain = append(r.plain, name)
	return "/dev/x", nil
}

func (r *recImporter) ImportLvolPeer(name, uuid, peer string) (string, error) {
	r.peer = append(r.peer, name+"@"+peer)
	return "/dev/x", nil
}

type plainImporter struct{ calls int }

func (p *plainImporter) ImportLvol(string, string) (string, error) { p.calls++; return "", nil }

func TestWithPeerRoutesOnlyWhenSetAndSupported(t *testing.T) {
	r := &recImporter{}
	_, _ = withPeer(r, "10.0.1.1").ImportLvol("a", "u")
	_, _ = withPeer(r, "").ImportLvol("b", "u")
	if len(r.peer) != 1 || r.peer[0] != "a@10.0.1.1" {
		t.Fatalf("peer calls %v", r.peer)
	}
	if len(r.plain) != 1 || r.plain[0] != "b" {
		t.Fatalf("plain calls %v", r.plain)
	}
	// An engine without peer support keeps working, S3 only.
	p := &plainImporter{}
	_, _ = withPeer(p, "10.0.1.1").ImportLvol("c", "u")
	if p.calls != 1 {
		t.Fatalf("plain importer not used")
	}
}
