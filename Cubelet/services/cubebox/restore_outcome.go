// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
package cubebox

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/oci"
	"golang.org/x/sys/unix"
)

const recoveryRequestAnnotation = "cube.shim.recovery.attempt.v1"
const recoveryReceiptRoot = "/run/cube-recovery"
const recoveryStageLabel = "cube.master.recovery.failure_stage"
const recoveryCauseLabel = "cube.master.recovery.cause"
const recoveryProtocolLabel = "cube.master.recovery.protocol"
const recoveryAttemptLabel = "cube.master.recovery.attempt_id"

type restoreOutcome struct {
	Version          int    `json:"version"`
	SandboxID        string `json:"sandbox_id"`
	AttemptID        string `json:"attempt_id"`
	Mechanism        string `json:"mechanism"`
	FailureStage     string `json:"failure_stage"`
	Cause            string `json:"cause"`
	ProcessState     string `json:"process_state"`
	DiskOnlyFallback string `json:"disk_only_fallback"`
}

// A flat, exact schema rejects duplicate, missing, unknown and case-folded keys.
// No error message is used to classify a cause or authorize a boot.
func decodeRestoreOutcome(raw []byte, sandbox, attempt string) (*restoreOutcome, error) {
	if len(raw) > 4096 {
		return nil, fmt.Errorf("recovery receipt exceeds limit")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	tok, e := d.Token()
	if e != nil || tok != json.Delim('{') {
		return nil, fmt.Errorf("invalid recovery object")
	}
	fields := map[string]json.RawMessage{}
	allowed := map[string]bool{"version": true, "sandbox_id": true, "attempt_id": true, "mechanism": true, "failure_stage": true, "cause": true, "process_state": true, "disk_only_fallback": true}
	for d.More() {
		t, e := d.Token()
		if e != nil {
			return nil, e
		}
		k, ok := t.(string)
		if !ok || !allowed[k] || fields[k] != nil {
			return nil, fmt.Errorf("invalid recovery field")
		}
		var v json.RawMessage
		if e = d.Decode(&v); e != nil {
			return nil, e
		}
		fields[k] = v
	}
	if _, e = d.Token(); e != nil {
		return nil, e
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, fmt.Errorf("trailing recovery data")
	}
	if len(fields) != len(allowed) {
		return nil, fmt.Errorf("missing recovery field")
	}
	var o restoreOutcome
	if e = json.Unmarshal(raw, &o); e != nil {
		return nil, e
	}
	if o.Version != 1 || o.SandboxID != sandbox || o.AttemptID != attempt || sandbox == "" || !validRecoveryAttempt(attempt) || o.ProcessState != "unverified" {
		return nil, fmt.Errorf("recovery version, identity or process state mismatch")
	}
	if o.Mechanism == "memory_restored" && o.FailureStage == "none" && o.Cause == "none" && o.DiskOnlyFallback == "not_used" {
		return &o, nil
	}
	if o.Mechanism != "failed" || o.DiskOnlyFallback != "refused" {
		return nil, fmt.Errorf("invalid recovery mechanism")
	}
	valid := map[string]map[string]bool{
		"unknown": {"unknown": true}, "memory_reference": {"missing_memory_reference": true},
		"metadata": {"unknown": true}, "compatibility": {"configuration_mismatch": true, "restore_disabled": true}, "runtime": {"unknown": true},
	}
	if !valid[o.FailureStage][o.Cause] {
		return nil, fmt.Errorf("invalid recovery classification")
	}
	return &o, nil
}
func validRecoveryAttempt(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

type recoveryReceipt struct {
	dir                    *os.File
	path, sandbox, attempt string
	uid                    uint32
}

func newRecoveryReceipt(sandbox string) (*recoveryReceipt, error) {
	return newRecoveryReceiptAt(recoveryReceiptRoot, sandbox, 0)
}
func privateRecoveryDir(parent int, name string, uid uint32) (*os.File, error) {
	fd, e := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), name)
	var st unix.Stat_t
	if e = unix.Fstat(fd, &st); e != nil || st.Uid != uid || st.Mode&0777 != 0700 {
		f.Close()
		return nil, fmt.Errorf("untrusted recovery directory")
	}
	return f, nil
}
func newRecoveryReceiptAt(root, sandbox string, uid uint32) (*recoveryReceipt, error) {
	if e := os.Mkdir(root, 0700); e != nil && !os.IsExist(e) {
		return nil, e
	}
	base, e := privateRecoveryDir(unix.AT_FDCWD, root, uid)
	if e != nil {
		return nil, e
	}
	defer base.Close()
	var token [32]byte
	if _, e = rand.Read(token[:]); e != nil {
		return nil, e
	}
	attempt := hex.EncodeToString(token[:])
	if e = unix.Mkdirat(int(base.Fd()), attempt, 0700); e != nil {
		return nil, e
	}
	dir, e := privateRecoveryDir(int(base.Fd()), attempt, uid)
	if e != nil {
		return nil, e
	}
	return &recoveryReceipt{dir: dir, path: filepath.Join(root, attempt), sandbox: sandbox, attempt: attempt, uid: uid}, nil
}
func (r *recoveryReceipt) close() {
	if r != nil {
		r.dir.Close()
		os.Remove(filepath.Join(r.path, "outcome.json"))
		os.Remove(filepath.Join(r.path, "outcome.tmp"))
		os.Remove(r.path)
	}
}
func (r *recoveryReceipt) read() (*restoreOutcome, error) {
	fd, e := unix.Openat(int(r.dir.Fd()), "outcome.json", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), "outcome.json")
	defer f.Close()
	var st unix.Stat_t
	if e = unix.Fstat(fd, &st); e != nil || st.Uid != r.uid || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0777 != 0600 || st.Nlink != 1 || st.Size > 4096 {
		return nil, fmt.Errorf("untrusted recovery receipt")
	}
	raw, e := io.ReadAll(io.LimitReader(f, 4097))
	if e != nil {
		return nil, e
	}
	return decodeRestoreOutcome(raw, r.sandbox, r.attempt)
}

// Applied LAST, after all caller/template annotations. Ordinary creates get no
// recovery request. The response is never read from annotations or labels.
func withRecoveryRequest(r *recoveryReceipt) oci.SpecOpts {
	return func(_ context.Context, _ oci.Client, _ *containers.Container, s *oci.Spec) error {
		for k := range s.Annotations {
			if strings.HasPrefix(strings.TrimSpace(k), "cube.shim.recovery.") {
				delete(s.Annotations, k)
			}
		}
		if r != nil {
			if s.Annotations == nil {
				s.Annotations = map[string]string{}
			}
			s.Annotations[recoveryRequestAnnotation] = r.attempt
		}
		return nil
	}
}
