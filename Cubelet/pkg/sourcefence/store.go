// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

// Package sourcefence implements a node-local, non-expiring recovery deny gate.
// It is not a distributed lease and never authorizes destination takeover.
package sourcefence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrFenced           = errors.New("source recovery fence denies mutation")
	ErrDisabled         = errors.New("source fencing admission disabled")
	ErrBinding          = errors.New("source recovery fence binding mismatch")
	ErrDrainUnsupported = errors.New("runtime stopped, but native storage I/O drain is unproven; destination recovery unsupported")
)

// Binding is immutable, including the exact selected checkpoint and operation.
// Incarnation is derived from authoritative local metadata, never cached STATE.
type Binding struct {
	OperationID      string `json:"operation_id"`
	SandboxID        string `json:"sandbox_id"`
	NodeID           string `json:"node_id"`
	Incarnation      string `json:"incarnation"`
	CheckpointID     string `json:"checkpoint_id"`
	CheckpointDigest string `json:"checkpoint_digest"`
}

func (b Binding) Validate() error {
	for _, v := range []string{b.OperationID, b.SandboxID, b.NodeID, b.Incarnation, b.CheckpointID} {
		if v == "" || v != strings.TrimSpace(v) || len(v) > 256 || strings.ContainsAny(v, "\x00\r\n") {
			return ErrBinding
		}
	}
	d, err := hex.DecodeString(b.CheckpointDigest)
	if err != nil || len(d) != 32 || b.CheckpointDigest != strings.ToLower(b.CheckpointDigest) {
		return ErrBinding
	}
	return nil
}

type Process struct {
	PID        int    `json:"pid"`
	StartTime  string `json:"start_time"`
	BootID     string `json:"boot_id"`
	Executable string `json:"executable"`
}

type Record struct {
	Version   int       `json:"version"`
	Binding   Binding   `json:"binding"`
	Stage     string    `json:"stage"`
	Processes []Process `json:"processes"`
}

const Admitted = "admitted"
const RuntimeStopped = "runtime-stopped-storage-unproven"

// Barrier serializes admission with package-wide writers/cleanup. Sandbox
// mutations also hold the existing lifecycle mutex. Admission uses TryLock
// and releases/retries its lifecycle lock on contention, avoiding inversion
// with package operations. The durable gate has no timeout/lease expiry.
var Barrier sync.RWMutex

type Store struct{ root string }

func Open(root string) (*Store, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("source fence root must be absolute")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	fi, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("invalid fence root")
	}
	if err = syncDir(filepath.Dir(root)); err != nil {
		return nil, err
	}
	return &Store{root: root}, nil
}

func (s *Store) filename(id string) string {
	h := sha256.Sum256([]byte(id))
	return filepath.Join(s.root, hex.EncodeToString(h[:])+".json")
}

// Check always enforces existing records, including malformed records, even
// when new admission is disabled. No cache, TTL, unlock, or delete API exists.
func (s *Store) Check(id string) error {
	if s == nil {
		return nil
	} // Unit fixtures only; production initializes before recovery.
	if _, err := os.Lstat(s.root); err != nil {
		return fmt.Errorf("%w: durable directory unavailable: %v", ErrFenced, err)
	}
	_, err := os.Lstat(s.filename(id))
	if errors.Is(err, os.ErrNotExist) {
		// A crash after native binding but before runtime admission still denies
		// mutation. Never treat an orphaned or corrupt sidecar as permission.
		_, err = os.Lstat(s.filename(id) + ".storage-v1")
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", ErrFenced, id)
}

// Any conservatively denies package cleanup while any fence exists. Corrupt
// or unrecognized files also deny cleanup; no selected reference is removed.
func (s *Store) Any() error {
	if s == nil {
		return nil
	}
	es, err := os.ReadDir(s.root)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrFenced, err)
	}
	if len(es) != 0 {
		return ErrFenced
	}
	return nil
}

func (s *Store) load(id string) (*Record, error) {
	fi, err := os.Lstat(s.filename(id))
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() || fi.Size() > 65536 {
		return nil, fmt.Errorf("%w: invalid record", ErrFenced)
	}
	b, err := os.ReadFile(s.filename(id))
	if err != nil {
		return nil, err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	var r Record
	if err = d.Decode(&r); err != nil {
		return nil, fmt.Errorf("%w: corrupt record: %v", ErrFenced, err)
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, fmt.Errorf("%w: trailing record", ErrFenced)
	}
	if r.Version != 1 || r.Binding.SandboxID != id || r.Binding.Validate() != nil || (r.Stage != Admitted && r.Stage != RuntimeStopped) || len(r.Processes) < 1 || len(r.Processes) > 2 {
		return nil, fmt.Errorf("%w: invalid record", ErrFenced)
	}
	for _, p := range r.Processes {
		if p.PID <= 1 || p.StartTime == "" || p.BootID == "" || p.Executable == "" {
			return nil, fmt.Errorf("%w: invalid process identity", ErrFenced)
		}
	}
	return &r, nil
}

func syncDir(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}

func (s *Store) persist(r *Record, first bool) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if first {
		// O_EXCL makes admission binding immutable across duplicate callers.
		// A partial write is deliberately retained as a fail-closed record.
		f, e := os.OpenFile(s.filename(r.Binding.SandboxID), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return e
		}
		_, e = f.Write(b)
		if e == nil {
			e = f.Sync()
		}
		ce := f.Close()
		if e != nil {
			return e
		}
		if ce != nil {
			return ce
		}
		return syncDir(s.root)
	}
	f, err := os.CreateTemp(s.root, ".progress-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err != nil {
		return err
	}
	if ce != nil {
		return ce
	}
	if err = os.Rename(name, s.filename(r.Binding.SandboxID)); err != nil {
		return err
	}
	return syncDir(s.root)
}

// Stop runs under the caller's lifecycle and CubeBox locks and Barrier.Lock.
// capture must prove local identity and pin exact runtime processes before
// admission. stop must wait for those processes to exit without deleting data.
// No code path returns success: the native per-source I/O drain primitive is
// not implemented. A runtime-stopped record is NOT a fencing receipt.
func (s *Store) Stop(ctx context.Context, enabled bool, b Binding, capture func() ([]Process, error), stop func(context.Context, []Process) error) (*Record, error) {
	if s == nil {
		return nil, ErrDisabled
	}
	if err := b.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r, err := s.load(b.SandboxID)
	if err == nil {
		if r.Binding != b {
			return nil, ErrBinding
		}
		// Retry sync even if a prior caller lost the directory-fsync result.
		f, e := os.Open(s.filename(b.SandboxID))
		if e != nil {
			return r, e
		}
		e = f.Sync()
		f.Close()
		if e != nil {
			return r, e
		}
		if e = syncDir(s.root); e != nil {
			return r, e
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if !enabled {
			return nil, ErrDisabled
		}
		ps, e := capture()
		if e != nil {
			return nil, e
		}
		if len(ps) < 1 || len(ps) > 2 {
			return nil, fmt.Errorf("source runtime identity incomplete")
		}
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		r = &Record{Version: 1, Binding: b, Stage: Admitted, Processes: ps}
		if e = s.persist(r, true); e != nil {
			return r, e
		}
	} else {
		return nil, err
	}
	if r.Stage == RuntimeStopped {
		return r, ErrDrainUnsupported
	}
	if err = ctx.Err(); err != nil {
		return r, err
	}
	if err = stop(ctx, r.Processes); err != nil {
		return r, err
	}
	r.Stage = RuntimeStopped
	if err = s.persist(r, false); err != nil {
		return r, err
	}
	return r, ErrDrainUnsupported
}
