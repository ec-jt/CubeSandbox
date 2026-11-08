// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sourcefence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Eviction records live in their own Store root, never in the recovery fence
// root. The fence root is a deny gate: Any() refuses node-wide package cleanup
// while any file exists there, and Check() denies every mutation of a fenced
// sandbox. An evicted sandbox must do neither, so its record uses the same
// LocalDrainResult format and LoadLocalDrain validation in a separate root.

// EvictedNativeKind tags the native_binding of a local-quiesced record that
// LRU paused eviction produced. FenceRecoverySource never writes this kind.
const EvictedNativeKind = "paused-eviction-v1"

// evictedIncarnationDomain separates the evicted-source identity hash from the
// template and fresh-image domains.
const evictedIncarnationDomain = "cube.recovery.source.incarnation/paused-evicted-v1"

// EvictedNativeBinding is the native_binding body of an evicted record. The
// checkpoint commit is the exact native commit record the origin verified;
// its sha256 is the binding's checkpoint digest, so any holder of the record
// can recompute the digest from the evidence it carries.
type EvictedNativeBinding struct {
	Kind             string          `json:"kind"`
	CheckpointCommit json.RawMessage `json:"checkpoint_commit"`
}

// EvictedIncarnation names the evicted source incarnation. A paused sandbox
// has no live workload epoch, so the identity is the persisted create time
// plus the exact committed pause checkpoint it was evicted at.
func EvictedIncarnation(sandboxID string, created int64, checkpointID, digest string) (string, error) {
	if sandboxID == "" || created <= 0 || checkpointID == "" || digest == "" {
		return "", fmt.Errorf("missing authoritative evicted source incarnation")
	}
	b, err := json.Marshal(struct {
		Domain     string `json:"domain"`
		Sandbox    string `json:"sandbox"`
		Created    int64  `json:"created"`
		Checkpoint string `json:"checkpoint"`
		Digest     string `json:"digest"`
	}{evictedIncarnationDomain, sandboxID, created, checkpointID, digest})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

// NewEvictedNativeBinding builds the native_binding for an evicted record. The
// commit must be the exact canonical bytes whose sha256 is digest.
func NewEvictedNativeBinding(digest string, commit []byte) ([]byte, error) {
	if err := checkEvictedCommit(digest, commit); err != nil {
		return nil, err
	}
	return json.Marshal(EvictedNativeBinding{Kind: EvictedNativeKind, CheckpointCommit: commit})
}

func checkEvictedCommit(digest string, commit []byte) error {
	if len(commit) == 0 || !json.Valid(commit) {
		return fmt.Errorf("%w: evicted checkpoint commit is not valid json", ErrBinding)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, commit); err != nil || !bytes.Equal(compact.Bytes(), commit) {
		// The digest is over the exact bytes; a non-compact copy would not
		// survive a JSON round trip unchanged.
		return fmt.Errorf("%w: evicted checkpoint commit is not canonical", ErrBinding)
	}
	sum := sha256.Sum256(commit)
	if hex.EncodeToString(sum[:]) != digest {
		return fmt.Errorf("%w: evicted checkpoint commit does not match digest", ErrBinding)
	}
	return nil
}

// EvictedStage is a fully written, fsynced but unpublished evicted record.
// Publishing is one rename plus a directory fsync, so the window between
// dropping the PAUSED tombstone and the record becoming durable is minimal.
type EvictedStage struct {
	s      *Store
	path   string
	tmp    string
	result *LocalDrainResult
	raw    []byte
	done   bool
}

// StageEvicted prepares the local-quiesced record of one evicted sandbox in
// this (eviction) store. It refuses when the recovery fence store holds any
// record for the sandbox: eviction never races or shadows FenceRecoverySource.
// Caller holds the sandbox lifecycle lock.
func (s *Store) StageEvicted(fences *Store, b Binding, native []byte) (*EvictedStage, error) {
	if s == nil {
		return nil, ErrDisabled
	}
	if err := b.Validate(); err != nil {
		return nil, err
	}
	if fences == s {
		return nil, fmt.Errorf("%w: eviction records must not share the fence root", ErrBinding)
	}
	if err := fences.Check(b.SandboxID); err != nil {
		return nil, err
	}
	var nb EvictedNativeBinding
	d := json.NewDecoder(bytes.NewReader(native))
	d.DisallowUnknownFields()
	if err := d.Decode(&nb); err != nil || nb.Kind != EvictedNativeKind {
		return nil, fmt.Errorf("%w: invalid evicted native binding", ErrBinding)
	}
	if err := checkEvictedCommit(b.CheckpointDigest, nb.CheckpointCommit); err != nil {
		return nil, err
	}
	result := &LocalDrainResult{Version: 1, Binding: b, Stage: LocalQuiesced, NativeBinding: native}
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if len(raw) > 256<<10 {
		return nil, fmt.Errorf("%w: evicted record too large", ErrBinding)
	}
	f, err := os.CreateTemp(s.root, ".evicted-")
	if err != nil {
		return nil, err
	}
	tmp := f.Name()
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	if ce := f.Close(); err == nil {
		err = ce
	}
	if err != nil {
		os.Remove(tmp)
		return nil, err
	}
	return &EvictedStage{s: s, path: s.filename(b.SandboxID) + ".local-drain-v1", tmp: tmp, result: result, raw: raw}, nil
}

// Publish makes the staged record the durable evicted record of the sandbox,
// replacing an older eviction of the same sandbox on this node. It returns the
// exact bytes ReadRecoverySourceQuiesced serves.
func (st *EvictedStage) Publish() (*LocalDrainResult, []byte, error) {
	if st == nil || st.done {
		return nil, nil, errors.New("evicted record already published or aborted")
	}
	if fi, err := os.Lstat(st.path); err == nil && !fi.Mode().IsRegular() {
		return nil, nil, ErrBinding
	} else if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	if err := os.Rename(st.tmp, st.path); err != nil {
		return nil, nil, err
	}
	st.done = true
	if err := syncDir(st.s.root); err != nil {
		return nil, nil, err
	}
	return st.result, append([]byte(nil), st.raw...), nil
}

// Abort discards an unpublished stage.
func (st *EvictedStage) Abort() {
	if st == nil || st.done {
		return
	}
	st.done = true
	os.Remove(st.tmp)
}

// EvictedCheckpointBlocked reports whether a restore of checkpointID for
// sandboxID must be refused on this node because that exact checkpoint was
// evicted here: the record is this node's promise to Master that it will never
// run or write that checkpoint again. A record that exists but cannot be read
// or validated blocks every checkpoint of the sandbox (fail closed).
func (s *Store) EvictedCheckpointBlocked(sandboxID, checkpointID string) error {
	if s == nil || sandboxID == "" {
		return nil
	}
	r, err := s.LoadLocalDrain(sandboxID)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: unreadable eviction record for %s: %v", ErrFenced, sandboxID, err)
	}
	if checkpointID == "" || r.Binding.CheckpointID == checkpointID {
		return fmt.Errorf("%w: checkpoint %s of %s was evicted from this node", ErrFenced, r.Binding.CheckpointID, sandboxID)
	}
	return nil
}
