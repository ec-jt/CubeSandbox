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
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
	// The staged name carries the sandbox key, so startup recovery can match
	// even a corrupt or truncated stage to its sandbox's PAUSED tombstone.
	f, err := os.CreateTemp(s.root, evictedStagePrefix+SandboxKey(b.SandboxID)+"-")
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

// Retain keeps an unpublished stage on disk for startup recovery and makes a
// later Abort a no-op. Call it once the PAUSED tombstone is gone: from then on
// the staged record is the only proof of the eviction.
func (st *EvictedStage) Retain() {
	if st == nil || st.done {
		return
	}
	st.done = true
	st.s.markUnresolved(st.result.Binding.SandboxID, "eviction record staged but not yet published")
}

func (s *Store) markUnresolved(sandboxID, reason string) {
	m := s.UnresolvedEvictions()
	m[SandboxKey(sandboxID)] = reason
	s.SetUnresolvedEvictions(m)
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
//
// A sandbox whose staged eviction record startup recovery could not resolve
// (kept unpublished, see SetUnresolvedEvictions) is also blocked: its PAUSED
// tombstone is already gone, so whether it was evicted is unknown here.
func (s *Store) EvictedCheckpointBlocked(sandboxID, checkpointID string) error {
	if s == nil || sandboxID == "" {
		return nil
	}
	if reason, ok := s.unresolvedEviction(sandboxID); ok {
		return fmt.Errorf("%w: unresolved staged eviction record for %s: %s", ErrFenced, sandboxID, reason)
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

// evictedStagePrefix names staged (written, fsynced, unpublished) records:
// ".evicted-<SandboxKey>-<random>". Records staged by an older build are
// ".evicted-<random>" and carry no key.
const evictedStagePrefix = ".evicted-"

// SandboxKey is the per-sandbox file key of this store (sha256 hex of the id).
func SandboxKey(sandboxID string) string {
	h := sha256.Sum256([]byte(sandboxID))
	return hex.EncodeToString(h[:])
}

// StagedEviction is one staged eviction record a crash left behind. Result is
// set only when the bytes strictly decode and validate as an evicted record
// (version, local-quiesced stage, binding shape, paused-eviction native kind,
// commit digest equal to the binding digest, and a sandbox matching the key in
// the file name). Otherwise Err says why and Result is nil.
type StagedEviction struct {
	Name       string
	SandboxKey string // "" for a legacy name without a key
	Result     *LocalDrainResult
	Raw        []byte
	Err        error
}

// StagedEvictions lists staged eviction records in name order. It only reads.
func (s *Store) StagedEvictions() ([]StagedEviction, error) {
	if s == nil {
		return nil, ErrDisabled
	}
	es, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	var out []StagedEviction
	for _, e := range es {
		name := e.Name()
		if !strings.HasPrefix(name, evictedStagePrefix) {
			continue
		}
		st := StagedEviction{Name: name, SandboxKey: stagedKey(name)}
		st.Raw, st.Result, st.Err = s.readStaged(name)
		if st.Err == nil && st.SandboxKey != "" && SandboxKey(st.Result.Binding.SandboxID) != st.SandboxKey {
			st.Result, st.Err = nil, fmt.Errorf("%w: staged record sandbox does not match its file key", ErrBinding)
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func stagedKey(name string) string {
	rest := strings.TrimPrefix(name, evictedStagePrefix)
	if len(rest) <= 65 || rest[64] != '-' {
		return ""
	}
	if d, err := hex.DecodeString(rest[:64]); err != nil || len(d) != 32 || rest[:64] != strings.ToLower(rest[:64]) {
		return ""
	}
	return rest[:64]
}

func (s *Store) readStaged(name string) ([]byte, *LocalDrainResult, error) {
	path := filepath.Join(s.root, name)
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if !fi.Mode().IsRegular() || fi.Size() == 0 || fi.Size() > 256<<10 {
		return nil, nil, fmt.Errorf("%w: invalid staged record file", ErrBinding)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	r, err := DecodeEvictedRecord(raw)
	return raw, r, err
}

// DecodeEvictedRecord strictly decodes and validates evicted record bytes.
// It checks shape and self-consistency only; the caller binds the node,
// sandbox and the currently committed checkpoint.
func DecodeEvictedRecord(raw []byte) (*LocalDrainResult, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var r LocalDrainResult
	if err := d.Decode(&r); err != nil {
		return nil, fmt.Errorf("%w: corrupt evicted record: %v", ErrBinding, err)
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, fmt.Errorf("%w: trailing evicted record data", ErrBinding)
	}
	if r.Version != 1 || r.Stage != LocalQuiesced || r.Binding.Validate() != nil {
		return nil, fmt.Errorf("%w: invalid evicted record", ErrBinding)
	}
	var nb EvictedNativeBinding
	nd := json.NewDecoder(bytes.NewReader(r.NativeBinding))
	nd.DisallowUnknownFields()
	if err := nd.Decode(&nb); err != nil || nb.Kind != EvictedNativeKind {
		return nil, fmt.Errorf("%w: invalid evicted native binding", ErrBinding)
	}
	if err := checkEvictedCommit(r.Binding.CheckpointDigest, nb.CheckpointCommit); err != nil {
		return nil, err
	}
	// Exactly the bytes StageEvicted writes: a re-encode must be identical.
	if again, err := json.Marshal(&r); err != nil || !bytes.Equal(again, raw) {
		return nil, fmt.Errorf("%w: evicted record is not canonical", ErrBinding)
	}
	return &r, nil
}

// EvictedCommit returns the checkpoint commit embedded in a validated record.
func EvictedCommit(r *LocalDrainResult) []byte {
	var nb EvictedNativeBinding
	if r == nil || json.Unmarshal(r.NativeBinding, &nb) != nil {
		return nil
	}
	return nb.CheckpointCommit
}

// PublishStaged publishes one staged record exactly like EvictedStage.Publish
// (rename plus directory fsync). It re-reads and re-validates the file and
// refuses if its bytes changed since st was listed. Caller holds the sandbox
// lifecycle lock and has already proven the tombstone is gone and the commit
// still matches. Crash-safe: a crash before the rename leaves the stage, after
// it leaves the published record (and no stage), both handled on re-run.
func (s *Store) PublishStaged(st StagedEviction) ([]byte, error) {
	if s == nil {
		return nil, ErrDisabled
	}
	if st.Result == nil || st.Err != nil {
		return nil, fmt.Errorf("%w: staged record is not valid", ErrBinding)
	}
	raw, r, err := s.readStaged(st.Name)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(raw, st.Raw) || r.Binding != st.Result.Binding {
		return nil, fmt.Errorf("%w: staged record changed", ErrBinding)
	}
	path := s.filename(r.Binding.SandboxID) + ".local-drain-v1"
	if fi, err := os.Lstat(path); err == nil && !fi.Mode().IsRegular() {
		return nil, ErrBinding
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.Rename(filepath.Join(s.root, st.Name), path); err != nil {
		return nil, err
	}
	if err := syncDir(s.root); err != nil {
		return nil, err
	}
	return raw, nil
}

// DiscardStaged deletes one staged record and fsyncs the directory. A record
// that is already gone is not an error (idempotent re-run).
func (s *Store) DiscardStaged(name string) error {
	if s == nil {
		return ErrDisabled
	}
	if !strings.HasPrefix(name, evictedStagePrefix) || name != filepath.Base(name) {
		return ErrBinding
	}
	if err := os.Remove(filepath.Join(s.root, name)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(s.root)
}

// unresolvedEvictions is process-local status: sandboxes (by SandboxKey) whose
// staged record recovery kept unpublished, with the reason. It fails origin
// restores closed and is exported for operators; it never authorizes anything.
var unresolvedEvictions sync.Map // map[*Store]map[string]string

// SetUnresolvedEvictions replaces this store's unresolved set (key -> reason).
func (s *Store) SetUnresolvedEvictions(m map[string]string) {
	if s == nil {
		return
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	unresolvedEvictions.Store(s, cp)
}

// UnresolvedEvictions returns a copy of this store's unresolved set.
func (s *Store) UnresolvedEvictions() map[string]string {
	out := map[string]string{}
	if s == nil {
		return out
	}
	if v, ok := unresolvedEvictions.Load(s); ok {
		for k, r := range v.(map[string]string) {
			out[k] = r
		}
	}
	return out
}

func (s *Store) unresolvedEviction(sandboxID string) (string, bool) {
	v, ok := unresolvedEvictions.Load(s)
	if !ok {
		return "", false
	}
	r, ok := v.(map[string]string)[SandboxKey(sandboxID)]
	return r, ok
}
