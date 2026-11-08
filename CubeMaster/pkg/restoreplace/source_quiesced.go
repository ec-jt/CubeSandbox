// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package restoreplace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/log"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/cubelet"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	cubeleterrorcode "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

// LocalQuiesced mirrors Cubelet/pkg/sourcefence.LocalQuiesced. A local drain
// result is NOT a global fence: it proves the origin runtime was stopped and
// its selected storage sources were drained on that node only. It authorizes
// no takeover by itself.
const LocalQuiesced = "local-quiesced-not-global-fenced"

// SourceQuiescedEnv gates the cross-node restore precondition. Default off so
// every existing placement decision is unchanged until an operator opts in.
// Any unparsable value is treated as enabled (fail closed).
const SourceQuiescedEnv = "CUBE_CROSSNODE_REQUIRE_SOURCE_QUIESCED"

const (
	// sourceQuiescedAction is served by the Cubelet private Update RPC. It is a
	// read-only accessor for the durable origin drain record; it cannot stop,
	// drain, clear or fence anything.
	sourceQuiescedAction = "ReadRecoverySourceQuiesced"
	// sourceQuiescedExtInfoKey is the same key the fence path already uses for
	// the local result, so the wire shape has one meaning.
	sourceQuiescedExtInfoKey = "source_local_quiesced_not_global_fenced"
	sourceQuiescedTimeout    = 10 * time.Second
	sourceQuiescedMaxBytes   = 256 << 10
)

// SourceQuiescedBinding mirrors Cubelet/pkg/sourcefence.Binding. It is the
// immutable origin identity of one fence operation.
type SourceQuiescedBinding struct {
	OperationID      string `json:"operation_id"`
	SandboxID        string `json:"sandbox_id"`
	NodeID           string `json:"node_id"`
	Incarnation      string `json:"incarnation"`
	CheckpointID     string `json:"checkpoint_id"`
	CheckpointDigest string `json:"checkpoint_digest"`
}

// SourceQuiescedRecord mirrors Cubelet/pkg/sourcefence.LocalDrainResult.
// Version, Binding, Stage and NativeBinding are the whole durable record; no
// extra field is accepted, so a record from another producer cannot pass.
type SourceQuiescedRecord struct {
	Version       int                   `json:"version"`
	Binding       SourceQuiescedBinding `json:"binding"`
	Stage         string                `json:"stage"`
	NativeBinding json.RawMessage       `json:"native_binding"`
}

func validSourceQuiescedDigest(v string) bool {
	d, err := hex.DecodeString(v)
	return err == nil && len(d) == 32 && v == strings.ToLower(v)
}

func validSourceQuiescedToken(v string) bool {
	return v != "" && v == strings.TrimSpace(v) && len(v) <= 256 && !strings.ContainsAny(v, "\x00\r\n")
}

// decodeSourceQuiescedRecord strictly decodes the origin local drain record.
// Every malformed, truncated or foreign record is an error: this function is
// only ever used to authorize a cross-node restore.
func decodeSourceQuiescedRecord(raw []byte) (*SourceQuiescedRecord, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("origin local-quiesced record absent")
	}
	if len(raw) > sourceQuiescedMaxBytes {
		return nil, fmt.Errorf("origin local-quiesced record too large")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var r SourceQuiescedRecord
	if err := d.Decode(&r); err != nil {
		return nil, fmt.Errorf("decode origin local-quiesced record: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("origin local-quiesced record has trailing data")
	}
	if r.Version != 1 {
		return nil, fmt.Errorf("origin local-quiesced record version %d is not 1", r.Version)
	}
	if r.Stage != LocalQuiesced {
		return nil, fmt.Errorf("origin record stage %q is not %q", strings.TrimSpace(r.Stage), LocalQuiesced)
	}
	for _, v := range []string{r.Binding.OperationID, r.Binding.SandboxID, r.Binding.NodeID,
		r.Binding.Incarnation, r.Binding.CheckpointID} {
		if !validSourceQuiescedToken(v) {
			return nil, fmt.Errorf("origin local-quiesced record binding is incomplete")
		}
	}
	if !validSourceQuiescedDigest(r.Binding.CheckpointDigest) {
		return nil, fmt.Errorf("origin local-quiesced record checkpoint digest is invalid")
	}
	if len(r.NativeBinding) == 0 || !json.Valid(r.NativeBinding) {
		return nil, fmt.Errorf("origin local-quiesced record has no native binding")
	}
	return &r, nil
}

// ValidateSourceQuiesced binds one origin record to the restore the caller is
// about to place off the origin. A missing expected identity is a refusal, not
// a wildcard: Master must know the exact sandbox, checkpoint and digest.
func ValidateSourceQuiesced(in Input, rec *SourceQuiescedRecord) error {
	if rec == nil {
		return fmt.Errorf("origin local-quiesced record absent")
	}
	if rec.Stage != LocalQuiesced {
		return fmt.Errorf("origin record stage %q is not %q", strings.TrimSpace(rec.Stage), LocalQuiesced)
	}
	wantSandbox := strings.TrimSpace(in.SourceSandboxID)
	if wantSandbox == "" {
		return fmt.Errorf("expected source sandbox identity unknown")
	}
	if rec.Binding.SandboxID != wantSandbox {
		return fmt.Errorf("origin record sandbox_id %q does not match %q", rec.Binding.SandboxID, wantSandbox)
	}
	wantCheckpoint := strings.TrimSpace(in.SourceCheckpointID)
	if wantCheckpoint == "" {
		return fmt.Errorf("expected source checkpoint id unknown")
	}
	if rec.Binding.CheckpointID != wantCheckpoint {
		return fmt.Errorf("origin record checkpoint_id %q does not match %q", rec.Binding.CheckpointID, wantCheckpoint)
	}
	wantDigest := strings.TrimSpace(in.SourceCheckpointDigest)
	if wantDigest == "" {
		return fmt.Errorf("expected source checkpoint digest unknown")
	}
	if rec.Binding.CheckpointDigest != wantDigest {
		return fmt.Errorf("origin record checkpoint_digest %q does not match %q", rec.Binding.CheckpointDigest, wantDigest)
	}
	wantNode := strings.TrimSpace(in.OriginNodeID)
	if wantNode == "" {
		return fmt.Errorf("origin node identity unknown")
	}
	if rec.Binding.NodeID != wantNode {
		return fmt.Errorf("origin record node_id %q does not match origin %q", rec.Binding.NodeID, wantNode)
	}
	return nil
}

// sourceQuiescedRequired reads the opt-in flag on every call so a rollout can
// change it without a restart. Unparsable values fail closed.
func sourceQuiescedRequired() bool {
	raw := strings.TrimSpace(os.Getenv(SourceQuiescedEnv))
	if raw == "" {
		return false
	}
	on, err := strconv.ParseBool(raw)
	if err != nil {
		return true
	}
	return on
}

// EvictedNativeKind mirrors Cubelet/pkg/sourcefence.EvictedNativeKind: the
// native_binding kind of a local-quiesced record produced by LRU paused
// eviction. FenceRecoverySource never produces it.
const EvictedNativeKind = "paused-eviction-v1"

// evictedNativeBinding mirrors Cubelet/pkg/sourcefence.EvictedNativeBinding.
type evictedNativeBinding struct {
	Kind             string          `json:"kind"`
	CheckpointCommit json.RawMessage `json:"checkpoint_commit"`
}

// EvictionRecordDigest strictly decodes an eviction record and returns the
// checkpoint digest it binds. It does not authorize anything by itself: the
// caller must still run ValidateEvictionRecord with that digest, which also
// requires the embedded commit to reproduce it.
func EvictionRecordDigest(raw []byte) (string, error) {
	rec, err := decodeSourceQuiescedRecord(raw)
	if err != nil {
		return "", err
	}
	return rec.Binding.CheckpointDigest, nil
}

// ValidateEvictionRecord strictly decodes a persisted eviction record and
// binds it to the restore identity, exactly like the origin read path, and in
// addition requires the eviction native binding whose embedded commit hashes
// to the binding digest. A record from FenceRecoverySource, or one whose
// evidence does not reproduce the digest, is refused.
func ValidateEvictionRecord(in Input, raw []byte) (*SourceQuiescedRecord, error) {
	rec, err := decodeSourceQuiescedRecord(raw)
	if err != nil {
		return nil, err
	}
	if err := ValidateSourceQuiesced(in, rec); err != nil {
		return nil, err
	}
	var nb evictedNativeBinding
	d := json.NewDecoder(bytes.NewReader(rec.NativeBinding))
	d.DisallowUnknownFields()
	if err := d.Decode(&nb); err != nil || nb.Kind != EvictedNativeKind || len(nb.CheckpointCommit) == 0 {
		return nil, fmt.Errorf("eviction record native binding is not a paused-eviction binding")
	}
	sum := sha256.Sum256(nb.CheckpointCommit)
	if hex.EncodeToString(sum[:]) != rec.Binding.CheckpointDigest {
		return nil, fmt.Errorf("eviction record checkpoint commit does not reproduce its digest")
	}
	return rec, nil
}

// fetchSourceQuiescedFn is the trusted server-side read of the origin's
// durable drain record. It is a seam so placement tests can exercise every
// outcome without a Cubelet.
var fetchSourceQuiescedFn = fetchSourceQuiescedFromOrigin

// fetchSourceQuiescedFromOrigin asks the origin Cubelet for the record over
// the existing private Update RPC. There is no request annotation and no
// caller-supplied flag: the record is durable node state or nothing.
func fetchSourceQuiescedFromOrigin(ctx context.Context, originIP, originNodeID, sandboxID string) (*SourceQuiescedRecord, error) {
	raw, err := ReadOriginSourceQuiescedRaw(ctx, originIP, sandboxID)
	if err != nil {
		return nil, err
	}
	return decodeSourceQuiescedRecord(raw)
}

// ReadOriginSourceQuiescedRaw returns the origin's record bytes as served by
// ReadRecoverySourceQuiesced, without validating them. Callers must validate
// with decodeSourceQuiescedRecord / ValidateEvictionRecord before trusting it.
func ReadOriginSourceQuiescedRaw(ctx context.Context, originIP, sandboxID string) ([]byte, error) {
	ip := strings.TrimSpace(originIP)
	if ip == "" {
		return nil, fmt.Errorf("origin ip unknown")
	}
	endpoint := cubelet.GetCubeletAddr(ip)
	if strings.TrimSpace(endpoint) == "" {
		return nil, fmt.Errorf("origin cubelet endpoint unknown")
	}
	rsp, err := cubelet.UpdateWithTimeout(ctx, endpoint, &cubebox.UpdateCubeSandboxRequest{
		RequestID: "source-quiesced-read-" + strings.TrimSpace(sandboxID),
		SandboxID: strings.TrimSpace(sandboxID),
		Annotations: map[string]string{
			constants.CubeAnnotationsUpdateAction: sourceQuiescedAction,
		},
	}, sourceQuiescedTimeout)
	if err != nil {
		return nil, err
	}
	if rsp == nil || rsp.GetRet() == nil {
		return nil, fmt.Errorf("origin returned no response")
	}
	if rsp.GetRet().GetRetCode() != cubeleterrorcode.ErrorCode_Success {
		return nil, fmt.Errorf("origin refused: %s", strings.TrimSpace(rsp.GetRet().GetRetMsg()))
	}
	raw := rsp.GetExtInfo()[sourceQuiescedExtInfoKey]
	if len(raw) == 0 {
		return nil, fmt.Errorf("origin local-quiesced record absent")
	}
	return raw, nil
}

// checkCrossNodeSourceQuiesced enforces the cross-node precondition. It is
// called only where placement would actually leave the origin, so the default
// path is untouched while the flag is off.
func checkCrossNodeSourceQuiesced(ctx context.Context, in Input) error {
	if !sourceQuiescedRequired() {
		return nil
	}
	snapshot := strings.TrimSpace(in.SnapshotID)
	sandbox := strings.TrimSpace(in.SourceSandboxID)
	if sandbox == "" {
		return fmt.Errorf("cross-node restore of %s refused: expected source sandbox identity unknown", snapshot)
	}
	originIP := strings.TrimSpace(in.OriginNodeIP)
	originID := strings.TrimSpace(in.OriginNodeID)
	// An evicted binding carries the origin's record persisted by Master at
	// eviction. It is validated with the same strict decoder and binding
	// check plus the eviction evidence; a present but invalid record is a
	// refusal, never a reason to ask the origin instead. Only when no record
	// was persisted does an evicted restore fall back to the origin read.
	if in.ExcludeOrigin && len(in.PersistedSourceQuiescedRecord) > 0 {
		rec, err := ValidateEvictionRecord(in, in.PersistedSourceQuiescedRecord)
		if err != nil {
			return fmt.Errorf("cross-node restore of %s refused: persisted eviction record: %w", snapshot, err)
		}
		log.G(ctx).Infof("restoreplace: cross-node source quiesced (persisted eviction record) snapshot=%s sandbox=%s origin=%s operation=%s",
			snapshot, sandbox, originID, rec.Binding.OperationID)
		return nil
	}
	if originIP == "" {
		return fmt.Errorf("cross-node restore of %s refused: origin node address unknown", snapshot)
	}
	rec, err := fetchSourceQuiescedFn(ctx, originIP, originID, sandbox)
	if err != nil {
		return fmt.Errorf("cross-node restore of %s refused: origin local-quiesced record unavailable: %w", snapshot, err)
	}
	if err := ValidateSourceQuiesced(in, rec); err != nil {
		return fmt.Errorf("cross-node restore of %s refused: %w", snapshot, err)
	}
	log.G(ctx).Infof("restoreplace: cross-node source quiesced snapshot=%s sandbox=%s origin=%s operation=%s",
		snapshot, sandbox, originID, rec.Binding.OperationID)
	return nil
}
