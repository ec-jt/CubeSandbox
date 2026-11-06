// SPDX-License-Identifier: Apache-2.0
package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// Remote-only protection. This MUST NOT set DiskAdmission.Retention or permit
// boot: native destination lvol deletion/unload is not fenced by these holds.
// No automatic release exists. Even successful runtime initialization may keep
// using the exports; only a future durable independence/owner-transfer protocol
// can discharge them. An interrupted acquire is retained, never TTL-reaped.
type restoreRetentionIntent struct {
	Version     int              `json:"version"`
	Attempt     string           `json:"attempt"`
	Destination string           `json:"destination"`
	CommitKey   string           `json:"commit_key"`
	Commit      CheckpointCommit `json:"commit"`
	Phase       string           `json:"phase"`
}

type restoreAttemptContextKey struct{}

// This unfinished protocol has permanent remote holds and no destination-local
// protection or terminal release. Enable only for explicit server-side trials.
// Disabled mode preserves the prior import-origin gate; it is not strong disk
// admission. No request annotation can select this policy.
const restoreRetentionEnv = "CUBE_RESTORE_RETENTION_ENABLED"

func restoreRetentionEnabled() (bool, error) {
	switch os.Getenv(restoreRetentionEnv) {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, fmt.Errorf("invalid %s: expected true or false", restoreRetentionEnv)
	}
}

// WithRestoreRetentionAttempt creates an identity in trusted host code, never
// from request annotations. Durability comes from attempt-put before any hold.
// Existing protected attempts cannot be downgraded by a later config change.
func WithRestoreRetentionAttempt(ctx context.Context) (context.Context, error) {
	if attempt, _ := ctx.Value(restoreAttemptContextKey{}).(string); attempt != "" {
		return ctx, nil
	}
	enabled, err := restoreRetentionEnabled()
	if err != nil || !enabled {
		return ctx, err
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, restoreAttemptContextKey{}, hex.EncodeToString(nonce[:])), nil
}

func checkpointExportSetKey(root, metadata, memory string) string {
	b, _ := json.Marshal([]string{"checkpoint-export-set-v1", root, metadata, memory})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (s *SandboxImport) acquireRemoteRestoreRetention(ctx context.Context, store *S3Cow) (string, error) {
	attempt, _ := ctx.Value(restoreAttemptContextKey{}).(string)
	if attempt == "" {
		var err error
		ctx, err = WithRestoreRetentionAttempt(ctx)
		if err != nil {
			return "", err
		}
		attempt, _ = ctx.Value(restoreAttemptContextKey{}).(string)
	}
	if attempt == "" {
		// No remote API calls or mutations in compatibility mode. Existing
		// native-origin validation still runs in importOne before activation.
		return "", nil
	}
	if s.UUIDs == nil {
		return "", fmt.Errorf("missing restore export set")
	}
	return acquireRemoteRestoreRetention(ctx, s.SandboxID, s.UUIDs, attempt, store.checkpointRPC)
}

type retentionRPC func(context.Context, map[string]string) (string, error)

func acquireRemoteRestoreRetention(ctx context.Context, destination string, uuids *cow.RemoteUUIDs, attempt string, rpc retentionRPC) (string, error) {
	a, err := hex.DecodeString(attempt)
	if err != nil || len(a) != 32 || destination == "" || uuids == nil || uuids.Rootfs == "" || uuids.Metadata == "" {
		return "", fmt.Errorf("invalid restore retention identity")
	}
	index := checkpointExportSetKey(uuids.Rootfs, uuids.Metadata, uuids.Memory)
	raw, err := rpc(ctx, map[string]string{"action": "index-get", "key": index})
	if err != nil {
		return "", fmt.Errorf("protected restore requires committed export index: %w", err)
	}
	var record CheckpointCommit
	if err = json.Unmarshal([]byte(raw), &record); err != nil {
		return "", err
	}
	key := checkpointKey(record.Binding)
	committed, err := rpc(ctx, map[string]string{"action": "get", "key": key})
	if err != nil {
		return "", err
	}
	var exact CheckpointCommit
	if err = json.Unmarshal([]byte(committed), &exact); err != nil || !reflect.DeepEqual(record, exact) || record.Version != 1 || record.Binding.NativeRootfsUUID == "" {
		return "", fmt.Errorf("restore index is not the committed capture")
	}
	roles := []struct{ role, uuid string }{{"rootfs", uuids.Rootfs}, {"metadata", uuids.Metadata}}
	if uuids.Memory != "" {
		roles = append(roles, struct{ role, uuid string }{"memory", uuids.Memory})
	}
	if len(record.Objects) != len(roles) {
		return "", fmt.Errorf("incomplete restore export set")
	}
	seen := map[string]bool{}
	for _, role := range roles {
		e := record.Objects[role.role]
		// v2/v3 REF exports have legacy permanent holds but cannot establish
		// this attempt protocol against an unknown old-writer set.
		if !validCheckpointExport(e) || e.ManifestVersion != 4 || e.ExportUUID != role.uuid || seen[e.ExportUUID] {
			return "", fmt.Errorf("unsupported or mismatched committed role %s", role.role)
		}
		seen[e.ExportUUID] = true
	}
	if record.Objects["rootfs"].SnapshotUUID != record.Binding.NativeRootfsUUID {
		return "", fmt.Errorf("rootfs capture identity mismatch")
	}
	intent := restoreRetentionIntent{Version: 1, Attempt: attempt, Destination: destination, CommitKey: key, Commit: record, Phase: "acquiring"}
	persist := func(phase string) error {
		intent.Phase = phase
		body, err := json.Marshal(intent)
		if err != nil {
			return err
		}
		h := sha256.Sum256([]byte("restore-attempt-v1/" + attempt + "/" + phase))
		reply, err := rpc(ctx, map[string]string{"action": "attempt-put", "key": hex.EncodeToString(h[:]), "body": string(body)})
		if err != nil {
			return err
		}
		if reply != string(body) {
			return fmt.Errorf("restore intent read-back mismatch")
		}
		return nil
	}
	// Complete immutable intent BEFORE the first possibly ambiguous CAS. This
	// records every candidate object, including the one whose reply may be lost.
	if err := persist("acquiring"); err != nil {
		return "", err
	}
	for _, role := range roles {
		reply, err := rpc(ctx, map[string]string{"action": "hold", "key": key, "uuid": role.uuid, "attempt": attempt})
		if err != nil {
			return "", fmt.Errorf("retained incomplete attempt %s (%s): %w", attempt, role.role, err)
		}
		if reply != "held_nonexpiring_remote_only" {
			return "", fmt.Errorf("retained uncertain attempt %s", attempt)
		}
	}
	if err := persist("all_remote_held"); err != nil {
		return "", err
	}
	return attempt, ctx.Err()
}
