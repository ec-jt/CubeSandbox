// SPDX-License-Identifier: Apache-2.0
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// VerifyCommittedPauseCheckpoint proves that the pause checkpoint snapshotID of
// sandboxID is durably committed in the native S3 store before a paused
// sandbox may be evicted from this node. It reads the authoritative commit
// record through the native checkpoint RPC (never a caller-supplied copy),
// requires it to bind exactly this sandbox and checkpoint, and re-checks every
// committed object: export DONE, unchanged native identity, unchanged native
// evidence. It returns the lowercase sha256 of the commit record, which is the
// checkpoint digest the cross-node source-quiesced gate binds to.
//
// It is read-only: it never writes, re-commits, deletes or revokes anything.
// Any absent, legacy, foreign, incomplete or changed record is an error, so a
// caller can never evict a sandbox whose checkpoint is not committed.
func VerifyCommittedPauseCheckpoint(ctx context.Context, sandboxID, snapshotID string) (string, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	snapshotID = strings.TrimSpace(snapshotID)
	if sandboxID == "" || snapshotID == "" {
		return "", fmt.Errorf("sandbox and checkpoint ids are required")
	}
	if localStorage == nil {
		return "", fmt.Errorf("storage is not initialized")
	}
	m, ok := localStorage.s3CowManager.(*S3Cow)
	if !ok || m == nil {
		return "", fmt.Errorf("native s3 store unavailable")
	}
	entry, prepared, err := readS3FinalizeCatalog(snapshotID)
	if err != nil {
		return "", fmt.Errorf("pause checkpoint catalog: %w", err)
	}
	if !prepared {
		return "", fmt.Errorf("pause checkpoint not sealed")
	}
	b := entry.DiskBinding
	if b == nil || b.NativeRootfsUUID == "" {
		return "", fmt.Errorf("legacy pause checkpoint has no committed identity")
	}
	if b.Backend != cow.BackendS3 || b.SandboxID != sandboxID || b.CheckpointID != snapshotID {
		return "", fmt.Errorf("pause checkpoint binding does not match sandbox %s checkpoint %s", sandboxID, snapshotID)
	}
	raw, err := m.checkpointRPC(ctx, map[string]string{"action": "get", "key": checkpointKey(*b)})
	if err != nil {
		return "", fmt.Errorf("pause checkpoint commit: %w", err)
	}
	var record CheckpointCommit
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if err = d.Decode(&record); err != nil {
		return "", fmt.Errorf("decode pause checkpoint commit: %w", err)
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return "", fmt.Errorf("trailing pause checkpoint commit")
	}
	if record.Version != 1 || !reflect.DeepEqual(record.Binding, *b) || len(record.Objects) < 2 || len(record.Objects) > 3 {
		return "", fmt.Errorf("foreign or incomplete pause checkpoint commit")
	}
	root, rootOK := record.Objects["rootfs"]
	if _, metaOK := record.Objects["metadata"]; !rootOK || !metaOK {
		return "", fmt.Errorf("pause checkpoint commit misses a required role")
	}
	if root.SnapshotUUID != b.NativeRootfsUUID || root.Snapshot != b.RootfsObject || root.SizeBytes != b.RootfsSizeBytes {
		return "", fmt.Errorf("committed rootfs differs from capture")
	}
	for role, object := range record.Objects {
		if (role != "rootfs" && role != "metadata" && role != "memory") || !validCheckpointExport(object) {
			return "", fmt.Errorf("invalid committed %s", role)
		}
		info, err := m.GetVolumeInfo(ctx, object.Snapshot)
		if err != nil {
			return "", err
		}
		if info == nil || info.ExportStatus != cow.ExportStatusDone || info.ExportUUID != object.ExportUUID ||
			info.VolumeUUID != object.SnapshotUUID || info.SizeBytes != object.SizeBytes {
			return "", fmt.Errorf("committed %s export missing, incomplete or substituted", role)
		}
		evidence, err := m.checkpointRPC(ctx, map[string]string{"action": "evidence", "name": object.Snapshot, "uuid": object.ExportUUID})
		if err != nil {
			return "", err
		}
		var observed CheckpointExport
		if err = json.Unmarshal([]byte(evidence), &observed); err != nil || observed != object {
			return "", fmt.Errorf("committed %s native evidence changed", role)
		}
	}
	body, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), ctx.Err()
}
