// SPDX-License-Identifier: Apache-2.0
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// Eviction may only proceed against a durable, verified native commit. An
// uncommitted, foreign or changed checkpoint must be refused, and the check
// itself must never delete or rewrite native state.
func TestVerifyCommittedPauseCheckpointRequiresDurableCommit(t *testing.T) {
	m, e, entry := commitFixture(t)
	ctx := context.Background()
	sandbox, snap := entry.DiskBinding.SandboxID, entry.SnapshotID

	_, err := VerifyCommittedPauseCheckpoint(ctx, sandbox, snap)
	require.Error(t, err, "sealed but uncommitted checkpoint must not be evictable")

	ok, err := m.commitCheckpoint(ctx, snap)
	require.NoError(t, err)
	require.True(t, ok)
	records := len(e.records)

	digest, err := VerifyCommittedPauseCheckpoint(ctx, sandbox, snap)
	require.NoError(t, err)
	sum := sha256.Sum256([]byte(e.records[checkpointKey(*entry.DiskBinding)]))
	require.Equal(t, hex.EncodeToString(sum[:]), digest, "digest binds the exact commit record")

	_, err = VerifyCommittedPauseCheckpoint(ctx, "other-sandbox", snap)
	require.Error(t, err, "foreign sandbox must not match")
	_, err = VerifyCommittedPauseCheckpoint(ctx, sandbox, "")
	require.Error(t, err)

	// A changed export after commit is no longer a verified checkpoint.
	info := e.volumeInfos[entry.MemoryVol]
	v := e.evidence[info.ExportUUID]
	v.Generation++
	e.evidence[info.ExportUUID] = v
	_, err = VerifyCommittedPauseCheckpoint(ctx, sandbox, snap)
	require.Error(t, err)
	v.Generation--
	e.evidence[info.ExportUUID] = v
	info.ExportStatus = "INPROGRESS"
	_, err = VerifyCommittedPauseCheckpoint(ctx, sandbox, snap)
	require.Error(t, err)

	require.Len(t, e.records, records, "verification must not write records")
	require.Empty(t, e.deletedVolumes, "verification must not delete native objects")
	require.Empty(t, e.deletedSnapshots, "verification must not delete native snapshots")
}
