package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

func TestSourceNativeBinding(t *testing.T) {
	for _, scenario := range []string{"valid", "digest", "missing", "foreign", "generation", "substitute", "pending", "cancel", "backend", "live-substitute", "duplicate", "unknown-role"} {
		t.Run(scenario, func(t *testing.T) {
			m, e, entry := commitFixture(t)
			ctx := context.Background()
			ok, err := m.commitCheckpoint(ctx, entry.SnapshotID)
			require.NoError(t, err)
			require.True(t, ok)
			key := checkpointKey(*entry.DiskBinding)
			var committed CheckpointCommit
			require.NoError(t, json.Unmarshal([]byte(e.records[key]), &committed))
			body, _ := json.Marshal(committed)
			sum := sha256.Sum256(body)
			digest := hex.EncodeToString(sum[:])
			live := &CowSnapshotObject{Name: SandboxRootfsName("sb-1", 11), Kind: cowKindVolume, Gen: 11, SizeBytes: 8192}
			e.volumeInfos[live.Name] = &cubecow.Volume{Name: live.Name, VolumeUUID: "live-native", SizeBytes: 8192}
			backend := cow.BackendS3
			switch scenario {
			case "digest":
				digest = strings.Repeat("f", 64)
			case "missing":
				delete(e.records, key)
			case "foreign":
				committed.Binding.SandboxID = "foreign"
			case "generation":
				committed.Binding.RootfsGeneration++
			case "substitute":
				e.volumeInfos[entry.RootfsVol].VolumeUUID = "replacement"
			case "pending":
				e.volumeInfos[entry.RootfsVol].ExportStatus = cow.ExportStatusInProgress
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "backend":
				backend = cow.BackendXFS
			case "live-substitute":
				e.volumeInfos[live.Name].VolumeUUID = committed.Objects["rootfs"].SnapshotUUID
			case "duplicate":
				committed.Objects["memory"] = committed.Objects["rootfs"]
			case "unknown-role":
				committed.Objects["extra"] = committed.Objects["memory"]
				delete(committed.Objects, "memory")
			}
			if scenario == "foreign" || scenario == "generation" || scenario == "duplicate" || scenario == "unknown-role" {
				b, _ := json.Marshal(committed)
				e.records[key] = string(b)
			}
			got, err := BindRecoverySource(ctx, backend, "sb-1", entry.SnapshotID, digest, live)
			if scenario == "valid" {
				require.NoError(t, err)
				require.Equal(t, uint32(11), got.LiveGeneration)
				require.Equal(t, uint32(7), got.Commit.Binding.RootfsGeneration)
				require.Equal(t, "live-native", got.LiveUUID)
				// Canonical native bytes and source identity survive a new wrapper.
				m2 := &S3Cow{engine: e}
				again, err := m2.bindRecoverySource(ctx, sourcefence.CohortTemplateGenerationRoot, "sb-1", entry.SnapshotID, digest, "", live)
				require.NoError(t, err)
				require.Equal(t, got, again)
			} else {
				require.Error(t, err)
				require.Nil(t, got)
			}
			require.Empty(t, e.deletedVolumes)
			require.Empty(t, e.deletedSnapshots)
		})
	}
}
