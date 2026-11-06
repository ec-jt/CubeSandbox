package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

func TestRecoveryMembership(t *testing.T) {
	for _, scenario := range []string{"valid-shared-store", "shared-source-disk", "missing", "additional", "generation", "kind", "foreign-uuid", "endpoint", "caller-source", "missing-selected", "orphan-metadata"} {
		t.Run(scenario, func(t *testing.T) {
			b := sourcefence.Binding{"op", "sb", "node", strings.Repeat("b", 64), "cp", strings.Repeat("a", 64)}
			e := &SourceStorageBinding{CommitSHA256: b.CheckpointDigest, LiveName: "root", LiveUUID: "live", LiveGeneration: 7, LiveSize: 4096, Commit: CheckpointCommit{Binding: CheckpointDiskBinding{SandboxID: "sb", CheckpointID: "cp"}, Objects: map[string]CheckpointExport{"rootfs": {Snapshot: "cp-root", SnapshotUUID: "selected", SizeBytes: 4096, Endpoint: "native:9000", Bucket: "bucket", Prefix: "store", LvsName: "store"}}}}
			all := map[string]*StorageInfo{"sb": {SandboxID: "sb", Backend: cow.BackendS3, Volumes: map[string]*BackendFileInfo{"root": {VolumeName: "root", Kind: cowKindVolume, Gen: 7}}}, "other": {SandboxID: "other", Backend: cow.BackendS3, Volumes: map[string]*BackendFileInfo{"disk": {VolumeName: "other", Kind: cowKindVolume}}}}
			n := SourceMembership{StoreUUID: "store-uuid", OwnerNonce: "owner", Endpoint: "native:9000", Bucket: "bucket", Prefix: "store", Members: []SourceMember{{"root", "live", 4096, false, false}, {"other", "other-uuid", 4096, false, false}, {"cp-root", "selected", 4096, true, false}}}
			engine := &fakeCowEngine{volumeInfos: map[string]*cubecow.Volume{"root": {Name: "root", VolumeUUID: "live", SizeBytes: 4096}}}
			state := &s3MetadataState{}
			switch scenario {
			case "shared-source-disk":
				all["other"].Volumes["disk"].VolumeName = "root"
			case "missing":
				n.Members = n.Members[1:]
			case "additional":
				n.Members = append(n.Members, SourceMember{"unowned", "new", 4096, false, false})
			case "generation":
				all["sb"].Volumes["root"].Gen++
			case "kind":
				all["sb"].Volumes["root"].Kind = cowKindSnapshot
			case "foreign-uuid":
				n.Members[0].UUID = "changed"
			case "endpoint":
				n.Endpoint = "foreign:9000"
			case "caller-source":
				n.Members[1].Source = true
			case "orphan-metadata":
				state.Derived = map[string]s3MetadataDerivedRecord{"sb-rollback": {SnapshotID: "sb-rollback", VolName: "orphan"}}
				n.Members = append(n.Members, SourceMember{"orphan", "orphan-uuid", 4096, false, false})
			case "missing-selected":
				n.Members = n.Members[:2]
			}
			got, err := resolveRecoveryMembership(context.Background(), &S3Cow{engine: engine}, b, e, all, state, n)
			if scenario == "valid-shared-store" {
				require.NoError(t, err)
				require.Len(t, got.Token, 64)
				for _, v := range got.Members {
					require.Equal(t, v.Name != "other", v.Source)
				}
			} else {
				require.Error(t, err)
				require.Nil(t, got)
			}
		})
	}
}
