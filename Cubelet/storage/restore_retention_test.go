package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

func TestRestoreRetentionWholeSetAndUncertainWrite(t *testing.T) {
	for _, fail := range []string{"", "rootfs-export", "metadata-export", "memory-export", "intent", "complete", "index"} {
		t.Run("failure_"+fail, func(t *testing.T) {
			_, engine, entry := commitFixture(t)
			record := CheckpointCommit{Version: 1, Binding: *entry.DiskBinding, Objects: map[string]CheckpointExport{}}
			for _, role := range []string{"rootfs", "metadata", "memory"} {
				record.Objects[role] = engine.evidence[role+"-export"]
			}
			raw, err := json.Marshal(record)
			require.NoError(t, err)
			var calls []string
			intents := map[string]string{}
			holds := map[string]bool{}
			rpc := func(ctx context.Context, req map[string]string) (string, error) {
				calls = append(calls, req["action"]+":"+req["uuid"])
				switch req["action"] {
				case "index-get":
					if fail == "index" {
						return "", fmt.Errorf("no index")
					}
					return string(raw), nil
				case "get":
					return string(raw), nil
				case "attempt-put":
					var intent restoreRetentionIntent
					require.NoError(t, json.Unmarshal([]byte(req["body"]), &intent))
					require.Len(t, intent.Commit.Objects, 3)
					intents[intent.Phase] = req["body"]
					if fail == "intent" || (fail == "complete" && intent.Phase == "all_remote_held") {
						return "", fmt.Errorf("ambiguous durable write")
					}
					return req["body"], nil
				case "hold":
					require.Contains(t, intents, "acquiring")
					holds[req["uuid"]] = true // PUT may commit before reply is lost.
					if req["uuid"] == fail {
						return "", fmt.Errorf("lost reply")
					}
					return "held_nonexpiring_remote_only", nil
				default:
					t.Fatalf("unexpected action %s", req["action"])
					return "", nil
				}
			}
			id, err := acquireRemoteRestoreRetention(context.Background(), "destination", &cow.RemoteUUIDs{Rootfs: "rootfs-export", Metadata: "metadata-export", Memory: "memory-export"}, strings.Repeat("a", 64), rpc)
			if fail == "" {
				require.NoError(t, err)
				require.NotEmpty(t, id)
				require.Len(t, holds, 3)
				require.Contains(t, intents, "all_remote_held")
			} else {
				require.Error(t, err)
				require.Empty(t, id)
			}
			if fail == "metadata-export" {
				require.Len(t, holds, 2)
				require.NotContains(t, holds, "memory-export")
				require.NotContains(t, intents, "all_remote_held")
			}
			if fail == "index" || fail == "intent" {
				require.Empty(t, holds)
			}
			for _, call := range calls {
				require.NotContains(t, call, "release")
				require.NotContains(t, call, "drop")
			}
		})
	}
}

func TestRestoreRetentionRejectsWrongRoleAndLegacy(t *testing.T) {
	_, engine, entry := commitFixture(t)
	record := CheckpointCommit{Version: 1, Binding: *entry.DiskBinding, Objects: map[string]CheckpointExport{}}
	for _, role := range []string{"rootfs", "metadata", "memory"} {
		record.Objects[role] = engine.evidence[role+"-export"]
	}
	for _, legacy := range []bool{false, true} {
		r := record
		r.Objects = map[string]CheckpointExport{}
		for k, v := range record.Objects {
			r.Objects[k] = v
		}
		e := r.Objects["rootfs"]
		if legacy {
			e.ManifestVersion = 3
		} else {
			e.ExportUUID = "wrong-export"
		}
		r.Objects["rootfs"] = e
		body, _ := json.Marshal(r)
		_, err := acquireRemoteRestoreRetention(context.Background(), "dest", &cow.RemoteUUIDs{Rootfs: "rootfs-export", Metadata: "metadata-export", Memory: "memory-export"}, strings.Repeat("b", 64), func(_ context.Context, req map[string]string) (string, error) {
			require.Contains(t, []string{"index-get", "get"}, req["action"])
			return string(body), nil
		})
		require.Error(t, err)
	}
}

func TestRestoreRetentionRealImportStopsBeforeNativeUse(t *testing.T) {
	t.Setenv(restoreRetentionEnv, "true")
	_, engine, _ := commitFixture(t)
	imp := &SandboxImport{SandboxID: "dest", UUIDs: &cow.RemoteUUIDs{Rootfs: "rootfs-export", Metadata: "metadata-export", Memory: "memory-export"}}
	_, err := imp.importOne(context.Background(), S3MetadataVolumeName("dest"), "metadata", "metadata-export")
	require.ErrorContains(t, err, "protect cross-node restore before metadata use")
	// No index exists. The real caller cannot reach import or activation.
	require.Empty(t, engine.deletedVolumes)
	require.Empty(t, engine.importedLvols)
	require.Empty(t, engine.activatedVolumes)
	require.Empty(t, engine.records)
}

type retentionTestEngine struct {
	*fakeCowEngine
	rpc retentionRPC
}

func (e *retentionTestEngine) CheckpointRecord(raw string) (string, error) {
	var req map[string]string
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		return "", err
	}
	return e.rpc(context.Background(), req)
}

func TestRestoreRetentionPartialRealCallerNeverImports(t *testing.T) {
	t.Setenv(restoreRetentionEnv, "true")
	for _, fail := range []string{"rootfs-export", "metadata-export", "memory-export"} {
		t.Run(fail, func(t *testing.T) {
			store, engine, entry := commitFixture(t)
			record := CheckpointCommit{Version: 1, Binding: *entry.DiskBinding, Objects: map[string]CheckpointExport{}}
			for _, role := range []string{"rootfs", "metadata", "memory"} {
				record.Objects[role] = engine.evidence[role+"-export"]
			}
			body, err := json.Marshal(record)
			require.NoError(t, err)
			store.engine = &retentionTestEngine{fakeCowEngine: engine.fakeCowEngine, rpc: func(_ context.Context, req map[string]string) (string, error) {
				switch req["action"] {
				case "index-get", "get":
					return string(body), nil
				case "attempt-put":
					return req["body"], nil
				case "hold":
					if req["uuid"] == fail {
						return "", fmt.Errorf("uncertain hold write")
					}
					return "held_nonexpiring_remote_only", nil
				}
				t.Fatalf("unexpected operation %s", req["action"])
				return "", nil
			}}
			imp := &SandboxImport{SandboxID: "dest", UUIDs: &cow.RemoteUUIDs{Rootfs: "rootfs-export", Metadata: "metadata-export", Memory: "memory-export"}}
			_, err = imp.importOne(context.Background(), S3MetadataVolumeName("dest"), "metadata", "metadata-export")
			require.ErrorContains(t, err, "uncertain hold write")
			require.Empty(t, engine.importedLvols)
			require.Empty(t, engine.activatedVolumes)
		})
	}
}

func TestRestoreRetentionOptInPolicy(t *testing.T) {
	for _, value := range []string{"", "false", "true", "invalid", "TRUE", "1"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(restoreRetentionEnv, value)
			ctx, err := WithRestoreRetentionAttempt(context.Background())
			if value != "" && value != "false" && value != "true" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			attempt, _ := ctx.Value(restoreAttemptContextKey{}).(string)
			if value != "true" {
				require.Empty(t, attempt)
				return
			}
			require.Len(t, attempt, 64)
			next, err := WithRestoreRetentionAttempt(ctx)
			require.NoError(t, err)
			require.Equal(t, attempt, next.Value(restoreAttemptContextKey{}))
		})
	}
}

func TestRestoreRetentionCompatibilityNoRemoteMutation(t *testing.T) {
	for _, value := range []string{"", "false"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(restoreRetentionEnv, value)
			store, engine, _ := commitFixture(t)
			store.engine = &retentionTestEngine{fakeCowEngine: engine.fakeCowEngine, rpc: func(_ context.Context, _ map[string]string) (string, error) {
				t.Fatal("compatibility mode must not invoke retention RPC")
				return "", nil
			}}
			ann := crossNodeAnnotations()
			ann[restoreRetentionEnv] = "true" // Caller input is not server policy.
			imp := CrossNodeSandboxImport(ann)
			_, _, err := imp.Rootfs(context.Background(), 0)
			require.NoError(t, err)
			require.Len(t, engine.importedLvols, 1)
			require.NotEmpty(t, engine.activatedVolumes)
			require.Empty(t, engine.deletedVolumes)
		})
	}
}

func TestRestoreRetentionInvalidPolicyAndProtectedContext(t *testing.T) {
	_, engine, _ := commitFixture(t)
	imp := CrossNodeSandboxImport(crossNodeAnnotations())
	t.Setenv(restoreRetentionEnv, "invalid")
	_, _, err := imp.Rootfs(context.Background(), 0)
	require.ErrorContains(t, err, "invalid "+restoreRetentionEnv)
	require.Empty(t, engine.importedLvols)
	require.Empty(t, engine.activatedVolumes)
	require.Empty(t, engine.records)

	t.Setenv(restoreRetentionEnv, "true")
	ctx, err := WithRestoreRetentionAttempt(context.Background())
	require.NoError(t, err)
	t.Setenv(restoreRetentionEnv, "false")
	_, _, err = imp.Rootfs(ctx, 0)
	require.ErrorContains(t, err, "protected restore requires committed export index")
	require.Empty(t, engine.importedLvols)
	require.Empty(t, engine.activatedVolumes)
}
