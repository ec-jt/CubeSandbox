package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// The live fixture replaces only the Rust FFI transport with the same private
// Unix JSON-RPC protocol. No native success endpoint or storage callback is fake.
type liveSourceEngine struct {
	*fakeCowEngine
	socket string
}

func (e *liveSourceEngine) rpc(method string, params any) (json.RawMessage, error) {
	c, err := net.DialTimeout("unix", e.socket, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(45 * time.Second))
	if err = json.NewEncoder(c).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params}); err != nil {
		return nil, err
	}
	var r struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err = json.NewDecoder(c).Decode(&r); err != nil {
		return nil, err
	}
	if len(r.Error) > 0 {
		return nil, fmt.Errorf("native RPC: %s", r.Error)
	}
	return r.Result, nil
}
func (e *liveSourceEngine) CheckpointRecord(raw string) (string, error) {
	var p any
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return "", err
	}
	r, err := e.rpc("rcow_checkpoint_record", p)
	if err != nil {
		return "", err
	}
	var v struct {
		Bool  *bool  `json:"bool_value"`
		Value string `json:"string_value"`
	}
	if err = json.Unmarshal(r, &v); err != nil {
		return "", err
	}
	if v.Bool != nil && !*v.Bool {
		return "", fmt.Errorf("native refusal: %s", v.Value)
	}
	return v.Value, nil
}
func (e *liveSourceEngine) GetVolumeInfo(name string) (*cubecow.Volume, error) {
	r, err := e.rpc("rcow_get_lvol", map[string]string{"lvol_name": name})
	if err != nil {
		return nil, err
	}
	var v struct {
		Bool  bool   `json:"bool_value"`
		Value string `json:"string_value"`
	}
	if err = json.Unmarshal(r, &v); err != nil {
		return nil, err
	}
	if !v.Bool {
		return nil, fmt.Errorf("native lookup: %s", v.Value)
	}
	var n struct {
		Name    string `json:"name"`
		UUID    string `json:"uuid"`
		Cluster uint64 `json:"cluster_size"`
		Count   uint64 `json:"total_clusters"`
	}
	if err = json.Unmarshal([]byte(v.Value), &n); err != nil {
		return nil, err
	}
	return &cubecow.Volume{Name: n.Name, VolumeUUID: n.UUID, SizeBytes: n.Cluster * n.Count}, nil
}

func TestRecoveryMembershipLiveRPC(t *testing.T) {
	socket := os.Getenv("INTEGRATED_NATIVE_SOCKET")
	if socket == "" {
		t.Skip("requires isolated native storage fixture")
	}
	dir := os.Getenv("INTEGRATED_NODE_DIR")
	require.NotEmpty(t, dir)
	require.NoError(t, os.MkdirAll(dir, 0700))
	engine := &liveSourceEngine{&fakeCowEngine{}, socket}
	db, err := utils.NewCubeStore(filepath.Join(dir, "storage.db"), nil)
	require.NoError(t, err)
	defer db.Close()
	previous := localStorage
	defer func() { localStorage = previous }()
	m := &S3Cow{engine: engine}
	localStorage = &local{config: &Config{StorageBackend: "cubecow", RootPath: dir}, db: db, s3CowManager: m}
	ctx := context.Background()
	raw, err := m.checkpointRPC(ctx, map[string]string{"action": "source-inventory"})
	require.NoError(t, err)
	var n SourceMembership
	require.NoError(t, decodeSourceJSON([]byte(raw), &n))
	byName := map[string]SourceMember{}
	for _, v := range n.Members {
		byName[v.Name] = v
	}
	root := byName["sb-integrated-rootfs-gen7"]
	selected := byName["selected-root"]
	require.NotEmpty(t, root.UUID)
	require.True(t, selected.ReadOnly)
	for id, name := range map[string]string{"integrated": root.Name, "unrelated": "unrelated"} {
		i := &StorageInfo{SandboxID: id, Backend: cow.BackendS3, Volumes: map[string]*BackendFileInfo{"root": {VolumeName: name, Kind: cowKindVolume, Gen: 7}}}
		require.NoError(t, localStorage.writeBackendFileInfo(ctx, id, i))
	}
	// These selected-object facts are native inventory, not an export/commit
	// qualification. Existing selected-commit verification is deliberately not
	// re-proven by this narrower node->RPC->drain integration fixture.
	e := &SourceStorageBinding{LiveName: root.Name, LiveUUID: root.UUID, LiveGeneration: 7, LiveSize: root.Size, Commit: CheckpointCommit{Binding: CheckpointDiskBinding{SandboxID: "integrated", CheckpointID: "selected"}, Objects: map[string]CheckpointExport{"rootfs": {Snapshot: selected.Name, SnapshotUUID: selected.UUID, SizeBytes: selected.Size, Endpoint: n.Endpoint, Bucket: n.Bucket, Prefix: n.Prefix, LvsName: n.Prefix}}}}
	h := sha256.Sum256([]byte(raw))
	e.CommitSHA256 = hex.EncodeToString(h[:])
	b := sourcefence.Binding{"integrated-live-01", "integrated", "private-node", strings.Repeat("b", 64), "selected", e.CommitSHA256}
	membership, err := BindRecoveryMembership(ctx, b, e)
	require.NoError(t, err)
	body, err := json.Marshal(membership)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "membership.json"), body, 0600))
	evidence, err := json.Marshal(struct {
		Native     *SourceStorageBinding `json:"native"`
		Membership *SourceMembership     `json:"membership"`
	}{e, membership})
	require.NoError(t, err)
	fences, err := sourcefence.Open(filepath.Join(dir, "fences"))
	require.NoError(t, err)
	_, err = fences.Stop(ctx, true, b, func() ([]sourcefence.Process, error) {
		require.NoError(t, fences.BindStorage(b, evidence))
		_, err := RecoverySourceNativeCall(ctx, "source-prepare", body)
		require.NoError(t, err)
		return []sourcefence.Process{{PID: 99, StartTime: "fixture", BootID: "fixture", Executable: "NO-GUEST-FIXTURE"}}, nil
	}, func(context.Context, []sourcefence.Process) error {
		// No guest exists in this fixture. Runtime qualification is NOT claimed.
		require.ErrorIs(t, fences.Check(b.SandboxID), sourcefence.ErrFenced)
		return nil
	})
	require.ErrorIs(t, err, sourcefence.ErrDrainUnsupported)
	foreign := append([]byte(nil), body...)
	foreign = []byte(strings.Replace(string(foreign), membership.Token, strings.Repeat("f", 64), 1))
	_, err = RecoverySourceNativeCall(ctx, "source-prepare", foreign)
	require.Error(t, err)
	result, err := fences.DrainLocal(ctx, b, body, func(ctx context.Context, raw []byte) ([]byte, error) {
		return RecoverySourceNativeCall(ctx, "source-drain", raw)
	})
	require.NoError(t, err)
	encoded, _ := json.Marshal(result)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "result.json"), encoded, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(socket), "receipt-ready"), []byte("ready"), 0600))
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(filepath.Dir(socket), "native-verified"))
		return err == nil
	}, 30*time.Second, 20*time.Millisecond)
	fences, err = sourcefence.Open(filepath.Join(dir, "fences"))
	require.NoError(t, err)
	_, err = fences.DrainLocal(ctx, b, body, func(context.Context, []byte) ([]byte, error) {
		t.Fatal("durable publication replay invoked RPC")
		return nil, nil
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(socket), "node-complete"), []byte("PASS"), 0600))
	t.Log("PASS real Go native Unix RPC binding, scoped drain, durable local publication, restart replay; guest stop NOT exercised")
}
