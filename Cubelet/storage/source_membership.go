package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// SourceMembership is an exact native inventory plus node-resolved membership.
// Source is never accepted from the Update request. The policy deliberately
// forbids whole-store exclusion: unrelated writable volumes stay open.
type SourceMember struct {
	Name     string `json:"name"`
	UUID     string `json:"uuid"`
	Size     uint64 `json:"size"`
	ReadOnly bool   `json:"readonly"`
	Source   bool   `json:"source"`
}
type SourceMembership struct {
	Token       string         `json:"token"`
	Sandbox     string         `json:"sandbox"`
	Incarnation string         `json:"incarnation"`
	Generation  uint32         `json:"generation"`
	StoreUUID   string         `json:"store_uuid"`
	OwnerNonce  string         `json:"owner_nonce"`
	Endpoint    string         `json:"endpoint"`
	Bucket      string         `json:"bucket"`
	Prefix      string         `json:"prefix"`
	Members     []SourceMember `json:"members"`
}

func decodeSourceJSON(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return fmt.Errorf("trailing source binding")
	}
	return nil
}

// Do not use readAllStorageInfo: it intentionally skips unreadable records.
// Recovery membership must fail closed on any unreadable or conflicting record.
func recoveryStorageRecords() (map[string]*StorageInfo, error) {
	if localStorage == nil || localStorage.db == nil {
		return nil, fmt.Errorf("storage database unavailable")
	}
	all, err := localStorage.db.ReadAll(bucketName)
	if err != nil {
		return nil, err
	}
	if all == nil {
		all = map[string][]byte{}
	}
	es, err := os.ReadDir(localStorage.failoverDir())
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, e := range es {
		if !e.Type().IsRegular() {
			return nil, fmt.Errorf("unexpected storage failover entry")
		}
		raw, e2 := os.ReadFile(filepath.Join(localStorage.failoverDir(), e.Name()))
		if e2 != nil {
			return nil, e2
		}
		if old, ok := all[e.Name()]; ok && !bytes.Equal(old, raw) {
			return nil, fmt.Errorf("conflicting storage failover record")
		}
		all[e.Name()] = raw
	}
	out := map[string]*StorageInfo{}
	for id, raw := range all {
		// A zero-length value is a torn write artifact, not a record: bolt
		// returns it as an empty slice with no error, and the failover file can
		// be truncated the same way. Decoding it would abort every recovery
		// membership read with "unexpected end of JSON input". Skip it here and
		// keep failing closed on any non-empty record that does not decode.
		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}
		var info StorageInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			return nil, fmt.Errorf("storage record %q is unreadable: %w", id, err)
		}
		if info.SandboxID != id {
			return nil, fmt.Errorf("storage record identity mismatch")
		}
		out[id] = &info
	}
	return out, nil
}

// BindRecoveryMembership runs under lifecycle + package barriers. Native prepare
// pins these objects before runtime stop and revalidates the complete inventory.
func BindRecoveryMembership(ctx context.Context, b sourcefence.Binding, e *SourceStorageBinding) (*SourceMembership, error) {
	m, err := requireS3Cow()
	if err != nil {
		return nil, err
	}
	all, err := recoveryStorageRecords()
	if err != nil {
		return nil, err
	}
	s3MetadataMu.Lock()
	state, err := loadS3MetadataState()
	s3MetadataMu.Unlock()
	if err != nil {
		return nil, err
	}
	raw, err := m.checkpointRPC(ctx, map[string]string{"action": "source-inventory"})
	if err != nil {
		return nil, err
	}
	var inventory SourceMembership
	if err = decodeSourceJSON([]byte(raw), &inventory); err != nil {
		return nil, err
	}
	return resolveRecoveryMembership(ctx, m, b, e, all, state, inventory)
}

func resolveRecoveryMembership(ctx context.Context, m *S3Cow, b sourcefence.Binding, e *SourceStorageBinding, all map[string]*StorageInfo, state *s3MetadataState, n SourceMembership) (*SourceMembership, error) {
	if e == nil || state == nil || e.Commit.Binding.SandboxID != b.SandboxID || e.CommitSHA256 != b.CheckpointDigest || e.Commit.Binding.CheckpointID != b.CheckpointID {
		return nil, sourcefence.ErrBinding
	}
	if all[b.SandboxID] == nil || storageBackendFromInfo(all[b.SandboxID]) != cow.BackendS3 {
		return nil, fmt.Errorf("source storage record absent or foreign")
	}
	// The ownership cohort is part of the durable evidence, not a hint. A
	// fresh-image source must still carry its typed marker and its persisted
	// rootfs mount; anything else is a foreign or downgraded record.
	switch e.Cohort {
	case "":
		if e.RootfsMount != "" {
			return nil, sourcefence.ErrBinding
		}
	case sourcefence.CohortFreshImageS3:
		if _, err := resolveFreshImageSourceVolume(ctx, m, all, b.SandboxID, e.RootfsMount); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown source ownership cohort")
	}
	root := e.Commit.Objects["rootfs"]
	if n.StoreUUID == "" || n.OwnerNonce == "" || n.Endpoint != root.Endpoint || n.Bucket != root.Bucket || n.Prefix != root.Prefix || n.Prefix != root.LvsName || len(n.Members) == 0 || len(n.Members) > 256 {
		return nil, fmt.Errorf("native store identity mismatch")
	}
	owners := map[string]string{}
	kinds := map[string]string{}
	claim := func(name, kind, owner string) error {
		if name == "" || (kind != cowKindVolume && kind != cowKindSnapshot) {
			return fmt.Errorf("untyped storage member")
		}
		if old, ok := owners[name]; ok && old != owner {
			return fmt.Errorf("shared source resource %s", name)
		}
		owners[name] = owner
		kinds[name] = kind
		return nil
	}
	for id, info := range all {
		backend, err := cow.NormalizeBackend(info.Backend)
		if err != nil {
			return nil, err
		}
		if backend != cow.BackendS3 {
			continue
		}
		if id == b.SandboxID && (info.CubePCIDiskInfo != nil || info.CubePCISystemDiskInfo != nil || len(info.HostDirBackendInfos) > 0 || len(info.PluginVolumeBackendInfos) > 0) {
			return nil, fmt.Errorf("source contains unsupported external storage")
		}
		for _, v := range info.Volumes {
			if v == nil {
				return nil, fmt.Errorf("missing persisted volume")
			}
			if err = claim(v.VolumeName, v.Kind, id); err != nil {
				return nil, err
			}
			if id == b.SandboxID && v.VolumeName == e.LiveName && (v.Kind != cowKindVolume || v.Gen != e.LiveGeneration) {
				return nil, fmt.Errorf("live root generation or kind changed")
			}
		}
		if info.ImportedMemoryVol != "" {
			if err = claim(info.ImportedMemoryVol, cowKindVolume, id); err != nil {
				return nil, err
			}
		}
	}
	if owners[e.LiveName] != b.SandboxID || kinds[e.LiveName] != cowKindVolume {
		return nil, fmt.Errorf("live root not in persisted source")
	}
	// The node metadata base is explicitly shared, not a source-owned disk.
	if state.Base.Name != "" {
		if err := claim(state.Base.Name, cowKindVolume, "@node-base"); err != nil {
			return nil, err
		}
	}
	for id, v := range state.Derived {
		if v.SnapshotID != id {
			return nil, fmt.Errorf("metadata owner mismatch")
		}
		// Package/rollback identifiers do not establish a sandbox owner.
		// Do not classify a writable orphan by suffix or silently exclude it.
		// This bounded cohort requires every derived writable disk to have a
		// persisted sandbox record. Sealed package snapshots are native RO.
		if all[id] == nil {
			for _, member := range n.Members {
				if member.Name == v.VolName && !member.ReadOnly {
					return nil, fmt.Errorf("derived writable metadata has no authoritative sandbox owner")
				}
			}
			continue
		}
		if err := claim(v.VolName, cowKindVolume, id); err != nil {
			return nil, err
		}
	}
	selected := map[string]CheckpointExport{}
	for _, v := range e.Commit.Objects {
		selected[v.Snapshot] = v
	}
	seen := map[string]bool{}
	uuids := map[string]bool{}
	for i := range n.Members {
		v := &n.Members[i]
		if v.Name == "" || v.UUID == "" || v.Size == 0 || seen[v.Name] || uuids[v.UUID] || v.Source {
			return nil, fmt.Errorf("invalid native inventory")
		}
		seen[v.Name] = true
		uuids[v.UUID] = true
		owner, known := owners[v.Name]
		if !v.ReadOnly && !known {
			return nil, fmt.Errorf("unowned native writable volume %s", v.Name)
		}
		if known && (v.ReadOnly != (kinds[v.Name] == cowKindSnapshot)) {
			return nil, fmt.Errorf("native kind mismatch")
		}
		v.Source = known && owner == b.SandboxID
		if v.Source {
			info, err := m.GetVolumeInfo(ctx, v.Name)
			if err != nil {
				return nil, err
			}
			if info == nil || info.VolumeUUID != v.UUID || info.SizeBytes != v.Size || info.ImportOrigin != nil {
				return nil, fmt.Errorf("native member substituted or imported")
			}
		}
		if v.Name == e.LiveName && (v.UUID != e.LiveUUID || v.Size != e.LiveSize || v.ReadOnly) {
			return nil, fmt.Errorf("native live root changed")
		}
		if s, ok := selected[v.Name]; ok {
			if !v.ReadOnly || v.UUID != s.SnapshotUUID || v.Size != s.SizeBytes {
				return nil, fmt.Errorf("selected object changed")
			}
			v.Source = true
			delete(selected, v.Name)
		}
	}
	for name, owner := range owners {
		if owner == b.SandboxID && !seen[name] {
			return nil, fmt.Errorf("missing native source volume %s", name)
		}
	}
	if len(selected) != 0 {
		return nil, fmt.Errorf("missing selected native object")
	}
	sort.Slice(n.Members, func(i, j int) bool { return n.Members[i].Name < n.Members[j].Name })
	n.Sandbox = b.SandboxID
	n.Incarnation = b.Incarnation
	n.Generation = e.LiveGeneration
	bound, _ := json.Marshal(struct {
		Binding    sourcefence.Binding
		Evidence   *SourceStorageBinding
		Membership SourceMembership
	}{b, e, n})
	sum := sha256.Sum256(bound)
	n.Token = hex.EncodeToString(sum[:])
	return &n, nil
}

func RecoverySourceNativeCall(ctx context.Context, action string, body []byte) ([]byte, error) {
	if action != "source-prepare" && action != "source-drain" {
		return nil, fmt.Errorf("invalid source action")
	}
	m, err := requireS3Cow()
	if err != nil {
		return nil, err
	}
	raw, err := m.checkpointRPC(ctx, map[string]string{"action": action, "body": string(body)})
	if err != nil {
		return nil, err
	}
	if !bytes.Equal([]byte(raw), body) {
		return nil, fmt.Errorf("native source result binding mismatch")
	}
	return []byte(raw), nil
}
