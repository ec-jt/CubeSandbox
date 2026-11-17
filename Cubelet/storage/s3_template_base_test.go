// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// fakeS3lvol models the s3lvol template-base registry the RPCs read: which
// snapshot is base <uuid>, and what an lvol's nearest base is.
type fakeS3lvol struct {
	bases     map[string]templateBaseEntry // by base uuid
	below     map[string]string            // lvol -> base uuid
	delta     bool
	marks     [][3]string
	exports   []string
	exportErr error
	calls     []string
	// priorExports is what rcow_get_exports lists.
	priorExports []map[string]any
}

func useFakeS3lvol(t *testing.T) *fakeS3lvol {
	t.Helper()
	f := &fakeS3lvol{bases: map[string]templateBaseEntry{}, below: map[string]string{}}
	prev := s3lvolCall
	s3lvolCall = func(ctx context.Context, method string, params any, out any) error {
		f.calls = append(f.calls, method)
		p, _ := params.(map[string]any)
		str := func(k string) string { s, _ := p[k].(string); return s }
		var reply any
		switch method {
		case "rcow_get_template_bases":
			r := templateBasesReply{ExportTemplateDelta: f.delta}
			for _, b := range f.bases {
				r.Bases = append(r.Bases, b)
			}
			if u := str("base_uuid"); u != "" {
				r.BaseUUID = u
				if b, ok := f.bases[u]; ok && b.Valid {
					r.Present, r.Snapshot = true, b.Snapshot
				}
			}
			if l := str("lvol_name"); l != "" {
				r.BaseBelow, r.LvolFound = f.below[l], true
			}
			reply = r
		case "rcow_mark_template_base":
			u := str("base_uuid")
			if b, ok := f.bases[u]; ok && b.Snapshot != str("snapshot_name") {
				return &s3lvolRPCError{Code: -17, Message: "File exists"}
			}
			f.marks = append(f.marks, [3]string{str("snapshot_name"), str("anchor_name"), u})
			f.bases[u] = templateBaseEntry{BaseUUID: u, Snapshot: str("snapshot_name"), Anchor: str("anchor_name"), Role: "imported", Valid: true}
		case "rcow_export_snapshot":
			if f.exportErr != nil {
				return f.exportErr
			}
			f.exports = append(f.exports, str("snapshot_name"))
			if p["release_safe"] != true {
				return errors.New("template bases must be exported release_safe")
			}
			reply = rpcStringValue{BoolValue: true, StringValue: fmt.Sprintf("11111111-2222-3333-4444-%012d", len(f.exports))}
		case "rcow_get_snapshot_status":
			reply = map[string]string{"export_status": "DONE"}
		case "rcow_get_exports":
			reply = f.priorExports
		default:
			return errors.New("unexpected " + method)
		}
		if out != nil && reply != nil {
			raw, _ := json.Marshal(reply)
			return json.Unmarshal(raw, out)
		}
		return nil
	}
	t.Cleanup(func() { s3lvolCall = prev })
	return f
}

const testBase = "6d1f2a90-1b2c-4d3e-8f40-5a6b7c8d9e01"

// Several bases of one template must coexist: the name carries the base, so a
// rebuild never replaces the base an old paused sandbox still needs.
func TestTemplateBaseNamesAreVersioned(t *testing.T) {
	a := TemplateBaseSnapshotName("tpl-x", testBase, TemplateBaseRoleRootfs)
	b := TemplateBaseSnapshotName("tpl-x", "aaaaaaaa-1b2c-4d3e-8f40-5a6b7c8d9e01", TemplateBaseRoleRootfs)
	require.Equal(t, "tpl-tpl-x-b6d1f2a901b2c-rootfs", a)
	require.NotEqual(t, a, b)
	require.NotEqual(t, a, TemplateBaseSnapshotName("tpl-x", testBase, TemplateBaseRoleMemory))
	require.NotEqual(t, a, cowTemplateRootfsName("tpl-x"), "must not clash with the legacy name")
	require.Equal(t, "tbase-6d1f2a901b2c-memory", TemplateBaseSnapshotName("", testBase, TemplateBaseRoleMemory))
	require.Equal(t, a+"-anchor", TemplateBaseAnchorName("tpl-x", testBase, TemplateBaseRoleRootfs))
	require.True(t, isInstalledBaseName(a))
	require.True(t, isInstalledBaseName("tbase-6d1f2a901b2c-memory"))
	require.False(t, isInstalledBaseName(cowTemplateRootfsName("tpl-x")))
	require.False(t, isInstalledBaseName("sb-1-rootfs-gen0"))
}

func TestLookupTemplateBase(t *testing.T) {
	f := useFakeS3lvol(t)
	snap, err := LookupTemplateBase(context.Background(), testBase)
	require.NoError(t, err)
	require.Empty(t, snap)

	f.bases[testBase] = templateBaseEntry{BaseUUID: testBase, Snapshot: "tpl-x-rootfs", Valid: true}
	snap, err = LookupTemplateBase(context.Background(), testBase)
	require.NoError(t, err)
	require.Equal(t, "tpl-x-rootfs", snap)

	// An invalid mark (anchor written to, parent replaced) is not a base.
	f.bases[testBase] = templateBaseEntry{BaseUUID: testBase, Snapshot: "tpl-x-rootfs", Valid: false}
	snap, err = LookupTemplateBase(context.Background(), testBase)
	require.NoError(t, err)
	require.Empty(t, snap)
}

// Install: import the base export, wait for it to be local, snapshot under the
// versioned name, mark. The import origin is checked before anything else.
func TestInstallTemplateBase(t *testing.T) {
	f := useFakeS3lvol(t)
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)

	snap, err := InstallTemplateBase(context.Background(), "tpl-x", TemplateBaseRoleRootfs, testBase)
	require.NoError(t, err)
	want := TemplateBaseSnapshotName("tpl-x", testBase, TemplateBaseRoleRootfs)
	require.Equal(t, want, snap)
	require.Equal(t, [][2]string{{want + "-anchor", testBase}}, engine.importedLvols)
	require.Equal(t, [][2]string{{want + "-anchor", want}}, engine.createSnapshots)
	require.Equal(t, [][3]string{{want, want + "-anchor", testBase}}, f.marks)
	require.Contains(t, engine.deactivatedVolumes, want+"-anchor")

	// Idempotent: a second install finds the base and does nothing.
	snap, err = InstallTemplateBase(context.Background(), "tpl-x", TemplateBaseRoleRootfs, testBase)
	require.NoError(t, err)
	require.Equal(t, want, snap)
	require.Len(t, engine.importedLvols, 1)
}

// A base whose export is gone fails closed with ErrTemplateBaseMissing; the
// restore must not fall back to anything that could mix bases.
func TestInstallTemplateBaseFailsClosed(t *testing.T) {
	useFakeS3lvol(t)
	engine := &fakeCowEngine{importErr: &cubecow.CowError{Code: cubecow.SemNotFound, RawRC: int32(cubecow.SemNotFound), Message: "no such export"}}
	useTestCowStorageAt(t, engine)

	err := ensureDeltaBase(context.Background(), "tpl-x", TemplateBaseRoleMemory, testBase)
	require.ErrorIs(t, err, ErrTemplateBaseMissing)
	require.Empty(t, engine.createSnapshots, "nothing may be snapshotted from a failed import")

	// No base named: nothing to do (a full export).
	require.NoError(t, ensureDeltaBase(context.Background(), "tpl-x", TemplateBaseRoleMemory, ""))
}

// An anchor whose origin names another export is not this base.
func TestInstallTemplateBaseRefusesWrongOrigin(t *testing.T) {
	useFakeS3lvol(t)
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)
	anchor := TemplateBaseAnchorName("tpl-x", testBase, TemplateBaseRoleRootfs)
	engine.volumeInfos = map[string]*cubecow.Volume{anchor: {Name: anchor, VolumeUUID: "v", SizeBytes: 1,
		ImportOrigin: &cubecow.ImportOrigin{Version: 1, VolumeUUID: "v", LvstoreUUID: "s", ExportUUID: "other", SizeBytes: 1}}}

	_, err := InstallTemplateBase(context.Background(), "tpl-x", TemplateBaseRoleRootfs, testBase)
	require.Error(t, err)
	require.Contains(t, err.Error(), "refuse base anchor")
	require.Empty(t, engine.createSnapshots)
}

// A not-yet-local base (prefetch failed) is never snapshotted.
func TestInstallTemplateBaseWaitsForLocal(t *testing.T) {
	useFakeS3lvol(t)
	engine := &fakeCowEngine{importStatusFn: func(string, int) (string, error) {
		return `{"state":"failed","done":false,"error":-5,"error_msg":"Input/output error"}`, nil
	}}
	useTestCowStorageAt(t, engine)
	_, err := InstallTemplateBase(context.Background(), "tpl-x", TemplateBaseRoleRootfs, testBase)
	require.Error(t, err)
	require.Empty(t, engine.createSnapshots)
}

func TestPublishTemplateBases(t *testing.T) {
	f := useFakeS3lvol(t)
	engine := &fakeCowEngine{volumeInfos: map[string]*cubecow.Volume{
		cowTemplateMemoryName("tpl-x") + "-snap": {Name: "m"},
		S3MetadataSnapshotName("tpl-x"):          {Name: "md"},
	}}
	useTestCowStorageAt(t, engine)

	b, err := PublishTemplateBases(context.Background(), "tpl-x")
	require.NoError(t, err)
	require.Len(t, f.exports, 3)
	require.NotEmpty(t, b.Rootfs)
	require.NotEmpty(t, b.Memory)
	require.NotEqual(t, b.Rootfs, b.Memory)
	require.Len(t, f.marks, 3)

	// Publishing again reuses the marks rather than exporting twice.
	_, err = PublishTemplateBases(context.Background(), "tpl-x")
	require.NoError(t, err)
	require.Len(t, f.exports, 3)
}

// A publish whose export already exists (an earlier attempt failed after the
// export started) must adopt the finished dense export instead of failing
// forever with "File exists". Live 2026-11-15: every reconcile tick failed.
func TestPublishTemplateBasesAdoptsFinishedExport(t *testing.T) {
	f := useFakeS3lvol(t)
	useTestCowStorageAt(t, &fakeCowEngine{volumeInfos: map[string]*cubecow.Volume{}})
	f.exportErr = &s3lvolRPCError{Code: -32603, Message: "could not start exporting snapshot: File exists"}
	rootfs := cowTemplateRootfsName("tpl-x")
	f.priorExports = []map[string]any{
		{"export_uuid": "aaaaaaaa-0000-0000-0000-000000000001", "snapshot": rootfs, "layout": "ref", "generation": 0},
		{"export_uuid": "aaaaaaaa-0000-0000-0000-000000000002", "snapshot": rootfs, "layout": "dense", "generation": 0, "publication_pending": true},
		{"export_uuid": "aaaaaaaa-0000-0000-0000-000000000003", "snapshot": rootfs, "layout": "dense", "generation": 0},
		{"export_uuid": "aaaaaaaa-0000-0000-0000-000000000004", "snapshot": "other", "layout": "dense", "generation": 0},
	}
	b, err := PublishTemplateBases(context.Background(), "tpl-x")
	require.NoError(t, err)
	require.Equal(t, "aaaaaaaa-0000-0000-0000-000000000003", b.Rootfs)
	require.Empty(t, b.Memory, "no memory snapshot on this node")

	// Nothing adoptable: the original error stands.
	f2 := useFakeS3lvol(t)
	f2.exportErr = errors.New("File exists")
	_, err = PublishTemplateBases(context.Background(), "tpl-y")
	require.ErrorContains(t, err, "File exists")
}

// s3lvol wraps its JSON-document replies in {bool_value, string_value}.
func TestDecodeS3lvolResultUnwrapsEnvelope(t *testing.T) {
	var bases templateBasesReply
	raw := json.RawMessage(`{"bool_value":true,"string_value":"{\"base_uuid\":\"u\",\"snapshot\":\"s\",\"present\":true,\"bases\":[],\"export_template_delta\":true}"}`)
	require.NoError(t, decodeS3lvolResult("rcow_get_template_bases", raw, &bases))
	require.True(t, bases.Present)
	require.Equal(t, "s", bases.Snapshot)
	require.True(t, bases.ExportTemplateDelta)

	var st struct {
		ExportStatus string `json:"export_status"`
	}
	raw = json.RawMessage(`{"bool_value":true,"string_value":"{\"export_status\":\"DONE\",\"deletable\":\"YES\"}"}`)
	require.NoError(t, decodeS3lvolResult("rcow_get_snapshot_status", raw, &st))
	require.Equal(t, "DONE", st.ExportStatus)

	var list []map[string]any
	require.NoError(t, decodeS3lvolResult("rcow_get_exports", json.RawMessage(`[{"export_uuid":"e"}]`), &list), "bare documents still decode")
	require.Len(t, list, 1)

	var env rpcStringValue
	raw = json.RawMessage(`{"bool_value":true,"string_value":"6d1f2a90-1b2c-4d3e-8f40-5a6b7c8d9e01"}`)
	require.NoError(t, decodeS3lvolResult("rcow_export_snapshot", raw, &env))
	require.Equal(t, testBase, env.StringValue, "callers that want the envelope get it")

	raw = json.RawMessage(`{"bool_value":false,"string_value":"lvol not found"}`)
	require.ErrorContains(t, decodeS3lvolResult("rcow_get_template_bases", raw, &bases), "lvol not found")

	// Refusals travel as bool_value false for envelope callers and callers
	// without an out too: rcow_export_snapshot must not take the message for
	// a uuid, and rcow_mark_template_base must not swallow it.
	raw = json.RawMessage(`{"bool_value":false,"string_value":"could not start exporting snapshot 's': File exists"}`)
	env = rpcStringValue{}
	require.ErrorContains(t, decodeS3lvolResult("rcow_export_snapshot", raw, &env), "File exists")
	require.Empty(t, env.StringValue)
	require.ErrorContains(t, decodeS3lvolResult("rcow_mark_template_base", raw, nil), "File exists")
	require.NoError(t, decodeS3lvolResult("rcow_mark_template_base", json.RawMessage(`{"bool_value":true,"string_value":"u","anchor":"a"}`), nil))
}

func TestHandleTemplateBaseRequest(t *testing.T) {
	f := useFakeS3lvol(t)
	handled, _, _ := HandleTemplateBaseRequest(context.Background(), map[string]string{"x": "y"})
	require.False(t, handled, "ordinary CreateImage passes through")

	t.Setenv(envTemplateS3Distribution, "")
	handled, _, err := HandleTemplateBaseRequest(context.Background(), map[string]string{TemplateBaseOpAnnotation: "publish"})
	require.True(t, handled)
	require.Error(t, err, "publish is refused while the flag is off")

	f.bases[testBase] = templateBaseEntry{BaseUUID: testBase, Snapshot: "s", Valid: true}
	handled, msg, err := HandleTemplateBaseRequest(context.Background(), map[string]string{
		TemplateBaseOpAnnotation: "lookup", TemplateBaseUUIDAnnotation: testBase})
	require.True(t, handled)
	require.NoError(t, err)
	require.Contains(t, msg, `"present":true`)

	_, _, err = HandleTemplateBaseRequest(context.Background(), map[string]string{
		TemplateBaseOpAnnotation: "install", TemplateBaseRoleAnnotation: "disk"})
	require.Error(t, err)
	_, _, err = HandleTemplateBaseRequest(context.Background(), map[string]string{TemplateBaseOpAnnotation: "frob"})
	require.Error(t, err)
}

// The base recorded at pause comes from s3lvol, and only when its writer is on.
func TestExportedBaseOfFollowsTheWriterFlag(t *testing.T) {
	f := useFakeS3lvol(t)
	f.below["tpl-snap-1-rootfs"] = testBase
	require.Empty(t, exportedBaseOf(context.Background(), "tpl-snap-1-rootfs"), "writer off: full export, no base")
	f.delta = true
	require.Equal(t, testBase, exportedBaseOf(context.Background(), "tpl-snap-1-rootfs"))
}

func TestRemoteUUIDsCarryBasesBackwardCompatibly(t *testing.T) {
	u := cow.ParseRemoteUUIDs(`{"rootfs":"r","memory":"m","metadata":"x","rootfs_base":"b1","memory_base":"b2","template_id":"tpl-x"}`)
	require.Equal(t, "b1", u.RootfsBase)
	require.Equal(t, "b2", u.MemoryBase)
	require.Equal(t, "tpl-x", u.TemplateID)
	old := cow.ParseRemoteUUIDs(`{"rootfs":"r","memory":"m","metadata":"x"}`)
	require.Empty(t, old.RootfsBase)
	require.False(t, strings.Contains((&cow.RemoteUUIDs{Rootfs: "r"}).JSON(), "base"), "omitted when empty")
}

const (
	testMemBase  = "7e2f3b01-2c3d-4e4f-9051-6b7c8d9e0f12"
	testMetaBase = "8f304c12-3d4e-4f50-a162-7c8d9e0f1023"
)

func withTemplateMemory(t *testing.T, has bool) {
	t.Helper()
	prev := templateHasLocalMemory
	templateHasLocalMemory = func(context.Context, map[string]string) bool { return has }
	t.Cleanup(func() { templateHasLocalMemory = prev })
}

func TestTemplateBaseSnapshotForIsGated(t *testing.T) {
	f := useFakeS3lvol(t)
	withTemplateMemory(t, false)
	f.bases[testBase] = templateBaseEntry{BaseUUID: testBase, Snapshot: "base-snap", Valid: true}
	f.bases[testMetaBase] = templateBaseEntry{BaseUUID: testMetaBase, Snapshot: "meta-snap", Valid: true}
	ann := map[string]string{templateBaseAnnotationRootfs: testBase, templateBaseAnnotationMetadata: testMetaBase}

	t.Setenv(envTemplateS3Distribution, "")
	require.Empty(t, templateBaseSnapshotFor(context.Background(), ann, "", TemplateBaseRoleRootfs))
	t.Setenv(envTemplateS3Distribution, "true")
	require.Equal(t, "base-snap", templateBaseSnapshotFor(context.Background(), ann, "", TemplateBaseRoleRootfs))
	require.Equal(t, "meta-snap", templateBaseSnapshotFor(context.Background(), ann, "", TemplateBaseRoleMetadata))
	require.Empty(t, templateBaseSnapshotFor(context.Background(), ann, "", TemplateBaseRoleMemory))
	// A base not on this node yet: fall back to the template's own snapshot.
	ann[templateBaseAnnotationRootfs] = "aaaaaaaa-1b2c-4d3e-8f40-5a6b7c8d9e01"
	require.Empty(t, templateBaseSnapshotFor(context.Background(), ann, "", TemplateBaseRoleRootfs))
	require.Empty(t, templateBaseSnapshotFor(context.Background(), ann, "", TemplateBaseRoleMetadata),
		"a missing rootfs base drops the metadata base too")
}

// Live 2026-11-17 (tpl-afe6, app2): the memory base was cloned while the
// vmstate came from the node's own build. Every role must come from the bases
// together, or none of them.
func TestTemplateBaseSetNeverMixesBuilds(t *testing.T) {
	t.Setenv(envTemplateS3Distribution, "true")
	f := useFakeS3lvol(t)
	withTemplateMemory(t, true)
	f.bases[testBase] = templateBaseEntry{BaseUUID: testBase, Snapshot: "root-base", Valid: true}
	f.bases[testMemBase] = templateBaseEntry{BaseUUID: testMemBase, Snapshot: "mem-base", Valid: true}
	f.bases[testMetaBase] = templateBaseEntry{BaseUUID: testMetaBase, Snapshot: "meta-base", Valid: true}
	full := map[string]string{
		templateBaseAnnotationRootfs:   testBase,
		templateBaseAnnotationMemory:   testMemBase,
		templateBaseAnnotationMetadata: testMetaBase,
	}
	ctx := context.Background()

	set := resolveTemplateBaseSet(ctx, full)
	require.Equal(t, templateBaseSet{Rootfs: "root-base", Memory: "mem-base", Metadata: "meta-base"}, set)

	// Old Master: memory base named, no metadata base. Must not clone memory.
	old := map[string]string{templateBaseAnnotationRootfs: testBase, templateBaseAnnotationMemory: testMemBase}
	require.True(t, resolveTemplateBaseSet(ctx, old).empty())

	// The template has memory but no memory base is named: local build only.
	noMem := map[string]string{templateBaseAnnotationRootfs: testBase, templateBaseAnnotationMetadata: testMetaBase}
	require.True(t, resolveTemplateBaseSet(ctx, noMem).empty())

	// Metadata base not installed here yet: nothing from the bases.
	delete(f.bases, testMetaBase)
	require.True(t, resolveTemplateBaseSet(ctx, full).empty())

	// Memory base invalid (anchor written to): nothing from the bases.
	f.bases[testMetaBase] = templateBaseEntry{BaseUUID: testMetaBase, Snapshot: "meta-base", Valid: true}
	f.bases[testMemBase] = templateBaseEntry{BaseUUID: testMemBase, Snapshot: "mem-base", Valid: false}
	require.True(t, resolveTemplateBaseSet(ctx, full).empty())
}

// The rootfs, memory and metadata steps of one create must see one decision,
// even when a base install lands between them.
func TestTemplateBaseSetIsDecidedOncePerSandbox(t *testing.T) {
	t.Setenv(envTemplateS3Distribution, "true")
	f := useFakeS3lvol(t)
	withTemplateMemory(t, true)
	full := map[string]string{
		templateBaseAnnotationRootfs:   testBase,
		templateBaseAnnotationMemory:   testMemBase,
		templateBaseAnnotationMetadata: testMetaBase,
	}
	ctx := context.Background()
	sb := "sb-decided-once"
	t.Cleanup(func() { forgetTemplateBaseSet(sb) })
	f.bases[testBase] = templateBaseEntry{BaseUUID: testBase, Snapshot: "root-base", Valid: true}
	f.bases[testMemBase] = templateBaseEntry{BaseUUID: testMemBase, Snapshot: "mem-base", Valid: true}

	// Metadata base missing at the rootfs step: the local build is chosen.
	require.Empty(t, templateBaseSnapshotFor(ctx, full, sb, TemplateBaseRoleRootfs))
	// The install completes before the memory and metadata steps run.
	f.bases[testMetaBase] = templateBaseEntry{BaseUUID: testMetaBase, Snapshot: "meta-base", Valid: true}
	require.Empty(t, templateBaseSnapshotFor(ctx, full, sb, TemplateBaseRoleMemory))
	require.Empty(t, TemplateBaseMetadataFor(ctx, full, sb))

	// A new sandbox sees the complete set.
	sb2 := "sb-decided-once-2"
	t.Cleanup(func() { forgetTemplateBaseSet(sb2) })
	require.Equal(t, "mem-base", templateBaseSnapshotFor(ctx, full, sb2, TemplateBaseRoleMemory))
	require.Equal(t, "meta-base", TemplateBaseMetadataFor(ctx, full, sb2))
	require.Equal(t, "root-base", templateBaseSnapshotFor(ctx, full, sb2, TemplateBaseRoleRootfs))
}

// The builder publishes the metadata disk with the memory image; a memory
// image without a sealed metadata snapshot is refused rather than published
// as a set that would make importers mix builds.
func TestPublishTemplateBasesIncludesMetadata(t *testing.T) {
	f := useFakeS3lvol(t)
	engine := &fakeCowEngine{volumeInfos: map[string]*cubecow.Volume{
		cowTemplateMemoryName("tpl-x") + "-snap": {Name: "m"},
		S3MetadataSnapshotName("tpl-x"):          {Name: "md"},
	}}
	useTestCowStorageAt(t, engine)
	b, err := PublishTemplateBases(context.Background(), "tpl-x")
	require.NoError(t, err)
	require.Equal(t, []string{cowTemplateRootfsName("tpl-x"), cowTemplateMemoryName("tpl-x") + "-snap",
		S3MetadataSnapshotName("tpl-x")}, f.exports)
	require.NotEmpty(t, b.Metadata)
	require.Len(t, f.marks, 3)

	f2 := useFakeS3lvol(t)
	useTestCowStorageAt(t, &fakeCowEngine{volumeInfos: map[string]*cubecow.Volume{
		cowTemplateMemoryName("tpl-y") + "-snap": {Name: "m"},
	}})
	_, err = PublishTemplateBases(context.Background(), "tpl-y")
	require.ErrorContains(t, err, "no sealed metadata snapshot")
	_ = f2
}

func TestInstallTemplateBaseMetadataRole(t *testing.T) {
	useFakeS3lvol(t)
	engine := &fakeCowEngine{}
	useTestCowStorageAt(t, engine)
	_, _, err := HandleTemplateBaseRequest(context.Background(), map[string]string{
		TemplateBaseOpAnnotation: "install", TemplateBaseTemplateAnnotation: "tpl-x",
		TemplateBaseRoleAnnotation: TemplateBaseRoleMetadata, TemplateBaseUUIDAnnotation: testMetaBase})
	require.NoError(t, err)
	want := TemplateBaseSnapshotName("tpl-x", testMetaBase, TemplateBaseRoleMetadata)
	require.Equal(t, [][2]string{{want + "-anchor", want}}, engine.createSnapshots)
	require.True(t, isInstalledBaseName(want))
}

// The async export worker reports export uuids only through UploadStatus; the
// bases its delta exports stand on must be recorded there too (live
// 2026-11-17: pause rows of base-cloned sandboxes had no rootfs_base).
func TestRecordRemoteRoleRecordsTheBase(t *testing.T) {
	f := useFakeS3lvol(t)
	f.delta = true
	f.below["tpl-snap-1-rootfs"] = testBase
	f.below["tpl-snap-1-memory-snap"] = testMemBase
	ctx := context.Background()
	u := &cow.RemoteUUIDs{}
	recordRemoteRole(ctx, u, "rootfs", "tpl-snap-1-rootfs", "r1")
	recordRemoteRole(ctx, u, "memory", "tpl-snap-1-memory-snap", "m1")
	recordRemoteRole(ctx, u, "metadata", "s3-meta-snap-1-snap", "d1")
	require.Equal(t, cow.RemoteUUIDs{Rootfs: "r1", RootfsBase: testBase, Memory: "m1", MemoryBase: testMemBase, Metadata: "d1"}, *u)

	// A different uuid for a role already recorded never overwrites it or
	// borrows its base.
	recordRemoteRole(ctx, u, "rootfs", "tpl-snap-2-rootfs", "r2")
	require.Equal(t, "r1", u.Rootfs)
	require.Equal(t, testBase, u.RootfsBase)

	// Writer off: no base recorded.
	f.delta = false
	u2 := &cow.RemoteUUIDs{}
	recordRemoteRole(ctx, u2, "rootfs", "tpl-snap-1-rootfs", "r1")
	require.Empty(t, u2.RootfsBase)
}

// A delta metadata export names its base, and the importer installs it first.
func TestRemoteUUIDsCarryMetadataBase(t *testing.T) {
	u := cow.ParseRemoteUUIDs(`{"rootfs":"r","metadata":"x","metadata_base":"b3"}`)
	require.Equal(t, "b3", u.MetadataBase)
}
