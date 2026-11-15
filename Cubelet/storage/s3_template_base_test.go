// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"encoding/json"
	"errors"
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
			reply = rpcStringValue{BoolValue: true, StringValue: "11111111-2222-3333-4444-55555555" + str("snapshot_name")[len(str("snapshot_name"))-4:]}
		case "rcow_get_snapshot_status":
			reply = map[string]string{"export_status": "DONE"}
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
	}}
	useTestCowStorageAt(t, engine)

	b, err := PublishTemplateBases(context.Background(), "tpl-x")
	require.NoError(t, err)
	require.Equal(t, []string{cowTemplateRootfsName("tpl-x"), cowTemplateMemoryName("tpl-x") + "-snap"}, f.exports)
	require.NotEmpty(t, b.Rootfs)
	require.NotEmpty(t, b.Memory)
	require.NotEqual(t, b.Rootfs, b.Memory)
	require.Len(t, f.marks, 2)

	// Publishing again reuses the marks rather than exporting twice.
	_, err = PublishTemplateBases(context.Background(), "tpl-x")
	require.NoError(t, err)
	require.Len(t, f.exports, 2)
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

func TestTemplateBaseSnapshotForIsGated(t *testing.T) {
	f := useFakeS3lvol(t)
	f.bases[testBase] = templateBaseEntry{BaseUUID: testBase, Snapshot: "base-snap", Valid: true}
	ann := map[string]string{templateBaseAnnotationRootfs: testBase}

	t.Setenv(envTemplateS3Distribution, "")
	require.Empty(t, templateBaseSnapshotFor(context.Background(), ann, TemplateBaseRoleRootfs))
	t.Setenv(envTemplateS3Distribution, "true")
	require.Equal(t, "base-snap", templateBaseSnapshotFor(context.Background(), ann, TemplateBaseRoleRootfs))
	require.Empty(t, templateBaseSnapshotFor(context.Background(), ann, TemplateBaseRoleMemory))
	// A base not on this node yet: fall back to the template's own snapshot.
	ann[templateBaseAnnotationRootfs] = "aaaaaaaa-1b2c-4d3e-8f40-5a6b7c8d9e01"
	require.Empty(t, templateBaseSnapshotFor(context.Background(), ann, TemplateBaseRoleRootfs))
}
