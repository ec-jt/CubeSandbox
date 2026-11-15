// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

// Template bases: one build, identical bytes on every node.
//
// A template built on node A and rebuilt on node B yields two different
// snapshots, so a sandbox paused on A cannot be restored on B as a delta: B
// has nothing byte-identical to put under it. The fix is to build once and
// distribute the result:
//
//   - publish (builder node): export the template's rootfs and memory
//     snapshots release-safe; the export uuid becomes the base uuid. The
//     snapshot is marked as template base <uuid> in s3lvol.
//   - install (every other node): import that export, wait for it to be
//     fully local (decouple), snapshot it under a versioned name, and mark
//     the snapshot as template base <uuid>. The import origin is the proof of
//     identity; s3lvol refuses the mark without it.
//
// Several bases of one template may live on a node at once (a rebuild
// publishes a new uuid). Names carry a short form of the base uuid, so a new
// base never replaces an old one; the old one goes only when nothing reads it.
//
// s3lvol does the delta work. With S3LVOL_EXPORT_TEMPLATE_DELTA on, a pause
// export of a sandbox cloned from a base names only the sandbox's own chunks,
// and the importing node clones its own copy of the base under them. This
// file supplies the base where the manifest needs one.
//
// Gated by CUBE_TEMPLATE_S3_DISTRIBUTION (default off) for everything that
// changes which snapshot a new sandbox is cloned from. Installing a base that
// a delta manifest names is not gated: a delta can only exist once the writer
// was enabled, and refusing its base would fail the restore.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	CubeLog "github.com/tencentcloud/CubeSandbox/pkgs/CubeLog"
)

const (
	envTemplateS3Distribution = "CUBE_TEMPLATE_S3_DISTRIBUTION"

	// TemplateBaseRoleRootfs and TemplateBaseRoleMemory name the two bases a
	// template has.
	TemplateBaseRoleRootfs = "rootfs"
	TemplateBaseRoleMemory = "memory"

	templateBaseRPCTimeout     = 30 * time.Second
	templateBaseExportDeadline = 15 * time.Minute
)

// ErrTemplateBaseMissing is a delta that names a base this node does not have
// and cannot install. Never papered over with a full copy.
var ErrTemplateBaseMissing = errors.New("template base is not available on this node")

// TemplateS3DistributionEnabled reports CUBE_TEMPLATE_S3_DISTRIBUTION.
func TemplateS3DistributionEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(envTemplateS3Distribution))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// TemplateBases is what a publish produces and what Master stores per
// template build.
type TemplateBases struct {
	TemplateID string `json:"template_id"`
	Rootfs     string `json:"rootfs,omitempty"`
	Memory     string `json:"memory,omitempty"`
}

func shortBase(baseUUID string) string {
	u := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(baseUUID)), "-", "")
	if len(u) > 12 {
		u = u[:12]
	}
	return u
}

// TemplateBaseSnapshotName is the versioned name an installed base lives
// under: tpl-<template>-b<short>-<role>. Two builds of one template therefore
// never collide. An empty template id (a restore that only knows the uuid)
// gives tbase-<short>-<role>.
func TemplateBaseSnapshotName(templateID, baseUUID, role string) string {
	if t := strings.TrimSpace(templateID); t != "" {
		return fmt.Sprintf("tpl-%s-b%s-%s", t, shortBase(baseUUID), role)
	}
	return fmt.Sprintf("tbase-%s-%s", shortBase(baseUUID), role)
}

// TemplateBaseAnchorName is the empty clone that carries the base mark.
func TemplateBaseAnchorName(templateID, baseUUID, role string) string {
	return TemplateBaseSnapshotName(templateID, baseUUID, role) + "-anchor"
}

// templateBaseSourceName is the snapshot a builder publishes for role.
func templateBaseSourceName(templateID, role string) string {
	if role == TemplateBaseRoleMemory {
		return cowTemplateMemoryName(templateID) + "-snap"
	}
	return cowTemplateRootfsName(templateID)
}

// ---------------------------------------------------------------------------
// s3lvol JSON-RPC. cubecow does not expose the template-base calls, so they go
// to the s3lvol socket directly. One short-lived connection per call.
// ---------------------------------------------------------------------------

type s3lvolRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *s3lvolRPCError) Error() string {
	return fmt.Sprintf("s3lvol rpc error %d: %s", e.Code, e.Message)
}

var s3lvolRPCID atomic.Uint64

// s3lvolSocketPath is resolved from the storage config; tests override.
var s3lvolSocketPath = func() string {
	if localStorage != nil && localStorage.config != nil {
		cfg := localStorage.config.s3BackendConfig()
		if cfg.S3.SocketPath != nil && strings.TrimSpace(*cfg.S3.SocketPath) != "" {
			return strings.TrimSpace(*cfg.S3.SocketPath)
		}
	}
	return defaultS3SocketPath
}

// s3lvolCall is the seam the tests replace.
var s3lvolCall = func(ctx context.Context, method string, params any, out any) error {
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "unix", s3lvolSocketPath())
	if err != nil {
		return fmt.Errorf("s3lvol %s: dial: %w", method, err)
	}
	defer conn.Close()
	deadline := time.Now().Add(templateBaseRPCTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = conn.SetDeadline(deadline)

	req := map[string]any{"jsonrpc": "2.0", "method": method, "id": s3lvolRPCID.Add(1)}
	if params != nil {
		req["params"] = params
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return fmt.Errorf("s3lvol %s: write: %w", method, err)
	}
	var resp struct {
		Result json.RawMessage `json:"result"`
		Error  *s3lvolRPCError `json:"error"`
	}
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return fmt.Errorf("s3lvol %s: read: %w", method, err)
	}
	if resp.Error != nil {
		return fmt.Errorf("s3lvol %s: %w", method, resp.Error)
	}
	return decodeS3lvolResult(method, resp.Result, out)
}

// decodeS3lvolResult decodes an s3lvol reply into out. Most s3lvol replies,
// including every JSON-document reply (rcow_get_template_bases,
// rcow_get_snapshot_status, rcow_get_exports), travel inside the envelope
// {"bool_value": true, "string_value": "<document>"}, and most failures come
// back the same way with bool_value false and the message in string_value --
// not as a JSON-RPC error. So:
//   - bool_value false is an error for every caller, including out == nil
//     (rcow_mark_template_base) and *rpcStringValue (rcow_export_snapshot,
//     which otherwise took the error text for an export uuid);
//   - a caller that asks for the envelope (*rpcStringValue) gets it as is;
//   - any other out gets the document carried in string_value. Decoding the
//     envelope straight into a document struct succeeds with every field
//     empty, which is how a live base looked absent and every export status
//     looked like "".
func decodeS3lvolResult(method string, raw json.RawMessage, out any) error {
	if len(raw) == 0 {
		return nil
	}
	var env struct {
		BoolValue   *bool   `json:"bool_value"`
		StringValue *string `json:"string_value"`
	}
	isEnvelope := json.Unmarshal(raw, &env) == nil && env.BoolValue != nil && env.StringValue != nil
	if isEnvelope && !*env.BoolValue {
		return fmt.Errorf("s3lvol %s: %s", method, *env.StringValue)
	}
	if out == nil {
		return nil
	}
	if _, wantsEnvelope := out.(*rpcStringValue); !wantsEnvelope && isEnvelope {
		doc := strings.TrimSpace(*env.StringValue)
		if strings.HasPrefix(doc, "{") || strings.HasPrefix(doc, "[") {
			raw = json.RawMessage(doc)
		}
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("s3lvol %s: decode: %w", method, err)
	}
	return nil
}

type templateBaseEntry struct {
	BaseUUID string `json:"base_uuid"`
	Anchor   string `json:"anchor"`
	Snapshot string `json:"snapshot"`
	Role     string `json:"role"`
	Valid    bool   `json:"valid"`
	Error    int    `json:"error"`
}

type templateBasesReply struct {
	BaseUUID            string              `json:"base_uuid"`
	Snapshot            string              `json:"snapshot"`
	Present             bool                `json:"present"`
	Bases               []templateBaseEntry `json:"bases"`
	LvolFound           bool                `json:"lvol_found"`
	BaseBelow           string              `json:"base_below"`
	ExportTemplateDelta bool                `json:"export_template_delta"`
}

func getTemplateBases(ctx context.Context, params map[string]any) (*templateBasesReply, error) {
	var out templateBasesReply
	if err := s3lvolCall(ctx, "rcow_get_template_bases", params, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// LookupTemplateBase answers the local snapshot that is verified template
// base baseUUID -- by import origin on an importing node, by the node's own
// export on the builder -- or "" when there is none.
func LookupTemplateBase(ctx context.Context, baseUUID string) (string, error) {
	baseUUID = strings.TrimSpace(baseUUID)
	if baseUUID == "" {
		return "", nil
	}
	r, err := getTemplateBases(ctx, map[string]any{"base_uuid": baseUUID})
	if err != nil {
		return "", err
	}
	if !r.Present {
		return "", nil
	}
	return r.Snapshot, nil
}

// templateBaseBelow answers the base a delta export of lvol's next snapshot
// would be relative to, but only when the s3lvol writer is actually on.
func templateBaseBelow(ctx context.Context, lvol string) (string, error) {
	r, err := getTemplateBases(ctx, map[string]any{"lvol_name": lvol})
	if err != nil {
		return "", err
	}
	if !r.ExportTemplateDelta || !r.LvolFound {
		return "", nil
	}
	return strings.TrimSpace(r.BaseBelow), nil
}

func markTemplateBase(ctx context.Context, snapshot, anchor, baseUUID string) error {
	return s3lvolCall(ctx, "rcow_mark_template_base", map[string]any{
		"snapshot_name": snapshot, "anchor_name": anchor, "base_uuid": baseUUID,
	}, nil)
}

type rpcStringValue struct {
	BoolValue   bool   `json:"bool_value"`
	StringValue string `json:"string_value"`
}

// publishOne exports source release-safe, waits until the export is durable,
// and marks source as template base <export uuid>. Idempotent: a source that
// already carries a valid mark answers its uuid without exporting again.
func publishOne(ctx context.Context, templateID, role string) (string, error) {
	source := templateBaseSourceName(templateID, role)
	existing, err := getTemplateBases(ctx, nil)
	if err != nil {
		return "", err
	}
	for _, b := range existing.Bases {
		if b.Valid && b.Snapshot == source {
			return b.BaseUUID, nil
		}
	}

	var exp rpcStringValue
	uuid := ""
	if err := s3lvolCall(ctx, "rcow_export_snapshot", map[string]any{
		"snapshot_name": source, "release_safe": true,
	}, &exp); err != nil {
		// A release-safe export is refused once the snapshot already has a
		// published export -- typically this publish's own export from an
		// earlier attempt that failed after the export started. Adopt that
		// export when it is a finished dense one; anything else stays an
		// error.
		prior, perr := priorDenseExport(ctx, source)
		if perr != nil || prior == "" {
			return "", fmt.Errorf("export %s %s: %w", role, source, err)
		}
		CubeLog.Infof("template %s: %s export of %s refused (%v); adopting finished export %s",
			templateID, role, source, err, prior)
		uuid = prior
	} else {
		uuid = strings.TrimSpace(exp.StringValue)
	}
	if uuid == "" {
		return "", fmt.Errorf("export %s %s returned no uuid", role, source)
	}
	if err := waitExportDone(ctx, uuid); err != nil {
		return "", fmt.Errorf("export %s %s (%s): %w", role, source, uuid, err)
	}
	if err := markTemplateBase(ctx, source, TemplateBaseAnchorName(templateID, uuid, role), uuid); err != nil {
		return "", fmt.Errorf("mark %s %s as base %s: %w", role, source, uuid, err)
	}
	CubeLog.Infof("template %s: published %s base %s from %s", templateID, role, uuid, source)
	return uuid, nil
}

// priorDenseExport answers the uuid of a finished dense export of snapshot, or
// "" when there is none. Only dense exports qualify: they carry every chunk
// of the snapshot themselves, which is what a base must be.
func priorDenseExport(ctx context.Context, snapshot string) (string, error) {
	var exports []struct {
		ExportUUID         string `json:"export_uuid"`
		Snapshot           string `json:"snapshot"`
		Layout             string `json:"layout"`
		Generation         uint32 `json:"generation"`
		PublicationPending bool   `json:"publication_pending"`
		Reaping            bool   `json:"reaping"`
	}
	if err := s3lvolCall(ctx, "rcow_get_exports", nil, &exports); err != nil {
		return "", err
	}
	for _, e := range exports {
		if e.Snapshot != snapshot || e.Layout != "dense" || e.Generation != 0 ||
			e.PublicationPending || e.Reaping || strings.TrimSpace(e.ExportUUID) == "" {
			continue
		}
		var st struct {
			ExportStatus string `json:"export_status"`
		}
		if err := s3lvolCall(ctx, "rcow_get_snapshot_status", map[string]any{"export_uuid": e.ExportUUID}, &st); err != nil {
			continue
		}
		if strings.EqualFold(st.ExportStatus, "DONE") {
			return e.ExportUUID, nil
		}
	}
	return "", nil
}

var exportPollInterval = 500 * time.Millisecond

func waitExportDone(ctx context.Context, uuid string) error {
	ctx, cancel := context.WithTimeout(ctx, templateBaseExportDeadline)
	defer cancel()
	for {
		var st struct {
			ExportStatus string `json:"export_status"`
		}
		if err := s3lvolCall(ctx, "rcow_get_snapshot_status", map[string]any{"export_uuid": uuid}, &st); err != nil {
			return err
		}
		switch strings.ToUpper(st.ExportStatus) {
		case "DONE":
			return nil
		case "INPROGRESS":
		default:
			return fmt.Errorf("export status %q", st.ExportStatus)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(exportPollInterval):
		}
	}
}

// PublishTemplateBases runs on the node that built templateID.
func PublishTemplateBases(ctx context.Context, templateID string) (*TemplateBases, error) {
	templateID = strings.TrimSpace(templateID)
	if templateID == "" {
		return nil, fmt.Errorf("template id is required")
	}
	out := &TemplateBases{TemplateID: templateID}
	var err error
	if out.Rootfs, err = publishOne(ctx, templateID, TemplateBaseRoleRootfs); err != nil {
		return nil, err
	}
	store, serr := requireS3Cow()
	if serr != nil {
		return nil, serr
	}
	memSnap := templateBaseSourceName(templateID, TemplateBaseRoleMemory)
	present, perr := cowObjectPresent(store.GetVolumeInfo(ctx, memSnap))
	if perr != nil {
		return nil, perr
	}
	if present {
		if out.Memory, err = publishOne(ctx, templateID, TemplateBaseRoleMemory); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// InstallTemplateBase makes base baseUUID local: import, wait for it to be
// fully local, snapshot under the versioned name, mark. Answers the base
// snapshot's name. Idempotent, and safe to resume after a crash at any step.
func InstallTemplateBase(ctx context.Context, templateID, role, baseUUID string) (string, error) {
	baseUUID = strings.TrimSpace(baseUUID)
	if baseUUID == "" {
		return "", fmt.Errorf("base uuid is required")
	}
	if snap, err := LookupTemplateBase(ctx, baseUUID); err != nil {
		return "", err
	} else if snap != "" {
		return snap, nil
	}
	store, err := requireS3Cow()
	if err != nil {
		return "", err
	}
	anchor := TemplateBaseAnchorName(templateID, baseUUID, role)
	snap := TemplateBaseSnapshotName(templateID, baseUUID, role)

	snapInfo, snapErr := store.GetVolumeInfo(ctx, snap)
	snapPresent, err := cowObjectPresent(snapInfo, snapErr)
	if err != nil {
		return "", err
	}
	if !snapPresent {
		importer, ok := store.engine.(cowVolumeImporter)
		if !ok {
			return "", fmt.Errorf("cubecow engine cannot import")
		}
		info, lookupErr := store.GetVolumeInfo(ctx, anchor)
		present, err := cowObjectPresent(info, lookupErr)
		if err != nil {
			return "", err
		}
		if !present {
			if _, err := importer.ImportLvol(anchor, baseUUID); err != nil && !isCowSemantic(err, cubecow.SemAlreadyExists) {
				if isCowSemantic(err, cubecow.SemNotFound) {
					return "", fmt.Errorf("%w: base %s export is gone: %v", ErrTemplateBaseMissing, baseUUID, err)
				}
				return "", fmt.Errorf("import base %s as %s: %w", baseUUID, anchor, err)
			}
			if info, err = store.GetVolumeInfo(ctx, anchor); err != nil {
				return "", err
			}
		}
		if err := requireImportedOrigin(info, anchor, baseUUID); err != nil {
			return "", fmt.Errorf("refuse base anchor %s: %w", anchor, err)
		}
		statuser, ok := store.engine.(cowImportStatuser)
		if !ok {
			return "", fmt.Errorf("cubecow engine cannot report import status")
		}
		if err := waitImportPrefetch(ctx, statuser, anchor, "template base "+role, importPrefetchTimeout); err != nil {
			return "", fmt.Errorf("base %s is not local: %w", baseUUID, err)
		}
		if _, err := store.engine.CreateSnapshotFromVolume(anchor, snap, false); err != nil &&
			!isCowSemantic(err, cubecow.SemAlreadyExists) {
			return "", fmt.Errorf("snapshot base %s: %w", baseUUID, err)
		}
	}
	if err := markTemplateBase(ctx, snap, anchor, baseUUID); err != nil {
		return "", fmt.Errorf("mark %s as base %s: %w", snap, baseUUID, err)
	}
	// The anchor is never written; nothing needs its device.
	if err := store.engine.DeactivateVolume(anchor); err != nil && !isCowSemantic(err, cubecow.SemNotFound) {
		CubeLog.Warnf("template base %s: deactivate anchor %s: %v", baseUUID, anchor, err)
	}
	got, err := LookupTemplateBase(ctx, baseUUID)
	if err != nil {
		return "", err
	}
	if got != snap {
		return "", fmt.Errorf("base %s installed as %s but resolves to %q", baseUUID, snap, got)
	}
	CubeLog.Infof("template %s: installed %s base %s as %s", templateID, role, baseUUID, snap)
	return snap, nil
}

// ReleaseTemplateBase deletes an installed base and its anchor. blobstore
// refuses to delete a snapshot that still has clones, so a base some sandbox
// still reads is never removed here, whatever the caller believed.
func ReleaseTemplateBase(ctx context.Context, baseUUID string) error {
	snap, err := LookupTemplateBase(ctx, baseUUID)
	if err != nil || snap == "" {
		return err
	}
	r, err := getTemplateBases(ctx, nil)
	if err != nil {
		return err
	}
	store, err := requireS3Cow()
	if err != nil {
		return err
	}
	builder := false
	for _, b := range r.Bases {
		if b.BaseUUID != baseUUID || !b.Valid {
			continue
		}
		builder = builder || b.Role == "builder"
		if err := store.DeleteByKind(ctx, b.Anchor, cowKindVolume); err != nil && !isCowSemantic(err, cubecow.SemNotFound) {
			return fmt.Errorf("delete base anchor %s: %w", b.Anchor, err)
		}
	}
	if builder || !isInstalledBaseName(snap) {
		// A builder's base is the template's own snapshot; the template
		// lifecycle owns it. Only the mark (the anchor) goes.
		return nil
	}
	if err := store.DeleteByKind(ctx, snap, cowKindSnapshot); err != nil && !isCowSemantic(err, cubecow.SemNotFound) {
		return fmt.Errorf("delete base snapshot %s: %w", snap, err)
	}
	return nil
}

// isInstalledBaseName is true for names TemplateBaseSnapshotName produces.
func isInstalledBaseName(name string) bool {
	if strings.HasPrefix(name, "tbase-") {
		return true
	}
	if !strings.HasPrefix(name, "tpl-") {
		return false
	}
	for _, role := range []string{TemplateBaseRoleRootfs, TemplateBaseRoleMemory} {
		rest, ok := strings.CutSuffix(name, "-"+role)
		if !ok {
			continue
		}
		i := strings.LastIndex(rest, "-b")
		if i > len("tpl-") && len(rest)-i-2 == 12 {
			return true
		}
	}
	return false
}

// ensureDeltaBase is called before a cross-node import whose remote uuids
// name a template base: it installs the base when this node lacks it, so the
// delta import finds it. A base that cannot be installed fails the restore.
func ensureDeltaBase(ctx context.Context, templateID, role, baseUUID string) error {
	if strings.TrimSpace(baseUUID) == "" {
		return nil
	}
	if _, err := InstallTemplateBase(ctx, templateID, role, baseUUID); err != nil {
		if errors.Is(err, ErrTemplateBaseMissing) {
			return err
		}
		return fmt.Errorf("%w: %v", ErrTemplateBaseMissing, err)
	}
	return nil
}

// templateBaseSnapshotFor answers the local base snapshot to clone a new
// sandbox of templateID from, or "" to use the template's own snapshot.
// Only when distribution is on and Master named a base.
func templateBaseSnapshotFor(ctx context.Context, ann map[string]string, role string) string {
	if !TemplateS3DistributionEnabled() || len(ann) == 0 {
		return ""
	}
	key := templateBaseAnnotationRootfs
	if role == TemplateBaseRoleMemory {
		key = templateBaseAnnotationMemory
	}
	uuid := strings.TrimSpace(ann[key])
	if uuid == "" {
		return ""
	}
	snap, err := LookupTemplateBase(ctx, uuid)
	if err != nil {
		CubeLog.Warnf("template base %s (%s): lookup failed, using the template snapshot: %v", uuid, role, err)
		return ""
	}
	if snap == "" {
		CubeLog.Warnf("template base %s (%s) is not on this node yet; using the template snapshot", uuid, role)
	}
	return snap
}

// ---------------------------------------------------------------------------
// Master-facing operations, carried in CreateImage annotations so no proto
// changes. The reply's RetMsg is the JSON result.
// ---------------------------------------------------------------------------

const (
	// TemplateBaseOpAnnotation selects the operation: publish | install |
	// release | lookup.
	TemplateBaseOpAnnotation       = "cube.master.template.base.op"
	TemplateBaseTemplateAnnotation = "cube.master.template.base.template_id"
	TemplateBaseRoleAnnotation     = "cube.master.template.base.role"
	TemplateBaseUUIDAnnotation     = "cube.master.template.base.uuid"
)

// HandleTemplateBaseRequest answers handled=false for an ordinary CreateImage.
func HandleTemplateBaseRequest(ctx context.Context, ann map[string]string) (bool, string, error) {
	op := strings.TrimSpace(ann[TemplateBaseOpAnnotation])
	if op == "" {
		return false, "", nil
	}
	templateID := strings.TrimSpace(ann[TemplateBaseTemplateAnnotation])
	role := strings.TrimSpace(ann[TemplateBaseRoleAnnotation])
	uuid := strings.TrimSpace(ann[TemplateBaseUUIDAnnotation])
	reply := func(v any) (bool, string, error) {
		raw, err := json.Marshal(v)
		return true, string(raw), err
	}
	switch op {
	case "publish":
		if !TemplateS3DistributionEnabled() {
			return true, "", fmt.Errorf("%s is off on this node", envTemplateS3Distribution)
		}
		b, err := PublishTemplateBases(ctx, templateID)
		if err != nil {
			return true, "", err
		}
		return reply(b)
	case "install":
		if role != TemplateBaseRoleRootfs && role != TemplateBaseRoleMemory {
			return true, "", fmt.Errorf("invalid template base role %q", role)
		}
		snap, err := InstallTemplateBase(ctx, templateID, role, uuid)
		if err != nil {
			return true, "", err
		}
		return reply(map[string]string{"base_uuid": uuid, "snapshot": snap})
	case "release":
		return true, "", ReleaseTemplateBase(ctx, uuid)
	case "lookup":
		snap, err := LookupTemplateBase(ctx, uuid)
		if err != nil {
			return true, "", err
		}
		return reply(map[string]any{"base_uuid": uuid, "snapshot": snap, "present": snap != ""})
	}
	return true, "", fmt.Errorf("unknown template base op %q", op)
}

// Create annotations Master sets when CUBE_TEMPLATE_S3_DISTRIBUTION is on.
const (
	templateBaseAnnotationRootfs = "cube.master.template.base.rootfs"
	templateBaseAnnotationMemory = "cube.master.template.base.memory"
)
