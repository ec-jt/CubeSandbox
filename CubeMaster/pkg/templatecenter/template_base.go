// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package templatecenter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/db/models"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/log"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/cubelet"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	imagev1 "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/images/v1"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Template S3 distribution (CUBE_TEMPLATE_S3_DISTRIBUTION, default off).
//
// WHY
// ---
// Every node builds its own copy of a template, so the template snapshots
// differ byte for byte across nodes. A sandbox export therefore cannot be a
// delta over "the template": the importing node's template is not the one the
// sandbox ran on. With distribution on, ONE node publishes its build as a
// base (an s3lvol export of the template's rootfs and memory snapshots), the
// other nodes import it, and new sandboxes clone from the base. Every node
// then holds identical base bytes, and s3lvol can export a paused sandbox as
// only its own chunks over that base (S3LVOL_EXPORT_TEMPLATE_DELTA).
//
// LIFECYCLE (one reconcile tick, inside the image-job advisory lock)
// -----------------------------------------------------------------
//  1. publish: a READY s3 template whose current artifact has no READY base
//     is published on one node that holds a READY replica. The base rows go
//     READY and every older base of the template goes RETIRED.
//  2. install: every healthy in-scope node lacking a READY replica of a
//     READY base imports it. The replica is marked READY only after a
//     separate lookup on that node confirms the base is installed and valid.
//  3. gc: a RETIRED base is released (and its rows dropped) only after a
//     grace period AND when no pause or snapshot export references it. A
//     sandbox exported as a delta over a base cannot be restored anywhere
//     the base is gone, so the reference check is the correctness gate.
//
// Transport: Cubelet's CreateImage with template-base op annotations (see
// Cubelet storage.HandleTemplateBaseRequest); no proto change.

const (
	envTemplateS3Distribution = "CUBE_TEMPLATE_S3_DISTRIBUTION"
	envTemplateBaseGCGrace    = "CUBE_TEMPLATE_BASE_GC_GRACE"

	TemplateBaseRoleRootfs = "rootfs"
	TemplateBaseRoleMemory = "memory"
	// TemplateBaseRoleMetadata is the builder's sealed metadata disk: the
	// vmstate its memory image belongs to. A node that clones the memory
	// base must clone this one too, never pair it with its own vmstate.
	TemplateBaseRoleMetadata = "metadata"

	TemplateBaseStatusReady   = "READY"
	TemplateBaseStatusRetired = "RETIRED"

	TemplateBaseReplicaReady  = "READY"
	TemplateBaseReplicaFailed = "FAILED"

	// Annotations Cubelet reads (must match Cubelet storage).
	templateBaseOpAnnotation             = "cube.master.template.base.op"
	templateBaseTemplateAnnotation       = "cube.master.template.base.template_id"
	templateBaseRoleAnnotation           = "cube.master.template.base.role"
	templateBaseUUIDAnnotation           = "cube.master.template.base.uuid"
	TemplateBaseRootfsCreateAnnotation   = "cube.master.template.base.rootfs"
	TemplateBaseMemoryCreateAnnotation   = "cube.master.template.base.memory"
	TemplateBaseMetadataCreateAnnotation = "cube.master.template.base.metadata"

	defaultTemplateBaseGCGrace = 24 * time.Hour
)

// TemplateS3DistributionEnabled reports the Master-side flag.
func TemplateS3DistributionEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(envTemplateS3Distribution))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func templateBaseGCGrace() time.Duration {
	if d, err := time.ParseDuration(strings.TrimSpace(os.Getenv(envTemplateBaseGCGrace))); err == nil && d >= 0 {
		return d
	}
	return defaultTemplateBaseGCGrace
}

// publishedBases is Cubelet's publish reply.
type publishedBases struct {
	TemplateID string `json:"template_id"`
	Rootfs     string `json:"rootfs,omitempty"`
	Memory     string `json:"memory,omitempty"`
	Metadata   string `json:"metadata,omitempty"`
}

type baseLookupReply struct {
	BaseUUID string `json:"base_uuid"`
	Snapshot string `json:"snapshot"`
	Present  bool   `json:"present"`
}

// templateBaseDefinition is the definition projection the sweep needs.
type templateBaseDefinition struct {
	TemplateID       string `gorm:"column:template_id"`
	InstanceType     string `gorm:"column:instance_type"`
	RootfsArtifactID string `gorm:"column:rootfs_artifact_id"`
	StorageBackend   string `gorm:"column:storage_backend"`
}

// Seams so the orchestration is testable without live nodes.
var (
	templateBaseCall = func(ctx context.Context, nodeIP string, ann map[string]string) (string, error) {
		rsp, err := cubelet.CreateImage(ctx, cubelet.GetCubeletAddr(nodeIP), &imagev1.CreateImageRequest{
			RequestID: uuid.New().String(),
			Spec:      &imagev1.ImageSpec{Annotations: ann},
		})
		if err != nil {
			return "", err
		}
		if rsp.GetRet() == nil {
			return "", fmt.Errorf("cubelet %s: empty template base reply", nodeIP)
		}
		if int(rsp.GetRet().GetRetCode()) != int(errorcode.ErrorCode_Success) {
			return "", fmt.Errorf("cubelet %s: %s", nodeIP, rsp.GetRet().GetRetMsg())
		}
		return rsp.GetRet().GetRetMsg(), nil
	}
	templateBaseDefinitions = func(ctx context.Context, limit int) ([]templateBaseDefinition, error) {
		var defs []templateBaseDefinition
		err := store.db.WithContext(ctx).
			Table(constants.TemplateDefinitionTableName).
			Select("template_id, instance_type, rootfs_artifact_id, storage_backend").
			Where("status = ? AND deleted_at IS NULL", StatusReady).
			Limit(limit).
			Find(&defs).Error
		return defs, err
	}
	templateBaseReplicas = ListReplicas
	templateBaseNodes    = func(instanceType string) ([]*node.Node, error) { return resolveTemplateNodes(instanceType, nil) }
	templateBaseNow      = time.Now
)

// reconcileTemplateBases runs publish, install and gc. Runs inside the
// image-job reconcile pass (advisory lock held), so one master sweeps.
func reconcileTemplateBases(ctx context.Context) error {
	if !isReady() || !TemplateS3DistributionEnabled() {
		return nil
	}
	logger := log.G(ctx).WithFields(map[string]any{"component": "template_base"})
	defs, err := templateBaseDefinitions(ctx, imageJobReconcileBatch)
	if err != nil {
		return fmt.Errorf("scan ready definitions: %w", err)
	}
	for _, def := range defs {
		if !constants.IsS3Backend(def.StorageBackend) {
			continue
		}
		if err := publishTemplateBase(ctx, def); err != nil {
			logger.Warnf("template base publish: template=%s err=%v", def.TemplateID, err)
			continue
		}
		if err := installTemplateBases(ctx, def); err != nil {
			logger.Warnf("template base install: template=%s err=%v", def.TemplateID, err)
		}
	}
	if err := gcTemplateBases(ctx); err != nil {
		logger.Warnf("template base gc: %v", err)
	}
	return nil
}

func templateBasesOf(ctx context.Context, templateID string, status string) ([]models.TemplateBase, error) {
	var rows []models.TemplateBase
	q := store.db.WithContext(ctx).Where("template_id = ?", templateID)
	if status != "" {
		q = q.Where("status = ?", status)
	}
	err := q.Order("id asc").Find(&rows).Error
	return rows, err
}

// publishedSetComplete reports whether the READY bases of the current
// artifact form a complete set: a rootfs base, and when there is a memory
// base, the metadata base of the same build. A set published before the
// metadata role existed is incomplete and gets published again (Cubelet's
// publish is idempotent per role, so only the metadata base is new).
func publishedSetComplete(ready []models.TemplateBase, artifactID string) bool {
	roles := map[string]bool{}
	for _, b := range ready {
		if b.ArtifactID == artifactID {
			roles[b.Role] = true
		}
	}
	if !roles[TemplateBaseRoleRootfs] {
		return false
	}
	return !roles[TemplateBaseRoleMemory] || roles[TemplateBaseRoleMetadata]
}

// publishTemplateBase publishes the template's current build when no READY
// base exists for it. The publishing node is the first node (by id) holding
// a READY, clean replica of the CURRENT artifact.
func publishTemplateBase(ctx context.Context, def templateBaseDefinition) error {
	ready, err := templateBasesOf(ctx, def.TemplateID, TemplateBaseStatusReady)
	if err != nil {
		return err
	}
	if publishedSetComplete(ready, def.RootfsArtifactID) {
		return nil
	}
	// Republish on the node that published the existing partial set: the
	// bases of one template must all come from one build.
	prevBuilder := ""
	for _, b := range ready {
		if b.ArtifactID == def.RootfsArtifactID && b.BuilderNodeID != "" {
			prevBuilder = b.BuilderNodeID
			break
		}
	}
	replicas, err := templateBaseReplicas(ctx, def.TemplateID)
	if err != nil {
		return fmt.Errorf("list replicas: %w", err)
	}
	var builder *models.TemplateReplica
	for i := range replicas {
		r := replicas[i]
		if replicaNeedsRedo(r) || strings.TrimSpace(r.NodeIP) == "" {
			continue
		}
		if def.RootfsArtifactID != "" && r.ArtifactID != "" && r.ArtifactID != def.RootfsArtifactID {
			continue
		}
		if prevBuilder != "" && r.NodeID != prevBuilder {
			continue
		}
		builder = &replicas[i]
		break
	}
	if builder == nil {
		return nil // nothing built yet (or the publisher is gone); the next tick retries
	}
	msg, err := templateBaseCall(ctx, builder.NodeIP, map[string]string{
		templateBaseOpAnnotation:       "publish",
		templateBaseTemplateAnnotation: def.TemplateID,
	})
	if err != nil {
		return fmt.Errorf("publish on %s: %w", builder.NodeIP, err)
	}
	var pub publishedBases
	if err := json.Unmarshal([]byte(msg), &pub); err != nil {
		return fmt.Errorf("decode publish reply from %s: %w", builder.NodeIP, err)
	}
	if strings.TrimSpace(pub.Rootfs) == "" {
		return fmt.Errorf("publish on %s returned no rootfs base", builder.NodeIP)
	}
	if strings.TrimSpace(pub.Memory) != "" && strings.TrimSpace(pub.Metadata) == "" {
		return fmt.Errorf("publish on %s returned a memory base without its metadata base; not recording a set that would mix builds", builder.NodeIP)
	}
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := templateBaseNow()
		for role, id := range map[string]string{TemplateBaseRoleRootfs: pub.Rootfs, TemplateBaseRoleMemory: pub.Memory, TemplateBaseRoleMetadata: pub.Metadata} {
			if id = strings.TrimSpace(id); id == "" {
				continue
			}
			row := models.TemplateBase{
				CreatedAt: now, UpdatedAt: now, TemplateID: def.TemplateID, Role: role, BaseUUID: id,
				ArtifactID: def.RootfsArtifactID, BuilderNodeID: builder.NodeID, BuilderNodeIP: builder.NodeIP,
				Status: TemplateBaseStatusReady,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "base_uuid"}},
				DoUpdates: clause.AssignmentColumns([]string{"status", "artifact_id", "updated_at"}),
			}).Create(&row).Error; err != nil {
				return err
			}
			// The builder holds the base by construction: Cubelet's publish
			// marks it only after the export completed.
			if err := upsertBaseReplica(tx, id, builder.NodeID, builder.NodeIP, TemplateBaseReplicaReady, ""); err != nil {
				return err
			}
		}
		keep := []string{pub.Rootfs}
		if pub.Memory != "" {
			keep = append(keep, pub.Memory)
		}
		if pub.Metadata != "" {
			keep = append(keep, pub.Metadata)
		}
		return tx.Model(&models.TemplateBase{}).
			Where("template_id = ? AND status = ? AND base_uuid NOT IN ?", def.TemplateID, TemplateBaseStatusReady, keep).
			Updates(map[string]any{"status": TemplateBaseStatusRetired, "updated_at": now}).Error
	})
}

func upsertBaseReplica(tx *gorm.DB, baseUUID, nodeID, nodeIP, status, lastErr string) error {
	now := templateBaseNow()
	row := models.TemplateBaseReplica{CreatedAt: now, UpdatedAt: now, BaseUUID: baseUUID, NodeID: nodeID, NodeIP: nodeIP, Status: status, LastError: lastErr}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "base_uuid"}, {Name: "node_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"node_ip", "status", "last_error", "updated_at"}),
	}).Create(&row).Error
}

// installTemplateBases pushes every READY base of the template to the healthy
// in-scope nodes without a READY replica, and verifies each install.
func installTemplateBases(ctx context.Context, def templateBaseDefinition) error {
	bases, err := templateBasesOf(ctx, def.TemplateID, TemplateBaseStatusReady)
	if err != nil || len(bases) == 0 {
		return err
	}
	targets, err := templateBaseNodes(def.InstanceType)
	if err != nil {
		return err
	}
	var errs []error
	for _, b := range bases {
		var have []models.TemplateBaseReplica
		if err := store.db.WithContext(ctx).Where("base_uuid = ? AND status = ?", b.BaseUUID, TemplateBaseReplicaReady).Find(&have).Error; err != nil {
			return err
		}
		held := make(map[string]struct{}, len(have))
		for _, r := range have {
			held[r.NodeID] = struct{}{}
		}
		for _, t := range targets {
			if t == nil {
				continue
			}
			if _, ok := held[t.ID()]; ok {
				continue
			}
			if err := installOneBase(ctx, def.TemplateID, b, t); err != nil {
				errs = append(errs, err)
				_ = upsertBaseReplica(store.db.WithContext(ctx), b.BaseUUID, t.ID(), t.HostIP(), TemplateBaseReplicaFailed, err.Error())
				continue
			}
			if err := upsertBaseReplica(store.db.WithContext(ctx), b.BaseUUID, t.ID(), t.HostIP(), TemplateBaseReplicaReady, ""); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// installOneBase imports the base on one node, then proves it with a lookup:
// the install reply alone is not taken as evidence.
func installOneBase(ctx context.Context, templateID string, b models.TemplateBase, t *node.Node) error {
	if _, err := templateBaseCall(ctx, t.HostIP(), map[string]string{
		templateBaseOpAnnotation:       "install",
		templateBaseTemplateAnnotation: templateID,
		templateBaseRoleAnnotation:     b.Role,
		templateBaseUUIDAnnotation:     b.BaseUUID,
	}); err != nil {
		return fmt.Errorf("install base %s on %s: %w", b.BaseUUID, t.HostIP(), err)
	}
	ok, err := lookupBaseOn(ctx, t.HostIP(), b.BaseUUID)
	if err != nil {
		return fmt.Errorf("verify base %s on %s: %w", b.BaseUUID, t.HostIP(), err)
	}
	if !ok {
		return fmt.Errorf("verify base %s on %s: not present after install", b.BaseUUID, t.HostIP())
	}
	return nil
}

func lookupBaseOn(ctx context.Context, nodeIP, baseUUID string) (bool, error) {
	msg, err := templateBaseCall(ctx, nodeIP, map[string]string{
		templateBaseOpAnnotation:   "lookup",
		templateBaseUUIDAnnotation: baseUUID,
	})
	if err != nil {
		return false, err
	}
	var r baseLookupReply
	if err := json.Unmarshal([]byte(msg), &r); err != nil {
		return false, fmt.Errorf("decode lookup reply: %w", err)
	}
	return r.Present && r.BaseUUID == baseUUID, nil
}

// templateBaseReferences counts pause and snapshot rows whose export uuids
// name the base (Cubelet records rootfs_base / memory_base there). Base uuids
// are unique export uuids, so a substring match on the quoted uuid is exact.
var templateBaseReferences = func(ctx context.Context, baseUUID string) (int64, error) {
	pattern := "%\"" + baseUUID + "\"%"
	var total int64
	for _, table := range []string{constants.PauseSnapshotTableName, constants.SnapshotTableName} {
		var n int64
		if err := store.db.WithContext(ctx).Table(table).Where("export_uuids LIKE ?", pattern).Count(&n).Error; err != nil {
			return 0, fmt.Errorf("count %s references: %w", table, err)
		}
		total += n
	}
	return total, nil
}

// retireTemplateBasesOf marks every READY base of a deleted template RETIRED,
// so gcTemplateBases releases them (and their replicas on every node) once
// the grace period passed and no pause or snapshot row references them. A
// deleted template's bases used to stay READY forever: nothing else retires
// a base except a rebuild of the same template.
func retireTemplateBasesOf(ctx context.Context, templateID string) error {
	if strings.TrimSpace(templateID) == "" || store.db == nil {
		return nil
	}
	return store.db.WithContext(ctx).Model(&models.TemplateBase{}).
		Where("template_id = ? AND status = ?", templateID, TemplateBaseStatusReady).
		Updates(map[string]any{"status": TemplateBaseStatusRetired, "updated_at": templateBaseNow()}).Error
}

// gcTemplateBases releases RETIRED bases that nothing references any more.
func gcTemplateBases(ctx context.Context) error {
	var retired []models.TemplateBase
	cutoff := templateBaseNow().Add(-templateBaseGCGrace())
	if err := store.db.WithContext(ctx).
		Where("status = ? AND updated_at <= ?", TemplateBaseStatusRetired, cutoff).
		Order("id asc").Limit(imageJobReconcileBatch).Find(&retired).Error; err != nil {
		return err
	}
	var errs []error
	for _, b := range retired {
		refs, err := templateBaseReferences(ctx, b.BaseUUID)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if refs > 0 {
			continue // still restorable state depends on it
		}
		if err := releaseBaseEverywhere(ctx, b); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func releaseBaseEverywhere(ctx context.Context, b models.TemplateBase) error {
	var replicas []models.TemplateBaseReplica
	if err := store.db.WithContext(ctx).Where("base_uuid = ?", b.BaseUUID).Find(&replicas).Error; err != nil {
		return err
	}
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].NodeID < replicas[j].NodeID })
	for _, r := range replicas {
		if _, err := templateBaseCall(ctx, r.NodeIP, map[string]string{
			templateBaseOpAnnotation:   "release",
			templateBaseUUIDAnnotation: b.BaseUUID,
		}); err != nil {
			// Keep the rows: the next tick retries this node. A node that is
			// gone for good keeps the base row until an operator drops it.
			return fmt.Errorf("release base %s on %s: %w", b.BaseUUID, r.NodeIP, err)
		}
		if err := store.db.WithContext(ctx).Delete(&models.TemplateBaseReplica{}, r.ID).Error; err != nil {
			return err
		}
	}
	return store.db.WithContext(ctx).Delete(&models.TemplateBase{}, b.ID).Error
}

// TemplateBaseCreateAnnotations names the READY bases a new sandbox of the
// template should clone from. Empty when distribution is off or nothing is
// published; Cubelet falls back to its own template snapshot when the base
// is not installed on the chosen node yet.
func TemplateBaseCreateAnnotations(ctx context.Context, templateID string) map[string]string {
	if !isReady() || !TemplateS3DistributionEnabled() || strings.TrimSpace(templateID) == "" {
		return nil
	}
	bases, err := templateBasesOf(ctx, templateID, TemplateBaseStatusReady)
	if err != nil {
		log.G(ctx).Warnf("template base annotations for %s: %v", templateID, err)
		return nil
	}
	out := map[string]string{}
	for _, b := range bases {
		switch b.Role {
		case TemplateBaseRoleRootfs:
			out[TemplateBaseRootfsCreateAnnotation] = b.BaseUUID
		case TemplateBaseRoleMemory:
			out[TemplateBaseMemoryCreateAnnotation] = b.BaseUUID
		case TemplateBaseRoleMetadata:
			out[TemplateBaseMetadataCreateAnnotation] = b.BaseUUID
		}
	}
	// Never name a memory base without the metadata base of the same build:
	// Cubelet would refuse the set anyway, but an older Cubelet would not.
	if out[TemplateBaseMemoryCreateAnnotation] != "" && out[TemplateBaseMetadataCreateAnnotation] == "" {
		return nil
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
