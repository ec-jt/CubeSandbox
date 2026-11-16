// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package templatecenter

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/db/models"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// fakeCubelets models the Cubelet side of the template-base ops: which node
// holds which base, what publish returns, and which installs fail.
type fakeCubelets struct {
	mu          sync.Mutex
	held        map[string]map[string]bool // ip -> base -> present
	publish     publishedBases
	installErr  map[string]error // ip -> error
	installLies map[string]bool  // ip -> install "succeeds" without the base
	calls       []string
}

func (f *fakeCubelets) call(_ context.Context, ip string, ann map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	op := ann[templateBaseOpAnnotation]
	f.calls = append(f.calls, op+"@"+ip)
	if f.held[ip] == nil {
		f.held[ip] = map[string]bool{}
	}
	id := ann[templateBaseUUIDAnnotation]
	switch op {
	case "publish":
		f.held[ip][f.publish.Rootfs] = true
		if f.publish.Memory != "" {
			f.held[ip][f.publish.Memory] = true
		}
		raw, _ := json.Marshal(f.publish)
		return string(raw), nil
	case "install":
		if err := f.installErr[ip]; err != nil {
			return "", err
		}
		if !f.installLies[ip] {
			f.held[ip][id] = true
		}
		return `{}`, nil
	case "lookup":
		raw, _ := json.Marshal(baseLookupReply{BaseUUID: id, Present: f.held[ip][id]})
		return string(raw), nil
	case "release":
		delete(f.held[ip], id)
		return "", nil
	}
	return "", errors.New("bad op " + op)
}

func newTemplateBaseTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dbName := "file:" + strings.ReplaceAll(t.Name(), "/", "_") + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.TemplateBase{}, &models.TemplateBaseReplica{},
		&models.TemplateReplica{}, &models.PauseSnapshotRecord{}, &models.SnapshotRecord{}))
	// gorm's AutoMigrate does not know the unique keys the migration adds.
	require.NoError(t, db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS uniq_template_base_uuid ON t_cube_template_base (base_uuid)").Error)
	require.NoError(t, db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS uniq_template_base_replica ON t_cube_template_base_replica (base_uuid, node_id)").Error)
	old := store.db
	store.db = db
	t.Cleanup(func() { store.db = old })
	return db
}

func useFakeCubelets(t *testing.T, nodes []*node.Node) *fakeCubelets {
	t.Helper()
	f := &fakeCubelets{held: map[string]map[string]bool{}, installErr: map[string]error{}, installLies: map[string]bool{}}
	oldCall, oldNodes := templateBaseCall, templateBaseNodes
	templateBaseCall = f.call
	templateBaseNodes = func(string) ([]*node.Node, error) { return nodes, nil }
	t.Cleanup(func() { templateBaseCall, templateBaseNodes = oldCall, oldNodes })
	t.Setenv(envTemplateS3Distribution, "true")
	return f
}

const (
	baseR1 = "11111111-0000-4000-8000-000000000001"
	baseM1 = "11111111-0000-4000-8000-000000000002"
	baseR2 = "22222222-0000-4000-8000-000000000001"
	baseM2 = "22222222-0000-4000-8000-000000000002"
)

func seedBuilt(t *testing.T, db *gorm.DB, templateID, artifactID string, nodes ...*node.Node) templateBaseDefinition {
	for _, n := range nodes {
		require.NoError(t, db.Create(&models.TemplateReplica{TemplateID: templateID, NodeID: n.ID(), NodeIP: n.HostIP(),
			Status: ReplicaStatusReady, ArtifactID: artifactID}).Error)
	}
	return templateBaseDefinition{TemplateID: templateID, InstanceType: "cubebox", RootfsArtifactID: artifactID, StorageBackend: "s3"}
}

func readyReplicaNodes(t *testing.T, db *gorm.DB, base string) []string {
	var rows []models.TemplateBaseReplica
	require.NoError(t, db.Where("base_uuid = ? AND status = ?", base, TemplateBaseReplicaReady).Order("node_id").Find(&rows).Error)
	out := []string{}
	for _, r := range rows {
		out = append(out, r.NodeID)
	}
	return out
}

// Publish once on a builder, then the reconciler pushes the base to every
// other node and records READY only for installs a lookup confirmed.
func TestTemplateBaseReconcilerPublishesAndInstalls(t *testing.T) {
	db := newTemplateBaseTestDB(t)
	a, b, c := testNode("node-a", "10.0.0.1"), testNode("node-b", "10.0.0.2"), testNode("node-c", "10.0.0.3")
	f := useFakeCubelets(t, []*node.Node{a, b, c})
	f.publish = publishedBases{TemplateID: "tpl-1", Rootfs: baseR1, Memory: baseM1}
	f.installLies["10.0.0.3"] = true // install says ok, lookup says absent
	def := seedBuilt(t, db, "tpl-1", "art-1", a)

	require.NoError(t, publishTemplateBase(context.Background(), def))
	err := installTemplateBases(context.Background(), def)
	require.Error(t, err, "the unverified install is reported")

	require.Equal(t, []string{"node-a", "node-b"}, readyReplicaNodes(t, db, baseR1))
	require.Equal(t, []string{"node-a", "node-b"}, readyReplicaNodes(t, db, baseM1))
	var failed models.TemplateBaseReplica
	require.NoError(t, db.Where("base_uuid = ? AND node_id = ?", baseR1, "node-c").First(&failed).Error)
	require.Equal(t, TemplateBaseReplicaFailed, failed.Status)
	require.Contains(t, failed.LastError, "not present after install")

	// A second tick republishes nothing and retries only node-c.
	f.installLies["10.0.0.3"] = false
	f.calls = nil
	require.NoError(t, publishTemplateBase(context.Background(), def))
	require.NoError(t, installTemplateBases(context.Background(), def))
	for _, c := range f.calls {
		require.NotContains(t, c, "publish@")
		require.True(t, strings.HasSuffix(c, "@10.0.0.3"), "only the missing node is touched: %s", c)
	}
	require.Equal(t, []string{"node-a", "node-b", "node-c"}, readyReplicaNodes(t, db, baseR1))

	ann := TemplateBaseCreateAnnotations(context.Background(), "tpl-1")
	require.Equal(t, map[string]string{TemplateBaseRootfsCreateAnnotation: baseR1, TemplateBaseMemoryCreateAnnotation: baseM1}, ann)
}

func TestTemplateBaseNothingWhenFlagOffOrNotBuilt(t *testing.T) {
	db := newTemplateBaseTestDB(t)
	a := testNode("node-a", "10.0.0.1")
	f := useFakeCubelets(t, []*node.Node{a})
	def := templateBaseDefinition{TemplateID: "tpl-1", RootfsArtifactID: "art-1", StorageBackend: "s3"}
	require.NoError(t, publishTemplateBase(context.Background(), def))
	require.Empty(t, f.calls, "no READY replica: nothing to publish")

	t.Setenv(envTemplateS3Distribution, "")
	seedBuilt(t, db, "tpl-1", "art-1", a)
	require.NoError(t, reconcileTemplateBases(context.Background()))
	require.Empty(t, f.calls)
	require.Nil(t, TemplateBaseCreateAnnotations(context.Background(), "tpl-1"))
}

// A rebuild publishes a new base and retires the old one; the old one stays
// installed while a paused sandbox's export still references it.
func TestTemplateBaseGCBlockedByReferences(t *testing.T) {
	db := newTemplateBaseTestDB(t)
	a, b := testNode("node-a", "10.0.0.1"), testNode("node-b", "10.0.0.2")
	f := useFakeCubelets(t, []*node.Node{a, b})
	t.Setenv(envTemplateBaseGCGrace, "0s")
	ctx := context.Background()

	f.publish = publishedBases{Rootfs: baseR1, Memory: baseM1}
	def := seedBuilt(t, db, "tpl-1", "art-1", a)
	require.NoError(t, publishTemplateBase(ctx, def))
	require.NoError(t, installTemplateBases(ctx, def))

	// A sandbox paused as a delta over the first build.
	require.NoError(t, db.Create(&models.PauseSnapshotRecord{SnapshotID: "snap-p1", SandboxID: "sb-1",
		ExportUUIDs: `{"rootfs":"r","memory":"m","rootfs_base":"` + baseR1 + `","memory_base":"` + baseM1 + `"}`}).Error)

	// Rebuild.
	require.NoError(t, db.Model(&models.TemplateReplica{}).Where("template_id = ?", "tpl-1").Update("artifact_id", "art-2").Error)
	def.RootfsArtifactID = "art-2"
	f.publish = publishedBases{Rootfs: baseR2, Memory: baseM2}
	require.NoError(t, publishTemplateBase(ctx, def))
	var old models.TemplateBase
	require.NoError(t, db.Where("base_uuid = ?", baseR1).First(&old).Error)
	require.Equal(t, TemplateBaseStatusRetired, old.Status)
	require.Equal(t, map[string]string{TemplateBaseRootfsCreateAnnotation: baseR2, TemplateBaseMemoryCreateAnnotation: baseM2},
		TemplateBaseCreateAnnotations(ctx, "tpl-1"), "new sandboxes use the new base")

	require.NoError(t, installTemplateBases(ctx, def))
	require.True(t, f.held["10.0.0.2"][baseR2])

	templateBaseNow = func() time.Time { return time.Now().Add(time.Minute) }
	t.Cleanup(func() { templateBaseNow = time.Now })

	require.NoError(t, gcTemplateBases(ctx))
	require.True(t, f.held["10.0.0.2"][baseR1], "referenced base must stay installed")
	require.True(t, f.held["10.0.0.2"][baseM1])

	// Resume consumed the pause binding: nothing references the base now.
	require.NoError(t, db.Where("snapshot_id = ?", "snap-p1").Delete(&models.PauseSnapshotRecord{}).Error)
	require.NoError(t, gcTemplateBases(ctx))
	require.False(t, f.held["10.0.0.1"][baseR1])
	require.False(t, f.held["10.0.0.2"][baseR1])
	var n int64
	require.NoError(t, db.Model(&models.TemplateBase{}).Where("base_uuid IN ?", []string{baseR1, baseM1}).Count(&n).Error)
	require.Zero(t, n)
	require.NoError(t, db.Model(&models.TemplateBaseReplica{}).Where("base_uuid IN ?", []string{baseR1, baseM1}).Count(&n).Error)
	require.Zero(t, n)
	// The current base is untouched.
	require.True(t, f.held["10.0.0.2"][baseR2])
}

// A commit snapshot exported over a base blocks GC just like a pause.
func TestTemplateBaseReferencesCountSnapshots(t *testing.T) {
	db := newTemplateBaseTestDB(t)
	require.NoError(t, db.Create(&models.SnapshotRecord{SnapshotID: "snap-c1", RequestJSON: "{}",
		ExportUUIDs: `{"rootfs":"r","rootfs_base":"` + baseR1 + `"}`}).Error)
	n, err := templateBaseReferences(context.Background(), baseR1)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	n, err = templateBaseReferences(context.Background(), baseR2)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Equal(t, constants.SnapshotTableName, (models.SnapshotRecord{}).TableName())
}

// Deleting a template retires its bases; GC then releases them everywhere.
func TestDeletedTemplateBasesAreRetiredAndCollected(t *testing.T) {
	db := newTemplateBaseTestDB(t)
	a, b := testNode("node-a", "10.0.0.1"), testNode("node-b", "10.0.0.2")
	f := useFakeCubelets(t, []*node.Node{a, b})
	t.Setenv(envTemplateBaseGCGrace, "0s")
	ctx := context.Background()

	f.publish = publishedBases{Rootfs: baseR1, Memory: baseM1}
	def := seedBuilt(t, db, "tpl-del", "art-1", a)
	require.NoError(t, publishTemplateBase(ctx, def))
	require.NoError(t, installTemplateBases(ctx, def))
	require.True(t, f.held["10.0.0.2"][baseR1])

	// A READY base is never collected.
	require.NoError(t, gcTemplateBases(ctx))
	require.True(t, f.held["10.0.0.2"][baseR1])

	require.NoError(t, retireTemplateBasesOf(ctx, "tpl-del"))
	var n int64
	require.NoError(t, db.Model(&models.TemplateBase{}).Where("template_id = ? AND status = ?", "tpl-del", TemplateBaseStatusRetired).Count(&n).Error)
	require.EqualValues(t, 2, n)
	require.Empty(t, TemplateBaseCreateAnnotations(ctx, "tpl-del"))

	templateBaseNow = func() time.Time { return time.Now().Add(time.Minute) }
	t.Cleanup(func() { templateBaseNow = time.Now })
	require.NoError(t, gcTemplateBases(ctx))
	for _, ip := range []string{"10.0.0.1", "10.0.0.2"} {
		require.False(t, f.held[ip][baseR1], ip)
		require.False(t, f.held[ip][baseM1], ip)
	}
	require.NoError(t, db.Model(&models.TemplateBase{}).Where("template_id = ?", "tpl-del").Count(&n).Error)
	require.Zero(t, n)
	require.NoError(t, db.Model(&models.TemplateBaseReplica{}).Where("base_uuid IN ?", []string{baseR1, baseM1}).Count(&n).Error)
	require.Zero(t, n)
}
