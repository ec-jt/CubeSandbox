// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package remotestatus

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/db/models"
	snapshotv1 "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/snapshot/v1"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupReconcilerDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "remote.db")), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&models.SnapshotRecord{}, &models.PauseSnapshotRecord{}); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestReconcileOncePromotesInProgressOnly(t *testing.T) {
	db := setupReconcilerDB(t)
	ctx := context.Background()
	requireNoErr(t, db.Create(&models.SnapshotRecord{
		SnapshotID:   "snap-up",
		OriginNodeIP: "10.0.0.1",
		ExportUUIDs:  `{"rootfs":"r","metadata":"m"}`,
		Backend:      constants.SnapshotBackendS3,
		RemoteStatus: constants.RemoteStatusInProgress,
		Status:       "READY",
	}).Error)
	requireNoErr(t, db.Create(&models.SnapshotRecord{
		SnapshotID:   "snap-ready",
		OriginNodeIP: "10.0.0.1",
		Backend:      constants.SnapshotBackendS3,
		RemoteStatus: constants.RemoteStatusReady,
		Status:       "READY",
	}).Error)
	requireNoErr(t, db.Create(&models.PauseSnapshotRecord{
		SnapshotID:   "pause-up",
		SandboxID:    "sb-1",
		NodeIP:       "10.0.0.2",
		ExportUUIDs:  `{"rootfs":"r","metadata":"m"}`,
		Backend:      constants.SnapshotBackendS3,
		RemoteStatus: constants.RemoteStatusInProgress,
		Status:       "READY",
	}).Error)
	requireNoErr(t, db.Create(&models.PauseSnapshotRecord{
		SnapshotID:   "pause-pending",
		SandboxID:    "sb-2",
		NodeIP:       "10.0.0.2",
		Backend:      constants.SnapshotBackendS3,
		RemoteStatus: constants.RemoteStatusPending,
		Status:       "CREATING",
	}).Error)

	queried := map[string]int{}
	prevQuery := queryStatus
	prevAddr := cubeletAddr
	queryStatus = func(_ context.Context, _ string, req *snapshotv1.StatusRequest) (*snapshotv1.StatusResponse, error) {
		queried[req.GetSnapshotId()]++
		return &snapshotv1.StatusResponse{State: constants.RemoteStatusReady, RemoteReady: true}, nil
	}
	cubeletAddr = func(hostIP string) string { return hostIP }
	t.Cleanup(func() {
		queryStatus = prevQuery
		cubeletAddr = prevAddr
	})

	ReconcileOnce(ctx, db)

	if queried["snap-up"] != 1 || queried["pause-up"] != 1 {
		t.Fatalf("queried=%v", queried)
	}
	if queried["snap-ready"] != 0 || queried["pause-pending"] != 0 {
		t.Fatalf("must only query inprogress rows, got %v", queried)
	}
	assertRemote(t, db, constants.SnapshotTableName, "snap-up", constants.RemoteStatusReady)
	assertRemote(t, db, constants.SnapshotTableName, "snap-ready", constants.RemoteStatusReady)
	assertRemote(t, db, constants.PauseSnapshotTableName, "pause-up", constants.RemoteStatusReady)
	assertRemote(t, db, constants.PauseSnapshotTableName, "pause-pending", constants.RemoteStatusPending)
}

func TestReconcileOnceLeavesInProgressWhenCubeletNotReady(t *testing.T) {
	db := setupReconcilerDB(t)
	requireNoErr(t, db.Create(&models.SnapshotRecord{
		SnapshotID:   "snap-wait",
		OriginNodeIP: "10.0.0.1",
		Backend:      constants.SnapshotBackendS3,
		RemoteStatus: constants.RemoteStatusInProgress,
	}).Error)
	prevQuery := queryStatus
	prevAddr := cubeletAddr
	queryStatus = func(_ context.Context, _ string, req *snapshotv1.StatusRequest) (*snapshotv1.StatusResponse, error) {
		return &snapshotv1.StatusResponse{SnapshotId: req.GetSnapshotId(), State: "pending"}, nil
	}
	cubeletAddr = func(hostIP string) string { return hostIP }
	t.Cleanup(func() {
		queryStatus = prevQuery
		cubeletAddr = prevAddr
	})

	ReconcileOnce(context.Background(), db)
	assertRemote(t, db, constants.SnapshotTableName, "snap-wait", constants.RemoteStatusInProgress)
}

func TestReconcileOnceDoesNotOverwriteAfterPeerUpdate(t *testing.T) {
	db := setupReconcilerDB(t)
	requireNoErr(t, db.Create(&models.SnapshotRecord{
		SnapshotID:   "snap-race",
		OriginNodeIP: "10.0.0.1",
		Backend:      constants.SnapshotBackendS3,
		RemoteStatus: constants.RemoteStatusInProgress,
	}).Error)
	prevQuery := queryStatus
	prevAddr := cubeletAddr
	queryStatus = func(_ context.Context, _ string, _ *snapshotv1.StatusRequest) (*snapshotv1.StatusResponse, error) {
		requireNoErr(t, db.Table(constants.SnapshotTableName).
			Where("snapshot_id = ?", "snap-race").
			Update("remote_status", constants.RemoteStatusReady).Error)
		return &snapshotv1.StatusResponse{State: constants.RemoteStatusFailed, RemoteReady: false}, nil
	}
	cubeletAddr = func(hostIP string) string { return hostIP }
	t.Cleanup(func() {
		queryStatus = prevQuery
		cubeletAddr = prevAddr
	})

	ReconcileOnce(context.Background(), db)
	assertRemote(t, db, constants.SnapshotTableName, "snap-race", constants.RemoteStatusReady)
}

func TestReconcileOnceSkipsQueryError(t *testing.T) {
	db := setupReconcilerDB(t)
	requireNoErr(t, db.Create(&models.PauseSnapshotRecord{
		SnapshotID:   "pause-err",
		SandboxID:    "sb-err",
		NodeIP:       "10.0.0.9",
		Backend:      constants.SnapshotBackendS3,
		RemoteStatus: constants.RemoteStatusInProgress,
	}).Error)
	prevQuery := queryStatus
	prevAddr := cubeletAddr
	queryStatus = func(_ context.Context, _ string, _ *snapshotv1.StatusRequest) (*snapshotv1.StatusResponse, error) {
		return nil, errors.New("dial failed")
	}
	cubeletAddr = func(hostIP string) string { return hostIP }
	t.Cleanup(func() {
		queryStatus = prevQuery
		cubeletAddr = prevAddr
	})

	ReconcileOnce(context.Background(), db)
	assertRemote(t, db, constants.PauseSnapshotTableName, "pause-err", constants.RemoteStatusInProgress)
}

func TestTerminalRemoteStatusRunningStaysOpen(t *testing.T) {
	if next, ok := terminalRemoteStatus(constants.RemoteStatusRunning, false); ok {
		t.Fatalf("running must not be terminal, got %q", next)
	}
	if next, ok := terminalRemoteStatus(constants.RemoteStatusInProgress, false); ok {
		t.Fatalf("inprogress must not be terminal, got %q", next)
	}
	if next, ok := terminalRemoteStatus(constants.RemoteStatusPending, false); ok {
		t.Fatalf("pending must not be terminal, got %q", next)
	}
	if next, ok := terminalRemoteStatus("", false); ok {
		t.Fatalf("empty state must not be terminal, got %q", next)
	}
	next, ok := terminalRemoteStatus(constants.RemoteStatusReady, true)
	if !ok || next != constants.RemoteStatusReady {
		t.Fatalf("ready terminal=%v %q", ok, next)
	}
}

func assertRemote(t *testing.T, db *gorm.DB, table, snapshotID, want string) {
	t.Helper()
	var got string
	if err := db.Table(table).Select("remote_status").Where("snapshot_id = ?", snapshotID).Scan(&got).Error; err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s %s remote_status=%q, want %q", table, snapshotID, got, want)
	}
}

func requireNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// B15: an async-export row is inprogress with empty export_uuids. Ready must
// fill the uuids from Cubelet, and never flip to ready without them.
func TestReconcileOnceFillsExportUUIDsOnReady(t *testing.T) {
	db := setupReconcilerDB(t)
	requireNoErr(t, db.Create(&models.PauseSnapshotRecord{
		SnapshotID: "pause-async", SandboxID: "sb-a", NodeIP: "10.0.0.2",
		Backend: constants.SnapshotBackendS3, RemoteStatus: constants.RemoteStatusInProgress, Status: "READY",
	}).Error)
	requireNoErr(t, db.Create(&models.SnapshotRecord{
		SnapshotID: "snap-async", OriginNodeIP: "10.0.0.1",
		Backend: constants.SnapshotBackendS3, RemoteStatus: constants.RemoteStatusInProgress, Status: "READY",
	}).Error)
	requireNoErr(t, db.Create(&models.PauseSnapshotRecord{
		SnapshotID: "pause-nouuid", SandboxID: "sb-b", NodeIP: "10.0.0.2",
		Backend: constants.SnapshotBackendS3, RemoteStatus: constants.RemoteStatusInProgress, Status: "READY",
	}).Error)
	prevQuery, prevAddr := queryStatus, cubeletAddr
	queryStatus = func(_ context.Context, _ string, req *snapshotv1.StatusRequest) (*snapshotv1.StatusResponse, error) {
		rsp := &snapshotv1.StatusResponse{SnapshotId: req.GetSnapshotId(), State: constants.RemoteStatusReady, RemoteReady: true}
		if req.GetSnapshotId() != "pause-nouuid" {
			rsp.RemoteUuids = `{"rootfs":"u-root","memory":"u-mem","metadata":"u-meta"}`
		}
		return rsp, nil
	}
	cubeletAddr = func(hostIP string) string { return hostIP }
	t.Cleanup(func() { queryStatus, cubeletAddr = prevQuery, prevAddr })

	ReconcileOnce(context.Background(), db)
	assertRemote(t, db, constants.PauseSnapshotTableName, "pause-async", constants.RemoteStatusReady)
	assertRemote(t, db, constants.SnapshotTableName, "snap-async", constants.RemoteStatusReady)
	assertRemote(t, db, constants.PauseSnapshotTableName, "pause-nouuid", constants.RemoteStatusInProgress)
	var p models.PauseSnapshotRecord
	requireNoErr(t, db.Table(constants.PauseSnapshotTableName).Where("snapshot_id = ?", "pause-async").First(&p).Error)
	if p.ExportUUIDs != `{"rootfs":"u-root","memory":"u-mem","metadata":"u-meta"}` {
		t.Fatalf("pause export_uuids=%q", p.ExportUUIDs)
	}
	var sr models.SnapshotRecord
	requireNoErr(t, db.Table(constants.SnapshotTableName).Where("snapshot_id = ?", "snap-async").First(&sr).Error)
	if sr.ExportUUIDs == "" {
		t.Fatal("snapshot export_uuids not filled")
	}
}

// A legacy (old Cubelet) row already carries its accepted uuids; ready keeps
// them even when the status response repeats a different JSON spelling.
func TestReconcileOnceKeepsLegacyExportUUIDs(t *testing.T) {
	db := setupReconcilerDB(t)
	requireNoErr(t, db.Create(&models.PauseSnapshotRecord{
		SnapshotID: "pause-legacy", SandboxID: "sb-l", NodeIP: "10.0.0.2", ExportUUIDs: "legacy",
		Backend: constants.SnapshotBackendS3, RemoteStatus: constants.RemoteStatusInProgress, Status: "READY",
	}).Error)
	prevQuery, prevAddr := queryStatus, cubeletAddr
	queryStatus = func(_ context.Context, _ string, _ *snapshotv1.StatusRequest) (*snapshotv1.StatusResponse, error) {
		return &snapshotv1.StatusResponse{State: constants.RemoteStatusReady, RemoteReady: true}, nil
	}
	cubeletAddr = func(hostIP string) string { return hostIP }
	t.Cleanup(func() { queryStatus, cubeletAddr = prevQuery, prevAddr })
	ReconcileOnce(context.Background(), db)
	assertRemote(t, db, constants.PauseSnapshotTableName, "pause-legacy", constants.RemoteStatusReady)
	var p models.PauseSnapshotRecord
	requireNoErr(t, db.Table(constants.PauseSnapshotTableName).Where("snapshot_id = ?", "pause-legacy").First(&p).Error)
	if p.ExportUUIDs != "legacy" {
		t.Fatalf("export_uuids=%q", p.ExportUUIDs)
	}
}

// A retrying Cubelet reports running for a natively FAILED role; only its
// terminal give-up is failed.
func TestReconcileOnceRetryingStaysInProgress(t *testing.T) {
	db := setupReconcilerDB(t)
	requireNoErr(t, db.Create(&models.PauseSnapshotRecord{
		SnapshotID: "pause-retry", SandboxID: "sb-r", NodeIP: "10.0.0.2",
		Backend: constants.SnapshotBackendS3, RemoteStatus: constants.RemoteStatusInProgress, Status: "READY",
	}).Error)
	state := constants.RemoteStatusRunning
	prevQuery, prevAddr := queryStatus, cubeletAddr
	queryStatus = func(_ context.Context, _ string, _ *snapshotv1.StatusRequest) (*snapshotv1.StatusResponse, error) {
		return &snapshotv1.StatusResponse{State: state, Message: "export retrying: drain timeout"}, nil
	}
	cubeletAddr = func(hostIP string) string { return hostIP }
	t.Cleanup(func() { queryStatus, cubeletAddr = prevQuery, prevAddr })
	ReconcileOnce(context.Background(), db)
	assertRemote(t, db, constants.PauseSnapshotTableName, "pause-retry", constants.RemoteStatusInProgress)
	state = constants.RemoteStatusFailed
	ReconcileOnce(context.Background(), db)
	assertRemote(t, db, constants.PauseSnapshotTableName, "pause-retry", constants.RemoteStatusFailed)
}
