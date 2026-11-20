// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

// Package remotestatus polls Cubelet Snapshot.Status for S3 rows that are
// still uploading (remote_status=inprogress or peer_ready) and writes the
// next state: peer_ready (importable from the origin's peer only), then
// ready or failed. Multiple CubeMaster replicas may run this loop; updates use
// WHERE remote_status=inprogress so a replica that already finished the
// row is not overwritten.
package remotestatus

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/db/models"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/log"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/cubelet"
	snapshotv1 "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/snapshot/v1"
	"gorm.io/gorm"
)

// Short: a peer_ready row is what lets a cross-node resume start, and it is
// seconds after the pause. One Status RPC per row still uploading.
const reconcileInterval = 2 * time.Second

// pollingStatuses are the remote_status values still moving toward ready.
var pollingStatuses = []string{constants.RemoteStatusInProgress, constants.RemoteStatusPeerReady}

type workItem struct {
	kind        string
	status      string
	table       string
	snapshotID  string
	hostIP      string
	backend     string
	exportUUIDs string
}

const (
	kindSnapshot = "snapshot"
	kindPause    = "pause"
)

var (
	startOnce    sync.Once
	queryStatus  = cubelet.SnapshotStatus
	cubeletAddr  = cubelet.GetCubeletAddr
	nowTimestamp = func() any { return gorm.Expr("CURRENT_TIMESTAMP") }
)

// Start runs the inprogress poller until ctx is cancelled. Safe to call
// once per process; extra calls are ignored.
func Start(ctx context.Context, db *gorm.DB) {
	if db == nil {
		return
	}
	startOnce.Do(func() {
		go func() {
			for {
				ReconcileOnce(ctx, db)
				select {
				case <-ctx.Done():
					return
				case <-time.After(reconcileInterval):
				}
			}
		}()
	})
}

// ReconcileOnce lists inprogress S3 rows on both snapshot tables, queries
// Cubelet Status, and writes ready／failed only while the row is still
// inprogress.
func ReconcileOnce(ctx context.Context, db *gorm.DB) {
	if db == nil {
		return
	}
	items, err := listInProgress(ctx, db)
	if err != nil {
		log.G(ctx).Warnf("remote_status list inprogress failed: %v", err)
		return
	}
	for _, item := range items {
		if ctx.Err() != nil {
			return
		}
		reconcileItem(ctx, db, item)
	}
}

func listInProgress(ctx context.Context, db *gorm.DB) ([]workItem, error) {
	var items []workItem
	var snaps []models.SnapshotRecord
	if err := db.WithContext(ctx).Table(constants.SnapshotTableName).
		Where("backend = ? AND remote_status IN ?", constants.SnapshotBackendS3, pollingStatuses).
		Find(&snaps).Error; err != nil {
		return nil, err
	}
	for _, rec := range snaps {
		items = append(items, workItem{
			kind:        kindSnapshot,
			table:       constants.SnapshotTableName,
			snapshotID:  rec.SnapshotID,
			hostIP:      rec.OriginNodeIP,
			backend:     rec.Backend,
			status:      rec.RemoteStatus,
			exportUUIDs: rec.ExportUUIDs,
		})
	}
	var pauses []models.PauseSnapshotRecord
	if err := db.WithContext(ctx).Table(constants.PauseSnapshotTableName).
		Where("backend = ? AND remote_status IN ?", constants.SnapshotBackendS3, pollingStatuses).
		Find(&pauses).Error; err != nil {
		return nil, err
	}
	for _, rec := range pauses {
		items = append(items, workItem{
			kind:        kindPause,
			table:       constants.PauseSnapshotTableName,
			snapshotID:  rec.SnapshotID,
			hostIP:      rec.NodeIP,
			backend:     rec.Backend,
			status:      rec.RemoteStatus,
			exportUUIDs: rec.ExportUUIDs,
		})
	}
	return items, nil
}

func reconcileItem(ctx context.Context, db *gorm.DB, item workItem) {
	hostIP := strings.TrimSpace(item.hostIP)
	snapshotID := strings.TrimSpace(item.snapshotID)
	if hostIP == "" || snapshotID == "" {
		return
	}
	st, err := queryStatus(ctx, cubeletAddr(hostIP), &snapshotv1.StatusRequest{
		SnapshotId: snapshotID,
		Backend:    item.backend,
	})
	if err != nil || st == nil {
		log.G(ctx).Warnf("remote_status query %s %s on %s failed: %v", item.kind, snapshotID, hostIP, err)
		return
	}
	next, ok := terminalRemoteStatus(st.GetState(), st.GetRemoteReady())
	if !ok || strings.EqualFold(strings.TrimSpace(item.status), next) {
		return
	}
	updates := map[string]any{
		"remote_status": next,
		"updated_at":    nowTimestamp(),
	}
	if next == constants.RemoteStatusReady || next == constants.RemoteStatusPeerReady {
		// Async export (B15) rows are inprogress with empty export_uuids; the
		// identities arrive here. A row must never become ready without them,
		// since cross-node restore imports exactly these uuids. Rows that
		// already carry uuids (legacy Cubelet) keep them.
		if strings.TrimSpace(item.exportUUIDs) == "" {
			raw := strings.TrimSpace(st.GetRemoteUuids())
			if raw == "" {
				log.G(ctx).Warnf("remote_status %s %s: cubelet ready without remote_uuids; keeping inprogress", item.kind, snapshotID)
				return
			}
			updates["export_uuids"] = raw
		}
	}
	res := db.WithContext(ctx).Table(item.table).
		Where("snapshot_id = ? AND remote_status IN ?", snapshotID, pollingStatuses).
		Updates(updates)
	if res.Error != nil {
		log.G(ctx).Warnf("remote_status update %s %s failed: %v", item.kind, snapshotID, res.Error)
		return
	}
	if res.RowsAffected == 0 {
		return
	}
	log.G(ctx).Infof("remote_status %s %s -> %s", item.kind, snapshotID, next)
}

func terminalRemoteStatus(state string, remoteReady bool) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case constants.RemoteStatusReady:
		return constants.RemoteStatusReady, true
	case constants.RemoteStatusFailed:
		return constants.RemoteStatusFailed, true
	case constants.RemoteStatusPeerReady:
		return constants.RemoteStatusPeerReady, true
	case constants.RemoteStatusRunning, constants.RemoteStatusInProgress, constants.RemoteStatusPending:
		return "", false
	}
	if remoteReady {
		return constants.RemoteStatusReady, true
	}
	return "", false
}
