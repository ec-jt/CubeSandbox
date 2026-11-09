// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package nodemeta

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newTestLegacy builds a legacy service on sqlite with the production column
// set and unique keys (node_id; node_id+component).
func newTestLegacy(t *testing.T) *legacyService {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "nodemeta.db")), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&legacyRegistration{}, &legacyStatus{}, &legacyComponentVersion{}))
	require.NoError(t, db.Exec("CREATE UNIQUE INDEX uk_reg ON t_cube_node_registration(node_id)").Error)
	require.NoError(t, db.Exec("CREATE UNIQUE INDEX uk_st ON t_cube_node_status(node_id)").Error)
	require.NoError(t, db.Exec("CREATE UNIQUE INDEX uk_ver ON t_cube_node_component_version(node_id, component)").Error)
	s := newLegacyService(db)
	s.healthWindow = func() time.Duration { return 40 * time.Second }
	return s
}

// TestLegacyReloadServesPersistedNodes is the B1 regression: a DB populated
// by v0.6 CubeMaster (registration + status rows, empty t_cube_host_info)
// must produce healthy scheduler nodes without CubeOps.
func TestLegacyReloadServesPersistedNodes(t *testing.T) {
	s := newTestLegacy(t)
	now := time.Unix(1_800_000_000, 0)
	s.now = func() time.Time { return now }
	for _, id := range []string{"10.0.1.1", "10.0.1.2", "10.0.1.20"} {
		require.NoError(t, s.db.Create(&legacyRegistration{NodeID: id, HostIP: id, LabelsJSON: "{}",
			CapacityJSON: `{"milli_cpu":40000,"memory_mb":241029}`, QuotaCPU: 40000, QuotaMemMB: 241029, MaxMvmNum: 470}).Error)
		require.NoError(t, s.db.Create(&legacyStatus{NodeID: id, HeartbeatUnix: now.Add(-5 * time.Second).Unix(), Healthy: true,
			LocalTemplatesJSON: `[{"template_id":"tpl-a"}]`}).Error)
	}
	require.NoError(t, s.reload())
	nodes, err := s.schedulerNodes(context.Background())
	require.NoError(t, err)
	require.Len(t, nodes, 3)
	for _, n := range nodes {
		require.True(t, n.Healthy, n.InsID)
		require.Equal(t, n.InsID, n.IP)
		require.Equal(t, 40, n.CpuTotal)
		require.Equal(t, int64(470), n.MaxMvmLimit)
		require.Equal(t, []string{"tpl-a"}, n.LocalTemplates)
		require.False(t, n.SchedulingDisabled())
	}
}

func TestLegacyStaleHeartbeatIsUnhealthy(t *testing.T) {
	s := newTestLegacy(t)
	now := time.Unix(1_800_000_000, 0)
	s.now = func() time.Time { return now }
	require.NoError(t, s.db.Create(&legacyRegistration{NodeID: "n1", HostIP: "n1", LabelsJSON: "{}"}).Error)
	require.NoError(t, s.db.Create(&legacyStatus{NodeID: "n1", HeartbeatUnix: now.Add(-5 * time.Minute).Unix(), Healthy: true}).Error)
	require.NoError(t, s.reload())
	got := s.list()
	require.Len(t, got, 1)
	require.False(t, got[0].Healthy)
}

func TestLegacyStatusWithoutRegistrationIgnored(t *testing.T) {
	s := newTestLegacy(t)
	require.NoError(t, s.db.Create(&legacyStatus{NodeID: "ghost", HeartbeatUnix: time.Now().Unix(), Healthy: true}).Error)
	require.NoError(t, s.reload())
	require.Empty(t, s.list())
}

func TestLegacyCordonLabelPreservedAcrossRegister(t *testing.T) {
	s := newTestLegacy(t)
	require.NoError(t, s.db.Create(&legacyRegistration{NodeID: "n1", HostIP: "n1",
		LabelsJSON: `{"` + constants.LabelSchedulingDisabled + `":"true","zone":"a"}`}).Error)
	require.NoError(t, s.reload())
	merged := mergeCubeletLabels(map[string]string{constants.LabelSchedulingDisabled: "true", "zone": "a"},
		map[string]string{"zone": "b", constants.LabelSchedulingDisabled: "evil"})
	require.Equal(t, "true", merged[constants.LabelSchedulingDisabled])
	require.Equal(t, "b", merged["zone"])
	got := s.list()
	require.Len(t, got, 1)
	require.True(t, got[0].SchedulingDisabled)
}

func TestLegacyCorruptLabelsFailClosed(t *testing.T) {
	s := newTestLegacy(t)
	require.NoError(t, s.db.Create(&legacyRegistration{NodeID: "n1", HostIP: "n1", LabelsJSON: "{bad"}).Error)
	require.NoError(t, s.reload())
	got := s.list()
	require.Len(t, got, 1)
	require.True(t, got[0].SchedulingDisabled)
}

func TestLegacyReloadKeepsNewerInMemoryHeartbeat(t *testing.T) {
	s := newTestLegacy(t)
	now := time.Unix(1_800_000_000, 0)
	s.now = func() time.Time { return now }
	require.NoError(t, s.db.Create(&legacyRegistration{NodeID: "n1", HostIP: "n1", LabelsJSON: "{}"}).Error)
	require.NoError(t, s.db.Create(&legacyStatus{NodeID: "n1", HeartbeatUnix: now.Add(-time.Hour).Unix(), Healthy: true}).Error)
	require.NoError(t, s.reload())
	s.mu.Lock()
	s.nodes["n1"].HeartbeatTime = now
	s.nodes["n1"].ReportedReady = true
	s.mu.Unlock()
	require.NoError(t, s.reload())
	got := s.list()
	require.True(t, got[0].Healthy)
}

func TestLegacyDisabledWithoutInit(t *testing.T) {
	legacyMu.Lock()
	prev := legacySvc
	legacySvc = nil
	legacyMu.Unlock()
	t.Cleanup(func() { legacyMu.Lock(); legacySvc = prev; legacyMu.Unlock() })
	_, err := LegacyListNodes(context.Background())
	require.ErrorIs(t, err, ErrLegacyDisabled)
	require.False(t, LegacyEnabled())
}
