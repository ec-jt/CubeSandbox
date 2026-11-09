// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package nodemeta

// Legacy (CubeOps-less) node metadata service.
//
// Before CubeOps took over node management (#1384), CubeMaster itself served
// /internal/meta/nodes/register and /internal/meta/nodes/:id/status, persisted
// t_cube_node_registration / t_cube_node_status / t_cube_node_component_version
// and fed localcache from that view. #1384 removed the routes and made CubeOps
// the only node source. A cluster that upgrades CubeMaster while its Cubelets
// (and CubeOps) still run the previous release therefore ends up with an empty
// scheduler view: Cubelet heartbeats hit a removed route, localcache falls back
// to the long-empty t_cube_host_info, node list and sandbox list go empty.
//
// When cube_ops_addr is NOT configured, this file restores the pre-#1384
// behaviour: CubeMaster accepts registration/heartbeats itself (on both the
// legacy /internal/meta/* paths and the new /internal/v1/node-agent/* paths,
// so old and new Cubelets that point at CubeMaster both work), persists them in
// the same tables CubeOps uses, and registers a DB-backed node loader into
// localcache. With cube_ops_addr set, nothing here is active.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/config"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/log"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/nodehealth"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/recov"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/localcache"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	corev1 "k8s.io/api/core/v1"
)

// ErrLegacyDisabled is returned by the legacy handlers when CubeOps owns node
// metadata (cube_ops_addr configured).
var ErrLegacyDisabled = errors.New("node metadata is served by CubeOps (cube_ops_addr is set)")

// ErrLabelsJSONCorrupt marks a registration whose labels_json cannot be parsed.
var ErrLabelsJSONCorrupt = errors.New("node labels_json is corrupt")

type ResourceSnapshot struct {
	MilliCPU int64 `json:"milli_cpu,omitempty"`
	MemoryMB int64 `json:"memory_mb,omitempty"`
}

type ContainerImage struct {
	Names     []string `json:"names,omitempty"`
	SizeBytes int64    `json:"size_bytes,omitempty"`
	Namespace string   `json:"namespace,omitempty"`
	MediaType string   `json:"media_type,omitempty"`
}

type LocalTemplate struct {
	TemplateID string `json:"template_id,omitempty"`
	ID         string `json:"id,omitempty"`
	Media      string `json:"media,omitempty"`
	Path       string `json:"path,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
}

// RegisterNodeRequest is the Cubelet registration payload. It is a superset
// of the v0.6 (no host_facts) and v0.7 (host_facts) Cubelet shapes.
type RegisterNodeRequest struct {
	RequestID           string             `json:"requestID,omitempty"`
	NodeID              string             `json:"node_id,omitempty"`
	HostIP              string             `json:"host_ip,omitempty"`
	GRPCPort            int                `json:"grpc_port,omitempty"`
	Labels              map[string]string  `json:"labels,omitempty"`
	Capacity            ResourceSnapshot   `json:"capacity,omitempty"`
	Allocatable         ResourceSnapshot   `json:"allocatable,omitempty"`
	InstanceType        string             `json:"instance_type,omitempty"`
	ClusterLabel        string             `json:"cluster_label,omitempty"`
	QuotaCPU            int64              `json:"quota_cpu,omitempty"`
	QuotaMemMB          int64              `json:"quota_mem_mb,omitempty"`
	CreateConcurrentNum int64              `json:"create_concurrent_num,omitempty"`
	MaxMvmNum           int64              `json:"max_mvm_num,omitempty"`
	Versions            []ComponentVersion `json:"versions,omitempty"`
	InventoryIncomplete bool               `json:"inventory_incomplete,omitempty"`
	HostFacts           *HostFacts         `json:"host_facts,omitempty"`
}

type AllocatedResources struct {
	MilliCPU      int64 `json:"milli_cpu,omitempty"`
	MemoryMB      int64 `json:"memory_mb,omitempty"`
	MvmNum        int64 `json:"mvm_num,omitempty"`
	MvmRunningNum int64 `json:"mvm_running_num,omitempty"`
	NicQueues     int64 `json:"nic_queues,omitempty"`
	DataDiskMB    int64 `json:"data_disk_mb,omitempty"`
	StorageDiskMB int64 `json:"storage_disk_mb,omitempty"`
}

type DiskUsage struct {
	DataDiskUsagePer    float64 `json:"data_disk_usage_per,omitempty"`
	StorageDiskUsagePer float64 `json:"storage_disk_usage_per,omitempty"`
	SysDiskUsagePer     float64 `json:"sys_disk_usage_per,omitempty"`
}

type RealMetrics struct {
	CpuUtilPct   float64   `json:"cpu_util_pct,omitempty"`
	PerCoreUtils []float64 `json:"per_core_utils,omitempty"`
	Load1        float64   `json:"load1,omitempty"`
	Load5        float64   `json:"load5,omitempty"`
	Load15       float64   `json:"load15,omitempty"`
	DiskIOPS     float64   `json:"disk_iops,omitempty"`
	DiskReadBps  float64   `json:"disk_read_bps,omitempty"`
	DiskWriteBps float64   `json:"disk_write_bps,omitempty"`
}

type UpdateNodeStatusRequest struct {
	RequestID           string                 `json:"requestID,omitempty"`
	Conditions          []corev1.NodeCondition `json:"conditions,omitempty"`
	Images              []ContainerImage       `json:"images,omitempty"`
	LocalTemplates      []LocalTemplate        `json:"local_templates,omitempty"`
	HeartbeatTime       time.Time              `json:"heartbeat_time,omitempty"`
	Allocated           *AllocatedResources    `json:"allocated,omitempty"`
	DiskUsage           *DiskUsage             `json:"disk_usage,omitempty"`
	MetricTime          time.Time              `json:"metric_time,omitempty"`
	RealMetrics         *RealMetrics           `json:"real_metrics,omitempty"`
	Versions            []ComponentVersion     `json:"versions,omitempty"`
	InventoryIncomplete bool                   `json:"inventory_incomplete,omitempty"`
	HostFacts           *HostFacts             `json:"host_facts,omitempty"`
}

// NodeSnapshot is the legacy per-node view served by /internal/meta/nodes.
type NodeSnapshot struct {
	NodeID              string                 `json:"node_id,omitempty"`
	HostIP              string                 `json:"host_ip,omitempty"`
	GRPCPort            int                    `json:"grpc_port,omitempty"`
	Labels              map[string]string      `json:"labels,omitempty"`
	Capacity            ResourceSnapshot       `json:"capacity,omitempty"`
	Allocatable         ResourceSnapshot       `json:"allocatable,omitempty"`
	InstanceType        string                 `json:"instance_type,omitempty"`
	ClusterLabel        string                 `json:"cluster_label,omitempty"`
	QuotaCPU            int64                  `json:"quota_cpu,omitempty"`
	QuotaMemMB          int64                  `json:"quota_mem_mb,omitempty"`
	CreateConcurrentNum int64                  `json:"create_concurrent_num,omitempty"`
	MaxMvmNum           int64                  `json:"max_mvm_num,omitempty"`
	Conditions          []corev1.NodeCondition `json:"conditions,omitempty"`
	LocalTemplates      []LocalTemplate        `json:"local_templates,omitempty"`
	Versions            []ComponentVersion     `json:"versions,omitempty"`
	HostFacts           *HostFacts             `json:"host_facts,omitempty"`
	HeartbeatTime       time.Time              `json:"heartbeat_time,omitempty"`
	ReportedReady       bool                   `json:"reported_ready"`
	Healthy             bool                   `json:"healthy"`
	UnhealthyReason     string                 `json:"unhealthy_reason,omitempty"`
	SchedulingDisabled  bool                   `json:"scheduling_disabled"`

	labelsCorrupt bool
	versionsHash  string
}

// Row models. Same tables and columns CubeOps writes; the schema is owned by
// pkgs/cubedb migrations (baseline + 0004 + 20260810120000_host_facts).
type legacyRegistration struct {
	gorm.Model
	NodeID              string `gorm:"column:node_id"`
	HostIP              string `gorm:"column:host_ip"`
	GRPCPort            int    `gorm:"column:grpc_port"`
	LabelsJSON          string `gorm:"column:labels_json"`
	CapacityJSON        string `gorm:"column:capacity_json"`
	AllocatableJSON     string `gorm:"column:allocatable_json"`
	InstanceType        string `gorm:"column:instance_type"`
	ClusterLabel        string `gorm:"column:cluster_label"`
	QuotaCPU            int64  `gorm:"column:quota_cpu"`
	QuotaMemMB          int64  `gorm:"column:quota_mem_mb"`
	CreateConcurrentNum int64  `gorm:"column:create_concurrent_num"`
	MaxMvmNum           int64  `gorm:"column:max_mvm_num"`
	HostFactsJSON       string `gorm:"column:host_facts_json"`
	CPUVendor           string `gorm:"column:cpu_vendor"`
	CPUModel            string `gorm:"column:cpu_model"`
	CPUIDHash           string `gorm:"column:cpuid_hash"`
	HostKernelRelease   string `gorm:"column:host_kernel_release"`
}

func (legacyRegistration) TableName() string { return constants.NodeMetaRegistrationTable }

type legacyStatus struct {
	gorm.Model
	NodeID             string `gorm:"column:node_id"`
	ConditionsJSON     string `gorm:"column:conditions_json"`
	ImagesJSON         string `gorm:"column:images_json"`
	LocalTemplatesJSON string `gorm:"column:local_templates_json"`
	HeartbeatUnix      int64  `gorm:"column:heartbeat_unix"`
	Healthy            bool   `gorm:"column:healthy"`
}

func (legacyStatus) TableName() string { return constants.NodeMetaStatusTable }

type legacyComponentVersion struct {
	ID           uint   `gorm:"primarykey"`
	NodeID       string `gorm:"column:node_id"`
	Component    string `gorm:"column:component"`
	Version      string `gorm:"column:version"`
	Commit       string `gorm:"column:commit"`
	BuildTime    string `gorm:"column:build_time"`
	Source       string `gorm:"column:source"`
	ReportedUnix int64  `gorm:"column:reported_unix"`
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

func (legacyComponentVersion) TableName() string { return constants.NodeComponentVersionTable }

type legacyService struct {
	db           *gorm.DB
	mu           sync.RWMutex
	nodes        map[string]*NodeSnapshot
	healthWindow func() time.Duration
	now          func() time.Time
}

var (
	legacyMu  sync.RWMutex
	legacySvc *legacyService
)

// LegacyEnabled reports whether CubeMaster serves node metadata itself.
func LegacyEnabled() bool {
	legacyMu.RLock()
	defer legacyMu.RUnlock()
	return legacySvc != nil
}

func currentLegacy() (*legacyService, error) {
	legacyMu.RLock()
	defer legacyMu.RUnlock()
	if legacySvc == nil {
		return nil, ErrLegacyDisabled
	}
	return legacySvc, nil
}

func newLegacyService(db *gorm.DB) *legacyService {
	return &legacyService{
		db:    db,
		nodes: map[string]*NodeSnapshot{},
		healthWindow: func() time.Duration {
			return nodehealth.MetadataTimeout(config.GetConfig().Common.SyncMetaDataInterval)
		},
		now: time.Now,
	}
}

// initLegacy loads the persisted node view and registers it as the localcache
// node loader. Called from Init when cube_ops_addr is empty.
func initLegacy(ctx context.Context, db *gorm.DB) error {
	if db == nil {
		return errors.New("nodemeta legacy: nil db")
	}
	s := newLegacyService(db)
	if err := s.reload(); err != nil {
		return fmt.Errorf("nodemeta legacy reload: %w", err)
	}
	legacyMu.Lock()
	legacySvc = s
	legacyMu.Unlock()
	localcache.RegisterNodeLoader(s.schedulerNodes)
	log.G(ctx).Infof("nodemeta: legacy DB node source active (no cube_ops_addr); %d nodes loaded", s.count())
	go s.loopReload(ctx)
	return nil
}

func (s *legacyService) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.nodes)
}

func (s *legacyService) loopReload(ctx context.Context) {
	interval := config.GetConfig().Common.SyncMetaDataInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recov.WithRecover(func() {
				if err := s.reload(); err != nil {
					log.G(ctx).Warnf("nodemeta legacy periodic reload failed: %v", err)
				}
			}, func(panicError interface{}) {
				log.G(context.Background()).Errorf("nodemeta legacy reload panic: %v", panicError)
			})
		}
	}
}

// reload merges the DB view into memory. Registration and versions take the
// DB value; heartbeat state keeps whichever is newer (another replica may
// have received the latest heartbeat).
func (s *legacyService) reload() error {
	var regs []legacyRegistration
	if err := s.db.Find(&regs).Error; err != nil {
		return err
	}
	var statuses []legacyStatus
	if err := s.db.Find(&statuses).Error; err != nil {
		return err
	}
	var versions []legacyComponentVersion
	if err := s.db.Find(&versions).Error; err != nil {
		return err
	}
	next := make(map[string]*NodeSnapshot, len(regs))
	for i := range regs {
		next[regs[i].NodeID] = snapshotFromRegistration(&regs[i])
	}
	for i := range statuses {
		st := &statuses[i]
		snap, ok := next[st.NodeID]
		if !ok {
			// A status row without a registration (deleted node) is not a
			// scheduling candidate; matches CubeOps.
			continue
		}
		_ = json.Unmarshal([]byte(st.ConditionsJSON), &snap.Conditions)
		_ = json.Unmarshal([]byte(st.LocalTemplatesJSON), &snap.LocalTemplates)
		snap.HeartbeatTime = time.Unix(st.HeartbeatUnix, 0)
		snap.ReportedReady = st.Healthy
	}
	for i := range versions {
		v := &versions[i]
		if snap, ok := next[v.NodeID]; ok {
			snap.Versions = append(snap.Versions, ComponentVersion{
				Component: v.Component, Version: v.Version, Commit: v.Commit,
				BuildTime: v.BuildTime, Source: v.Source,
			})
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for id, n := range next {
		n.versionsHash = versionsHash(n.Versions)
		if cur, ok := s.nodes[id]; ok && cur.HeartbeatTime.After(n.HeartbeatTime) {
			n.Conditions = cur.Conditions
			n.LocalTemplates = cur.LocalTemplates
			n.HeartbeatTime = cur.HeartbeatTime
			n.ReportedReady = cur.ReportedReady
		}
		s.applyHealth(n, now)
	}
	s.nodes = next
	return nil
}

func snapshotFromRegistration(reg *legacyRegistration) *NodeSnapshot {
	snap := &NodeSnapshot{
		NodeID:              reg.NodeID,
		HostIP:              reg.HostIP,
		GRPCPort:            reg.GRPCPort,
		InstanceType:        reg.InstanceType,
		ClusterLabel:        reg.ClusterLabel,
		QuotaCPU:            reg.QuotaCPU,
		QuotaMemMB:          reg.QuotaMemMB,
		CreateConcurrentNum: reg.CreateConcurrentNum,
		MaxMvmNum:           reg.MaxMvmNum,
	}
	labels, err := parseLabelsJSON(reg.LabelsJSON)
	if err != nil {
		snap.labelsCorrupt = true
		labels = map[string]string{}
	}
	snap.Labels = labels
	_ = json.Unmarshal([]byte(reg.CapacityJSON), &snap.Capacity)
	_ = json.Unmarshal([]byte(reg.AllocatableJSON), &snap.Allocatable)
	if strings.TrimSpace(reg.HostFactsJSON) != "" {
		hf := &HostFacts{}
		if json.Unmarshal([]byte(reg.HostFactsJSON), hf) == nil && !hf.IsZero() {
			snap.HostFacts = hf
		}
	}
	return snap
}

func (s *legacyService) applyHealth(snap *NodeSnapshot, now time.Time) {
	st := nodehealth.EvaluateFromFacts(snap.ReportedReady, snap.HeartbeatTime, now, s.healthWindow())
	snap.Healthy = st.Healthy
	snap.UnhealthyReason = st.UnhealthyReason
	snap.SchedulingDisabled = snap.labelsCorrupt || node.DecodeSchedulingDisabled(snap.Labels)
}

func (s *legacyService) register(ctx context.Context, req *RegisterNodeRequest) (*NodeSnapshot, error) {
	if req == nil || strings.TrimSpace(req.NodeID) == "" {
		return nil, errors.New("node_id is required")
	}
	if req.HostIP == "" {
		req.HostIP = req.NodeID
	}
	reg := &legacyRegistration{
		NodeID:              req.NodeID,
		HostIP:              req.HostIP,
		GRPCPort:            req.GRPCPort,
		CapacityJSON:        mustJSON(req.Capacity),
		AllocatableJSON:     mustJSON(req.Allocatable),
		InstanceType:        req.InstanceType,
		ClusterLabel:        req.ClusterLabel,
		QuotaCPU:            req.QuotaCPU,
		QuotaMemMB:          req.QuotaMemMB,
		CreateConcurrentNum: req.CreateConcurrentNum,
		MaxMvmNum:           req.MaxMvmNum,
		LabelsJSON:          "{}",
	}
	updateCols := []string{
		"host_ip", "grpc_port", "capacity_json", "allocatable_json",
		"instance_type", "cluster_label", "quota_cpu", "quota_mem_mb",
		"create_concurrent_num", "max_mvm_num", "updated_at", "deleted_at",
	}
	if req.HostFacts != nil && !req.HostFacts.IsZero() {
		setRegistrationHostFacts(reg, req.HostFacts)
		updateCols = append(updateCols, "host_facts_json", "cpu_vendor", "cpu_model", "cpuid_hash", "host_kernel_release")
	}
	var merged map[string]string
	err := s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "node_id"}},
			DoUpdates: clause.AssignmentColumns(updateCols),
		}).Create(reg).Error; err != nil {
			return err
		}
		var cur legacyRegistration
		if err := tx.Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("node_id = ?", req.NodeID).Take(&cur).Error; err != nil {
			return err
		}
		existing, err := parseLabelsJSON(cur.LabelsJSON)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrLabelsJSONCorrupt, err)
		}
		merged = mergeCubeletLabels(existing, req.Labels)
		return tx.Model(&legacyRegistration{}).Unscoped().Where("node_id = ?", req.NodeID).
			Update("labels_json", mustJSON(merged)).Error
	})
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	snap, ok := s.nodes[req.NodeID]
	if !ok {
		snap = &NodeSnapshot{NodeID: req.NodeID}
		s.nodes[req.NodeID] = snap
	}
	snap.HostIP = req.HostIP
	snap.GRPCPort = req.GRPCPort
	snap.Labels = merged
	snap.labelsCorrupt = false
	snap.Capacity = req.Capacity
	snap.Allocatable = req.Allocatable
	snap.InstanceType = req.InstanceType
	snap.ClusterLabel = req.ClusterLabel
	snap.QuotaCPU = req.QuotaCPU
	snap.QuotaMemMB = req.QuotaMemMB
	snap.CreateConcurrentNum = req.CreateConcurrentNum
	snap.MaxMvmNum = req.MaxMvmNum
	if req.HostFacts != nil && !req.HostFacts.IsZero() {
		hf := *req.HostFacts
		snap.HostFacts = &hf
	}
	s.applyHealth(snap, s.now())
	out := cloneLegacySnapshot(snap)
	s.mu.Unlock()
	s.persistVersions(ctx, req.NodeID, req.Versions, req.InventoryIncomplete)
	localcache.UpsertNode(toSchedulerNode(out))
	log.G(ctx).Infof("nodemeta legacy: node registered node=%s host=%s", req.NodeID, req.HostIP)
	return out, nil
}

func (s *legacyService) updateStatus(ctx context.Context, nodeID string, req *UpdateNodeStatusRequest) (*NodeSnapshot, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return nil, errors.New("node_id is required")
	}
	if req == nil {
		req = &UpdateNodeStatusRequest{}
	}
	if req.HeartbeatTime.IsZero() {
		req.HeartbeatTime = s.now()
	}
	ready := nodehealth.ReadyConditionTrue(req.Conditions)
	st := &legacyStatus{
		NodeID:             nodeID,
		ConditionsJSON:     mustJSON(req.Conditions),
		ImagesJSON:         mustJSON(req.Images),
		LocalTemplatesJSON: mustJSON(req.LocalTemplates),
		HeartbeatUnix:      req.HeartbeatTime.Unix(),
		Healthy:            ready,
	}
	if err := s.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "node_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"conditions_json", "images_json", "local_templates_json",
			"heartbeat_unix", "healthy", "updated_at",
		}),
	}).Create(st).Error; err != nil {
		return nil, err
	}
	if req.HostFacts != nil && !req.HostFacts.IsZero() {
		reg := &legacyRegistration{}
		setRegistrationHostFacts(reg, req.HostFacts)
		if err := s.db.Model(&legacyRegistration{}).Where("node_id = ?", nodeID).Updates(map[string]interface{}{
			"host_facts_json": reg.HostFactsJSON, "cpu_vendor": reg.CPUVendor, "cpu_model": reg.CPUModel,
			"cpuid_hash": reg.CPUIDHash, "host_kernel_release": reg.HostKernelRelease,
		}).Error; err != nil {
			log.G(ctx).Warnf("nodemeta legacy: persist host facts node=%s: %v", nodeID, err)
		}
	}
	s.mu.Lock()
	snap, ok := s.nodes[nodeID]
	if !ok {
		// Heartbeat before this replica saw the registration: pull it from DB.
		var reg legacyRegistration
		if err := s.db.Where("node_id = ?", nodeID).Take(&reg).Error; err != nil {
			s.mu.Unlock()
			return nil, fmt.Errorf("node %s is not registered: %w", nodeID, err)
		}
		snap = snapshotFromRegistration(&reg)
		s.nodes[nodeID] = snap
	}
	snap.Conditions = append([]corev1.NodeCondition(nil), req.Conditions...)
	snap.LocalTemplates = append([]LocalTemplate(nil), req.LocalTemplates...)
	snap.HeartbeatTime = req.HeartbeatTime
	snap.ReportedReady = ready
	if req.HostFacts != nil && !req.HostFacts.IsZero() {
		hf := *req.HostFacts
		snap.HostFacts = &hf
	}
	s.applyHealth(snap, s.now())
	out := cloneLegacySnapshot(snap)
	s.mu.Unlock()

	sn := toSchedulerNode(out)
	localcache.UpsertNode(sn)
	localcache.SyncNodeTemplates(ctx, nodeID, sn.LocalTemplates)
	fanOutMetric(ctx, nodeID, req)
	s.persistVersions(ctx, nodeID, req.Versions, req.InventoryIncomplete)
	return out, nil
}

func fanOutMetric(ctx context.Context, nodeID string, req *UpdateNodeStatusRequest) {
	if req.Allocated == nil && req.DiskUsage == nil && req.RealMetrics == nil {
		return
	}
	mt := req.MetricTime
	if mt.IsZero() {
		mt = time.Now()
	}
	m := &localcache.NodeMetric{NodeID: nodeID, MetricTime: mt}
	if a := req.Allocated; a != nil {
		m.HasAllocated = true
		m.MilliCPUUsage, m.MemoryMBUsage, m.MvmNum, m.NicQueues = a.MilliCPU, a.MemoryMB, a.MvmNum, a.NicQueues
	}
	if d := req.DiskUsage; d != nil {
		m.HasDisk = true
		m.DataDiskUsagePer, m.StorageDiskUsagePer, m.SysDiskUsagePer = d.DataDiskUsagePer, d.StorageDiskUsagePer, d.SysDiskUsagePer
	}
	if r := req.RealMetrics; r != nil {
		m.HasRealMetrics = true
		m.CpuUtilPct = r.CpuUtilPct
		m.PerCoreUtils = append([]float64(nil), r.PerCoreUtils...)
		m.Load1, m.Load5, m.Load15 = r.Load1, r.Load5, r.Load15
		m.DiskIOPS, m.DiskReadBps, m.DiskWriteBps = r.DiskIOPS, r.DiskReadBps, r.DiskWriteBps
	}
	if err := localcache.WriteNodeMetric(ctx, m); err != nil {
		log.G(ctx).Warnf("nodemeta legacy: write node metric node=%s: %v", nodeID, err)
	}
	if err := localcache.UpdateNodeMetricInProcess(m); err != nil {
		log.G(ctx).Debugf("nodemeta legacy: in-process metric skipped node=%s: %v", nodeID, err)
	}
}

// persistVersions upserts component versions, skipping unchanged sets.
func (s *legacyService) persistVersions(ctx context.Context, nodeID string, versions []ComponentVersion, incomplete bool) {
	if len(versions) == 0 {
		return
	}
	s.mu.RLock()
	snap := s.nodes[nodeID]
	var prev []ComponentVersion
	prevHash := ""
	if snap != nil {
		prev = append(prev, snap.Versions...)
		prevHash = snap.versionsHash
	}
	s.mu.RUnlock()
	effective := versions
	if incomplete {
		effective = mergeComponentVersions(prev, versions)
	}
	h := versionsHash(effective)
	if h == prevHash {
		return
	}
	now := s.now().Unix()
	rows := make([]*legacyComponentVersion, 0, len(versions))
	keep := make([]string, 0, len(versions))
	for _, v := range versions {
		if v.Component == "" {
			continue
		}
		rows = append(rows, &legacyComponentVersion{NodeID: nodeID, Component: v.Component, Version: v.Version,
			Commit: v.Commit, BuildTime: v.BuildTime, Source: v.Source, ReportedUnix: now})
		keep = append(keep, v.Component)
	}
	err := s.db.Transaction(func(tx *gorm.DB) error {
		if len(rows) > 0 {
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "node_id"}, {Name: "component"}},
				DoUpdates: clause.AssignmentColumns([]string{"version", "commit", "build_time", "source", "reported_unix", "updated_at"}),
			}).Create(&rows).Error; err != nil {
				return err
			}
		}
		if incomplete {
			return nil
		}
		del := tx.Where("node_id = ?", nodeID)
		if len(keep) > 0 {
			del = del.Where("component NOT IN ?", keep)
		}
		return del.Delete(&legacyComponentVersion{}).Error
	})
	if err != nil {
		log.G(ctx).Warnf("nodemeta legacy: write versions node=%s: %v", nodeID, err)
		return
	}
	s.mu.Lock()
	if cur := s.nodes[nodeID]; cur != nil {
		cur.Versions = effective
		cur.versionsHash = h
	}
	s.mu.Unlock()
}

func (s *legacyService) list() []*NodeSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	out := make([]*NodeSnapshot, 0, len(s.nodes))
	for _, n := range s.nodes {
		s.applyHealth(n, now)
		out = append(out, cloneLegacySnapshot(n))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func (s *legacyService) schedulerNodes(ctx context.Context) ([]*node.Node, error) {
	_ = ctx
	snaps := s.list()
	out := make([]*node.Node, 0, len(snaps))
	for _, sn := range snaps {
		out = append(out, toSchedulerNode(sn))
	}
	return out, nil
}

// toSchedulerNode mirrors CubeOps service.ToSchedulerNode so localcache sees
// the same shape from either source.
func toSchedulerNode(snap *NodeSnapshot) *node.Node {
	quotaCPU := snap.QuotaCPU
	if quotaCPU == 0 {
		quotaCPU = snap.Allocatable.MilliCPU
	}
	quotaMem := snap.QuotaMemMB
	if quotaMem == 0 {
		quotaMem = snap.Allocatable.MemoryMB
	}
	hostIP := snap.HostIP
	if hostIP == "" {
		hostIP = snap.NodeID
	}
	instanceType := snap.InstanceType
	if instanceType == "" {
		instanceType = constants.DefaultInstanceTypeName
	}
	var templates []string
	for _, t := range snap.LocalTemplates {
		if t.TemplateID != "" {
			templates = append(templates, t.TemplateID)
		}
	}
	n := &node.Node{
		InsID:               snap.NodeID,
		UUID:                snap.NodeID,
		IP:                  hostIP,
		CpuTotal:            int(snap.Capacity.MilliCPU / 1000),
		MemMBTotal:          snap.Capacity.MemoryMB,
		QuotaCpu:            quotaCPU,
		QuotaMem:            quotaMem,
		ClusterLabel:        snap.ClusterLabel,
		OssClusterLabel:     snap.ClusterLabel,
		InstanceType:        instanceType,
		HostStatus:          constants.HostStatusRunning,
		ReportedReady:       snap.ReportedReady,
		Healthy:             snap.Healthy,
		UnhealthyReason:     snap.UnhealthyReason,
		CreateConcurrentNum: snap.CreateConcurrentNum,
		MaxMvmLimit:         snap.MaxMvmNum,
		MetaDataUpdateAt:    snap.HeartbeatTime,
		NodeLabels:          cloneLabels(snap.Labels),
		LocalTemplates:      templates,
		Versions:            append([]ComponentVersion(nil), snap.Versions...),
	}
	if snap.HostFacts != nil {
		hf := *snap.HostFacts
		n.HostFacts = &hf
	}
	n.SetSchedulingDisabled(snap.SchedulingDisabled)
	return n
}

func setRegistrationHostFacts(reg *legacyRegistration, f *HostFacts) {
	reg.HostFactsJSON = mustJSON(f)
	reg.CPUVendor = f.CPUVendor
	reg.CPUModel = f.CPUModel
	reg.CPUIDHash = f.CPUIDHash
	reg.HostKernelRelease = f.HostKernelRelease
}

// mergeCubeletLabels applies cubelet labels over the stored set while the
// control-plane cordon key stays exactly as stored.
func mergeCubeletLabels(existing, cubelet map[string]string) map[string]string {
	out := make(map[string]string, len(existing)+len(cubelet))
	for k, v := range existing {
		out[k] = v
	}
	for k, v := range cubelet {
		if k == constants.LabelSchedulingDisabled {
			continue
		}
		out[k] = v
	}
	return out
}

func parseLabelsJSON(raw string) (map[string]string, error) {
	if strings.TrimSpace(raw) == "" {
		return map[string]string{}, nil
	}
	m := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	if m == nil {
		m = map[string]string{}
	}
	return m, nil
}

func mergeComponentVersions(prev, next []ComponentVersion) []ComponentVersion {
	by := map[string]ComponentVersion{}
	for _, v := range prev {
		if v.Component != "" {
			by[v.Component] = v
		}
	}
	for _, v := range next {
		if v.Component != "" {
			by[v.Component] = v
		}
	}
	out := make([]ComponentVersion, 0, len(by))
	for _, v := range by {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Component < out[j].Component })
	return out
}

func versionsHash(versions []ComponentVersion) string {
	if len(versions) == 0 {
		return ""
	}
	sorted := append([]ComponentVersion(nil), versions...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Component < sorted[j].Component })
	h := fnv.New64a()
	for _, v := range sorted {
		fmt.Fprintf(h, "%s|%s|%s|%s|%s|%s\n", v.Component, v.Version, v.Commit, v.BuildTime, v.Source, v.Variant)
	}
	return strconv.FormatUint(h.Sum64(), 16)
}

func cloneLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneLegacySnapshot(in *NodeSnapshot) *NodeSnapshot {
	out := *in
	out.Labels = cloneLabels(in.Labels)
	out.Conditions = append([]corev1.NodeCondition(nil), in.Conditions...)
	out.LocalTemplates = append([]LocalTemplate(nil), in.LocalTemplates...)
	out.Versions = append([]ComponentVersion(nil), in.Versions...)
	if in.HostFacts != nil {
		hf := *in.HostFacts
		out.HostFacts = &hf
	}
	return &out
}

func mustJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// Public entry points used by the HTTP handlers.

func LegacyRegisterNode(ctx context.Context, req *RegisterNodeRequest) (*NodeSnapshot, error) {
	s, err := currentLegacy()
	if err != nil {
		return nil, err
	}
	return s.register(ctx, req)
}

func LegacyUpdateNodeStatus(ctx context.Context, nodeID string, req *UpdateNodeStatusRequest) (*NodeSnapshot, error) {
	s, err := currentLegacy()
	if err != nil {
		return nil, err
	}
	return s.updateStatus(ctx, nodeID, req)
}

func LegacyListNodes(ctx context.Context) ([]*NodeSnapshot, error) {
	_ = ctx
	s, err := currentLegacy()
	if err != nil {
		return nil, err
	}
	return s.list(), nil
}

func LegacyGetNode(ctx context.Context, nodeID string) (*NodeSnapshot, error) {
	_ = ctx
	s, err := currentLegacy()
	if err != nil {
		return nil, err
	}
	for _, n := range s.list() {
		if n.NodeID == nodeID {
			return n, nil
		}
	}
	return nil, gorm.ErrRecordNotFound
}
