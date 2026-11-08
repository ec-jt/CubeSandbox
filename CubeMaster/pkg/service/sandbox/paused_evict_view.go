// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sandbox

import (
	"context"
	"strings"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/localcache"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
)

// PausedEvictionNode is one node's scheduler quota and disk usage.
type PausedEvictionNode struct {
	NodeID              string  `json:"node_id"`
	NodeIP              string  `json:"node_ip"`
	Healthy             bool    `json:"healthy"`
	QuotaCpu            int64   `json:"quota_cpu"`
	QuotaCpuUsage       int64   `json:"quota_cpu_usage"`
	QuotaMem            int64   `json:"quota_mem"`
	QuotaMemUsage       int64   `json:"quota_mem_usage"`
	StorageDiskUsagePer float64 `json:"storage_disk_usage_per"`
}

// PausedEvictionBinding is one pause binding with its Master-side eviction
// eligibility (READY, S3, remote_ready, not evicted, origin known).
type PausedEvictionBinding struct {
	SandboxID     string `json:"sandbox_id"`
	SnapshotID    string `json:"snapshot_id"`
	NodeID        string `json:"node_id"`
	NodeIP        string `json:"node_ip"`
	InstanceType  string `json:"instance_type"`
	Backend       string `json:"backend"`
	RemoteStatus  string `json:"remote_status"`
	Status        string `json:"status"`
	Evicted       bool   `json:"evicted"`
	Eligible      bool   `json:"eligible"`
	Reason        string `json:"reason,omitempty"`
	PausedAtMilli int64  `json:"paused_at_ms"`
}

// PausedEvictionView is the read-only payload of GET /internal/paused_eviction.
// CLM owns the pressure decision and the LRU order, because only it knows
// last activity and pin state.
type PausedEvictionView struct {
	Ret      *types.Ret              `json:"ret"`
	Enabled  bool                    `json:"enabled"`
	Nodes    []PausedEvictionNode    `json:"nodes"`
	Bindings []PausedEvictionBinding `json:"bindings"`
}

var (
	pausedEvictionNodesFn    = func() node.NodeList { return localcache.GetNodes(-1) }
	pausedEvictionBindingsFn = func(ctx context.Context) ([]*pausesnap.Record, error) {
		return pausesnap.List(ctx, pausesnap.ListOptions{})
	}
)

// BuildPausedEvictionView assembles node pressure inputs and pause bindings.
func BuildPausedEvictionView(ctx context.Context) *PausedEvictionView {
	rsp := &PausedEvictionView{
		Enabled: PausedEvictionEnabled(),
		Ret:     &types.Ret{RetCode: int(errorcode.ErrorCode_Success), RetMsg: errorcode.ErrorCode_Success.String()},
	}
	for _, n := range pausedEvictionNodesFn() {
		if n == nil {
			continue
		}
		rsp.Nodes = append(rsp.Nodes, PausedEvictionNode{
			NodeID: n.ID(), NodeIP: n.HostIP(), Healthy: n.Healthy,
			QuotaCpu: n.QuotaCpu, QuotaCpuUsage: n.QuotaCpuUsage,
			QuotaMem: n.QuotaMem, QuotaMemUsage: n.QuotaMemUsage,
			StorageDiskUsagePer: n.StorageDiskUsagePer,
		})
	}
	records, err := pausedEvictionBindingsFn(ctx)
	if err != nil {
		rsp.Ret.RetCode = int(errorcode.ErrorCode_DBError)
		rsp.Ret.RetMsg = err.Error()
		return rsp
	}
	for _, rec := range records {
		if rec == nil {
			continue
		}
		reason := EvictionIneligibleReason(rec)
		rsp.Bindings = append(rsp.Bindings, PausedEvictionBinding{
			SandboxID: rec.SandboxID, SnapshotID: rec.SnapshotID,
			NodeID: rec.NodeID, NodeIP: rec.NodeIP, InstanceType: rec.InstanceType,
			Backend: strings.TrimSpace(rec.Backend), RemoteStatus: rec.RemoteStatus,
			Status: rec.Status, Evicted: rec.Evicted(),
			Eligible: reason == "", Reason: reason,
			PausedAtMilli: rec.UpdatedAt.UnixMilli(),
		})
	}
	return rsp
}
