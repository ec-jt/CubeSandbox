// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package models

import (
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
)

// TemplateBase is one published template base: the S3 export of one role
// (rootfs | memory) of a template build, which other nodes import so every
// node holds identical bytes and sandbox exports can be deltas over it.
// Several bases of one template coexist: a rebuild publishes a new base and
// RETIRES the old one, which is released only once no pause or snapshot
// export still references it.
type TemplateBase struct {
	ID            uint      `gorm:"primaryKey"`
	CreatedAt     time.Time `gorm:"column:created_at"`
	UpdatedAt     time.Time `gorm:"column:updated_at"`
	TemplateID    string    `json:"template_id" gorm:"column:template_id"`
	Role          string    `json:"role" gorm:"column:role"`
	BaseUUID      string    `json:"base_uuid" gorm:"column:base_uuid"`
	ArtifactID    string    `json:"artifact_id" gorm:"column:artifact_id"`
	BuilderNodeID string    `json:"builder_node_id" gorm:"column:builder_node_id"`
	BuilderNodeIP string    `json:"builder_node_ip" gorm:"column:builder_node_ip"`
	Status        string    `json:"status" gorm:"column:status"`
}

func (TemplateBase) TableName() string { return constants.TemplateBaseTableName }

// TemplateBaseReplica records that a node holds a verified copy of a base.
type TemplateBaseReplica struct {
	ID        uint      `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
	BaseUUID  string    `json:"base_uuid" gorm:"column:base_uuid"`
	NodeID    string    `json:"node_id" gorm:"column:node_id"`
	NodeIP    string    `json:"node_ip" gorm:"column:node_ip"`
	Status    string    `json:"status" gorm:"column:status"`
	LastError string    `json:"last_error" gorm:"column:last_error"`
}

func (TemplateBaseReplica) TableName() string { return constants.TemplateBaseReplicaTableName }
