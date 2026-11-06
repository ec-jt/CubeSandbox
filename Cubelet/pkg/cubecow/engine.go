// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubecow

import (
	"sync"
	"unsafe"
)

type Engine struct {
	ptr unsafe.Pointer
	mu  sync.RWMutex
}

type Volume struct {
	VolumeUUID    string        `json:"volume_uuid"`
	ImportOrigin  *ImportOrigin `json:"import_origin"`
	Name          string        `json:"name"`
	SizeBytes     uint64        `json:"size_bytes"`
	DevicePath    string        `json:"device_path"`
	SnapshotCount int32         `json:"snapshot_count"`
	CreatedAt     string        `json:"created_at"`
	// ExportUUID / ExportStatus / Deletable come from cubecow_get_volume_info
	// JSON (s3-support). Empty for plain volumes and unexported snapshots.
	// ExportStatus is one of "", "NONE", "INPROGRESS", "DONE".
	ExportUUID   string `json:"export_uuid"`
	ExportStatus string `json:"export_status"`
	Deletable    *bool  `json:"deletable"`
}

// ImportOrigin is read live from native destination blob metadata. It does not
// certify checkpoint commit, current contents, or retention of remote objects.
type ImportOrigin struct {
	Version         uint32 `json:"version"`
	VolumeUUID      string `json:"volume_uuid"`
	LvstoreUUID     string `json:"lvstore_uuid"`
	ExportUUID      string `json:"export_uuid"`
	SnapshotUUID    string `json:"snapshot_uuid"`
	Endpoint        string `json:"endpoint"`
	Bucket          string `json:"bucket"`
	Prefix          string `json:"prefix"`
	LvsName         string `json:"lvs_name"`
	Snapshot        string `json:"snapshot"`
	ManifestVersion uint32 `json:"manifest_version"`
	Generation      uint32 `json:"generation"`
	ClusterSize     uint32 `json:"cluster_size"`
	ChunkSize       uint32 `json:"chunk_size"`
	BlockSize       uint32 `json:"block_size"`
	BlobID          uint64 `json:"blob_id"`
	SourceBlobID    uint64 `json:"source_blob_id"`
	SizeBytes       uint64 `json:"size_bytes"`
}

type Snapshot struct {
	Name         string `json:"name"`
	SizeBytes    uint64 `json:"size_bytes"`
	DevicePath   string `json:"device_path"`
	OriginVolume string `json:"origin_volume"`
	CreatedAt    string `json:"created_at"`
	ExportUUID   string `json:"export_uuid"`
	ExportStatus string `json:"export_status"`
	Deletable    *bool  `json:"deletable"`
}

type VolumeBlockInfo struct {
	NumBlocks uint64
	BlockSize uint32
}

type ListVolumesResult struct {
	Volumes       []Volume
	NextPageToken string
	TotalCount    uint64
}

type ListSnapshotsResult struct {
	Snapshots     []Snapshot
	NextPageToken string
}

func (e *Engine) openHandle() (unsafe.Pointer, error) {
	if e == nil {
		return nil, &CowError{Code: ErrClosed, Action: ActBug, RawRC: int32(ErrClosed), Message: "engine is nil"}
	}
	e.mu.RLock()
	if e.ptr == nil {
		e.mu.RUnlock()
		return nil, &CowError{Code: ErrClosed, Action: ActBug, RawRC: int32(ErrClosed), Message: "engine is closed"}
	}
	return e.ptr, nil
}
