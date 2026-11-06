// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

type s3UploadEntry struct {
	state   string
	message string
	uuids   *cow.RemoteUUIDs
}

type cowSnapshotExporter interface {
	ExportSnapshot(name string) (string, error)
}

type cowVolumeImporter interface {
	ImportLvol(name, remoteUUID string) (string, error)
}

// Upload implements [cow.Uploader]. Calls cubecow_export_snapshot per
// snapshot disk (rootfs / memory / metadata). Failure returns an error and
// does not persist remote_uuids — Master treats that as upload failed.
// Success returns real export uuids and records local state as running;
// terminal ready comes later from cubecow export_status via UploadStatus.
func (m *S3Cow) Upload(ctx context.Context, snapshotID string) (*cow.RemoteUUIDs, error) {
	id := strings.TrimSpace(snapshotID)
	if id == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	uuids := &cow.RemoteUUIDs{}
	var exportable []CowObjectRef
	for _, ref := range activateObjectRefs(ctx, cow.BackendS3, id) {
		if IsS3MetadataBaseName(ref.Name) {
			continue
		}
		if ref.Role == "metadata" {
			info, infoErr := m.GetVolumeInfo(ctx, ref.Name)
			exists, existsErr := cowObjectPresent(info, infoErr)
			if existsErr != nil {
				m.setUpload(id, cow.RemoteStateFailed, existsErr.Error(), nil)
				return nil, fmt.Errorf("upload %s %s: %w", ref.Role, ref.Name, existsErr)
			}
			if !exists {
				continue
			}
		}
		exportable = append(exportable, ref)
	}

	// Native registry persistence is serialized per store. Acceptance is not
	// completion: wait for each accepted role before submitting the next.
	// Never retry an admission/transport error whose acceptance is ambiguous.
	settleCtx, cancel := context.WithTimeout(ctx, exportSettleBudget)
	defer cancel()
	for i, ref := range exportable {
		if err := settleCtx.Err(); err != nil {
			m.setUpload(id, cow.RemoteStateFailed, err.Error(), nil)
			return nil, err
		}
		uuid, err := m.uploadOne(ref.Name)
		if err != nil {
			m.setUpload(id, cow.RemoteStateFailed, err.Error(), nil)
			return nil, fmt.Errorf("upload %s %s: %w", ref.Role, ref.Name, err)
		}
		switch ref.Role {
		case "rootfs":
			uuids.Rootfs = uuid
		case "memory":
			uuids.Memory = uuid
		case "metadata":
			uuids.Metadata = uuid
		}
		if i < len(exportable)-1 {
			if err := m.waitExportSettled(settleCtx, ref.Name, uuid); err != nil {
				m.setUpload(id, cow.RemoteStateFailed, err.Error(), nil)
				return nil, fmt.Errorf("settle %s %s: %w", ref.Role, ref.Name, err)
			}
		}
	}

	if uuids.Empty() {
		m.setUpload(id, cow.RemoteStateFailed, "empty remote_uuids", nil)
		return nil, fmt.Errorf("s3 export returned empty remote_uuids for %s", id)
	}
	// Not persisted to catalog.json: by now the package is sealed and its
	// catalog lives on a read-only metadata snapshot, so the write would go
	// to the bare mount point on the host and shadow the real one. cubecow
	// keeps the id on the object, and UploadStatus reads it back from there.
	//
	// Export accepted; upload may still be in flight on the S3 backend.
	m.setUpload(id, cow.RemoteStateRunning, "export started", uuids)
	return uuids, nil
}

// exportSettleBudget caps the total time Upload spends spacing out a
// package's exports. Pause runs Upload and then a Destroy inside one 120s
// budget, so this leaves room for the Destroy even in the worst case. An
// export pins existing S3 objects rather than copying bytes, so the wait
// is a drain, not a transfer, and is normally seconds.
const exportSettleBudget = 75 * time.Second

// waitExportSettled requires durable completion of the exact accepted export.
// Missing/failed/changed identities and lookup errors fail closed. The shared
// Upload context bounds all role waits; timing out never starts the next role.
func (m *S3Cow) waitExportSettled(ctx context.Context, name, uuid string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := m.GetVolumeInfo(ctx, name)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if info == nil || strings.TrimSpace(info.ExportUUID) != uuid {
			return fmt.Errorf("accepted export %s identity missing or changed for %s", uuid, name)
		}
		status := strings.ToUpper(strings.TrimSpace(info.ExportStatus))
		switch status {
		case cow.ExportStatusDone:
			return nil
		case cow.ExportStatusInProgress, "":
			// An empty status is not a terminal native failure. Wait boundedly.
		default:
			return fmt.Errorf("accepted export %s has terminal or unknown status %q", uuid, status)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *S3Cow) uploadOne(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("object name is required")
	}
	if IsS3MetadataBaseName(name) {
		return "", fmt.Errorf("refusing to export node-local s3 metadata base %s", name)
	}
	exp, ok := m.engine.(cowSnapshotExporter)
	if !ok || exp == nil {
		return "", fmt.Errorf("cubecow engine does not support export_snapshot")
	}
	// Export is snapshot-only; volumes must be sealed to a snapshot before Upload.
	uuid, err := exp.ExportSnapshot(name)
	if err != nil {
		return "", err
	}
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return "", fmt.Errorf("cubecow_export_snapshot returned empty uuid for %s", name)
	}
	return uuid, nil
}

// UploadStatus implements [cow.Uploader].
// Aggregates cubecow_get_volume_info export_status:
// NONE → pending, empty/INPROGRESS → running, DONE → ready.
func (m *S3Cow) UploadStatus(ctx context.Context, snapshotID string) (*cow.RemoteStatus, error) {
	id := strings.TrimSpace(snapshotID)
	if id == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	st := &cow.RemoteStatus{
		SnapshotID: id,
		State:      cow.RemoteStatePending,
	}

	// No in-memory entry (a restart drops them) is not a gap: the per-object
	// loop below reads each export id straight from cubecow.
	m.uploadLock.Lock()
	entry, ok := m.uploadStates[id]
	m.uploadLock.Unlock()
	if ok {
		st.RemoteUUIDs = entry.uuids
		if entry.state == cow.RemoteStateFailed {
			st.State = cow.RemoteStateFailed
			st.Message = entry.message
			return st, nil
		}
	}

	refs := activateObjectRefs(ctx, cow.BackendS3, id)
	if len(refs) == 0 {
		st.State = cow.RemoteStateRunning
		st.Message = "waiting for local snapshot objects"
		return st, nil
	}

	var (
		sawAny       bool
		allDone      = true
		anyProgress  bool
		anyNone      bool
		anyFailed    bool
		localVolumes []cow.VolumeRemoteInfo
		messages     []string
	)
	for _, ref := range refs {
		if IsS3MetadataBaseName(ref.Name) {
			continue
		}
		info, infoErr := m.GetVolumeInfo(ctx, ref.Name)
		exists, existsErr := cowObjectPresent(info, infoErr)
		if existsErr != nil {
			// Lookup/RPC errors are not an s3lvol export failure. Bubble
			// them so Status leaves State unset and Master stays inprogress.
			return nil, existsErr
		}
		vol := cow.VolumeRemoteInfo{Name: ref.Name, Role: ref.Role, Exists: exists}
		if info != nil {
			vol.DevicePath = strings.TrimSpace(info.DevicePath)
			if ref.Role == "rootfs" && info.Deletable != nil {
				v := *info.Deletable
				st.RootfsDeletable = &v
			}
		}
		localVolumes = append(localVolumes, vol)
		if !exists {
			if ref.Role == "metadata" {
				continue
			}
			allDone = false
			anyProgress = true
			continue
		}
		sawAny = true
		status := ""
		uuid := ""
		if info != nil {
			status = strings.ToUpper(strings.TrimSpace(info.ExportStatus))
			uuid = strings.TrimSpace(info.ExportUUID)
		}
		if uuid != "" {
			if st.RemoteUUIDs == nil {
				st.RemoteUUIDs = &cow.RemoteUUIDs{}
			}
			switch ref.Role {
			case "rootfs":
				if st.RemoteUUIDs.Rootfs == "" {
					st.RemoteUUIDs.Rootfs = uuid
				}
			case "memory":
				if st.RemoteUUIDs.Memory == "" {
					st.RemoteUUIDs.Memory = uuid
				}
			case "metadata":
				if st.RemoteUUIDs.Metadata == "" {
					st.RemoteUUIDs.Metadata = uuid
				}
			}
		}
		switch {
		case status == cow.ExportStatusDone:
			// ok
		case cow.ExportStatusIsFailed(status):
			allDone = false
			anyFailed = true
			messages = append(messages, fmt.Sprintf("%s=%s", ref.Name, status))
		case status == cow.ExportStatusInProgress, status == "":
			// Empty status (with or without a uuid) is not a confirmed
			// s3lvol failure. Keep polling; Master must not stamp failed.
			allDone = false
			anyProgress = true
		case status == cow.ExportStatusNone:
			allDone = false
			anyNone = true
		default:
			allDone = false
			anyProgress = true
			messages = append(messages, fmt.Sprintf("%s=%s", ref.Name, status))
		}
	}
	st.LocalVolumes = localVolumes

	if !sawAny {
		st.State = cow.RemoteStateRunning
		st.Message = "waiting for snapshot volumes"
		return st, nil
	}

	switch {
	case anyFailed:
		st.State = cow.RemoteStateFailed
		st.Message = strings.Join(messages, "; ")
	case allDone:
		committed, err := m.commitCheckpoint(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("checkpoint commit: %w", err)
		}
		st.State = cow.RemoteStateReady
		st.Message = "export_status DONE"
		if committed {
			st.Message = "committed capture; export_status DONE"
		}
	case anyProgress:
		st.State = cow.RemoteStateRunning
		if len(messages) > 0 {
			st.Message = strings.Join(messages, "; ")
		} else {
			st.Message = "export_status INPROGRESS"
		}
	case anyNone:
		st.State = cow.RemoteStatePending
		st.Message = "export_status NONE"
	default:
		st.State = cow.RemoteStateRunning
		st.Message = "export in progress"
	}
	return st, nil
}

func (m *S3Cow) setUpload(id, state, message string, uuids *cow.RemoteUUIDs) {
	m.uploadLock.Lock()
	defer m.uploadLock.Unlock()
	if m.uploadStates == nil {
		m.uploadStates = make(map[string]s3UploadEntry)
	}
	m.uploadStates[id] = s3UploadEntry{state: state, message: message, uuids: uuids}
}

var _ cow.Uploader = (*S3Cow)(nil)
