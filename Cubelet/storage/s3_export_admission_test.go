package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// Model the native acceptance/completion split: another start is refused
// until the previous role's registry save has completed.
type admissionEngine struct {
	*fakeCowEngine
	active    string
	polls     int
	stuck     bool
	lookupErr error
	startErr  error
	status    string
	wrongID   bool
}

func (e *admissionEngine) ExportSnapshot(name string) (string, error) {
	if e.startErr != nil {
		e.exportSnapshots = append(e.exportSnapshots, name)
		return "", e.startErr
	}
	if e.active != "" {
		return "", errors.New("registry save busy")
	}
	uuid, err := e.fakeCowEngine.ExportSnapshot(name)
	if err == nil {
		e.active = name
		e.polls = 0
		e.volumeInfos[name].ExportStatus = cow.ExportStatusInProgress
	}
	return uuid, err
}

func (e *admissionEngine) GetVolumeInfo(name string) (*cubecow.Volume, error) {
	if e.active == name {
		if e.lookupErr != nil {
			return nil, e.lookupErr
		}
		e.polls++
		v := e.volumeInfos[name]
		if e.wrongID {
			v.ExportUUID = "wrong"
		}
		if e.status != "" {
			v.ExportStatus = e.status
		} else if !e.stuck && e.polls > 1 {
			v.ExportStatus = cow.ExportStatusDone
			e.active = ""
		}
	}
	return e.fakeCowEngine.GetVolumeInfo(name)
}

func admissionFixture(t *testing.T) (*S3Cow, *admissionEngine, string) {
	t.Helper()
	m, base, entry := commitFixture(t)
	e := &admissionEngine{fakeCowEngine: base.fakeCowEngine}
	m.engine = e
	return m, e, entry.SnapshotID
}

func TestS3ExportAdmissionSequentialRoles(t *testing.T) {
	m, e, id := admissionFixture(t)
	u, err := m.Upload(context.Background(), id)
	require.NoError(t, err)
	require.NotEmpty(t, u.Rootfs)
	require.NotEmpty(t, u.Memory)
	require.NotEmpty(t, u.Metadata)
	require.Len(t, e.exportSnapshots, 3)
	// Only the last accepted role may still be publishing.
	require.Equal(t, e.exportSnapshots[2], e.active)
}

func TestS3ExportAdmissionFailureStopsNextRole(t *testing.T) {
	for _, kind := range []string{"deadline", "cancelled", "lookup", "failed", "missing", "identity", "admission"} {
		t.Run(kind, func(t *testing.T) {
			m, e, id := admissionFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			sentinel := errors.New("injected transport or precondition failure")
			switch kind {
			case "deadline":
				e.stuck = true
			case "cancelled":
				cancel()
			case "lookup":
				e.lookupErr = sentinel
			case "failed":
				e.status = "FAILED"
			case "missing":
				e.status = cow.ExportStatusNone
			case "identity":
				e.wrongID = true
			case "admission":
				e.startErr = sentinel
			}
			u, err := m.Upload(ctx, id)
			require.Error(t, err)
			require.Nil(t, u)
			if kind == "deadline" {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			if kind == "cancelled" {
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, e.exportSnapshots)
			} else {
				require.Len(t, e.exportSnapshots, 1)
			}
			if kind == "lookup" || kind == "admission" {
				require.ErrorIs(t, err, sentinel)
			}
			st, statusErr := m.UploadStatus(context.Background(), id)
			require.NoError(t, statusErr)
			require.Equal(t, cow.RemoteStateFailed, st.State)
		})
	}
}

func TestS3ExportAdmissionCompletedIdentityIsStable(t *testing.T) {
	m, e, id := admissionFixture(t)
	u, err := m.Upload(context.Background(), id)
	require.NoError(t, err)
	require.NoError(t, m.waitExportSettled(context.Background(), e.active, u.Metadata))
	u2, err := m.Upload(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, u, u2)
}
