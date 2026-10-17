// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package pausesnap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	cubeleterrorcode "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

func TestPublicationHoldDenialRetainsPauseBinding(t *testing.T) {
	db := setupPauseDeleteTest(t)
	seedPauseBinding(t, db, "sb-held", "snap-held", StatusFailed, "10.0.0.1")
	destroyOnCubelet = func(context.Context, string, *cubebox.DestroyCubeSandboxRequest) (*cubebox.DestroyCubeSandboxResponse, error) {
		return &cubebox.DestroyCubeSandboxResponse{Ret: &cubeleterrorcode.Ret{
			RetCode: cubeleterrorcode.ErrorCode_PreConditionFailed,
			RetMsg:  "snapshot publication incomplete; artifacts retained; operator reconciliation required",
		}}, nil
	}
	cleanupTemplateOnCubelet = func(context.Context, string, *cubebox.CleanupTemplateRequest) (*cubebox.CleanupTemplateResponse, error) {
		t.Fatal("delete denial must not be followed by package cleanup")
		return nil, nil
	}
	handled, err := TryDeletePaused(context.Background(), "req", "sb-held", "10.0.0.1")
	require.True(t, handled)
	require.ErrorContains(t, err, "publication incomplete")
	rec, err := GetBySandbox(context.Background(), "sb-held")
	require.NoError(t, err)
	require.Equal(t, "snap-held", rec.SnapshotID)
	require.Equal(t, StatusFailed, rec.Status)
	require.Equal(t, "10.0.0.1", rec.NodeIP)
}
