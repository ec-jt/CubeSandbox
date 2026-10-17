// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package templatecenter

import (
	"context"
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/cubelet"
	cubeboxv1 "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

func TestSnapshotPublicationFailureRetainsArtifactsAndLocators(t *testing.T) {
	for _, rsp := range []*cubeboxv1.CommitSandboxResponse{nil, {SnapshotPath: "/retained/package"}} {
		t.Run("failure", func(t *testing.T) {
			patches := gomonkey.NewPatches()
			defer patches.Reset()
			patches.ApplyFunc(cubelet.CleanupTemplate, func(context.Context, string, *cubeboxv1.CleanupTemplateRequest) (*cubeboxv1.CleanupTemplateResponse, error) {
				t.Fatal("ambiguous/late failure must not delete package")
				return nil, nil
			})
			patches.ApplyFunc(deleteReplicasByTemplateID, func(context.Context, string) error {
				t.Fatal("replica locator must survive failed publication")
				return nil
			})
			var snapshotStatus, jobStatus string
			patches.ApplyFunc(updateSnapshotFields, func(_ context.Context, _ string, fields map[string]any) error {
				snapshotStatus, _ = fields["status"].(string)
				return nil
			})
			patches.ApplyFunc(updateTemplateImageJob, func(_ context.Context, _ string, fields map[string]any) error {
				jobStatus, _ = fields["status"].(string)
				return nil
			})
			patches.ApplyFunc(invalidateTemplateCaches, func(string) {})
			require.NoError(t, failSnapshotCreateJob(context.Background(), "job", "snap", "node-ip", "/retained/package", rsp, errors.New("publication incomplete"), "s3"))
			require.Equal(t, StatusFailed, snapshotStatus)
			require.Equal(t, JobStatusFailed, jobStatus)
		})
	}
}
