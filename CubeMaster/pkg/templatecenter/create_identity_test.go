package templatecenter

import (
	"context"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	sandboxtypes "github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	"gorm.io/gorm"
)

// Keep both normalizers real: mocking either hid the no-ID double-generation bug.
func TestCreateTemplateKeepsOneIdentity(t *testing.T) {
	for _, supplied := range []string{"", "tpl-explicit"} {
		t.Run("id="+supplied, func(t *testing.T) {
			originalDB := store.db
			store.db = &gorm.DB{}
			defer func() { store.db = originalDB }()
			patches := gomonkey.NewPatches()
			defer patches.Reset()
			req := &sandboxtypes.CreateCubeSandboxReq{
				Annotations: map[string]string{},
			}
			if supplied != "" {
				req.Annotations[constants.CubeAnnotationAppSnapshotTemplateID] = supplied
			}
			var selected string
			var definitionSeen, cacheSeen, replicaSeen bool
			patches.ApplyFunc(createDefinitionWithOptions, func(_ context.Context, id string, stored *sandboxtypes.CreateCubeSandboxReq, _, _ string, _ definitionCreateOptions) error {
				selected = id
				definitionSeen = true
				require.Equal(t, id, stored.Annotations[constants.CubeAnnotationAppSnapshotTemplateID])
				require.Empty(t, stored.Annotations[constants.CubeAnnotationsAppSnapshotCreate])
				return nil
			})
			patches.ApplyFunc(setTemplateRequestCache, func(id string, stored *sandboxtypes.CreateCubeSandboxReq) error {
				cacheSeen = true
				require.Equal(t, selected, id)
				require.Equal(t, id, stored.Annotations[constants.CubeAnnotationAppSnapshotTemplateID])
				return nil
			})
			patches.ApplyFunc(resolveTemplateNodes, func(string, []string) ([]*node.Node, error) {
				return []*node.Node{{InsID: "node-test", IP: "127.0.0.1", Healthy: true}}, nil
			})
			patches.ApplyFunc(createTemplateReplicasOnNodes, func(_ context.Context, id string, build *sandboxtypes.CreateCubeSandboxReq, _ []*node.Node, _ replicaRunOptions) ([]ReplicaStatus, error) {
				replicaSeen = true
				require.Equal(t, selected, id)
				require.Equal(t, id, build.Annotations[constants.CubeAnnotationAppSnapshotTemplateID])
				require.Equal(t, "true", build.Annotations[constants.CubeAnnotationsAppSnapshotCreate])
				return []ReplicaStatus{{NodeID: "node-test", Status: ReplicaStatusReady}}, nil
			})
			patches.ApplyFunc(finalizeTemplateReplicas, func(_ context.Context, id, _, _, _ string, _ []ReplicaStatus) (*TemplateInfo, string, error) {
				require.Equal(t, selected, id)
				return &TemplateInfo{TemplateID: id, Status: StatusReady}, "", nil
			})
			info, err := CreateTemplate(context.Background(), req)
			require.NoError(t, err)
			require.True(t, definitionSeen && cacheSeen && replicaSeen)
			require.Equal(t, selected, info.TemplateID)
			if supplied != "" {
				require.Equal(t, supplied, selected)
			} else {
				require.Regexp(t, `^tpl-[0-9a-f]{24}$`, selected)
			}
			require.Equal(t, supplied, req.Annotations[constants.CubeAnnotationAppSnapshotTemplateID])
			require.Empty(t, req.Annotations[constants.CubeAnnotationsAppSnapshotCreate])
		})
	}
}
