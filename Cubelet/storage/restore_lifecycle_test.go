package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"os"
	"path/filepath"
	"testing"
)

func TestRestoreLifecyclePreservesReusedResources(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"root", "memory", "metadata"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("existing"), 0600))
	}
	result := &StorageInfo{SandboxID: "reused", preserveOnCreateFailure: true, Volumes: map[string]*BackendFileInfo{"root": {FilePath: filepath.Join(dir, "root")}}, ImportedMemoryVol: "sb-reused-memory"}
	// No DB/store configured: preservation must not invoke deletion at all.
	l := &local{}
	require.NoError(t, l.cleanupCreateResult(context.Background(), result))
	ctx := workflow.WithLifecycleCleanup(context.Background(), &workflow.LifecycleCleanup{PreserveStorage: true})
	require.ErrorContains(t, l.Destroy(ctx, &workflow.DestroyContext{BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: "reused"}}), "retained")
	require.ErrorContains(t, l.CleanUp(ctx, &workflow.CleanContext{BaseWorkflowInfo: workflow.BaseWorkflowInfo{SandboxID: "reused"}}), "retained")
	for _, name := range []string{"root", "memory", "metadata"} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		require.Equal(t, "existing", string(b))
	}
}
