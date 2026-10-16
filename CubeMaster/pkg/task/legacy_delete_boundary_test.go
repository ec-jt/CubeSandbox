package task

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/legacydelete"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/localcache"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/sandboxlock"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

func TestLegacyAsyncDeleteFailurePreservesMetadata(t *testing.T) {
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyFunc(sandboxlock.WithLock, func(ctx context.Context, id string, opts sandboxlock.Options, fn func(context.Context) error) error {
		return fn(ctx)
	})
	patches.ApplyFunc(localcache.GetNodesByIp, func(string) (*node.Node, bool) { return nil, false })
	patches.ApplyFunc(legacydelete.TryDestroy, func(context.Context, string, *cubebox.DestroyCubeSandboxRequest) (bool, error) {
		return true, errors.New("legacy origin offline")
	})
	patches.ApplyFunc(pausesnap.TryDeletePaused, func(context.Context, string, string, string) (bool, error) {
		t.Fatal("native cleanup reached")
		return false, nil
	})
	patches.ApplyFunc(localcache.DeleteSandboxProxyMap, func(context.Context, string) error { t.Fatal("proxy metadata deleted"); return nil })
	patches.ApplyFunc(localcache.DeleteSandboxCache, func(string) { t.Fatal("cache metadata deleted") })
	task := &Task{BaseInfo: BaseInfo{InstanceType: "cubebox", SandboxID: "sb-x"}, CallEp: "10.0.1.1:9001", Request: &cubebox.DestroyCubeSandboxRequest{SandboxID: "sb-x"}}
	if err := (&DestroySandboxTaskHandler{}).HandleTask(context.Background(), task); err == nil || !strings.Contains(err.Error(), "legacy origin offline") {
		t.Fatalf("unexpected error: %v", err)
	}
}
