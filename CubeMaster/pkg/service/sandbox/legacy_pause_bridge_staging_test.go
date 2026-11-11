//go:build staging

package sandbox

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Staging proof (not a unit test): the new CubeMaster legacy bridge drives a
// v0.6 Cubelet endpoint (STAGING_V06_ADDR, built from the production 03b3605e
// protobuf) over real gRPC: manifest route resume, then auto-route pause via
// the node's reported v0.6 cubelet version, then a native-node control.
func TestStagingLegacyBridgeAgainstV06Cubelet(t *testing.T) {
	addr, sid := os.Getenv("STAGING_V06_ADDR"), os.Getenv("STAGING_SANDBOX")
	if addr == "" || sid == "" {
		t.Skip("STAGING_V06_ADDR/STAGING_SANDBOX not set")
	}
	manifest := filepath.Join(t.TempDir(), "m.json")
	if err := os.WriteFile(manifest, []byte(`{"version":1,"sessions":[{"sandbox_id":"`+sid+`","host_ip":"10.0.1.1"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CUBE_LEGACY_PAUSE_MANIFEST", manifest)
	legacyPauseLookup = func(context.Context, string) (*pausesnap.Record, error) { return nil, pausesnap.ErrNotFound }
	legacySpecLookup = func(context.Context, string) (*types.CreateCubeSandboxReq, error) { return &types.CreateCubeSandboxReq{}, nil }
	legacyCubeletAddr = func(string) string { return addr }
	legacyAfterUpdate = func(context.Context, string, string, string, string) {}
	legacyPublishTimeout = func(context.Context, *types.UpdateRequest) {}
	legacyCubeletUpdate = func(ctx context.Context, ep string, req *cubebox.UpdateCubeSandboxRequest, timeout time.Duration) (*cubebox.UpdateCubeSandboxResponse, error) {
		conn, err := grpc.NewClient(ep, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		c, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return cubebox.NewCubeboxMgrClient(conn).Update(c, req)
	}
	step := func(name, action, host string, wantHandled bool, wantCode errorcode.ErrorCode) {
		rsp, handled := tryLegacyPauseUpdate(context.Background(), &types.UpdateRequest{RequestID: name, SandboxID: sid, Action: action, InstanceType: "cubebox"}, host)
		if handled != wantHandled {
			t.Fatalf("%s: handled=%v", name, handled)
		}
		if handled && rsp.Ret.RetCode != int(wantCode) {
			t.Fatalf("%s: ret=%+v", name, rsp.Ret)
		}
		t.Logf("%s: handled=%v ret=%+v", name, handled, func() any { if rsp == nil { return nil }; return rsp.Ret }())
	}
	// 1. manifest-enrolled legacy sandbox: resume through the v0.6 endpoint.
	legacyNodeCubeletVersion = func(string) string { return "v0.7.1" }
	step("manifest-resume", "resume", "10.0.1.1", true, errorcode.ErrorCode_Success)
	// 2. not in the manifest, node still reports cubelet v0.6.0: auto-routed pause.
	t.Setenv("CUBE_LEGACY_PAUSE_MANIFEST", "")
	legacyNodeCubeletVersion = func(string) string { return "v0.6.0" }
	step("autoroute-pause", "pause", "10.0.1.1", true, errorcode.ErrorCode_Success)
	// 3. second cycle proves state really changed on the v0.6 side.
	step("autoroute-resume", "resume", "10.0.1.1", true, errorcode.ErrorCode_Success)
	step("autoroute-pause-2", "pause", "10.0.1.1", true, errorcode.ErrorCode_Success)
	// 4. native node: the bridge does not intercept.
	legacyNodeCubeletVersion = func(string) string { return "v0.7.1" }
	step("native-node", "resume", "10.0.1.1", false, 0)
}
