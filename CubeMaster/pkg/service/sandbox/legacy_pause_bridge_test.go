package sandbox

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	retcode "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

func TestLegacyBridgeDisabled(t *testing.T) {
	t.Setenv("CUBE_LEGACY_PAUSE_MANIFEST", "")
	rsp, handled := tryLegacyPauseUpdate(context.Background(), &types.UpdateRequest{SandboxID: "sb-x", Action: "resume"}, "10.0.1.1")
	if handled || rsp != nil {
		t.Fatal("disabled bridge intercepted native request")
	}
}

func TestLegacyBridgeManifestGate(t *testing.T) {
	for _, tc := range []struct {
		name, manifest string
		handled        bool
	}{
		{"invalid", `{"version":2}`, true},
		{"unlisted", `{"version":1,"sessions":[]}`, false},
		{"other-session", `{"version":1,"sessions":[{"sandbox_id":"sb-other","host_ip":"10.0.1.1"}]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cohort.json")
			if err := os.WriteFile(path, []byte(tc.manifest), 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("CUBE_LEGACY_PAUSE_MANIFEST", path)
			rsp, handled := tryLegacyPauseUpdate(context.Background(), &types.UpdateRequest{SandboxID: "sb-x", Action: "resume"}, "10.0.1.1")
			if handled != tc.handled {
				t.Fatalf("handled=%v", handled)
			}
			if handled && (rsp == nil || rsp.Ret.RetCode == int(errorcode.ErrorCode_Success)) {
				t.Fatal("invalid manifest did not fail closed")
			}
		})
	}
}

func TestLegacyBridgeMissingManifest(t *testing.T) {
	t.Setenv("CUBE_LEGACY_PAUSE_MANIFEST", filepath.Join(t.TempDir(), "missing.json"))
	rsp, handled := tryLegacyPauseUpdate(context.Background(), &types.UpdateRequest{SandboxID: "sb-x", Action: "resume"}, "10.0.1.1")
	if !handled || rsp == nil || rsp.Ret.RetCode == int(errorcode.ErrorCode_Success) {
		t.Fatal("missing manifest did not fail closed")
	}
}

func TestLegacyBridgeRoutingSafety(t *testing.T) {
	oldPause, oldSpec := legacyPauseLookup, legacySpecLookup
	oldAddr, oldUpdate := legacyCubeletAddr, legacyCubeletUpdate
	oldHook, oldTimeout := legacyAfterUpdate, legacyPublishTimeout
	t.Cleanup(func() {
		legacyPauseLookup, legacySpecLookup = oldPause, oldSpec
		legacyCubeletAddr, legacyCubeletUpdate = oldAddr, oldUpdate
		legacyAfterUpdate, legacyPublishTimeout = oldHook, oldTimeout
	})
	for _, name := range []string{"pause", "resume", "db-error", "native-binding", "spec-error", "s3", "wrong-host", "rpc-error", "nil-response", "nil-ret", "rpc-rejection"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manifest.json")
			if err := os.WriteFile(path, []byte(`{"version":1,"sessions":[{"sandbox_id":"sb-x","host_ip":"10.0.1.1"}]}`), 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("CUBE_LEGACY_PAUSE_MANIFEST", path)
			legacyPauseLookup = func(context.Context, string) (*pausesnap.Record, error) {
				if name == "db-error" {
					return nil, errors.New("db down")
				}
				if name == "native-binding" {
					return &pausesnap.Record{Status: "READY"}, nil
				}
				return nil, pausesnap.ErrNotFound
			}
			legacySpecLookup = func(context.Context, string) (*types.CreateCubeSandboxReq, error) {
				if name == "spec-error" {
					return nil, errors.New("spec down")
				}
				backend := "xfs"
				if name == "s3" {
					backend = "s3"
				}
				return &types.CreateCubeSandboxReq{Backend: backend}, nil
			}
			legacyCubeletAddr = func(host string) string { return host + ":test" }
			rpcs, hooks := 0, 0
			action := "resume"
			if name == "pause" {
				action = "pause"
			}
			legacyCubeletUpdate = func(ctx context.Context, endpoint string, req *cubebox.UpdateCubeSandboxRequest, timeout time.Duration) (*cubebox.UpdateCubeSandboxResponse, error) {
				rpcs++
				if endpoint != "10.0.1.1:test" || req.SandboxID != "sb-x" || req.Annotations[constants.CubeAnnotationsUpdateAction] != action || timeout != 120*time.Second {
					t.Fatalf("wrong RPC route/request: %s %+v %s", endpoint, req, timeout)
				}
				if name == "rpc-error" {
					return nil, errors.New("rpc down")
				}
				if name == "nil-response" {
					return nil, nil
				}
				if name == "nil-ret" {
					return &cubebox.UpdateCubeSandboxResponse{}, nil
				}
				code := errorcode.ErrorCode_Success
				if name == "rpc-rejection" {
					code = errorcode.ErrorCode_MasterParamsError
				}
				return &cubebox.UpdateCubeSandboxResponse{Ret: &retcode.Ret{RetCode: retcode.ErrorCode(code)}}, nil
			}
			legacyAfterUpdate = func(context.Context, string, string, string, string) { hooks++ }
			legacyPublishTimeout = func(context.Context, *types.UpdateRequest) { hooks++ }
			host := "10.0.1.1"
			if name == "wrong-host" {
				host = "10.0.1.2"
			}
			rsp, handled := tryLegacyPauseUpdate(context.Background(), &types.UpdateRequest{SandboxID: "sb-x", InstanceType: "cubebox", Action: action}, host)
			if !handled || rsp == nil {
				t.Fatal("enrolled session not handled")
			}
			success := name == "pause" || name == "resume"
			if (rsp.Ret.RetCode == int(errorcode.ErrorCode_Success)) != success {
				t.Fatalf("unexpected response: %+v", rsp.Ret)
			}
			guarded := name == "db-error" || name == "native-binding" || name == "spec-error" || name == "s3" || name == "wrong-host"
			if guarded && rpcs != 0 {
				t.Fatal("guard failure reached RPC")
			}
			if !guarded && rpcs != 1 {
				t.Fatal("expected one RPC")
			}
			if success && hooks != 2 || !success && hooks != 0 {
				t.Fatalf("unexpected success hooks: %d", hooks)
			}
		})
	}
}
