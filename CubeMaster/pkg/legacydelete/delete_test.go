package legacydelete

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	retcode "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

func TestLegacyDeleteSafety(t *testing.T) {
	oldLoad, oldPause, oldSpec := loadCohort, getPause, getSpec
	oldNode, oldEndpoint, oldDestroy, oldVolumes := nodeRegistered, endpointFor, destroy, applyVolumes
	t.Cleanup(func() {
		loadCohort, getPause, getSpec = oldLoad, oldPause, oldSpec
		nodeRegistered, endpointFor, destroy, applyVolumes = oldNode, oldEndpoint, oldDestroy, oldVolumes
	})
	for _, name := range []string{"disabled", "unlisted", "manifest-error", "db-error", "binding", "nil-record", "spec-error", "nil-spec", "s3", "wrong-node", "offline", "rpc-error", "nil-response", "nil-ret", "rejected", "success", "ok"} {
		t.Run(name, func(t *testing.T) {
			loadCohort = func() (map[string]string, error) {
				if name == "manifest-error" {
					return nil, errors.New("bad manifest")
				}
				if name == "disabled" {
					return nil, nil
				}
				if name == "unlisted" {
					return map[string]string{"sb-other": "10.0.1.1"}, nil
				}
				return map[string]string{"sb-x": "10.0.1.1"}, nil
			}
			getPause = func(context.Context, string) (*pausesnap.Record, error) {
				if name == "db-error" {
					return nil, errors.New("db down")
				}
				if name == "binding" {
					return &pausesnap.Record{}, nil
				}
				if name == "nil-record" {
					return nil, nil
				}
				return nil, pausesnap.ErrNotFound
			}
			getSpec = func(context.Context, string) (*types.CreateCubeSandboxReq, error) {
				if name == "spec-error" {
					return nil, errors.New("spec down")
				}
				if name == "nil-spec" {
					return nil, nil
				}
				backend := "xfs"
				if name == "s3" {
					backend = "s3"
				}
				return &types.CreateCubeSandboxReq{Backend: backend}, nil
			}
			nodeRegistered = func(string) bool { return name != "offline" }
			endpointFor = func(host string) string { return host + ":test" }
			rpcs, applied := 0, 0
			destroy = func(ctx context.Context, ep string, req *cubebox.DestroyCubeSandboxRequest, timeout time.Duration) (*cubebox.DestroyCubeSandboxResponse, error) {
				rpcs++
				if ep != "10.0.1.1:test" || req.SandboxID != "sb-x" || timeout != 120*time.Second {
					t.Fatal("wrong destroy request")
				}
				if name == "rpc-error" {
					return nil, errors.New("transport failure")
				}
				if name == "nil-response" {
					return nil, nil
				}
				if name == "nil-ret" {
					return &cubebox.DestroyCubeSandboxResponse{}, nil
				}
				code := retcode.ErrorCode_Success
				if name == "ok" {
					code = retcode.ErrorCode_OK
				}
				if name == "rejected" {
					code = retcode.ErrorCode_TaskStateInvalid
				}
				return &cubebox.DestroyCubeSandboxResponse{Ret: &retcode.Ret{RetCode: code}}, nil
			}
			applyVolumes = func(context.Context, map[string][]byte) { applied++ }
			host := "10.0.1.1"
			if name == "wrong-node" {
				host = "10.0.1.2"
			}
			handled, err := TryDestroy(context.Background(), host, &cubebox.DestroyCubeSandboxRequest{SandboxID: "sb-x"})
			if name == "disabled" || name == "unlisted" {
				if handled || err != nil || rpcs != 0 || applied != 0 {
					t.Fatal("native path intercepted")
				}
				return
			}
			if !handled {
				t.Fatal("legacy operation not handled")
			}
			success := name == "success" || name == "ok"
			if (err == nil) != success {
				t.Fatalf("unexpected result: %v", err)
			}
			if success && (rpcs != 1 || applied != 1) {
				t.Fatal("confirmed destroy not applied")
			}
			if !success && applied != 0 {
				t.Fatal("failed destroy allowed postprocessing")
			}
			expectsRPC := success || name == "rpc-error" || name == "nil-response" || name == "nil-ret" || name == "rejected"
			if !expectsRPC && rpcs != 0 {
				t.Fatal("guard failure reached RPC")
			}
		})
	}
}
