package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/constants"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/localcache"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/sandboxlock"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	retcode "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

func TestPortUpdateValidation(t *testing.T) {
	for _, tc := range []struct {
		action string
		port   int
		class  string
		valid  bool
	}{
		{"exposePort", 1, "", true}, {"exposePort", 65535, "infrastructure", true}, {"exposePort", 3000, "user", true},
		{"exposePort", 0, "", false}, {"closePort", 65536, "", false}, {"exposePort", -1, "", false},
		{"exposePort", 3000, "unknown", false}, {"closePort", 3000, "unknown", true},
		{"exposePort", 17300, "infrastructure", false}, {"closePort", 17301, "", false},
		{"exposePort", 49983, "", false}, {"closePort", 49999, "", false},
	} {
		req := &types.UpdateRequest{Action: tc.action, ContainerPort: tc.port, PortClass: tc.class, PortLimit: -10}
		if err := validatePortUpdate(req); (err == nil) != tc.valid {
			t.Fatalf("%+v: %v", req, err)
		}
	}
	var req types.UpdateRequest
	if err := json.Unmarshal([]byte(`{"action":"exposePort","container_port":3000,"port_class":"infrastructure","port_limit":1}`), &req); err != nil {
		t.Fatal(err)
	}
	if req.ContainerPort != 3000 || req.PortClass != "infrastructure" || req.PortLimit != 1 {
		t.Fatalf("wire fields lost: %+v", req)
	}
}

func TestPortUpdateForwardRefreshFailures(t *testing.T) {
	for _, name := range []string{"success", "read-error", "nil-target", "update-error", "nil-response", "nil-ret", "ret-zero", "ret-failure", "query-error", "nil-query", "empty-query", "nil-item", "wrong-id", "extra-item", "nil-mapping", "bad-container", "bad-host", "duplicate", "stale-expose", "static-close", "merge-error", "late-update", "late-query"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := &types.UpdateRequest{RequestID: "req", SandboxID: "sb-port", InstanceType: "cubebox", Action: "exposePort", ContainerPort: 3000, PortLimit: 1}
			if name == "static-close" {
				req.Action = "closePort"
			}
			updates, queries, merges := 0, 0, 0
			target := &localcache.ProxyPortsTarget{HostIP: "10.0.1.20"}
			var deadline time.Time
			checkContext := func(ctx context.Context) {
				t.Helper()
				d, ok := ctx.Deadline()
				if !ok || time.Until(d) > portUpdateTimeout {
					t.Fatal("missing bounded deadline")
				}
				if deadline.IsZero() {
					deadline = d
				} else if !deadline.Equal(d) {
					t.Fatal("budget reset between calls")
				}
			}
			deps := portUpdateDeps{
				read: func(ctx context.Context, id string) (*localcache.ProxyPortsTarget, error) {
					checkContext(ctx)
					if id != req.SandboxID {
						t.Fatal("wrong lookup")
					}
					if name == "read-error" {
						return nil, errors.New("redis down")
					}
					if name == "nil-target" {
						return nil, nil
					}
					return target, nil
				},
				address: func(host string) string {
					if host != target.HostIP {
						t.Fatal("wrong owner")
					}
					return host + ":9000"
				},
				update: func(ctx context.Context, endpoint string, rpc *cubebox.UpdateCubeSandboxRequest) (*cubebox.UpdateCubeSandboxResponse, error) {
					checkContext(ctx)
					updates++
					want := map[string]string{constants.CubeAnnotationsUpdateAction: req.Action, constants.CubeAnnotationsInsType: req.InstanceType,
						"cube.master.container_port": "3000", "cube.master.port_class": "user", "cube.master.port_limit": "1"}
					if endpoint != "10.0.1.20:9000" || rpc.SandboxID != req.SandboxID || rpc.RequestID != req.RequestID || !reflect.DeepEqual(rpc.Annotations, want) {
						t.Fatalf("bad forwarding: %s %+v", endpoint, rpc)
					}
					switch name {
					case "update-error":
						return nil, errors.New("rpc failed")
					case "nil-response":
						return nil, nil
					case "nil-ret":
						return &cubebox.UpdateCubeSandboxResponse{}, nil
					case "ret-failure":
						return &cubebox.UpdateCubeSandboxResponse{Ret: &retcode.Ret{RetCode: 123, RetMsg: "rejected"}}, nil
					case "ret-zero":
						return &cubebox.UpdateCubeSandboxResponse{Ret: &retcode.Ret{}}, nil
					case "late-update":
						cancel()
					}
					return &cubebox.UpdateCubeSandboxResponse{Ret: &retcode.Ret{RetCode: retcode.ErrorCode_Success}}, nil
				},
				list: func(ctx context.Context, endpoint string, rpc *cubebox.ListCubeSandboxRequest) (*cubebox.ListCubeSandboxResponse, error) {
					checkContext(ctx)
					queries++
					if endpoint != "10.0.1.20:9000" || rpc.GetId() != req.SandboxID {
						t.Fatal("query not pinned to update owner/id")
					}
					item := &cubebox.CubeSandbox{Id: req.SandboxID, PortMappings: []*cubebox.PortMapping{{ContainerPort: 3000, HostPort: 23000}, {ContainerPort: 17300, HostPort: 21000}}}
					result := &cubebox.ListCubeSandboxResponse{Items: []*cubebox.CubeSandbox{item}}
					switch name {
					case "query-error":
						return nil, errors.New("query failed")
					case "nil-query":
						return nil, nil
					case "empty-query":
						result.Items = nil
					case "nil-item":
						result.Items[0] = nil
					case "wrong-id":
						item.Id = "other"
					case "extra-item":
						result.Items = append(result.Items, item)
					case "nil-mapping":
						item.PortMappings = append(item.PortMappings, nil)
					case "bad-container":
						item.PortMappings[0].ContainerPort = 65536
					case "bad-host":
						item.PortMappings[0].HostPort = 0
					case "duplicate":
						item.PortMappings = append(item.PortMappings, item.PortMappings[0])
					case "stale-expose":
						item.PortMappings = item.PortMappings[1:]
					case "late-query":
						cancel()
					}
					return result, nil
				},
				merge: func(ctx context.Context, got *localcache.ProxyPortsTarget, ports map[string]string) error {
					checkContext(ctx)
					merges++
					if got != target || !reflect.DeepEqual(ports, map[string]string{"3000": "23000", "17300": "21000"}) {
						t.Fatalf("wrong actual mapping: %v", ports)
					}
					if name == "merge-error" {
						return errors.New("cache failed")
					}
					return nil
				},
			}
			res := updateSandboxPorts(ctx, req, deps)
			if (res.Ret.RetCode == int(errorcode.ErrorCode_Success)) != (name == "success" || name == "static-close") {
				t.Fatalf("unexpected result: %+v", res.Ret)
			}
			if name == "ret-failure" && res.Ret.RetCode != 123 {
				t.Fatal("node rejection not propagated")
			}
			if name != "success" && name != "static-close" && name != "merge-error" && merges != 0 {
				t.Fatal("failed RPC/query wrote cache")
			}
			if (name == "read-error" || name == "nil-target") && updates != 0 {
				t.Fatal("RPC without authoritative owner")
			}
			if (name == "update-error" || name == "nil-response" || name == "nil-ret" || name == "ret-zero" || name == "ret-failure" || name == "late-update") && queries != 0 {
				t.Fatal("query after update failure")
			}
		})
	}
}

func TestPortUpdateRetryRequeriesActualMappings(t *testing.T) {
	req := &types.UpdateRequest{SandboxID: "sb-port", Action: "exposePort", ContainerPort: 3000}
	updates, queries, merges := 0, 0, 0
	deps := portUpdateDeps{
		read: func(context.Context, string) (*localcache.ProxyPortsTarget, error) {
			return &localcache.ProxyPortsTarget{HostIP: "owner"}, nil
		},
		address: func(s string) string { return s },
		update: func(context.Context, string, *cubebox.UpdateCubeSandboxRequest) (*cubebox.UpdateCubeSandboxResponse, error) {
			updates++
			return &cubebox.UpdateCubeSandboxResponse{Ret: &retcode.Ret{RetCode: retcode.ErrorCode_Success}}, nil
		},
		list: func(context.Context, string, *cubebox.ListCubeSandboxRequest) (*cubebox.ListCubeSandboxResponse, error) {
			queries++
			ports := []*cubebox.PortMapping{}
			if req.Action == "exposePort" {
				ports = append(ports, &cubebox.PortMapping{ContainerPort: 3000, HostPort: int32(23000 + queries)})
			}
			return &cubebox.ListCubeSandboxResponse{Items: []*cubebox.CubeSandbox{{Id: req.SandboxID, PortMappings: ports}}}, nil
		},
		merge: func(_ context.Context, _ *localcache.ProxyPortsTarget, ports map[string]string) error {
			merges++
			if merges == 1 {
				return errors.New("write failed after node success")
			}
			if merges == 2 && ports["3000"] != "23002" {
				t.Fatalf("retry reused stale mapping: %v", ports)
			}
			if merges >= 3 && len(ports) != 0 {
				t.Fatal("close retained stale ports")
			}
			return nil
		},
	}
	for i := 0; i < 4; i++ {
		if i == 2 {
			req.Action = "closePort"
		}
		result := updateSandboxPorts(context.Background(), req, deps)
		if (result.Ret.RetCode == int(errorcode.ErrorCode_Success)) != (i > 0) {
			t.Fatalf("attempt %d: %+v", i, result.Ret)
		}
	}
	if updates != 4 || queries != 4 || merges != 4 {
		t.Fatalf("retry skipped work: %d/%d/%d", updates, queries, merges)
	}
}

func TestPortUpdateDispatchBypassesLegacyBridge(t *testing.T) {
	// Exercise the actual dispatcher with only its boundary dependencies patched.
	// An invalid manifest must not cause ports to enter the pause/resume bridge.
	t.Setenv("CUBE_LEGACY_PAUSE_MANIFEST", "/nonexistent-port-test-manifest")
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyFunc(normalizeSandboxIDInReq, func(context.Context, *string) *types.Ret { return nil })
	inLock := false
	patches.ApplyFunc(sandboxlock.WithLock, func(ctx context.Context, id string, opts sandboxlock.Options, fn func(context.Context) error) error {
		if !isPortAction(opts.Value) {
			t.Fatal("wrong lock action")
		}
		inLock = true
		defer func() { inLock = false }()
		return fn(ctx)
	})
	calls := 0
	patches.ApplyFunc(updateSandboxPorts, func(ctx context.Context, req *types.UpdateRequest, deps portUpdateDeps) *types.Res {
		if !inLock {
			t.Fatal("port update outside lifecycle lock")
		}
		calls++
		return &types.Res{Ret: &types.Ret{RetCode: int(errorcode.ErrorCode_Success)}}
	})
	for _, action := range []string{"exposePort", "closePort"} {
		result := Update(context.Background(), &types.UpdateRequest{SandboxID: "sb-port", InstanceType: "cubebox", Action: action, ContainerPort: 3000})
		if result.Ret.RetCode != int(errorcode.ErrorCode_Success) {
			t.Fatalf("port intercepted: %+v", result.Ret)
		}
	}
	if calls != 2 {
		t.Fatalf("dispatch calls: %d", calls)
	}
}
