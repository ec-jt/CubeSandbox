package sandbox

import (
	"context"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/localcache"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/sandboxlock"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	retcode "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

// Run with inlining disabled for this package, as for other gomonkey tests.
// These are Update-boundary tests, not live Cubelet/database integration tests.
func TestUpdateDoesNotInventErrorReceipt(t *testing.T) {
	for _, tc := range []struct {
		name, action, message string
		code                  int
		publish               bool
	}{
		{"create_lock_contention", "resume", "sandbox lifecycle operation is in progress; retry Create", int(retcode.ErrorCode_TaskStateInvalid), false},
		{"destroy_lock_contention", "pause", "sandbox lifecycle operation is in progress; retry DELETE after 2 seconds", int(retcode.ErrorCode_TaskStateInvalid), false},
		{"destroy_deadline", "pause", "insufficient time for delete; retry DELETE", int(retcode.ErrorCode_TaskStateInvalid), false},
		// Begin returns this same message regardless of binding status.
		{"creating_snapshot", "pause", "begin pause snapshot: sandbox sbx already has pause snapshot snap-creating", int(errorcode.ErrorCode_MasterParamsError), false},
		{"failed_snapshot", "pause", "begin pause snapshot: sandbox sbx already has pause snapshot snap-failed", int(errorcode.ErrorCode_MasterParamsError), false},
		{"unproven_ready_snapshot", "pause", "begin pause snapshot: sandbox sbx already has pause snapshot snap-ready", int(errorcode.ErrorCode_MasterParamsError), false},
		{"resume_success", "resume", "success", int(errorcode.ErrorCode_Success), true},
		{"pause_success", "pause", "success", int(errorcode.ErrorCode_Success), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := gomonkey.NewPatches()
			defer p.Reset()
			p.ApplyFunc(normalizeSandboxIDInReq, func(context.Context, *string) *types.Ret { return nil })
			locked := false
			p.ApplyFunc(sandboxlock.WithLock, func(ctx context.Context, sid string, opts sandboxlock.Options, fn func(context.Context) error) error {
				locked = true
				defer func() { locked = false }()
				return fn(ctx)
			})
			p.ApplyFunc(localcache.GetSandboxCache, func(string) *localcache.SandboxCache { return &localcache.SandboxCache{HostIP: "127.0.0.1"} })
			p.ApplyFunc(tryLegacyPauseUpdate, func(context.Context, *types.UpdateRequest, string) (*types.Res, bool) { return nil, false })
			p.ApplyFunc(publishUpdateTimeout, func(context.Context, *types.UpdateRequest) {})
			hooks := 0
			p.ApplyFunc(runAfterUpdateSandboxSuccessHook, func(ctx context.Context, sid, instance, action, requestID string) {
				hooks++
				if !locked || sid != "sbx" || action != tc.action || requestID != "request" {
					t.Fatal("receipt outside lock or wrong identity")
				}
			})
			transition := func(ctx context.Context, req *types.UpdateRequest, host string) *types.Res {
				if !locked {
					t.Fatal("transition outside lock")
				}
				if tc.publish {
					runAfterUpdateSandboxSuccessHook(ctx, req.SandboxID, req.InstanceType, req.Action, req.RequestID)
				}
				return &types.Res{Ret: &types.Ret{RetCode: tc.code, RetMsg: tc.message}}
			}
			p.ApplyFunc(pauseSandbox, transition)
			p.ApplyFunc(resumeFromPauseSnapshot, transition)
			rsp := Update(context.Background(), &types.UpdateRequest{SandboxID: "sbx", InstanceType: "cubebox", Action: tc.action, RequestID: "request"})
			want := 0
			if tc.publish {
				want = 1
			}
			if rsp.Ret.RetCode != tc.code || hooks != want {
				t.Fatalf("ret=%+v hooks=%d want=%d", rsp.Ret, hooks, want)
			}
		})
	}
}
