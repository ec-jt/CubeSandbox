package workflow

import (
	"context"
	"fmt"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/config"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/semaphore"
	"github.com/tencentcloud/CubeSandbox/pkgs/CubeLog"
	"testing"
	"time"
)

type lifecycleTestFlow struct {
	entered, release chan struct{}
	queued           *LifecycleCleanup
	cleanupCalls     int
	cancel           context.CancelFunc
	fail             bool
}

func (f *lifecycleTestFlow) ID() string                            { return "lifecycle-test" }
func (f *lifecycleTestFlow) Init(context.Context, *InitInfo) error { return nil }
func (f *lifecycleTestFlow) Create(ctx context.Context, o *CreateContext) error {
	if o.CleanupEvidence != nil {
		f.queued = o.CleanupEvidence
		return nil
	}
	if f.cancel != nil {
		f.cancel()
		return nil
	}
	return fmt.Errorf("create failed")
}
func (f *lifecycleTestFlow) Destroy(ctx context.Context, o *DestroyContext) error {
	close(f.entered)
	<-f.release
	if f.fail {
		return fmt.Errorf("rollback incomplete")
	}
	return nil
}
func (f *lifecycleTestFlow) CleanUp(context.Context, *CleanContext) error {
	f.cleanupCalls++
	return nil
}
func lifecycleEngine(f *lifecycleTestFlow) *Engine {
	e := &Engine{}
	for _, n := range []string{"create", "destroy", "cleanup"} {
		e.AddFlow(n, &Workflow{Name: n, Limiter: semaphore.NewLimiter(2), Steps: []*Step{{Actions: []Flow{f}}}})
	}
	e.AddCleaupFlow(f)
	return e
}
func TestLifecycleRollbackSettlesBeforeRetry(t *testing.T) {
	_, err := config.Init("", true)
	require.NoError(t, err)
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			id := "rollback-" + t.Name()
			unlock := SandboxLifecycleLocks.Lock(id)
			ctx := BeginSandboxLifecycle(context.Background(), id)
			ctx = namespaces.WithNamespace(ctx, "default")
			ctx = CubeLog.WithRequestTrace(ctx, &CubeLog.RequestTrace{RequestID: "test"})
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			f := &lifecycleTestFlow{entered: make(chan struct{}), release: make(chan struct{}), fail: true}
			if cancelled {
				f.cancel = cancel
			}
			e := lifecycleEngine(f)
			done := make(chan error, 1)
			go func() {
				done <- e.Create(ctx, &CreateContext{BaseWorkflowInfo: BaseWorkflowInfo{SandboxID: id}, Failover: true})
				unlock()
			}()
			select {
			case <-f.entered:
			case <-time.After(time.Second):
				t.Fatal("rollback not entered")
			}
			retry := make(chan struct{})
			go func() {
				u := SandboxLifecycleLocks.Lock(id)
				BeginSandboxLifecycle(context.Background(), id)
				u()
				close(retry)
			}()
			select {
			case <-done:
				t.Fatal("Create returned before rollback settled")
			case <-retry:
				t.Fatal("retry overtook rollback")
			case <-time.After(20 * time.Millisecond):
			}
			close(f.release)
			require.Error(t, <-done)
			<-retry
			require.NotNil(t, f.queued)
			require.Error(t, e.CleanUp(context.Background(), &CleanContext{BaseWorkflowInfo: BaseWorkflowInfo{SandboxID: id}, CleanupEvidence: f.queued}))
			require.Zero(t, f.cleanupCalls)
		})
	}
}
func TestLifecycleCleanupEvidence(t *testing.T) {
	id := t.Name()
	u := SandboxLifecycleLocks.Lock(id)
	defer u()
	ctx := BeginSandboxLifecycle(context.Background(), id)
	e := LifecycleCleanupFrom(ctx)
	require.NoError(t, CheckCleanupEpoch(id, e))
	require.Error(t, CheckCleanupEpoch(id, nil))
	require.False(t, RuntimeCleanupProven(ctx))
	MarkRuntimeGone(ctx)
	require.True(t, RuntimeCleanupProven(ContinueSandboxLifecycle(context.Background(), id)))
	BeginSandboxLifecycle(context.Background(), id)
	require.Error(t, CheckCleanupEpoch(id, e))
	require.Error(t, CheckCleanupEpoch("previous-process", &LifecycleCleanup{Epoch: "old"}))
}

func TestLifecycleUnresolvedRuntimeBlocksRetry(t *testing.T) {
	id := t.Name()
	u := SandboxLifecycleLocks.Lock(id)
	defer u()
	ctx := BeginSandboxLifecycle(context.Background(), id)
	require.NoError(t, CheckCreateRuntimeOwnership(id))
	MarkRuntimeStarting(ctx)
	require.Error(t, CheckCreateRuntimeOwnership(id))
	// An intervening rejected lifecycle request must not bypass the retry gate.
	previous := LifecycleCleanupFrom(ctx)
	ctx = BeginSandboxLifecycle(context.Background(), id)
	require.Error(t, CheckCleanupEpoch(id, previous))
	require.Error(t, CheckCreateRuntimeOwnership(id))
	MarkRuntimeGone(ctx)
	require.NoError(t, CheckCreateRuntimeOwnership(id))
	ctx = BeginSandboxLifecycle(context.Background(), id)
	require.True(t, RuntimeCleanupProven(ctx))
	MarkRuntimeStarting(ctx)
	require.False(t, RuntimeCleanupProven(ctx))
	require.Error(t, CheckCreateRuntimeOwnership(id))
}
