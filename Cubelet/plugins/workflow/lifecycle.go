package workflow

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
)

// SandboxLifecycleLocks is shared by service operations and queued cleanup.
// It is process-local, not a restart or cross-node ownership protocol.
var SandboxLifecycleLocks = utils.NewResourceLocks()

var lifecycleEpochs sync.Map

type lifecycleKey struct{}

// LifecycleCleanup records only evidence from this process and operation.
// Persisted queue entries from an earlier process cannot authorize cleanup.
type LifecycleCleanup struct {
	Epoch           string
	NeverStarted    bool
	RuntimeGone     bool
	RuntimeMayExist bool
	PreserveStorage bool
}

// BeginSandboxLifecycle must run under SandboxLifecycleLocks, before effects.
func BeginSandboxLifecycle(ctx context.Context, id string) context.Context {
	epoch := uuid.NewString()
	evidence := &LifecycleCleanup{Epoch: epoch}
	// A new request invalidates queued work, not facts about runtime ownership.
	// Even a rejected Update/Commit/Rollback must not erase an unresolved start.
	if prior, ok := lifecycleEpochs.Load(id); ok {
		previous := prior.(*LifecycleCleanup)
		evidence.RuntimeMayExist = previous.RuntimeMayExist
		evidence.RuntimeGone = previous.RuntimeGone
		evidence.PreserveStorage = previous.PreserveStorage
	}
	lifecycleEpochs.Store(id, evidence)
	return WithLifecycleCleanup(ctx, evidence)
}

func WithLifecycleCleanup(ctx context.Context, evidence *LifecycleCleanup) context.Context {
	return context.WithValue(ctx, lifecycleKey{}, evidence)
}

func LifecycleCleanupFrom(ctx context.Context) *LifecycleCleanup {
	evidence, _ := ctx.Value(lifecycleKey{}).(*LifecycleCleanup)
	return evidence
}

// CheckCleanupEpoch is called while holding the lifecycle lock. Unknown legacy
// jobs are allowed only for IDs never reused in this process. Tagged jobs from
// a previous process fail closed until explicit reconciliation.
func CheckCleanupEpoch(id string, evidence *LifecycleCleanup) error {
	current, exists := lifecycleEpochs.Load(id)
	if evidence == nil || evidence.Epoch == "" {
		if exists {
			return fmt.Errorf("unattributed cleanup for reused sandbox %s", id)
		}
		return nil
	}
	if !exists || current.(*LifecycleCleanup).Epoch != evidence.Epoch {
		return fmt.Errorf("stale or previous-process cleanup for sandbox %s", id)
	}
	return nil
}

// StaleCleanupVerifier is installed by the cubebox service. It reports, under
// the lifecycle lock, whether the persisted record for id provably never
// started a runtime and owns only attempt-owned storage. Any doubt is false.
var StaleCleanupVerifier func(ctx context.Context, id string) (bool, string)

func staleCleanupProven(ctx context.Context, id string) (bool, string) {
	v := StaleCleanupVerifier
	if v == nil {
		return false, ""
	}
	return v(ctx, id)
}

func RuntimeCleanupProven(ctx context.Context) bool {
	e := LifecycleCleanupFrom(ctx)
	return e != nil && (e.NeverStarted || e.RuntimeGone)
}

func MarkRuntimeGone(ctx context.Context) {
	if e := LifecycleCleanupFrom(ctx); e != nil {
		e.RuntimeGone = true
	}
}

// MarkRuntimeStarting invalidates shutdown evidence before runtime side effects.
func MarkRuntimeStarting(ctx context.Context) {
	if e := LifecycleCleanupFrom(ctx); e != nil {
		e.RuntimeMayExist = true
		e.RuntimeGone = false
		e.NeverStarted = false
	}
}

func PreserveRestoreStorage(ctx context.Context) bool {
	e := LifecycleCleanupFrom(ctx)
	return e != nil && e.PreserveStorage
}

// ContinueSandboxLifecycle retains proven shutdown across a failed storage
// delete retry. Call only under the lifecycle lock; Create always begins anew.
func ContinueSandboxLifecycle(ctx context.Context, id string) context.Context {
	if current, ok := lifecycleEpochs.Load(id); ok {
		return WithLifecycleCleanup(ctx, current.(*LifecycleCleanup))
	}
	return BeginSandboxLifecycle(ctx, id)
}

// CheckCreateRuntimeOwnership is checked under the lifecycle lock before a
// new attempt discards the previous epoch. A failed runtime start is ambiguous
// even if its runtime record disappeared during partial teardown.
func CheckCreateRuntimeOwnership(id string) error {
	if prior, ok := lifecycleEpochs.Load(id); ok {
		e := prior.(*LifecycleCleanup)
		if e.RuntimeMayExist && !e.RuntimeGone {
			return fmt.Errorf("sandbox %s: previous runtime teardown is unresolved", id)
		}
	}
	return nil
}
