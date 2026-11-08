// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/sourcefence"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

const sourceFenceAction = "FenceRecoverySource"
const sourceFenceBindingAnnotation = "cube.recovery.source_fence.binding"

// sourceQuiescedReadAction is the read-only counterpart of sourceFenceAction.
// Master uses it to prove the origin already stopped its runtime and drained
// the selected checkpoint sources before it may place a restore off the origin.
// It never mutates the fence store, so it can only report what already exists.
const sourceQuiescedReadAction = "ReadRecoverySourceQuiesced"
const sourceQuiescedExtInfoKey = "source_local_quiesced_not_global_fenced"

// readRecoverySourceQuiesced serves this node's durable local drain record for
// one sandbox. It returns Success only when a well-formed local-quiesced record
// already exists here; a missing, foreign or malformed record is a
// PreConditionFailed, so an unreachable or unquiesced origin cannot authorize a
// cross-node restore. No lock is needed: the record is immutable once written.
func (s *service) readRecoverySourceQuiesced(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest, rsp *cubebox.UpdateCubeSandboxResponse) (*cubebox.UpdateCubeSandboxResponse, error) {
	sandboxID := strings.TrimSpace(req.SandboxID)
	if sandboxID == "" {
		rsp.Ret.RetCode = errorcode.ErrorCode_InvalidParamFormat
		rsp.Ret.RetMsg = "source sandbox id required"
		return rsp, nil
	}
	local, err := workflow.SourceFences.LoadLocalDrain(sandboxID)
	if errors.Is(err, os.ErrNotExist) {
		// No recovery fence drain here: serve the LRU paused eviction record,
		// which uses the same format and the same strict loader.
		local, err = workflow.EvictionRecords.LoadLocalDrain(sandboxID)
	}
	if err != nil {
		rsp.Ret.RetCode = errorcode.ErrorCode_PreConditionFailed
		rsp.Ret.RetMsg = err.Error()
		return rsp, nil
	}
	raw, err := json.Marshal(local)
	if err != nil {
		rsp.Ret.RetCode = errorcode.ErrorCode_Unknown
		rsp.Ret.RetMsg = err.Error()
		return rsp, nil
	}
	if rsp.ExtInfo == nil {
		rsp.ExtInfo = make(map[string][]byte, 1)
	}
	rsp.ExtInfo[sourceQuiescedExtInfoKey] = raw
	return rsp, nil
}

// rootfsVolumeMountName returns the persisted rootfs volume mount name of a
// sandbox: the single volume mounted at "/" across its persisted containers.
// It is record data written at create time, not an Update annotation, and it is
// the only name either ownership cohort may resolve. The template cohort keeps
// its generation-root name behind that mount; the fresh-image cohort keeps the
// default-medium name Create minted.
func rootfsVolumeMountName(sb *cubeboxstore.CubeBox) (string, error) {
	if sb == nil {
		return "", fmt.Errorf("sandbox record missing")
	}
	name := ""
	for _, container := range sb.AllContainers() {
		if container == nil || container.Config == nil {
			continue
		}
		for _, mount := range container.Config.GetVolumeMounts() {
			if mount == nil || mount.GetContainerPath() != "/" {
				continue
			}
			if name != "" && name != mount.GetName() {
				return "", fmt.Errorf("multiple rootfs volume mounts found")
			}
			name = mount.GetName()
		}
	}
	if name == "" {
		return "", fmt.Errorf("sandbox %s has no writable rootfs volume mount", sb.ID)
	}
	return name, nil
}

// sourceIncarnation is read under the lifecycle and CubeBox locks. It includes
// create identity, workload epoch and the cohort-specific ownership identity,
// not cached STATE.
func sourceIncarnation(ctx context.Context, sb *cubeboxstore.CubeBox) (string, error) {
	return sourceIncarnationVersion(ctx, sb, false)
}

// sourceIncarnationVersion selects the identity cohort from the persisted
// record. A template-derived source keeps its original hash bytes. A
// fresh-image S3 source is identified only by the typed storage ownership marker
// plus the live native volume, and is hashed in its own versioned domain. A
// record that is neither is refused; nil LocalRunTemplate alone is not identity.
func sourceIncarnationVersion(ctx context.Context, sb *cubeboxstore.CubeBox, legacyEmpty bool) (string, error) {
	if sb == nil || sb.CreatedAt <= 0 {
		return "", fmt.Errorf("missing authoritative source incarnation")
	}
	epoch := sb.GuestMetricsEpochCopy()
	if epoch == nil || epoch.Generation == 0 || epoch.StartedAt.IsZero() {
		return "", fmt.Errorf("missing source workload epoch")
	}
	if sb.LocalRunTemplate == nil {
		// Fresh-image S3 cohort. The legacy empty-map compatibility flag does
		// not apply: no record in this cohort predates the typed marker.
		mount, err := rootfsVolumeMountName(sb)
		if err != nil {
			return "", err
		}
		v, err := storage.ResolveFreshImageSourceVolume(ctx, sb.ID, mount)
		if err != nil {
			return "", err
		}
		return sourcefence.FreshImageIncarnation(sb.ID, sb.CreatedAt, epoch.Generation, epoch.StartedAt,
			sourcefence.FreshImageVolume{Name: v.Name, Kind: v.Kind, Gen: v.Gen, UUID: v.UUID, Size: v.SizeBytes})
	}
	var volumes any = sb.LocalRunTemplate.Volumes
	// Canonical empty means null, preserving the old nil-map identity. Nonempty
	// maps are untouched. The old {} identity is accepted ONLY for exact replay.
	if len(sb.LocalRunTemplate.Volumes) == 0 {
		volumes = nil
		if legacyEmpty {
			volumes = map[string]any{}
		}
	}
	return sourcefence.TemplateIncarnation(sb.ID, sb.CreatedAt, epoch.Generation, epoch.StartedAt, volumes)
}

// This operation uses the existing private Cubelet Update RPC, not a new
// public route. It returns PreConditionFailed even after local quiescence:
// global ownership and destination recovery are not authorized.
// ExtInfo distinguishes diagnostic runtime progress from the local-only result.
func (s *service) stopRecoverySource(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest, rsp *cubebox.UpdateCubeSandboxResponse) (*cubebox.UpdateCubeSandboxResponse, error) {
	fail := func(err error) (*cubebox.UpdateCubeSandboxResponse, error) {
		rsp.Ret.RetCode = errorcode.ErrorCode_PreConditionFailed
		rsp.Ret.RetMsg = err.Error()
		return rsp, nil
	}
	var binding sourcefence.Binding
	d := json.NewDecoder(bytes.NewBufferString(req.Annotations[sourceFenceBindingAnnotation]))
	d.DisallowUnknownFields()
	if e := d.Decode(&binding); e != nil {
		return fail(e)
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return fail(sourcefence.ErrBinding)
	}
	if binding.OperationID != req.RequestID || binding.SandboxID != req.SandboxID {
		return fail(sourcefence.ErrBinding)
	}
	if e := binding.Validate(); e != nil {
		return fail(e)
	}
	if s.cubeboxMgr == nil || s.cubeboxMgr.config == nil || binding.NodeID != s.cubeboxMgr.config.SourceFenceNodeID {
		return fail(sourcefence.ErrBinding)
	}
	// Never wait for the package barrier while holding the lifecycle lock:
	// AppSnapshot owns the barrier while calling Create, and Destroy performs
	// package cleanup under the lifecycle lock. Drop and retry on contention.
	var unlock func()
	for {
		var err error
		unlock, err = s.sandboxLifecycleLocks.LockContext(ctx, req.SandboxID)
		if err != nil {
			return fail(err)
		}
		if sourcefence.Barrier.TryLock() {
			break
		}
		unlock()
		select {
		case <-ctx.Done():
			return fail(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	defer unlock()
	defer sourcefence.Barrier.Unlock()
	sb, e := s.cubeboxMgr.cubeboxManger.Get(ctx, req.SandboxID)
	if e != nil {
		return fail(fmt.Errorf("source missing/unreachable: %w", e))
	}
	if sb == nil {
		return fail(fmt.Errorf("source missing/unreachable: empty local record"))
	}
	sb.Lock()
	defer sb.Unlock()
	inc, e := sourceIncarnation(ctx, sb)
	if e != nil {
		return fail(e)
	}
	if inc != binding.Incarnation {
		legacy, err := sourceIncarnationVersion(ctx, sb, true)
		if err != nil || legacy != binding.Incarnation || !workflow.SourceFences.MatchesBinding(binding) {
			return fail(sourcefence.ErrBinding)
		}
	}
	work, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var runtime *sourcefence.RetainedRuntime
	defer func() {
		if runtime != nil {
			runtime.Close()
		}
	}()
	capture := func() ([]sourcefence.Process, error) {
		ns := sb.Namespace
		if ns == "" {
			ns = namespaces.Default
		}
		bundles := s.cubeboxMgr.shimBundlePaths(namespaces.WithNamespace(work, ns), sb)
		if len(bundles) != 1 {
			return nil, fmt.Errorf("source runtime bundle missing or ambiguous")
		}
		var err error
		runtime, err = sourcefence.CaptureRetained(work, s.cubeboxMgr.config.SourceFenceRuntime, bundles[0])
		if err != nil {
			return nil, err
		}
		// Resolve the selected commit and live generation before any runtime
		// mutation. Native evidence must survive in an immutable sidecar; the
		// caller's digest is only an expected value. Unsupported XFS commit
		// identity is explicit, not silently blessed by a local catalog copy.
		backend := pauseCatalogBackend(sb)
		// The rootfs volume name comes from the persisted record, so the
		// fresh-image cohort is resolved by its real default-medium name instead
		// of assuming the template convention sb-<id>-rootfs-gen<N>.
		mount, err := rootfsVolumeMountName(sb)
		if err != nil {
			return nil, err
		}
		root, err := storage.GetSandboxRootfsFor(work, backend, sb.ID, mount)
		if err != nil {
			return nil, err
		}
		var native *storage.SourceStorageBinding
		if sb.LocalRunTemplate == nil {
			native, err = storage.BindRecoverySourceFreshImage(work, sb.ID, binding.CheckpointID, binding.CheckpointDigest, mount)
		} else {
			native, err = storage.BindRecoverySource(work, backend, sb.ID, binding.CheckpointID, binding.CheckpointDigest, root)
		}
		if err != nil {
			return nil, err
		}
		membership, err := storage.BindRecoveryMembership(work, binding, native)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(struct {
			Native     *storage.SourceStorageBinding `json:"native"`
			Membership *storage.SourceMembership     `json:"membership"`
		}{native, membership})
		if err != nil {
			return nil, err
		}
		if err = workflow.SourceFences.BindStorage(binding, raw); err != nil {
			return nil, err
		}
		body, err := json.Marshal(membership)
		if err != nil {
			return nil, err
		}
		if _, err = storage.RecoverySourceNativeCall(work, "source-prepare", body); err != nil {
			return nil, err
		}
		return runtime.Processes(), nil
	}
	record, e := workflow.SourceFences.Stop(work, s.cubeboxMgr.config.SourceFenceEnabled, binding, capture, func(ctx context.Context, ps []sourcefence.Process) error {
		// A retry must qualify the current runtime again and compare it to the
		// immutable admitted membership. A missing/replaced runtime is denied.
		if runtime == nil {
			if _, err := capture(); err != nil {
				return err
			}
		}
		return runtime.Stop(ctx, sourcefence.Record{Version: 1, Binding: binding, Stage: sourcefence.Admitted, Processes: ps})
	})
	if record != nil {
		b, _ := json.Marshal(record)
		rsp.ExtInfo = map[string][]byte{"source_fence_progress_not_receipt": b}
	}
	if record != nil && record.Stage == sourcefence.RuntimeStopped && errors.Is(e, sourcefence.ErrDrainUnsupported) {
		raw, err := workflow.SourceFences.LoadStorage(binding)
		if err != nil {
			return fail(err)
		}
		var bound struct {
			Native     *storage.SourceStorageBinding `json:"native"`
			Membership json.RawMessage               `json:"membership"`
		}
		if err = json.Unmarshal(raw, &bound); err != nil || len(bound.Membership) == 0 {
			return fail(sourcefence.ErrBinding)
		}
		local, err := workflow.SourceFences.DrainLocal(work, binding, bound.Membership, func(ctx context.Context, body []byte) ([]byte, error) {
			return storage.RecoverySourceNativeCall(ctx, "source-drain", body)
		})
		if err != nil {
			return fail(err)
		}
		result, _ := json.Marshal(local)
		rsp.ExtInfo["source_local_quiesced_not_global_fenced"] = result
		return fail(fmt.Errorf("source locally quiesced; global ownership and destination recovery remain unsupported"))
	}
	if e == nil {
		e = sourcefence.ErrDrainUnsupported
	}
	return fail(e)
}
