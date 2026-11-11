// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/log"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

// Legacy pausevm compatibility (rolling upgrade from v0.6.x).
//
// A v0.6 Cubelet paused a sandbox in place: the old shim keeps running and
// writes its VM state under <root>/pausevm/<sandbox>; Update(resume) then
// calls task.Resume on that same shim. This Cubelet removed both paths
// (pause is CubeCow-only, resume is Master Create from a pause snapshot), so a
// node upgraded while it holds such sandboxes could never resume them again.
//
// The compat path forwards Update(resume|pause) to the still-running old shim
// task for a sandbox that this Cubelet did not create and never paused:
//   - no captured ComponentVersions (every Create on this version captures
//     them; all 95 sandboxes loaded from the v0.6 store on app1 had none),
//   - no CubeCow pause stamp (a native pause owns that sandbox), and
//   - for resume: the sandbox is paused and its legacy pausevm state exists.
// Anything else keeps the native behaviour and its error messages.

// legacyPauseVMRoot is the v0.6 shim's fixed pause directory.
var legacyPauseVMRoot = "/data/cubelet/root/pausevm"

func legacyPauseVMStatePresent(sandboxID string) bool {
	if sandboxID == "" || strings.ContainsAny(sandboxID, "/\\") || sandboxID == "." || sandboxID == ".." {
		return false
	}
	st, err := os.Stat(filepath.Join(legacyPauseVMRoot, sandboxID, "config.json"))
	return err == nil && st.Mode().IsRegular()
}

// isLegacyRuntimeSandbox reports whether the sandbox was created by a pre-0.7
// Cubelet and is still driven by its original shim.
func isLegacyRuntimeSandbox(sb *cubeboxstore.CubeBox) bool {
	if sb == nil {
		return false
	}
	if len(sb.ComponentVersions) > 0 {
		return false
	}
	return stampedPauseSnapshotID(sb) == ""
}

// legacyUpdateRoute decides whether Update(action) must take the legacy path.
func legacyUpdateRoute(action string, sb *cubeboxstore.CubeBox) bool {
	if !isLegacyRuntimeSandbox(sb) {
		return false
	}
	switch action {
	case constants.UpdateActionResume:
		return sb.GetStatus().IsPaused() && legacyPauseVMStatePresent(sb.ID)
	case constants.UpdateActionPause:
		return !sb.GetStatus().IsPaused() && !sb.GetStatus().IsTerminated()
	}
	return false
}

// containerLoader is the narrow containerd.Client surface used to attach a
// legacy sandbox's container handle.
type containerLoader interface {
	LoadContainer(ctx context.Context, id string) (containerd.Container, error)
}

// ensureLegacyContainer attaches the containerd container handle of a legacy
// sandbox. Startup recovery (RecoverPod) treats every paused sandbox as a
// shim-less CubeCow tombstone and never loads its container, but a v0.6
// in-place pause keeps the VM inside its still-running shim. Without the
// handle Resume fails with "failed to load task for paused sandbox".
func ensureLegacyContainer(ctx context.Context, client containerLoader, sb *cubeboxstore.CubeBox) error {
	fc := sb.FirstContainer()
	if fc == nil {
		return fmt.Errorf("legacy sandbox %s has no first container", sb.ID)
	}
	if fc.Container != nil {
		return nil
	}
	if client == nil {
		return fmt.Errorf("legacy sandbox %s: no containerd client", sb.ID)
	}
	ns := sb.Namespace
	if ns == "" {
		ns = namespaces.Default
	}
	loadCtx, cancel := context.WithTimeout(namespaces.WithNamespace(ctx, ns), reconcileStatusTimeout)
	defer cancel()
	id := fc.ID
	if id == "" {
		id = sb.ID
	}
	cntr, err := client.LoadContainer(loadCtx, id)
	if err != nil {
		return fmt.Errorf("load legacy container %s: %w", id, err)
	}
	fc.Container = cntr
	return nil
}

func (s *service) legacyContainerClient() containerLoader {
	if s == nil || s.cubeboxMgr == nil || s.cubeboxMgr.client == nil {
		return nil
	}
	return s.cubeboxMgr.client
}

// legacyResume resumes a v0.6 in-place paused sandbox through its own shim.
func (s *service) legacyResume(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest, sb *cubeboxstore.CubeBox) (*cubebox.UpdateCubeSandboxResponse, error) {
	rsp := &cubebox.UpdateCubeSandboxResponse{RequestID: req.RequestID}
	log.G(ctx).Warnf("legacy pausevm resume via original shim sandbox=%s", sb.ID)
	if err := ensureLegacyContainer(ctx, s.legacyContainerClient(), sb); err != nil {
		rsp.Ret = &errorcode.Ret{RetCode: errorcode.ErrorCode_TaskResumeFailed, RetMsg: err.Error()}
		return rsp, nil
	}
	now := time.Now()
	result := s.resumeLocked(ctx, sb, resumeOptions{
		taskDeadline:      now.Add(taskResumeTimeout),
		reconcileDeadline: now.Add(taskResumeTimeout + reconcileStatusTimeout),
		persist:           true,
	})
	rsp.Ret = result.ret
	return rsp, nil
}

// pauseTask is the narrow containerd.Task surface the legacy pause needs.
type pauseTask interface {
	Pause(context.Context) error
	Status(context.Context) (containerd.Status, error)
}

// legacyPause re-pauses a v0.6 sandbox in place, exactly as the v0.6 Cubelet
// did (old shim writes pausevm state; VM stays in the shim process).
func (s *service) legacyPause(ctx context.Context, req *cubebox.UpdateCubeSandboxRequest, sb *cubeboxstore.CubeBox) (*cubebox.UpdateCubeSandboxResponse, error) {
	rsp := &cubebox.UpdateCubeSandboxResponse{
		RequestID: req.RequestID,
		Ret:       &errorcode.Ret{RetCode: errorcode.ErrorCode_Success},
	}
	ns := sb.Namespace
	if ns == "" {
		ns = namespaces.Default
	}
	if err := ensureLegacyContainer(ctx, s.legacyContainerClient(), sb); err != nil {
		rsp.Ret = &errorcode.Ret{RetCode: errorcode.ErrorCode_TaskPauseFailed, RetMsg: err.Error()}
		return rsp, nil
	}
	ctx = namespaces.WithNamespace(ctx, ns)
	ctx = constants.WithPreStopType(ctx, constants.PreStopTypePause)
	fc := sb.FirstContainer()
	if fc == nil || fc.Container == nil {
		rsp.Ret = &errorcode.Ret{RetCode: errorcode.ErrorCode_TaskPauseFailed, RetMsg: "failed to load task for legacy sandbox"}
		return rsp, nil
	}
	task, err := fc.Container.Task(ctx, nil)
	if err != nil {
		rsp.Ret = &errorcode.Ret{RetCode: errorcode.ErrorCode_TaskPauseFailed, RetMsg: err.Error()}
		return rsp, nil
	}
	log.G(ctx).Warnf("legacy pausevm pause via original shim sandbox=%s", sb.ID)
	ctx = addPauseResumeMetaData(ctx, req)
	defer func() { _ = s.cubeboxMgr.cubeboxManger.SyncByID(ctx, sb.ID) }()
	for _, c := range sb.All() {
		doPreStop(ctx, c)
	}
	rsp.Ret = legacyPauseTaskLocked(ctx, sb, task)
	return rsp, nil
}

func legacyPauseTaskLocked(ctx context.Context, sb *cubeboxstore.CubeBox, task pauseTask) *errorcode.Ret {
	for _, c := range sb.AllContainers() {
		if c.Status != nil {
			c.Status.Update(func(status cubeboxstore.Status) (cubeboxstore.Status, error) {
				status.PausingAt = time.Now().UnixNano()
				return status, nil
			})
		}
	}
	pauseCtx, cancel := context.WithTimeout(ctx, taskPauseTimeout)
	defer cancel()
	if err := task.Pause(pauseCtx); err != nil {
		queryCtx, qcancel := context.WithTimeout(context.Background(), reconcileStatusTimeout)
		defer qcancel()
		if ns, ok := namespaces.Namespace(ctx); ok && ns != "" {
			queryCtx = namespaces.WithNamespace(queryCtx, ns)
		}
		if st, qerr := task.Status(queryCtx); qerr == nil {
			convergePauseStateFromShim(ctx, sb, st.Status, fmt.Sprintf("legacy pause RPC error: %v", err))
		}
		return &errorcode.Ret{RetCode: errorcode.ErrorCode_TaskPauseFailed, RetMsg: err.Error()}
	}
	for _, c := range sb.AllContainers() {
		if c.Status != nil {
			c.Status.Update(func(status cubeboxstore.Status) (cubeboxstore.Status, error) {
				status.PausedAt = time.Now().UnixNano()
				status.PausingAt = 0
				return status, nil
			})
		}
	}
	return &errorcode.Ret{RetCode: errorcode.ErrorCode_Success}
}
