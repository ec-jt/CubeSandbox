// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package cubebox

// Operator force destroy. A record that keeps failing Destroy with "runtime
// ownership unknown" (for example a never-started restore whose storage the
// ordinary policy preserves because it "may reuse retained disks") can be
// removed by an operator with the Destroy annotation
//
//	cube.operator.force_destroy=true
//
// Under the sandbox lifecycle lock the force path:
//  1. SIGKILLs every host process whose command line names the sandbox or one
//     of its container ids (shim, VMM), and waits for them to exit;
//  2. re-checks that containerd holds no task or shim and no such process is
//     left, failing closed on any trace or query error;
//  3. records runtime-gone evidence with storage release, so the ordinary
//     destroy workflow deletes the record, the sandbox volumes (through the
//     storage plugin, keeping the cubecow index consistent) and the network.
//
// It never overrides a snapshot publication hold (checked before this runs),
// a paused record (paused sandboxes own a pause snapshot and are deleted by
// the ordinary path), or a live containerd task.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/containerd/v2/pkg/namespaces"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/log"
	cubeboxstore "github.com/tencentcloud/CubeSandbox/Cubelet/pkg/store/cubebox"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/utils"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

// AnnotationOperatorForceDestroy requests the operator force destroy path.
const AnnotationOperatorForceDestroy = "cube.operator.force_destroy"

// forceKillWait bounds how long the force path waits for killed processes.
var forceKillWait = 10 * time.Second

// forceKill and processAlive are variables so tests can observe them.
var (
	forceKill    = func(pid int) error { return syscall.Kill(pid, syscall.SIGKILL) }
	processAlive = utils.ProcessAlive
)

func forceDestroyRequested(req *cubebox.DestroyCubeSandboxRequest) bool {
	if req == nil {
		return false
	}
	v := strings.TrimSpace(req.GetAnnotations()[AnnotationOperatorForceDestroy])
	return strings.EqualFold(v, "true") || v == "1"
}

// hostProcessPIDsMentioning returns the pids under root whose cmdline names id.
func hostProcessPIDsMentioning(root, id string) ([]int, error) {
	if strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("empty id")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	self := os.Getpid()
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid <= 1 || pid == self {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		if strings.Contains(string(raw), id) {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}

// forceReclaimRuntime runs steps 1 and 2 above and, on success, step 3.
// Call it under the lifecycle lock with the Destroy lifecycle context.
func (l *local) forceReclaimRuntime(ctx context.Context, sb *cubeboxstore.CubeBox) error {
	if sb == nil {
		return fmt.Errorf("force destroy: no record")
	}
	if sb.GetPublicationHold() != nil {
		return fmt.Errorf("force destroy %s: snapshot publication hold", sb.ID)
	}
	if st := sb.GetStatus(); st != nil {
		s := st.Get()
		if s.PausedAt != 0 || s.PausingAt != 0 {
			return fmt.Errorf("force destroy %s: record is paused; use the ordinary delete", sb.ID)
		}
	}
	ns := sb.Namespace
	if ns == "" {
		ns = namespaces.Default
	}
	pctx := namespaces.WithNamespace(ctx, ns)
	probe := l.runtimeEvidenceProbe()
	ids := sandboxShimLookupIDs(sb)
	for _, id := range ids {
		if exists, err := probe.TaskExists(pctx, id); err != nil {
			return fmt.Errorf("force destroy %s: cannot query containerd task %s: %w", sb.ID, id, err)
		} else if exists {
			return fmt.Errorf("force destroy %s: containerd task %s exists; use the ordinary delete", sb.ID, id)
		}
	}
	var killed []int
	for _, id := range ids {
		pids, err := hostProcessPIDsMentioning(procRoot, id)
		if err != nil {
			return fmt.Errorf("force destroy %s: scan processes: %w", sb.ID, err)
		}
		for _, pid := range pids {
			if err := forceKill(pid); err != nil && err != syscall.ESRCH {
				return fmt.Errorf("force destroy %s: kill %d: %w", sb.ID, pid, err)
			}
			killed = append(killed, pid)
		}
	}
	deadline := time.Now().Add(forceKillWait)
	for _, pid := range killed {
		for processAlive(pid) {
			if time.Now().After(deadline) {
				return fmt.Errorf("force destroy %s: pid %d still alive after SIGKILL", sb.ID, pid)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if reason, ok := l.noRuntimeArtifacts(ctx, sb, true, false); !ok {
		return fmt.Errorf("force destroy %s: runtime still present: %s", sb.ID, reason)
	}
	evidence := workflow.LifecycleCleanupFrom(ctx)
	if evidence == nil {
		return fmt.Errorf("force destroy %s: no lifecycle context", sb.ID)
	}
	evidence.NeverStarted = false
	evidence.RuntimeMayExist = false
	evidence.RuntimeGone = true
	evidence.PreserveStorage = false
	log.G(ctx).Warnf("sandbox %s: operator force destroy: killed %d process(es) %v; no task, shim or process left; releasing record and storage",
		sb.ID, len(killed), killed)
	return nil
}
