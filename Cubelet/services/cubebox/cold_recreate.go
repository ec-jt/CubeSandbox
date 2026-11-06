package cubebox

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/constants"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/pathutil"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

// Admission precedes any storage effects, under the same trusted RPC boundary
// as pause Resume. Public create callers must not supply cube.master.* keys.
// The initial implementation deliberately accepts only bound local XFS pause
// packages. Legacy pause directories and remote import are not disk adoption.
func validateColdRecreateRequest(req *cubebox.RunCubeSandboxRequest) error {
	policy := req.GetAnnotations()[workflow.ColdRecreatePolicy]
	if policy == "" {
		return nil
	}
	if policy != workflow.ColdRecreateDiscardProcesses || os.Getenv("CUBELET_ENABLE_RETAINED_DISK_COLD_RECREATE") != "true" {
		return fmt.Errorf("retained-disk cold recreate is disabled or process-loss policy is invalid")
	}
	ann := req.GetAnnotations()
	for _, key := range []string{constants.MasterAnnotationDesiredSandboxID, constants.MasterAnnotationPauseSnapshotID} {
		if err := pathutil.ValidateSafeID(ann[key]); err != nil {
			return fmt.Errorf("cold recreate requires explicit safe identity %s: %w", key, err)
		}
	}
	backend, err := storageBackendFromAnnotations(ann)
	if err != nil || backend != cow.BackendXFS || (req.GetBackend() != "" && req.GetBackend() != cow.BackendXFS) || isCrossNodeRestore(ann) || ann[constants.MasterAnnotationSnapshotRemoteUUIDs] != "" {
		return fmt.Errorf("cold recreate currently requires a local XFS pause package")
	}
	if len(req.Containers) != 0 || len(req.Volumes) != 0 || ann[constants.MasterAnnotationsAppSnapshotCreate] != "" || ann[constants.MasterAnnotationAppSnapshotTemplateID] != "" {
		return fmt.Errorf("cold recreate must retain the packed containers, volumes and application lower layers")
	}
	if runtimeID := ann[constants.MasterAnnotationRuntimeSnapshotID]; runtimeID != "" && runtimeID != ann[constants.MasterAnnotationPauseSnapshotID] {
		return fmt.Errorf("cold recreate snapshot identity mismatch")
	}
	return nil
}

func validateColdRecreatePackage(ctx context.Context, req, packed *cubebox.RunCubeSandboxRequest, backend, snapshot, dir string) error {
	entry, err := storage.GetLocalSnapshotFor(ctx, backend, snapshot)
	if err != nil {
		return err
	}
	sandbox := req.GetAnnotations()[constants.MasterAnnotationDesiredSandboxID]
	if err := storage.ValidateColdRecreateBinding(ctx, dir, entry, sandbox); err != nil {
		return err
	}
	// The first container uses the sandbox ID. Additional containers have
	// generated IDs and are deliberately outside this initial contract.
	// A mismatch would select an empty upper tree; never repair it by guessing.
	if len(packed.Containers) != 1 || len(packed.Volumes) != 1 {
		return fmt.Errorf("cold recreate currently requires one retained rootfs container and volume")
	}
	c := packed.Containers[0]
	if c == nil || c.GetId() != sandbox {
		return fmt.Errorf("cold recreate container identity mismatch")
	}
	if err := pathutil.ValidateSafeID(c.GetId()); err != nil {
		return fmt.Errorf("unsafe retained container identity: %w", err)
	}
	if c.GetImage().GetStorageMedia() != "ext4" || strings.TrimSpace(c.GetImage().GetImage()) == "" {
		return fmt.Errorf("cold recreate requires retained immutable ext4 application lower image")
	}
	if c.GetSecurityContext().GetReadonlyRootfs() || len(c.GetVolumeMounts()) != 1 || c.GetVolumeMounts()[0].GetContainerPath() != "/" || c.GetVolumeMounts()[0].GetName() != packed.Volumes[0].GetName() {
		return fmt.Errorf("cold recreate requires the original writable rootfs binding only")
	}
	return nil
}
