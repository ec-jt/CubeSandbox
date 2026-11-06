package storage

import (
	"context"
	"fmt"
	"reflect"

	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

// ValidateColdRecreateBinding validates the local producer catalog and its
// bounded metadata. It is not an S3 retention receipt or legacy-disk importer.
// Allocation subsequently resolves and clones the catalog snapshot through
// the existing native clone-origin checks; the source is never attached RW.
func ValidateColdRecreateBinding(ctx context.Context, dir string, entry *SnapshotCatalogEntry, sandbox string) error {
	if entry == nil || entry.Kind != CatalogKindPauseSnapshot || entry.Backend != cow.BackendXFS || entry.DiskBinding == nil {
		return fmt.Errorf("cold recreate requires a bound local pause package")
	}
	b := entry.DiskBinding
	if entry.RootfsVol != cowTemplateRootfsName(entry.SnapshotID) {
		return fmt.Errorf("cold recreate rootfs clone source mismatch")
	}
	if b.SandboxID != sandbox || sandbox == "" {
		return fmt.Errorf("cold recreate source sandbox mismatch")
	}
	observed := ObserveCheckpointDiskBinding(ctx, dir, entry.SnapshotID, sandbox, cow.BackendXFS, b.CaptureID)
	if observed.MetadataBinding != "capture_metadata_consistent" {
		return fmt.Errorf("cold recreate capture metadata is missing, corrupt or mismatched")
	}
	onDisk, err := readBindingCatalog(dir)
	if err != nil || !reflect.DeepEqual(onDisk, entry) || b.MetadataSHA256["sandbox_spec.json"] == "" {
		return fmt.Errorf("cold recreate catalog or sandbox specification changed")
	}
	return nil
}
