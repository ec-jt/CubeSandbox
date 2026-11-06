package workflow

// ColdRecreatePolicy is an internal Master-to-Cubelet policy, never a restore
// error fallback. The literal acknowledges loss of all guest process state.
const ColdRecreatePolicy = "cube.master.recreate.policy"
const ColdRecreateDiscardProcesses = "cold-retained-disk-discard-processes-v1"
const RetainedRootfsRequired = "cube.rootfs.retained.required"
const RetainedLowerSHA256 = "cube.rootfs.retained.lower.sha256"

func (b *CreateContext) IsColdRecreate() bool {
	return b != nil && b.ReqInfo != nil && b.ReqInfo.GetAnnotations()[ColdRecreatePolicy] == ColdRecreateDiscardProcesses
}
