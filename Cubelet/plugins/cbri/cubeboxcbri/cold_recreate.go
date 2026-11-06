package cubeboxcbri

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/pathutil"
	"github.com/tencentcloud/CubeSandbox/Cubelet/plugins/workflow"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
)

func (e *cubeboxInstancePlugin) bindRetainedLower(c *cubebox.ContainerConfig, cold bool) error {
	if err := pathutil.ValidateSafeID(c.GetImage().GetImage()); err != nil {
		return err
	}
	f, err := os.Open(e.getImageFilePath(c.GetImage().GetImage()))
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	// Bounded first slice: locally administered immutable ext4 image only.
	const maxLower = 1 << 30
	if !st.Mode().IsRegular() || st.Size() <= 0 || st.Size() > maxLower {
		return fmt.Errorf("retained lower image must be a regular file of at most 1 GiB")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, maxLower+1))
	if err != nil || n != st.Size() {
		return fmt.Errorf("retained lower image changed while hashing")
	}
	digest := hex.EncodeToString(h.Sum(nil))
	if cold && c.GetAnnotations()[workflow.RetainedLowerSHA256] != digest {
		return fmt.Errorf("retained application lower image is missing its binding or has changed")
	}
	if c.Annotations == nil {
		c.Annotations = map[string]string{}
	}
	c.Annotations[workflow.RetainedLowerSHA256] = digest
	return nil
}
