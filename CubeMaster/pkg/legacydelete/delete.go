// Package legacydelete protects the native metadata of enrolled legacy sessions
// until their original Cubelet explicitly confirms destruction.
package legacydelete

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/cubelet"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/legacyresume"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/localcache"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/pausesnap"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/sandboxspec"
	volrefcount "github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/volume/refcount"
	cubebox "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	retcode "github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

var (
	loadCohort     = legacyresume.LoadConfigured
	getPause       = pausesnap.GetBySandbox
	getSpec        = sandboxspec.Get
	nodeRegistered = func(host string) bool { _, ok := localcache.GetNodesByIp(host); return ok }
	endpointFor    = cubelet.GetCubeletAddr
	destroy        = cubelet.DestroyWithTimeout
	applyVolumes   = volrefcount.ApplyFromExtInfo
)

// TryDestroy must run under the existing sandbox lifecycle lock. handled=true
// means the caller must not use native pause-package cleanup. Any error means
// keep proxy/cache/spec metadata, including on retries after ambiguous RPCs.
// A missing/unknown response is deliberately not equivalent to confirmed delete.
func TryDestroy(ctx context.Context, observedHost string, req *cubebox.DestroyCubeSandboxRequest) (handled bool, err error) {
	cohort, err := loadCohort()
	if err != nil {
		return true, err
	}
	if req == nil {
		return true, fmt.Errorf("missing destroy request")
	}
	origin, enrolled := cohort[req.GetSandboxID()]
	if !enrolled {
		return false, nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	record, err := getPause(lookupCtx, req.GetSandboxID())
	if err != nil && !errors.Is(err, pausesnap.ErrNotFound) {
		return true, fmt.Errorf("cannot verify native pause metadata; retaining legacy state")
	}
	if err == nil && record == nil {
		return true, fmt.Errorf("inconsistent native pause metadata")
	}
	spec, err := getSpec(lookupCtx, req.GetSandboxID())
	if err != nil || spec == nil {
		return true, fmt.Errorf("cannot verify legacy sandbox spec")
	}
	if err := legacyresume.Validate(origin, observedHost, strings.TrimSpace(spec.Backend), record != nil); err != nil {
		return true, err
	}
	if !nodeRegistered(origin) {
		return true, fmt.Errorf("legacy origin unavailable; retaining recovery metadata")
	}
	rsp, err := destroy(ctx, endpointFor(origin), req, 120*time.Second)
	if err != nil {
		return true, fmt.Errorf("legacy destroy RPC failed; reconcile before retry: %w", err)
	}
	if rsp == nil || rsp.GetRet() == nil {
		return true, fmt.Errorf("legacy destroy was not confirmed; retaining recovery metadata")
	}
	code := rsp.GetRet().GetRetCode()
	if code != retcode.ErrorCode_Success && code != retcode.ErrorCode_OK {
		return true, fmt.Errorf("legacy destroy rejected (code %d); retaining recovery metadata", code)
	}
	applyVolumes(ctx, rsp.GetExtInfo())
	return true, nil
}
