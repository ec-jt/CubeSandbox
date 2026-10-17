// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package cubebox

import (
	"fmt"

	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/cubebox/v1"
	"github.com/tencentcloud/CubeSandbox/pkgs/proto/services/errorcode/v1"
)

// completeTemplatePublication acknowledges local catalog and package sealing,
// not remote export readiness. No cleanup runs here: either callback can fail
// after partial persistence, so stale object references must not be destroyed.
func completeTemplatePublication(rsp *cubebox.AppSnapshotResponse, persist, seal func() error) error {
	rsp.Ret = &errorcode.Ret{RetCode: errorcode.ErrorCode_Unknown, RetMsg: "template publication incomplete"}
	for _, step := range []struct {
		name string
		run  func() error
	}{{"catalog persistence", persist}, {"package finalization", seal}} {
		if err := step.run(); err != nil {
			wrapped := fmt.Errorf("template %s failed (artifacts retained for reconciliation): %w", step.name, err)
			rsp.Ret.RetMsg = wrapped.Error()
			return wrapped
		}
	}
	rsp.Ret = &errorcode.Ret{RetCode: errorcode.ErrorCode_Success, RetMsg: "success"}
	return nil
}
