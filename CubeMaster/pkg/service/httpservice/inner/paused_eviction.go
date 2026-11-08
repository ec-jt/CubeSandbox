// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package inner

import (
	"github.com/gin-gonic/gin"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/httpservice/common"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox"
)

// PausedEvictionAction serves GET /internal/paused_eviction (read-only): node
// quota/disk usage plus pause bindings with eviction eligibility. See
// sandbox.BuildPausedEvictionView.
const PausedEvictionAction = "/paused_eviction"

func pausedEvictionGinHandler(c *gin.Context) {
	common.WriteAPI(c, sandbox.BuildPausedEvictionView(c.Request.Context()))
}
