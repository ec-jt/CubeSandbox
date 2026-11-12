// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package inner

import (
	"github.com/gin-gonic/gin"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/httpservice/common"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
)

// PublicationReconcileAction serves POST /internal/publication/reconcile, the
// operator-only recovery for a sandbox left frozen behind a snapshot
// publication hold (B15). Body: sandbox.ReconcilePublicationRequest. It is
// never invoked automatically.
const PublicationReconcileAction = "/publication/reconcile"

func publicationReconcileGinHandler(c *gin.Context) {
	req := &sandbox.ReconcilePublicationRequest{}
	if err := c.ShouldBindJSON(req); err != nil {
		common.WriteAPI(c, &sandbox.ReconcilePublicationResponse{Ret: &types.Ret{
			RetCode: int(errorcode.ErrorCode_MasterParamsError),
			RetMsg:  "invalid body: " + err.Error(),
		}})
		return
	}
	common.WriteAPI(c, sandbox.ReconcilePublication(c.Request.Context(), req))
}
