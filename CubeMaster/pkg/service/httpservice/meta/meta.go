// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

// Package meta serves Cubelet node registration and heartbeats from
// CubeMaster when no CubeOps is configured (see nodemeta/legacy.go).
//
// Two path families are served with identical handlers:
//   - /internal/meta/...             Cubelet <= v0.6 (pre-CubeOps)
//   - /internal/v1/node-agent/...    Cubelet >= v0.7 (CubeOps agent API)
//
// so a mixed fleet keeps reporting to CubeMaster during a staged upgrade.
// When cube_ops_addr is set the handlers answer with an error envelope and
// Cubelets must be pointed at CubeOps instead.
package meta

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/errorcode"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/nodemeta"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/httpservice/common"
	sandboxtypes "github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/types"
	"gorm.io/gorm"
)

const (
	LegacyMetaURI = "/internal/meta"
	NodeAgentURI  = "/internal/v1/node-agent"
)

type nodesResponse struct {
	RequestID string                   `json:"requestID,omitempty"`
	Ret       *sandboxtypes.Ret        `json:"ret,omitempty"`
	Data      []*nodemeta.NodeSnapshot `json:"data,omitempty"`
}

type nodeResponse struct {
	RequestID string                 `json:"requestID,omitempty"`
	Ret       *sandboxtypes.Ret      `json:"ret,omitempty"`
	Data      *nodemeta.NodeSnapshot `json:"data,omitempty"`
}

// RegisterRoutes wires the node-agent routes onto g. Call once per path family.
func RegisterRoutes(g *gin.RouterGroup) {
	g.GET("/readyz", readyzHandler)
	g.POST("/nodes/register", registerHandler)
	g.GET("/nodes", listHandler)
	g.GET("/nodes/:node_id", getHandler)
	g.POST("/nodes/:node_id/status", statusHandler)
}

func readyzHandler(c *gin.Context) {
	if !nodemeta.Ready() || !nodemeta.LegacyEnabled() {
		writeErr(c, http.StatusOK, errors.New("metadata service not ready"))
		return
	}
	common.WriteAPI(c, &sandboxtypes.Res{Ret: successRet()})
}

func registerHandler(c *gin.Context) {
	req := &nodemeta.RegisterNodeRequest{}
	if err := common.GetBodyReq(c.Request, req); err != nil {
		writeErr(c, http.StatusBadRequest, err)
		return
	}
	data, err := nodemeta.LegacyRegisterNode(c.Request.Context(), req)
	if err != nil {
		writeErr(c, http.StatusOK, err)
		return
	}
	common.WriteAPI(c, &nodeResponse{RequestID: req.RequestID, Ret: successRet(), Data: data})
}

func statusHandler(c *gin.Context) {
	req := &nodemeta.UpdateNodeStatusRequest{}
	if err := common.GetBodyReq(c.Request, req); err != nil {
		writeErr(c, http.StatusBadRequest, err)
		return
	}
	data, err := nodemeta.LegacyUpdateNodeStatus(c.Request.Context(), c.Param("node_id"), req)
	if err != nil {
		writeErr(c, http.StatusOK, err)
		return
	}
	common.WriteAPI(c, &nodeResponse{RequestID: req.RequestID, Ret: successRet(), Data: data})
}

func listHandler(c *gin.Context) {
	data, err := nodemeta.LegacyListNodes(c.Request.Context())
	if err != nil {
		writeErr(c, http.StatusOK, err)
		return
	}
	common.WriteAPI(c, &nodesResponse{Ret: successRet(), Data: data})
}

func getHandler(c *gin.Context) {
	data, err := nodemeta.LegacyGetNode(c.Request.Context(), c.Param("node_id"))
	if err != nil {
		writeErr(c, http.StatusOK, err)
		return
	}
	common.WriteAPI(c, &nodeResponse{Ret: successRet(), Data: data})
}

func successRet() *sandboxtypes.Ret {
	return &sandboxtypes.Ret{
		RetCode: int(errorcode.ErrorCode_Success),
		RetMsg:  errorcode.ErrorCode_Success.String(),
	}
}

func writeErr(c *gin.Context, status int, err error) {
	code := int(errorcode.ErrorCode_MasterInternalError)
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		code = int(errorcode.ErrorCode_NotFound)
	case errors.Is(err, nodemeta.ErrLabelsJSONCorrupt):
		code = int(errorcode.ErrorCode_MasterParamsError)
	}
	common.WriteResponse(c.Writer, status, &sandboxtypes.Res{
		Ret: &sandboxtypes.Ret{RetCode: code, RetMsg: err.Error()},
	})
}
