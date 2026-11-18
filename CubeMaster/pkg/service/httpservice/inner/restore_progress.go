// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package inner

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/localcache"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/restoreprogress"
)

// RestoreProgressAction serves GET /internal/sandbox/:id/restore-progress
// (read-only). It returns the progress of a cross-node restore of the
// sandbox as reported by the Cubelet running it, with node_ip added, or 404
// when no node has one. Same auth as every other /internal route.
const RestoreProgressAction = "/sandbox/:id/restore-progress"

// restoreProgressFinder is swapped in tests.
var restoreProgressFinder = func(ctx context.Context, id string) (map[string]any, error) {
	f := &restoreprogress.Finder{Nodes: func() []string {
		var ips []string
		for _, n := range localcache.GetHealthyNodes(-1) {
			if n != nil && n.HostIP() != "" {
				ips = append(ips, n.HostIP())
			}
		}
		return ips
	}}
	return f.Find(ctx, id)
}

func restoreProgressGinHandler(c *gin.Context) {
	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sandbox id is required"})
		return
	}
	doc, err := restoreProgressFinder(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, restoreprogress.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "no restore in progress", "sandbox_id": id})
			return
		}
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error(), "sandbox_id": id})
		return
	}
	c.JSON(http.StatusOK, doc)
}
