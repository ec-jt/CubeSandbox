// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package inner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/service/sandbox/restoreprogress"
)

func TestRestoreProgressHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	prev := restoreProgressFinder
	t.Cleanup(func() { restoreProgressFinder = prev })
	restoreProgressFinder = func(_ context.Context, id string) (map[string]any, error) {
		switch id {
		case "running":
			return map[string]any{"sandbox_id": id, "phase": "importing", "local_chunks": 5, "total_chunks": 10, "node_ip": "10.0.1.2"}, nil
		case "broken":
			return nil, errors.New("cubelet unreachable")
		}
		return nil, restoreprogress.ErrNotFound
	}
	r := gin.New()
	RegisterInnerRoutes(r.Group(InnerURI()))

	get := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		return w
	}
	w := get("/internal/sandbox/running/restore-progress")
	if w.Code != http.StatusOK {
		t.Fatalf("code %d", w.Code)
	}
	var doc map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc["phase"] != "importing" || doc["node_ip"] != "10.0.1.2" {
		t.Fatalf("unexpected %v", doc)
	}
	if w := get("/internal/sandbox/idle/restore-progress"); w.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", w.Code)
	}
	if w := get("/internal/sandbox/broken/restore-progress"); w.Code != http.StatusBadGateway {
		t.Fatalf("want 502, got %d", w.Code)
	}
}
