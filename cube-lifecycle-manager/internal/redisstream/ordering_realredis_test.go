package redisstream

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/cubemasterclient"
	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/lifecycle"
	"go.uber.org/zap"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestCompletionOrderingRealRedis(t *testing.T) {
	if os.Getenv("CLM_TEST_REDIS_ADDR") == "" {
		t.Skip("isolated Redis required")
	}
	if os.Getenv("CLM_TEST_DISPOSABLE_REDIS") != "1" {
		t.Fatal("disposable Redis required")
	}
	db := redis.NewClient(&redis.Options{Addr: os.Getenv("CLM_TEST_REDIS_ADDR"), Password: os.Getenv("CLM_TEST_REDIS_PASSWORD")})
	defer db.Close()
	ctx := context.Background()
	for _, tc := range []struct{ name, initial, transition, result, external string }{
		{"resume_then_external_pause", "paused", "resuming", "running", "paused"},
		{"pause_then_external_resume", "running", "pausing", "paused", "running"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := db.FlushDB(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			c := New(db, zap.NewNop())
			c.SetState(ctx, "sbx", tc.initial, 0)
			ownedCtx := WithTransitionOwner(ctx)
			if tc.transition == "resuming" {
				_, ok, e := c.AcquireResume(ownedCtx, "sbx", time.Minute)
				if e != nil || !ok {
					t.Fatal(e)
				}
			} else {
				ok, e := c.AcquireState(ownedCtx, "sbx", tc.transition, time.Minute)
				if e != nil || !ok {
					t.Fatal(e)
				}
			}
			var latest string
			master := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					RequestID string `json:"requestID"`
				}
				json.NewDecoder(r.Body).Decode(&req)
				for i, state := range []string{tc.result, tc.external} {
					rid := req.RequestID
					if i == 1 {
						rid = "external"
					}
					payload, _ := json.Marshal(map[string]string{"state": state, "request_id": rid})
					latest = db.XAdd(ctx, &redis.XAddArgs{Stream: lifecycle.EventStreamKey, Values: map[string]interface{}{"op": "state", "sandbox_id": "sbx", "payload": string(payload)}}).Val()
					// The live statesync handler skips both events while the CLM token exists.
					applied, e := c.ApplyStateEvent(ctx, "sbx", tc.transition, state, latest, 0)
					if e != nil || applied {
						t.Errorf("event replaced owner: %v %v", applied, e)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"ret":{"ret_code":200}}`))
			}))
			defer master.Close()
			client := cubemasterclient.New(master.URL, 3*time.Second)
			var err error
			if tc.transition == "resuming" {
				err = client.Resume(ownedCtx, "sbx", "cubebox")
			} else {
				err = client.Pause(ownedCtx, "sbx", "cubebox")
			}
			if err != nil {
				t.Fatal(err)
			}
			_ = c.WriteState(ownedCtx, "sbx", tc.result, 0)
			got, _, err := c.GetState(ctx, "sbx")
			if err != nil || got != tc.external {
				t.Fatalf("newer external %s hidden by completion: got %s err %v", tc.external, got, err)
			}
			raw := db.Get(ctx, lifecycle.StateKey("sbx")).Val()
			if raw != tc.external+"|"+latest {
				t.Fatalf("wrong causal watermark: %s", raw)
			}
		})
	}
}

func TestMissingCompletionReceiptRealRedis(t *testing.T) {
	if os.Getenv("CLM_TEST_REDIS_ADDR") == "" {
		t.Skip("isolated Redis required")
	}
	if os.Getenv("CLM_TEST_DISPOSABLE_REDIS") != "1" {
		t.Fatal("disposable Redis required")
	}
	db := redis.NewClient(&redis.Options{Addr: os.Getenv("CLM_TEST_REDIS_ADDR"), Password: os.Getenv("CLM_TEST_REDIS_PASSWORD")})
	defer db.Close()
	ctx := context.Background()
	for _, tc := range []struct{ name, payload string }{
		{"lost_event", ""}, {"old_producer", `{"state":"running"}`},
		{"rollback_cannot_hide_external_success", `{"state":"paused","request_id":"external"}`},
		{"malformed_entry", "malformed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db.FlushDB(ctx)
			c := New(db, zap.NewNop())
			c.SetState(ctx, "sbx", "paused", 0)
			ownedCtx := WithTransitionOwner(ctx)
			c.AcquireResume(ownedCtx, "sbx", time.Minute)
			if tc.payload != "" {
				db.XAdd(ctx, &redis.XAddArgs{Stream: lifecycle.EventStreamKey, Values: map[string]interface{}{"op": "state", "sandbox_id": "sbx", "payload": tc.payload}})
			}
			if err := c.WriteState(ownedCtx, "sbx", "running", 0); err == nil {
				t.Fatal("missing receipt accepted")
			}
			got, _, _ := c.GetState(ctx, "sbx")
			if got != "unknown" {
				t.Fatalf("invented outcome %s", got)
			}
			if ttl := db.PTTL(ctx, lifecycle.StateKey("sbx")).Val(); ttl != -1 {
				t.Fatalf("unknown expired: %v", ttl)
			}
			recovered, err := c.RecoverStates(ctx, []string{"sbx"})
			if err != nil || recovered["sbx"] != "unknown" {
				t.Fatalf("old history revived after missing receipt: %v %v", recovered, err)
			}
		})
	}
}
