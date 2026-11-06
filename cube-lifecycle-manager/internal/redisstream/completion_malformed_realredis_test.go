package redisstream

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/lifecycle"
	"go.uber.org/zap"
)

func TestCompletionMalformedAfterReceiptRealRedis(t *testing.T) {
	if os.Getenv("CLM_TEST_REDIS_ADDR") == "" {
		t.Skip("isolated Redis required")
	}
	if os.Getenv("CLM_TEST_DISPOSABLE_REDIS") != "1" {
		t.Fatal("disposable Redis required")
	}
	db := redis.NewClient(&redis.Options{Addr: os.Getenv("CLM_TEST_REDIS_ADDR"), Password: os.Getenv("CLM_TEST_REDIS_PASSWORD")})
	defer db.Close()
	ctx := context.Background()
	for _, tc := range []struct {
		name, sid, op, payload string
		unknown                bool
	}{
		{"same_sid_missing_op", "sbx", "", "malformed", true},
		{"same_sid_bad_state_payload", "sbx", "state", "malformed", true},
		{"other_sid_missing_op", "other", "", "malformed", false},
		{"other_sid_bad_state_payload", "other", "state", "malformed", false},
		{"unidentified_missing_op", "", "", "malformed", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := db.FlushDB(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			c := New(db, zap.NewNop())
			if err := c.SetState(ctx, "sbx", "paused", 0); err != nil {
				t.Fatal(err)
			}
			ownedCtx := WithTransitionOwner(ctx)
			if _, ok, err := c.AcquireResume(ownedCtx, "sbx", time.Minute); err != nil || !ok {
				t.Fatalf("acquire: %v %v", ok, err)
			}
			payload, _ := json.Marshal(map[string]string{"state": "running", "request_id": owner(ownedCtx)})
			receipt, err := db.XAdd(ctx, &redis.XAddArgs{Stream: lifecycle.EventStreamKey, Values: map[string]interface{}{"op": "state", "sandbox_id": "sbx", "payload": string(payload)}}).Result()
			if err != nil {
				t.Fatal(err)
			}
			values := map[string]interface{}{"sandbox_id": tc.sid, "payload": tc.payload}
			if tc.op != "" {
				values["op"] = tc.op
			}
			later, err := db.XAdd(ctx, &redis.XAddArgs{Stream: lifecycle.EventStreamKey, Values: values}).Result()
			if err != nil {
				t.Fatal(err)
			}
			err = c.WriteState(ownedCtx, "sbx", "running", 0)
			want, cursor := "running", receipt
			if tc.unknown {
				want, cursor = UnknownState, later
			}
			if (err != nil) != tc.unknown {
				t.Fatalf("completion error=%v unknown=%v", err, tc.unknown)
			}
			if raw := db.Get(ctx, lifecycle.StateKey("sbx")).Val(); raw != want+"|"+cursor {
				t.Fatalf("state=%s want=%s|%s", raw, want, cursor)
			}
			if ttl := db.PTTL(ctx, lifecycle.StateKey("sbx")).Val(); ttl != -1 {
				t.Fatalf("terminal TTL=%v", ttl)
			}
			recovered, err := c.RecoverStates(ctx, []string{"sbx"})
			if err != nil || recovered["sbx"] != want {
				t.Fatalf("recovery=%v err=%v", recovered, err)
			}
		})
	}
}
