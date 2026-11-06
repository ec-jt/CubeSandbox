package redisstream

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/lifecycle"
	"go.uber.org/zap"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLifecycleRealRedis(t *testing.T) {
	addr := os.Getenv("CLM_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("isolated real Redis required")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("CLM_TEST_REDIS_PASSWORD")})
	defer rdb.Close()
	c := New(rdb, zap.NewNop())
	ctx := context.Background()
	// This test uses its own disposable Redis. Never point at a shared service.
	if os.Getenv("CLM_TEST_DISPOSABLE_REDIS") != "1" {
		t.Fatal("disposable Redis acknowledgement required")
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	t.Run("terminal_survives_old_TTL_and_new_client", func(t *testing.T) {
		if err := c.WriteState(ctx, "retained", "paused", 100*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		time.Sleep(160 * time.Millisecond)
		restarted := New(rdb, zap.NewNop())
		v, ok, e := restarted.GetState(ctx, "retained")
		if e != nil || !ok || v != "paused" {
			t.Fatalf("%q %v %v", v, ok, e)
		}
		if ttl := rdb.PTTL(ctx, lifecycle.StateKey("retained")).Val(); ttl != -1 {
			t.Fatalf("TTL=%v", ttl)
		}
	})
	t.Run("terminal_running_immediate_pause_single_winner", func(t *testing.T) {
		c.WriteState(ctx, "race", "running", time.Hour)
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ok, _ := c.AcquireState(WithTransitionOwner(ctx), "race", "pausing", time.Second)
				if ok {
					wins.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("winners=%d", wins.Load())
		}
	})
	t.Run("transitions_refuse_every_other_owner", func(t *testing.T) {
		for _, v := range []string{"pausing", "resuming", "killing", "killed", "unknown"} {
			c.SetState(ctx, "blocked", v, time.Minute)
			ok, e := c.AcquireState(WithTransitionOwner(ctx), "blocked", "pausing", time.Second)
			if e != nil || ok {
				t.Fatalf("pause over %s: %v %v", v, ok, e)
			}
			if v != "unknown" {
				_, ok, e = c.AcquireResume(WithTransitionOwner(ctx), "blocked", time.Second)
				if e != nil || ok {
					t.Fatalf("resume over %s", v)
				}
			}
			_, ok, e = c.AcquireKill(WithTransitionOwner(ctx), "blocked", time.Second)
			if e != nil || ok {
				t.Fatalf("kill over %s", v)
			}
		}
	})
	t.Run("expired_owner_cannot_complete_or_rollback_new_owner", func(t *testing.T) {
		old := WithTransitionOwner(ctx)
		c.SetState(ctx, "aba", "paused", time.Minute)
		_, ok, e := c.AcquireResume(old, "aba", 70*time.Millisecond)
		if !ok || e != nil {
			t.Fatal(e)
		}
		time.Sleep(100 * time.Millisecond)
		newer := WithTransitionOwner(ctx)
		_, ok, e = c.AcquireResume(newer, "aba", time.Second)
		if !ok || e != nil {
			t.Fatal(e)
		}
		if e = c.WriteState(old, "aba", "running", 0); e == nil {
			t.Fatal("stale completion accepted")
		}
		if e = c.ClearStateNotify(old, "aba"); e == nil {
			t.Fatal("stale rollback accepted")
		}
		payload, _ := json.Marshal(map[string]string{"state": "running", "request_id": owner(newer)})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: lifecycle.EventStreamKey, Values: map[string]interface{}{"op": "state", "sandbox_id": "aba", "payload": string(payload)}})
		if e = c.WriteState(newer, "aba", "running", 0); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("retained_legacy_pause_and_missing_history", func(t *testing.T) {
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: lifecycle.EventStreamKey, Values: map[string]interface{}{"op": "state", "sandbox_id": "legacy", "payload": `{"state":"paused"}`}})
		got, e := c.RecoverStates(ctx, []string{"legacy", "missing"})
		if e != nil || got["legacy"] != "paused" || got["missing"] != UnknownState {
			t.Fatalf("%v %v", got, e)
		}
		rdb.Del(ctx, lifecycle.EventStreamKey)
		got, e = c.RecoverStates(ctx, []string{"legacy", "missing"})
		if e != nil || got["legacy"] != "paused" || got["missing"] != UnknownState {
			t.Fatalf("%v %v", got, e)
		}
	})
	t.Run("delete_fences_completion_and_stale_state_event", func(t *testing.T) {
		c.SetState(ctx, "deleted", "paused", time.Minute)
		o := WithTransitionOwner(ctx)
		c.AcquireResume(o, "deleted", time.Second)
		c.ClearState(ctx, "deleted")
		if e := c.WriteState(o, "deleted", "running", 0); e == nil {
			t.Fatal("deleted resurrected")
		}
		ok, e := c.WriteStateCAS(ctx, "deleted", "paused", "running", 0)
		if e != nil || ok {
			t.Fatal("stale event resurrected")
		}
		got, e := c.RecoverStates(ctx, []string{})
		if e != nil || len(got) != 0 {
			t.Fatal("unregistered history resurrected")
		}
	})
	t.Run("stale_event_after_completion", func(t *testing.T) {
		id := rdb.XAdd(ctx, &redis.XAddArgs{Stream: lifecycle.EventStreamKey, Values: map[string]interface{}{"op": "state", "sandbox_id": "late", "payload": `{"state":"paused"}`}}).Val()
		c.SetState(ctx, "late", "paused", 0)
		o := WithTransitionOwner(ctx)
		c.AcquireResume(o, "late", time.Second)
		payload, _ := json.Marshal(map[string]string{"state": "running", "request_id": owner(o)})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: lifecycle.EventStreamKey, Values: map[string]interface{}{"op": "state", "sandbox_id": "late", "payload": string(payload)}})
		if e := c.WriteState(o, "late", "running", 0); e != nil {
			t.Fatal(e)
		}
		ok, e := c.ApplyStateEvent(ctx, "late", "running", "paused", id, 0)
		if e != nil || ok {
			t.Fatalf("stale event accepted: %v %v", ok, e)
		}
		id = rdb.XAdd(ctx, &redis.XAddArgs{Stream: lifecycle.EventStreamKey, Values: map[string]interface{}{"op": "state", "sandbox_id": "late", "payload": `{"state":"paused"}`}}).Val()
		ok, e = c.ApplyStateEvent(ctx, "late", "running", "paused", id, 0)
		if e != nil || !ok {
			t.Fatalf("new event refused: %v %v", ok, e)
		}
	})

	t.Run("offline_event_supersedes_retained_running", func(t *testing.T) {
		c.SetState(ctx, "offline", "running", 0)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: lifecycle.EventStreamKey, Values: map[string]interface{}{"op": "state", "sandbox_id": "offline", "payload": `{"state":"paused"}`}})
		got, e := c.RecoverStates(ctx, []string{"offline"})
		if e != nil || got["offline"] != "paused" {
			t.Fatalf("%v %v", got, e)
		}
	})
	t.Run("trimmed_history_unknown_no_idle_destruction", func(t *testing.T) {
		rdb.Del(ctx, lifecycle.EventStreamKey)
		got, e := c.RecoverStates(ctx, []string{"unrecoverable"})
		if e != nil || got["unrecoverable"] != UnknownState {
			t.Fatalf("%v %v", got, e)
		}
		ok, e := c.AcquireState(WithTransitionOwner(ctx), "unrecoverable", "pausing", time.Second)
		if e != nil || ok {
			t.Fatal("unknown paused")
		}
		_, ok, e = c.AcquireKill(WithTransitionOwner(ctx), "unrecoverable", time.Second)
		if e != nil || ok {
			t.Fatal("unknown killed")
		}
		o := WithTransitionOwner(ctx)
		_, ok, e = c.AcquireResume(o, "unrecoverable", time.Second)
		if e != nil || !ok {
			t.Fatal("unknown cannot reconcile via Master")
		}
	})
	t.Run("resume_and_kill_one_winner", func(t *testing.T) {
		c.SetState(ctx, "resume-kill", "paused", time.Minute)
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				var ok bool
				if i == 0 {
					_, ok, _ = c.AcquireResume(WithTransitionOwner(ctx), "resume-kill", time.Second)
				} else {
					_, ok, _ = c.AcquireKill(WithTransitionOwner(ctx), "resume-kill", time.Second)
				}
				if ok {
					wins.Add(1)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("winners=%d", wins.Load())
		}
	})
}
