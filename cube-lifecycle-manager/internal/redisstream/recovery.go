package redisstream

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/lifecycle"
)

// UnknownState is deliberately not running. Traffic may request an authenticated
// Master resume, but idle maintenance must not destroy or pause an unknown guest.
const UnknownState = "unknown"

// WithTransitionOwner scopes all completion/rollback writes to one operation.
// The token survives WithoutCancel, but must never be reused for another action.
func WithTransitionOwner(ctx context.Context) context.Context {
	return lifecycle.WithOperationID(ctx, uuid.NewString())
}
func owner(ctx context.Context) string { return lifecycle.OperationID(ctx) }
func stateValue(raw string) string {
	s, _, _ := strings.Cut(raw, ":")
	s, _, _ = strings.Cut(s, "|")
	return s
}
func transitionValue(ctx context.Context, state string) string {
	if token := owner(ctx); token != "" {
		return state + ":" + token
	}
	return state
}
func terminalTTL(state string, ttl time.Duration) time.Duration {
	if state == lifecycle.StatePaused || state == lifecycle.StateRunning || state == UnknownState {
		return 0
	}
	return ttl
}
func owned(raw, token string) bool {
	_, got, ok := strings.Cut(raw, ":")
	return ok && got == token
}

// RecoverStates reconstructs metadata-backed sandboxes. Transition owners win;
// terminal watermarks reject older events. Scan retained events newest-first: a last
// state or create is evidence, an update is not. Missing/invalid history stays
// unknown. Prefix trimming is safe: it cannot hide a newer event behind a
// retained older event. Deleted IDs are never recreated from history.
// This is a bounded migration/read fallback, not a journal or native READY proof.
func (c *Client) RecoverStates(ctx context.Context, ids []string) (map[string]string, error) {
	out, err := c.GetStates(ctx, ids)
	if err != nil {
		return nil, err
	}
	pending := make(map[string]bool)
	history := make(map[string]string)
	eventIDs := make(map[string]string)
	for _, id := range ids {
		pending[id] = true
	}
	upper := "+"
	for scanned := 0; len(pending) > 0 && scanned < lifecycle.EventStreamMaxLen; {
		batch, err := c.rdb.XRevRangeN(ctx, lifecycle.EventStreamKey, upper, "-", 256).Result()
		if err != nil {
			return nil, fmt.Errorf("recover lifecycle history: %w", err)
		}
		if len(batch) == 0 {
			break
		}
		for _, msg := range batch {
			scanned++
			sid, _ := msg.Values[lifecycle.FieldSandboxID].(string)
			if !pending[sid] {
				continue
			}
			ev := decodeEvent(msg)
			if ev == nil {
				delete(pending, sid)
				continue
			}
			switch ev.Op {
			case lifecycle.OpState:
				history[sid] = UnknownState
				if ev.State != nil && (ev.State.State == lifecycle.StatePaused || ev.State.State == lifecycle.StateRunning) {
					history[sid] = ev.State.State
				}
				eventIDs[sid] = msg.ID
				delete(pending, sid)
			case lifecycle.OpCreate:
				if ev.Meta != nil {
					history[sid] = lifecycle.StateRunning
					eventIDs[sid] = msg.ID
				}
				delete(pending, sid)
			case lifecycle.OpDelete:
				history[sid] = lifecycle.StateKilled
				eventIDs[sid] = msg.ID
				delete(pending, sid)
			}
		}
		upper = "(" + batch[len(batch)-1].ID
	}
	for _, id := range ids {
		current, exists, err := c.GetState(ctx, id)
		if err != nil {
			return nil, err
		}
		if current == "pausing" || current == "resuming" || current == "killing" || current == lifecycle.StateKilled {
			out[id] = current
			continue
		}
		next := history[id]
		if next != "" {
			_, err = c.ApplyStateEvent(ctx, id, current, next, eventIDs[id], 0)
		} else if !exists {
			_, err = c.WriteStateCAS(ctx, id, "", UnknownState, 0)
		} else {
			// Legacy terminal keys may still carry a TTL. Remove it without changing
			// their value or watermark, and without stealing a concurrent owner.
			_, err = c.WriteStateCAS(ctx, id, current, current, 0)
		}
		if err != nil {
			return nil, err
		}
		current, exists, err = c.GetState(ctx, id)
		if err != nil {
			return nil, err
		}
		if !exists {
			current = UnknownState
		}
		out[id] = current
	}
	return out, nil
}
