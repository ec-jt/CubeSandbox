// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package evictor

import (
	"context"
	"time"

	"github.com/tencentcloud/CubeSandbox/cube-lifecycle-manager/internal/cubemasterclient"
)

// stateStore is the redisstream.Client subset the evictor needs.
type stateStore interface {
	AcquireEvict(ctx context.Context, sandboxID string, ttl time.Duration) (string, bool, error)
	ReleaseEvict(ctx context.Context, sandboxID string) (bool, error)
}

// master is the cubemasterclient.Client subset the evictor needs.
type master interface {
	PausedEvictionView(ctx context.Context) (*cubemasterclient.PausedEvictionView, error)
	Evict(ctx context.Context, sandboxID, instanceType string) error
}
