package localcache

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gomodule/redigo/redis"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/rediskey"
)

func portCacheFixture(t *testing.T) (*miniredis.Miniredis, portRedisDo, *ProxyPortsTarget) {
	t.Helper()
	s := miniredis.RunT(t)
	c, err := redis.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	do := func(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
		return redis.DoContext(c, ctx, cmd, args...)
	}
	key, lock := rediskey.SandboxProxy("sb-port"), rediskey.LockSandbox("sb-port")
	s.Set(lock, "owner")
	s.HSet(key, "HostIP", "10.0.1.20", "SandboxIP", "172.16.0.2", "SandboxPort", "49999", "CreatedAt", "42",
		"status", "running", "lifecycle", "keep", "TrafficAccessToken", "token", "AllowPublicTraffic", "true", "FutureMetadata", "preserve",
		"17300", "21000", "3000", "23000", "003001", "23001")
	s.SetTTL(key, time.Hour)
	target, err := readProxyPortsTarget(context.Background(), do, "sb-port", lock, "owner")
	if err != nil {
		t.Fatal(err)
	}
	return s, do, target
}

func portHash(t *testing.T, do portRedisDo, key string) map[string]string {
	t.Helper()
	fields, err := redis.StringMap(do(context.Background(), "HGETALL", key))
	if err != nil {
		t.Fatal(err)
	}
	return fields
}

func TestProxyPortsMergeAtomicPreservesMetadataTTLAndRetry(t *testing.T) {
	s, do, target := portCacheFixture(t)
	before := portHash(t, do, target.key)
	ports := map[string]string{"17300": "21000", "3000": "33000", "4000": "24000"}
	calls := 0
	atomicDo := func(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
		calls++
		if cmd != "EVAL" {
			t.Fatalf("non-atomic mutation: %s", cmd)
		}
		return do(ctx, cmd, args...)
	}
	for i := 0; i < 2; i++ {
		if err := mergeProxyPorts(context.Background(), atomicDo, target, ports); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("expected one script per merge, got %d", calls)
	}
	after := portHash(t, do, target.key)
	for field, value := range before {
		if field == "3000" || field == "003001" {
			continue
		}
		if after[field] != value {
			t.Fatalf("field %s overwritten", field)
		}
	}
	if after["3000"] != "33000" || after["4000"] != "24000" || after["003001"] != "" {
		t.Fatalf("wrong mapping: %v", after)
	}
	if s.TTL(target.key) != time.Hour {
		t.Fatalf("TTL changed: %s", s.TTL(target.key))
	}
	delete(ports, "3000")
	if err := mergeProxyPorts(context.Background(), do, target, ports); err != nil {
		t.Fatal(err)
	}
	if s.HGet(target.key, "3000") != "" || s.HGet(target.key, "17300") != "21000" {
		t.Fatal("close did not remove only stale ports")
	}
	if err := mergeProxyPorts(context.Background(), do, target, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	if s.HGet(target.key, "17300") != "" || s.HGet(target.key, "status") != "running" || s.TTL(target.key) != time.Hour {
		t.Fatal("empty map damaged metadata or TTL")
	}
}

func TestProxyPortsMergeRejectsStaleOrMissingTarget(t *testing.T) {
	for _, change := range []string{"missing", "host", "sandbox-ip", "sandbox-port", "created", "owner", "expired", "wrongtype"} {
		t.Run(change, func(t *testing.T) {
			s, do, target := portCacheFixture(t)
			switch change {
			case "missing":
				s.Del(target.key)
			case "host":
				s.HSet(target.key, "HostIP", "10.0.1.21")
			case "sandbox-ip":
				s.HSet(target.key, "SandboxIP", "172.16.0.3")
			case "sandbox-port":
				s.HSet(target.key, "SandboxPort", "50000")
			case "created":
				s.HSet(target.key, "CreatedAt", "43")
			case "owner":
				s.Set(target.lockKey, "new-owner")
			case "expired":
				s.Del(target.lockKey)
			case "wrongtype":
				s.Del(target.key)
				s.Set(target.key, "not a hash")
			}
			before := s.Dump()
			if err := mergeProxyPorts(context.Background(), do, target, map[string]string{"4000": "24000"}); err == nil {
				t.Fatal("stale mutation succeeded")
			}
			if before != s.Dump() {
				t.Fatal("failed operation changed Redis")
			}
		})
	}
}

func TestProxyPortsMergeValidatesBeforeWriteAndReturnsFailures(t *testing.T) {
	_, do, target := portCacheFixture(t)
	before := portHash(t, do, target.key)
	for _, ports := range []map[string]string{nil, {"HostIP": "1"}, {"0": "1"}, {"65536": "1"}, {"3000": "0"}, {"3000": "65536"}, {"03000": "1"}, {"3000": "+1"}, {"3000": "n/a"}} {
		if err := mergeProxyPorts(context.Background(), do, target, ports); err == nil {
			t.Fatalf("accepted %v", ports)
		}
		if !reflect.DeepEqual(before, portHash(t, do, target.key)) {
			t.Fatal("invalid map changed Redis")
		}
	}
	for _, failure := range []portRedisDo{
		func(context.Context, string, ...interface{}) (interface{}, error) {
			return nil, errors.New("cache unavailable")
		},
		func(context.Context, string, ...interface{}) (interface{}, error) { return int64(0), nil },
	} {
		if err := mergeProxyPorts(context.Background(), failure, target, map[string]string{}); err == nil {
			t.Fatal("unacknowledged write succeeded")
		}
	}
	if _, err := ReadSandboxProxyPortsTarget(context.Background(), "sb-port"); err == nil {
		t.Fatal("read without lock succeeded")
	}
	if err := MergeSandboxProxyPorts(context.Background(), target, map[string]string{}); err == nil {
		t.Fatal("write without lock succeeded")
	}
}

func TestProxyPortsTargetFailsClosed(t *testing.T) {
	for _, change := range []string{"missing", "no-host", "owner", "wrongtype"} {
		t.Run(change, func(t *testing.T) {
			s, do, target := portCacheFixture(t)
			switch change {
			case "missing":
				s.Del(target.key)
			case "no-host":
				s.HDel(target.key, "HostIP")
			case "owner":
				s.Set(target.lockKey, "other")
			case "wrongtype":
				s.Del(target.key)
				s.Set(target.key, "bad")
			}
			if _, err := readProxyPortsTarget(context.Background(), do, "sb-port", target.lockKey, target.token); err == nil {
				t.Fatal("invalid target accepted")
			}
		})
	}
}
