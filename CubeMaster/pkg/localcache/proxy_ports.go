package localcache

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/gomodule/redigo/redis"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/rediskey"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/wrapredis"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/sandboxlock"
)

// ProxyPortsTarget binds a refresh to the shared routing incarnation read before
// the RPC. Private fields prevent callers from constructing an unguarded target.
type ProxyPortsTarget struct {
	HostIP                            string
	sandboxID, key, lockKey, token    string
	sandboxIP, sandboxPort, createdAt string
}

const readProxyPortsTargetScript = `
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return redis.error_reply('port update lock ownership lost') end
if redis.call('EXISTS', KEYS[1]) == 0 then return redis.error_reply('sandbox proxy metadata missing') end
return redis.call('HGETALL', KEYS[1])`

// All guards run before any mutation. Only decimal numeric fields belong to the
// port map; unknown metadata fields are preserved, as is the existing hash TTL.
const mergeProxyPortsScript = `
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return redis.error_reply('port update lock ownership lost') end
if redis.call('EXISTS', KEYS[1]) == 0 then return redis.error_reply('sandbox proxy metadata missing') end
local identity = {'HostIP', 'SandboxIP', 'SandboxPort', 'CreatedAt'}
for i, field in ipairs(identity) do
  if (redis.call('HGET', KEYS[1], field) or '') ~= ARGV[i+1] then
    return redis.error_reply('sandbox proxy routing incarnation changed')
  end
end
local desired = {}
for i = 6, #ARGV, 2 do desired[ARGV[i]] = ARGV[i+1] end
for _, field in ipairs(redis.call('HKEYS', KEYS[1])) do
  if string.match(field, '^%d+$') and not desired[field] then redis.call('HDEL', KEYS[1], field) end
end
for i = 6, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i+1]) end
return 1`

type portRedisDo func(context.Context, string, ...interface{}) (interface{}, error)

func portRedisCommand(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r := wrapredis.GetRedis()
	if r == nil || r.RedisConnPool == nil {
		return nil, sandboxlock.ErrRedisUnavailable
	}
	conn, err := r.RedisConnPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return redis.DoContext(conn, ctx, cmd, args...)
}

// ReadSandboxProxyPortsTarget fails closed for missing canonical routing data.
// It deliberately does not create a hash or fall back to a legacy read-only key.
func ReadSandboxProxyPortsTarget(ctx context.Context, sandboxID string) (*ProxyPortsTarget, error) {
	key, token, ok := sandboxlock.Owner(ctx, sandboxID)
	if !ok {
		return nil, errors.New("port update requires lifecycle lock owner")
	}
	return readProxyPortsTarget(ctx, portRedisCommand, sandboxID, key, token)
}

func readProxyPortsTarget(ctx context.Context, do portRedisDo, sandboxID, lockKey, token string) (*ProxyPortsTarget, error) {
	key := rediskey.SandboxProxy(sandboxID)
	fields, err := redis.StringMap(do(ctx, "EVAL", readProxyPortsTargetScript, 2, key, lockKey, token))
	if err != nil {
		return nil, err
	}
	if fields["HostIP"] == "" {
		return nil, errors.New("sandbox proxy HostIP missing")
	}
	return &ProxyPortsTarget{HostIP: fields["HostIP"], sandboxID: sandboxID, key: key, lockKey: lockKey, token: token,
		sandboxIP: fields["SandboxIP"], sandboxPort: fields["SandboxPort"], createdAt: fields["CreatedAt"]}, nil
}

// MergeSandboxProxyPorts publishes an authoritative, complete mapping in one
// script under the lifecycle owner token. It never calls the full proxy setter.
// No mapping revision exists in the node protocol: out-of-band node mutations
// and writers bypassing the lifecycle lock are not fenced by this operation.
func MergeSandboxProxyPorts(ctx context.Context, target *ProxyPortsTarget, ports map[string]string) error {
	if target == nil {
		return errors.New("nil proxy target")
	}
	key, token, ok := sandboxlock.Owner(ctx, target.sandboxID)
	if !ok || key != target.lockKey || token != target.token {
		return errors.New("proxy target belongs to a different lock acquisition")
	}
	return mergeProxyPorts(ctx, portRedisCommand, target, ports)
}

func mergeProxyPorts(ctx context.Context, do portRedisDo, target *ProxyPortsTarget, ports map[string]string) error {
	if target == nil || target.HostIP == "" || target.key == "" || target.lockKey == "" || target.token == "" {
		return errors.New("invalid proxy target")
	}
	if ports == nil {
		return errors.New("nil port mapping is not an authoritative empty map")
	}
	args := []interface{}{mergeProxyPortsScript, 2, target.key, target.lockKey, target.token, target.HostIP, target.sandboxIP, target.sandboxPort, target.createdAt}
	for container, host := range ports {
		for _, value := range []string{container, host} {
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != value {
				return fmt.Errorf("invalid port mapping %q -> %q", container, host)
			}
		}
		args = append(args, container, host)
	}
	written, err := redis.Int(do(ctx, "EVAL", args...))
	if err != nil {
		return err
	}
	if written != 1 {
		return errors.New("proxy port merge was not acknowledged")
	}
	return nil
}
