# cube-lifecycle-manager

Standalone service that owns sandbox auto-pause / auto-resume coordination
between CubeMaster, CubeProxy and Redis.

- Consumes lifecycle events from `cube:v1:shared:sandbox:lifecycle:events`
- Discovers every live CubeProxy replica in real time via
  `cube:v1:shared:cube_proxy:{registry,heartbeat}` and broadcasts sandbox
  metadata + state through their `/admin/*` endpoints
- Handles the synchronous `/internal/resume` callback CubeProxy invokes when
  a paused sandbox receives a request

## Local development

```sh
make test    # go test ./...
make build   # local image, tag: cube-lifecycle-manager:v0.7.1-<arch>
```

## Publishing images

Multi-arch release flow (mirrors `CubeEgress/Makefile`). Runs against both
`cube-sandbox-int.tencentcloudcr.com` and `cube-sandbox-cn.tencentcloudcr.com`.

```sh
# On an amd64 host:
make build push ARCH=amd64

# On an arm64 host:
make build push ARCH=arm64

# On either host (both arch-specific images must already be pushed):
make manifest
```

Overrides:

- `IMAGE_TAG=<tag>` — override the release tag (default `v0.7.1`)
- `V=1` — verbose docker commands

## Configuration

All configuration is via environment variables (prefix `CUBE_LCM_`); see
`internal/config/config.go` for the authoritative list.

When CubeMaster enables HTTP authentication, configure both
`CUBE_LCM_CUBEMASTER_USER_ID` and `CUBE_LCM_CUBEMASTER_SECRET_KEY` with a
user/secret entry from its `auth.secret_key_map`. CLM signs pause, resume,
and kill requests with CubeMaster's version 2023 HMAC-SHA256 protocol.
An incomplete credential pair fails validation. Leaving both unset retains
the unauthenticated client mode for deployments whose Master disables auth.
Keep the secret in a protected environment file or secret store, not command
arguments or logs. This credential is distinct from `CUBE_LCM_ADMIN_TOKEN`,
which protects CubeProxy admin requests, and sandbox traffic access tokens.
Master's signature protocol does not bind the request body or URI; keep this
service connection on trusted private transport or authenticated TLS.

CLM's `/internal/resume` listener is a trusted internal endpoint, not a public
user API. Network policy must restrict it to the intended proxy/control-plane
callers. Master authentication does not make this listener public-safe.

Kubernetes deployments run two warm replicas with Redis-backed leader
election. Both replicas consume lifecycle events and serve resume requests;
only the leader performs idle sweep/kill and stale CubeProxy pruning.
Leader election uses `SET NX PX` plus single-key `WATCH` transactions (no
Lua/EVAL). After acquiring the lease the new leader catches up the event
stream, waits one CubeProxy HTTP timeout to drain in-flight writes from the
previous leader, catches up again, then starts singleton work. Per-sandbox
state locks (`SET NX` with TTL) provide the fencing boundary for singleton
actions. Hydrating CubeProxy dicts is best-effort and does not gate leadership.
Host Docker/systemd deployments leave election disabled and retain
single-instance behavior.

### Lifecycle state retention and coordinated upgrade

The lifecycle state key now retains terminal running, paused, and unknown values
without a TTL. Transition ownership remains bounded by the configured lock TTL.
Transition values include an operation token; terminal values can include a Redis
stream watermark. These are private CLM encodings, not native storage READY or
snapshot validity claims. Pause atomically claims running; kill claims running or
paused; resume claims paused, unknown, or absent. An expired/deleted owner cannot
complete or roll back a newer owner. Unknown state does not authorize idle pause
or destruction.

Startup, stream-gap rebuild, and leader promotion recover state from retained
lifecycle events. Newer terminal watermarks reject older retained events. A legacy
key with no TTL is kept; a legacy terminal key with a TTL is migrated. If both key
and usable history are missing, CLM records unknown and replays it to CubeProxy.
Authorized traffic can request a real Master resume (subject to auto-resume policy);
CLM never assumes that an unknown sandbox is running. Master errors remain errors.
History is bounded and may be trimmed; the fallback does not require complete
history. Metadata remains the live-ID inventory. Delete writes a short-lived killed
tombstone and removes registry/proxy metadata, rather than retaining terminal keys
forever after deletion.

**Mixed old/new CLM writers are unsupported.** Stop and drain every old CLM before
starting new writers. Upgrade CubeProxy's unknown/resuming handling as part of the
same maintenance window. Old writers can overwrite tokens, restore terminal TTLs,
and fail to interpret watermarks. Do not normalize/delete all state keys to roll
back. Keep Master HMAC authentication enabled. This repository change does not
perform or authorize that upgrade.

Non-expiring Redis values are not a guarantee of Redis crash durability. Configure
and validate Redis persistence, backup, and failover separately. The isolated
regressions used Redis with AOF/RDB disabled and proved CLM/proxy restarts while
Redis stayed alive, not Redis crash recovery. Native recovery RPC remains ENOTSUP;
remote holds remain off and Gate RU remains closed.
