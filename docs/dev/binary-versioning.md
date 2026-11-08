# Binary Versioning and Per-Node Version Query

All cluster binaries report a meaningful `--version` of the form:

```
<binary> <version> (<commit>) built at <build-time>
```

Example: `cubemaster v0.6.0 (3ceecfaf9ddb45596e3af5ec228f04282377947c) built at 2026-09-14T22:32:49Z`

## How the version is embedded

The canonical triplet is `CUBE_VERSION` / `CUBE_COMMIT` / `CUBE_BUILD_TIME`,
computed once by the root `Makefile` and exported into the builder container:

| Variable         | Source                                                        |
|------------------|---------------------------------------------------------------|
| `CUBE_VERSION`   | `git describe --tags --abbrev=0 --match 'v*'` (else `0.0.0-dev`) |
| `CUBE_COMMIT`    | `git rev-parse HEAD` (else `unknown`)                          |
| `CUBE_BUILD_TIME`| `date -u +'%Y-%m-%dT%H:%M:%SZ'`                                |

### Go binaries (cubelet, cubemaster, network-agent)

Each has a `version` package with `Version` / `Commit` / `BuildTime` variables
injected at link time via ldflags `-X`:

- `Cubelet/pkg/version`           -> `github.com/tencentcloud/CubeSandbox/Cubelet/pkg/version`
- `CubeMaster/pkg/base/version`   -> `github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/version`
- `network-agent/pkg/version`     -> `github.com/tencentcloud/CubeSandbox/network-agent/pkg/version`

Build through the sub-Makefiles (`make build`) or the root Makefile targets
(`make cubelet cubemaster network-agent`), which pass
`-ldflags "-X <pkg>.Version=... -X <pkg>.Commit=... -X <pkg>.BuildTime=..."`.

**Fallback:** each `version` package also has an `init()` that reads
`runtime/debug.ReadBuildInfo()`. A binary built from a git checkout *without*
ldflags (default `-buildvcs=true`) still recovers the real commit (`-dirty`
when the tree is dirty) and commit time, so `--version` is never `unknown`
unless VCS metadata is genuinely unavailable (e.g. `-buildvcs=false` with no
ldflags).

### Rust binary (cube-api)

`CubeAPI/build.rs` reads `CUBE_VERSION` / `CUBE_COMMIT` / `CUBE_BUILD_TIME`
from the environment and emits them as `CUBE_VERSION_FULL` etc. via
`cargo:rustc-env`. The clap `version` attribute renders
`<version> (<commit>) built at <build-time>`. Set the three env vars before
`cargo build --release` (the root Makefile `cubeapi` target does this through
the builder container).

## Per-node version query (for the status page)

Do **not** SSH to each node or add new endpoints. CubeMaster already collects
every node's component versions via the cubelet heartbeat into
`t_cube_node_component_version` and exposes them aggregated:

```
GET http://<cubemaster>:8089/internal/meta/version-matrix
```

Response (`data`):

- `control_plane`: `{version, commit, build_time}` of the serving cubemaster.
- `components[]`: per-component `{component, declared_version, consistent, versions:[{version, nodes[]}]}`.
- `nodes[]`: per-node `{node_id, healthy, components:[{component, version, declared}]}`.

The cubelet reports its *binary* version (the ldflags/`debug.ReadBuildInfo`
value) in this heartbeat, so once binaries are built with the triplet the
matrix reflects real per-node versions. A status page consumes this single
endpoint plus `GET /internal/node` (telemetry) and merges by node ID.

## deploy-hetzner.sh

The `cube` component of `scripts/deploy-hetzner.sh` (dc-danus repo) builds all
four binaries with the triplet: Go via pre-expanded ldflags strings, cube-api
via the three env vars. It then verifies each installed binary runs
`--version` before swapping it in.
