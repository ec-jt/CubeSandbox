# Dynamic user-service port exposure

CubeSandbox exposes user-service TCP ports lazily. Creating a sandbox no longer
requires preallocating every port a user might deploy. A caller requests one
port through:

```http
POST /sandboxes/{sandboxID}/ports/{containerPort}
Content-Type: application/json

{"portLimit": 5}
```

The response contains `containerPort` and `publicURL`. Repeating the same
request is idempotent and does not consume another quota slot.

## Architecture

1. CubeAPI validates the caller quota (`1..100`) and asks CubeMaster to run the
   `exposePort` update action.
2. CubeMaster routes the update to the sandbox's current Cubelet node.
3. Cubelet validates the VM is running, the port is `1..65535`, the port is not
   reserved, and the desired mapping count stays within both the caller quota
   and the platform hard cap of 100.
4. Cubelet asks network-agent to reconcile the complete desired port set.
5. network-agent allocates one host port, updates CubeVS's existing eBPF port
   map, and persists the mapping. It serializes mutation per sandbox so
   concurrent duplicate requests converge on one mapping.
6. CubeMaster refreshes `ContainerToHostPorts` in Redis. CubeProxy's existing
   same-node TAP-IP and cross-node host-port selection then routes traffic
   without Lua changes.

Port mappings remain in network-agent's persisted sandbox state across VM
pause/resume and network-agent restart. Sandbox destruction follows the
existing `ReleaseNetwork` path, deleting CubeVS mappings and releasing host
ports. Cross-node placement is resolved from CubeMaster's current sandbox host;
the existing cross-node eBPF ingress implementation is unchanged.

## Reserved ports

User exposure rejects Cube control-plane ports `17300`, `17301`, and `49983`.
Additional template-specific control ports should be added to Cubelet's
reserved set before rollout.

## Client usage

Python SDK:

```python
url = sandbox.expose_port(3000, port_limit=5)
```

The quota is an authenticated platform entitlement, not user input in a UI.
dc-danus passes Free=1, Starter=2, Pro=5, and Max=10. CubeSandbox independently
enforces the absolute cap of 100.

## Rollout

Build and roll out network-agent, Cubelet, CubeMaster, CubeAPI, then dependent
SDK/application images. No schema migration is required. Existing static
template ports continue to work and count toward the quota. The guest template
does not contain these control-plane changes, but dc-danus's vendored SDK does,
so rebuild the backend image. Rebuild the CubeSandbox guest template only when
bundling the updated SDK into that template for guest-side callers.
