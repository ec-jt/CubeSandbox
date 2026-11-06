package.path = "./lua/?.lua;" .. package.path
local value, managed, calls = nil, false, 0
package.loaded.lifecycle_state = {read = function() return value or (managed and "unknown" or nil) end}
package.loaded.utils = {
    respond_unavailable = function() error("unavailable") end,
    respond_gone = function() error("gone") end,
}
ngx = {
    shared = {
        cube_sandbox_state = {get = function() return value end, set = function(_, _, v) value = v end},
        cube_sandbox_meta = {get = function() return managed and "{}" or nil end},
    },
    var = {cube_sidecar_addr = "fixture-clm", http_x_cube_request_id = "test"},
    header = {}, WARN = 1, ERR = 2, HTTP_POST = 8, HTTP_OK = 200,
    log = function() end, escape_uri = function(x) return x end,
    location = {capture = function() calls = calls + 1; value = "running"; return {status = 200} end},
}
local gate = require("sandbox_state").gate
gate("sbx"); assert(calls == 0, "unmanaged entry changed")
managed = true
gate("sbx"); assert(calls == 1 and value == "running", "managed missing state passed without Master")
value = "unknown"; gate("sbx"); assert(calls == 2)
for _, state in ipairs({"pausing", "resuming", "unexpected"}) do
    value = state
    assert(not pcall(gate, "sbx"), "transition/invalid state passed")
    assert(calls == 2, "transition triggered duplicate resume")
end
value = "unknown"; ngx.var.cube_sidecar_addr = ""
assert(not pcall(gate, "sbx"), "missing CLM passed unknown state")
print("unknown managed state reconciles; transitions and missing CLM fail closed")
