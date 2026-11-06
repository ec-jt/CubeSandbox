package.path = "./lua/?.lua;" .. package.path
local gates, resolutions = 0, 0
local permitted = false
package.loaded.utils = {is_null = function(_, x) return not x or x == "" end}
package.loaded.sandbox_backend = {
    authorize = function() if not permitted then error("denied") end end,
    resolve_backend = function() resolutions = resolutions + 1; return "127.0.0.1", 8080 end,
}
package.loaded.sandbox_state = {gate = function() gates = gates + 1 end}
package.loaded.request_host = {apply = function() end}
for _, file in ipairs({"rewrite_phase.lua", "path_rewrite_phase.lua"}) do
    ngx = {var = {http_host = "8080-test.example", uri = "/sandbox/test/8080/"}, req = {set_uri = function() end}}
    permitted = false
    local before = gates
    local ok = pcall(dofile, "lua/" .. file)
    assert(not ok and gates == before, "denied request reached resume gate")
    permitted = true
    dofile("lua/" .. file)
    assert(gates == before + 1 and resolutions == gates)
end
local writes = 0
ngx = {var = {msec = "1000.0", ins_id = "test"}, ctx = {},
    shared = {cube_sandbox_last_active = {get = function() return nil end,
        set = function() writes = writes + 1; return true end}}, now = function() return 1000 end}
dofile("lua/log_phase.lua")
assert(writes == 0, "denied traffic refreshed activity")
ngx.ctx.cube_traffic_authorized = true
dofile("lua/log_phase.lua")
assert(writes == 1)
print("authorization precedes resume; denied traffic does not refresh activity")
