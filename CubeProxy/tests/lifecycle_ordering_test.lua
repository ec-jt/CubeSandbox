package.path = "./lua/?.lua;" .. package.path
local shared, local_state, calls = "paused", "running", 0
package.loaded.lifecycle_state = {read = function() return shared end}
package.loaded.utils = {
 respond_unavailable = function() error("unavailable") end,
 respond_gone = function() error("gone") end,
}
ngx = {shared = {cube_sandbox_state = {get=function() return local_state end,
 set=function(_,_,v) local_state=v end}},
 var={cube_sidecar_addr="clm"}, header={}, WARN=1,ERR=2,HTTP_POST=8,HTTP_OK=200,
 log=function() end,escape_uri=function(x) return x end,
 location={capture=function() calls=calls+1;return {status=200} end}}
local gate=require("sandbox_state").gate
for _,newer in ipairs({"paused","pausing","resuming","killing","killed","unknown"}) do
 shared="paused"
 ngx.location.capture=function() calls=calls+1;shared=newer;return {status=200} end
 assert(not pcall(gate,"sbx"), "late HTTP response routed over "..newer)
 assert(shared==newer and local_state=="running", "response mutated state")
end
for _,newer in ipairs({"pausing","resuming","killing","killed"}) do
 shared=newer;local_state="running";local before=calls
 assert(not pcall(gate,"sbx"), "late running push bypassed "..newer)
 assert(calls==before,"transition started resume")
end
shared="running";local_state="paused";gate("sbx")
print("late push and late HTTP success cannot clobber authoritative lifecycle state")
