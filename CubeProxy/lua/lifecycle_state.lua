-- Lifecycle pushes are hints, not ordered evidence. Read the shared key at
-- the traffic boundary; never cache an unversioned push or resume response.
local redis = require "redis_iresty"
local _M = {}
function _M.read(sid)
    local red = redis:new({
        redis_ip = ngx.var.redis_ip, redis_port = ngx.var.redis_port,
        redis_pd = ngx.var.redis_pd, redis_index = ngx.var.redis_index,
        redis_master_name = ngx.var.redis_master_name,
        redis_sentinel_nodes = ngx.var.redis_sentinel_nodes,
        redis_sentinel_pd = ngx.var.redis_sentinel_pd,
        timeout = 1,
    })
    local raw, err = red:get("cube:v1:shared:sandbox:lifecycle:state:" .. sid)
    if err then return nil, err end
    if raw and raw ~= ngx.null and raw ~= "" then
        return raw:match("^[^:|]+")
    end
    local meta, meta_err = red:hget("cube:v1:shared:sandbox:lifecycle:meta", sid)
    if meta_err then return nil, meta_err end
    if meta and meta ~= ngx.null then return "unknown" end
    return nil
end
return _M
