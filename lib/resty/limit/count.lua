-- implement GitHub request rate limiting:
--    https://developer.github.com/v3/#rate-limiting

local ngx_shared = ngx.shared
local setmetatable = setmetatable
local assert = assert


local _M = {
   _VERSION = '0.06'
}


local mt = {
    __index = _M
}


-- the "limit" argument controls number of request allowed in a time window.
-- time "window" argument controls the time window in seconds.
function _M.new(dict_name, limit, window)
    local dict = ngx_shared[dict_name]
    if not dict then
        return nil, "shared dict not found"
    end

    assert(limit > 0 and window > 0)

    local self = {
        dict = dict,
        limit = limit,
        window = window,
    }

    return setmetatable(self, mt)
end

-- 流量进入
-- key 请求唯一标记，如ip,uid,业务方id
-- commit 是否提交计入计数器
function _M.incoming(self, key, commit)
    local dict = self.dict -- ngx_lua内存共享字典
    local limit = self.limit -- 限制的访问次数
    local window = self.window -- 限制的时间窗口（单位：秒）

    local remaining, ok, err

    if commit then
        -- 每次提交请求，次数 -1 
        remaining, err = dict:incr(key, -1, limit)
        -- 扣除失败，返回错误
        if not remaining then
            return nil, err
        end
        
        -- 剩余次数 == 总限制数 - 1, 即时间段内第一个请求进来
        if remaining == limit - 1 then
            -- 设置共享字典变量的过期时间
            ok, err = dict:expire(key, window)
            if not ok then
                -- 写入过程失败，共享字典变量已经不存在，如：过期，则重新开启一个计数窗口
                if err == "not found" then
                    remaining, err = dict:incr(key, -1, limit)
                    if not remaining then
                        return nil, err
                    end

                    ok, err = dict:expire(key, window)
                    if not ok then
                        return nil, err
                    end

                else
                    return nil, err
                end
            end
        end

    else
        remaining = (dict:get(key) or limit) - 1
    end

    if remaining < 0 then
        return nil, "rejected"
    end

    return 0, remaining
end


-- uncommit remaining and return remaining value
function _M.uncommit(self, key)
    assert(key)
    local dict = self.dict
    local limit = self.limit

    local remaining, err = dict:incr(key, 1)
    if not remaining then
        if err == "not found" then
            remaining = limit
        else
            return nil, err
        end
    end

    return remaining
end


return _M
