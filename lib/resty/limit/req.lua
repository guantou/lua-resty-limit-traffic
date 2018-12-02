-- Copyright (C) Yichun Zhang (agentzh)
--
-- This library is an approximate Lua port of the standard ngx_limit_req
-- module.


local ffi = require "ffi"
local math = require "math"


local ngx_shared = ngx.shared
local ngx_now = ngx.now
local setmetatable = setmetatable
local ffi_cast = ffi.cast
local ffi_str = ffi.string
local abs = math.abs
local tonumber = tonumber
local type = type
local assert = assert
local max = math.max


-- TODO: we could avoid the tricky FFI cdata when lua_shared_dict supports
-- hash-typed values as in redis.
ffi.cdef[[
    struct lua_resty_limit_req_rec {
        unsigned long        excess; // 已
        uint64_t             last;  /* time in milliseconds */
        /* integer value, 1 corresponds to 0.001 r/s */
    };
]]
local const_rec_ptr_type = ffi.typeof("const struct lua_resty_limit_req_rec*")
local rec_size = ffi.sizeof("struct lua_resty_limit_req_rec")

-- we can share the cdata here since we only need it temporarily for
-- serialization inside the shared dict:
local rec_cdata = ffi.new("struct lua_resty_limit_req_rec")


local _M = {
    _VERSION = '0.06'
}


local mt = {
    __index = _M
}

-- 实例化一个限流器
-- rate 每秒接受的请求数（滴水速度）
-- burst 每秒允许堆积的请求数（桶容量）
function _M.new(dict_name, rate, burst)
    -- ngx 共享内存字典变量
    local dict = ngx_shared[dict_name]
    if not dict then
        return nil, "shared dict not found"
    end

    assert(rate > 0 and burst >= 0)

    local self = {
        dict = dict,
        rate = rate * 1000, -- 一秒允许访问的请求数（滴水速度）
        burst = burst * 1000, -- 一秒允许堆积的请求数（桶容量）
    }

    return setmetatable(self, mt)
end


-- sees an new incoming event
-- the "commit" argument controls whether should we record the event in shm.
-- FIXME we have a (small) race-condition window between dict:get() and
-- dict:set() across multiple nginx worker processes. The size of the
-- window is proportional to the number of workers.
-- 请求提交进来，计算其需要等待的时间，是否需要被拒绝
-- commit：是否需要将此次请求记录到统计里
function _M.incoming(self, key, commit)
    local dict = self.dict
    local rate = self.rate
    --精确到毫秒的时间戳
    local now = ngx_now() * 1000

    local excess

    -- it's important to anchor the string value for the read-only pointer
    -- cdata:
    -- 处处当前应用的计数器
    local v = dict:get(key)
    if v then
        -- 验证已存储的数据格式
        if type(v) ~= "string" or #v ~= rec_size then
            return nil, "shdict abused by other users"
        end
        -- 以字符串v创建一个const_rec_ptr_type类型的变量
        local rec = ffi_cast(const_rec_ptr_type, v)
        -- 取出上次统计的时间,计算时差
        local elapsed = now - tonumber(rec.last)

        -- print("elapsed: ", elapsed, "ms")

        -- we do not handle changing rate values specifically. the excess value
        -- can get automatically adjusted by the following formula with new rate
        -- values rather quickly anyway.
        -- 可用请求数空间（桶剩余容量） = max(上次统计时已接受的请求数 - 每秒限制访问的请求数 * 上次统计时差/1000 + 1000, 0)
        -- 可用请求数空间（桶剩余容量）N = max(上次统计时已占用容量 - 这段时间腾出的空间 + 1000, 0)
        -- 如果N大于0，说明还可以接收 N个请求
        -- 如果N小于0，说明桶满了
        excess = max(tonumber(rec.excess) - rate * abs(elapsed) / 1000 + 1000,
                     0)

        -- print("excess: ", excess)
        -- 桶已满，拒绝访问
        if excess > self.burst then
            return nil, "rejected"
        end

    else
        -- 首次访问，直接放行
        excess = 0
    end

    -- 放行的请求，如果需要统计本次请求，则更新计数器
    if commit then
        rec_cdata.excess = excess
        rec_cdata.last = now
        dict:set(key, ffi_str(rec_cdata, rec_size))
    end

    -- return the delay in seconds, as well as excess
    -- 返回延迟的时间，
    return excess / rate, excess / 1000
end


function _M.uncommit(self, key)
    assert(key)
    local dict = self.dict

    local v = dict:get(key)
    if not v then
        return nil, "not found"
    end

    if type(v) ~= "string" or #v ~= rec_size then
        return nil, "shdict abused by other users"
    end

    local rec = ffi_cast(const_rec_ptr_type, v)

    local excess = max(tonumber(rec.excess) - 1000, 0)

    rec_cdata.excess = excess
    rec_cdata.last = rec.last
    dict:set(key, ffi_str(rec_cdata, rec_size))
    return true
end


function _M.set_rate(self, rate)
    self.rate = rate * 1000
end


function _M.set_burst(self, burst)
    self.burst = burst * 1000
end


return _M
