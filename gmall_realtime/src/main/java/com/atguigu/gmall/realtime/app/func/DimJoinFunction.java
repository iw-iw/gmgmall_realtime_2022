package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.app.func
 * @date 2022/8/30 14:07
 */
public interface DimJoinFunction<T> {
    public abstract String getKey(T input);
    public abstract void join(T input, JSONObject obj);
}
