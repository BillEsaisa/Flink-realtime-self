package com.atguigu.App.func;

import com.alibaba.fastjson.JSONObject;

public interface DimAsyncJoinFunction<T> {
     void join(T input, JSONObject dimInfo);

     String getid(T input);
}
