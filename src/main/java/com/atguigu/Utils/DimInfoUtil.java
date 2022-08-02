package com.atguigu.Utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;


/**
 * @PackageName:com.atguigu.Utils
 * @ClassNmae:DimInfoUtil
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/30 22:27
 */


public class DimInfoUtil {

    public static JSONObject getDimInfo(Jedis jedis, Connection connection, String TableName, String Id) throws Exception {
        //从redis 读取数据
        String RedisKey="DIM_"+TableName+":"+Id;
        String diminfo = jedis.get(RedisKey);
        if (diminfo!=null){
            //缓存中的数据被读，重置过期时间
            jedis.expire(RedisKey,24*60*60);
            return JSON.parseObject(diminfo);

        }



        //从Hbase读取数据
        //拼接sql  select * from t where id='13';
        String Sql="select * from " +TableName +" where id ='"+Id+"'";

        List<JSONObject> queryList = JDBCUtil.QueryList(connection, Sql, JSONObject.class, false);
        JSONObject jsonObject = queryList.get(0);
        //将数据写入到redis
        jedis.set(RedisKey,jsonObject.toJSONString());
        //返回结果
        return jsonObject;


    }

    //删除缓存
    public static void delrediscache(Jedis jedis ,String TableName, String Id){
        String key="DIM_"+TableName+":"+Id;
        jedis.del(key);

    }
}
