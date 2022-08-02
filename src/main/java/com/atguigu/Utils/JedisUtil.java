package com.atguigu.Utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @PackageName:com.atguigu.Utils
 * @ClassNmae:JedisUtil
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/30 20:57
 *
 * jedis工具类
 */


public class JedisUtil {
    private  static JedisPool jedisPool;
    //初始化Jedispool
    public static JedisPool JedisPoolInit(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxWaitMillis(100L);
        jedisPoolConfig.setMaxTotal(5);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000L);
        jedisPoolConfig.setTestOnBorrow(true);


        jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 6379, 10000);
        return jedisPool;

    }


    //获取Jedis连接方法

    public static Jedis getjedis(){
        if (jedisPool==null){
            //初始化连接池
            JedisPoolInit();
        }
        //获取jedis连接
        Jedis jedis = jedisPool.getResource();

        return jedis;
    }





}
