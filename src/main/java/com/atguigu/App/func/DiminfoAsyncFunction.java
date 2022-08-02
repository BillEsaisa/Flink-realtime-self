package com.atguigu.App.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.DimInfoUtil;
import com.atguigu.Utils.DruidDSUtil;
import com.atguigu.Utils.JedisUtil;
import com.atguigu.Utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @PackageName:com.atguigu.App.func
 * @ClassNmae:DiminfoFuncction
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/31 18:41
 */


public  abstract class DiminfoAsyncFunction<T> extends  RichAsyncFunction<T,T> implements DimAsyncJoinFunction<T> {
    private DruidDataSource druidDataSource;
    private JedisPool jedisPool;
    private ThreadPoolExecutor threadPoolExecutor;

    private String TableName;

    public DiminfoAsyncFunction(String tableName) {
        TableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化连接池，线程池
        druidDataSource= DruidDSUtil.creatdruiddataSource();
        jedisPool= JedisUtil.JedisPoolInit();
        threadPoolExecutor= ThreadPoolUtil.getThreadPoolExcutor();

    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut>>>>>>>>>>>>");
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                //获取连接
                DruidPooledConnection connection=null;
                Jedis jedis = jedisPool.getResource();
                try {
                    connection = druidDataSource.getConnection();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

                //获取diminfo
                String id=getid(input);
                JSONObject dimInfo = DimInfoUtil.getDimInfo(jedis, connection, TableName, id);

                //补充字段
                if (dimInfo!=null){
                    join(input,dimInfo);
                }


                //归还资源
                jedis.close();
                connection.close();


                //返回结果
                resultFuture.complete(Collections.singletonList(input));


            }
        });



    }


}
