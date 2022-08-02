package com.atguigu.App.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.DimInfoUtil;
import com.atguigu.Utils.DruidDSUtil;
import com.atguigu.Utils.JedisUtil;
import com.atguigu.common.Gmallconfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

/**
 * @PackageName:com.atguigu.App.func
 * @ClassNmae:PhoenixRichFunc
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/19 21:39
 */


public class PhoenixRichFunc extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource;
    private DruidPooledConnection connection =null;
    private PreparedStatement preparedStatement=null;
    @Override
    public void open(Configuration parameters) throws Exception {
    druidDataSource= DruidDSUtil.creatdruiddataSource();
    }

    @Override
    //upsert into db.tn(id,nm.ts) values('a','b','c')
    public void invoke(JSONObject value, Context context) throws Exception {
        String tn = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        Set<String> keys = data.keySet();
        String join = StringUtils.join(keys, ",");
        Collection<Object> values = data.values();
        String join2 = StringUtils.join(values, "','");

        //创建sql
        StringBuilder sql = new StringBuilder("upsert into ");
        sql.append(Gmallconfig.HBASE_SCHEMA)
                .append(".")
                .append(tn)
                .append("(")
                .append(join)
                .append(")")
                .append(" values ('")
                .append(join2)
                .append("')");


        //删除redis中缓存（维表发生改变）
        if ("update".equals(value.getString("type"))){
            Jedis jedis = JedisUtil.getjedis();
            DimInfoUtil.delrediscache(jedis,tn.toUpperCase(), data.getString("id"));


        }

        //预编译sql
        connection = druidDataSource.getConnection();
        preparedStatement= connection.prepareStatement(sql.toString());

        //执行
        preparedStatement.execute();


        //dml语句手动提交
        connection.commit();

        //关闭资源

        if (connection!=null){
            connection.close();
        }
        if (preparedStatement!=null){
            preparedStatement.close();

        }

    }
}
