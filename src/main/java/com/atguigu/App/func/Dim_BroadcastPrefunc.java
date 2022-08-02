package com.atguigu.App.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.DruidDSUtil;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.Gmallconfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * @PackageName:com.atguigu.App.func
 * @ClassNmae:Dim_BroadcastPrefunc
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/19 19:30
 */


public class Dim_BroadcastPrefunc extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private  MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    private DruidDataSource druidDataSource;

    public Dim_BroadcastPrefunc(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.creatdruiddataSource();
    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        //封装数据  javabean //Value:{"before":null,"after":{"id":6,"tm_name":"长粒香","logo_url":"/static/default.jpg"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1658192885916,"snapshot":"false","db":"gmall-211227-flink","sequence":null,"table":"base_trademark","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1658192885916,"transaction":null}
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess afterdata = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);


        //检查并建表

        chcektable(afterdata.getSinkTable(), afterdata.getSinkColumns(), afterdata.getSinkPk(), afterdata.getSinkExtend());


        //写入状态,广播
        String key = afterdata.getSourceTable();
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(key, afterdata);


    }

    private void chcektable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        DruidPooledConnection connection = null;
        try {
            //对主键和扩展字段为null的情况作出处理
            if (sinkPk == null) {
                sinkPk = "id";

            }
            if (sinkExtend == null) {
                sinkExtend = "";

            }


            //CREATE table if not exist db.tn (id varchar primary key ,name varchar,ts varchar)
            //创建sql
            StringBuilder sql = new StringBuilder("CREATE table if not exist " + Gmallconfig.HBASE_SCHEMA + "." + sinkTable + "(");
            String[] split = sinkColumns.split(",");
            //遍历数组
            for (int i = 0; i < split.length; i++) {
                if (split[i].equals(sinkPk)) {
                    sql.append(split[i] + " varchar primary key");
                } else {
                    sql.append(split[i] + " varchar");
                }
                if (i < split.length - 1) {
                    sql.append(",");

                }
            }
            sql.append(")").append(sinkExtend);


            //预编译sql

            connection = druidDataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            //关闭资源
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

        }


    }


    @Override
    //value:{"database":"gmall","table":"cart_info","type":"update","ts":1592270938,"xid":13090,"xoffset":1573,"data":{"id":100924,"u
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        String table = value.getString("table");
        //取出广播状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(table);

        if (tableProcess != null && ("bootstrap-insert".equals(value.getString("type")) || "update".equals(value.getString("type")) || "insert".equals(value.getString("type")))) {
            //过滤列字段（主流中的字段可能和写入phoenix的字段数不相同）
            flitercolumns(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //为value添加表名字段，方便后续写入phoenix写sql语句
            value.put("tablename", tableProcess.getSinkTable());
            out.collect(value);


        }


    }

    private void flitercolumns(JSONObject data, String sinkColumns) {
        Set<String> keySet = data.keySet();
        String[] split = sinkColumns.split(",");
        List<String> list = Arrays.asList(split);
        for (String s : keySet) {
            if (!list.contains(s)) {
                data.remove(s);
            }
        }

    }


}
