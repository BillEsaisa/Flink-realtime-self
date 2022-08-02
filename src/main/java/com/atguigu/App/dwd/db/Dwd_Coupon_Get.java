package com.atguigu.App.dwd.db;

import com.atguigu.Utils.KafkaUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:Dwd_Coupon_use
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/25 23:18
 */


public class Dwd_Coupon_Get {
    public static void main(String[] args) {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //消费kafka db数据，取出coupon_use表数据
        String Topic="topic_db";
        String GroupId ="Dwd_Coupon_use_0212";
        tableEnv.executeSql(" CREATE TABLE db_table (  " +
                "  `database` STRING,  " +
                "  `table` STRING,  " +
                "  `type` STRING,  " +
                "  `ts` BIGINT,  " +
                "  `pt` as PROCTIME(),  " +
                "  `data` Map<STRING,STRING>,  " +
                "  `old` Map<STRING,STRING>  " +
                ") " + KafkaUtils.getKafkaDDL(Topic,GroupId));

        Table coupon_use_table = tableEnv.sqlQuery(" SELECT  " +
                "`data`['id'] id, " +
                "`data`['coupon_id'] coupon_id, " +
                "`data`['user_id'] user_id, " +
                "`data`['order_id'] order_id, " +
                "`data`['coupon_status'] coupon_status, " +
                "`data`['create_time'] create_time, " +
                "`data`['get_time'] get_time, " +
                "`data`['using_time'] using_time, " +
                "`data`['used_time'] used_time, " +
                "`data`['expire_time'] expire_time, " +
                "`pt` " +
                "from  db_table " +
                "where `database`='gmall'  " +
                "and `table`='coupon_use'  " +
                "and `type`='insert'  ");
        tableEnv.createTemporaryView("coupon_use_table",coupon_use_table);



        //写入kafka
        String topic="Dwd_Coupon_use";
        tableEnv.executeSql(" CREATE TABLE coupon_table ( " +
                "`id` STRING, " +
                "`coupon_id` STRING, " +
                "`user_id` STRING, " +
                "`order_id` STRING, " +
                "`coupon_status` STRING, " +
                "`create_time` STRING, " +
                "`get_time` STRING, " +
                "`using_time` STRING, " +
                "`used_time` STRING, " +
                "`expire_time` STRING, " +
                "`pt` STRING " +
                ") " +KafkaUtils.getKafkaDDL(topic));
        tableEnv.executeSql(" insert into  coupon_table select * from coupon_use_table");


    }
}
