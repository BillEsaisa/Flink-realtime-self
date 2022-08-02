package com.atguigu.App.dwd.db;

import com.atguigu.Utils.KafkaUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:Dwd_FavorAdd
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/25 23:49
 */


public class Dwd_FavorAdd {
    public static void main(String[] args) {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //消费kafka db数据，取出 favor_info 表数据
        String Topic="topic_db";
        String GroupId ="Dwd_FavorAdd_0212";
        tableEnv.executeSql(" CREATE TABLE db_table (  " +
                "  `database` STRING,  " +
                "  `table` STRING,  " +
                "  `type` STRING,  " +
                "  `ts` BIGINT,  " +
                "  `pt` as PROCTIME(),  " +
                "  `data` Map<STRING,STRING>,  " +
                "  `old` Map<STRING,STRING>  " +
                ") " + KafkaUtils.getKafkaDDL(Topic,GroupId));


        Table favor_info_table = tableEnv.sqlQuery(" SELECT  " +
                "`data`['id'] id, " +
                "`data`['user_id'] user_id, " +
                "`data`['sku_id'] sku_id, " +
                "`data`['spu_id'] spu_id, " +
                "`data`['is_cancel'] is_cancel, " +
                "`data`['create_time'] create_time, " +
                "`data`['cancel_time'] cancel_time, " +
                "`pt` " +
                "from  db_table " +
                "where `database`='gmall'  " +
                "and `table`='favor_info'  " +
                "and `type`='insert' ");
        tableEnv.createTemporaryView("favor_info_table",favor_info_table);

        //写入kafka
        String topic="Dwd_FavorAdd";
        tableEnv.executeSql(" CREATE TABLE addfavor ( " +
                "`id` STRING, " +
                "`user_id` STRING, " +
                "`sku_id` STRING, " +
                "`spu_id` STRING, " +
                "`is_cancel` STRING, " +
                "`create_time` STRING, " +
                "`cancel_time` STRING, " +
                "`pt` STRING " +
                ") " +KafkaUtils.getKafkaDDL(topic));
        tableEnv.executeSql(" insert into addfavor select * from  favor_info_table");
    }
}
