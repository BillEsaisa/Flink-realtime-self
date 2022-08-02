package com.atguigu.App.dwd.db;

import com.atguigu.Utils.KafkaUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:Dwd_User_Info
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/26 0:27
 */


public class Dwd_User_Info {
    public static void main(String[] args) {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);



        //消费kafka db数据，取出 user_info 表数据


        String Topic="topic_db";
        String GroupId ="Dwd_User_Info_0212";
        tableEnv.executeSql(" CREATE TABLE db_table (  " +
                "  `database` STRING,  " +
                "  `table` STRING,  " +
                "  `type` STRING,  " +
                "  `ts` BIGINT,  " +
                "  `pt` as PROCTIME(),  " +
                "  `data` Map<STRING,STRING>,  " +
                "  `old` Map<STRING,STRING>  " +
                ") " + KafkaUtils.getKafkaDDL(Topic,GroupId));



        Table user_info = tableEnv.sqlQuery(" SELECT " +
                "`data`['id']  id, " +
                "`data`['login_name']  login_name, " +
                "`data`['nick_name']  nick_name, " +
                "`data`['passwd']  passwd, " +
                "`data`['name']  name, " +
                "`data`['phone_num']  phone_num, " +
                "`data`['email']  email, " +
                "`data`['head_img']  head_img, " +
                "`data`['user_level']  user_level, " +
                "`data`['birthday']  birthday, " +
                "`data`['gender']  gender, " +
                "`data`['create_time']  create_time, " +
                "`data`['operate_time']  operate_time, " +
                "`data`['status']  status, " +
                "`data`['pt']   pt " +
                "from  db_table " +
                "where `database`='gmall'  " +
                "and `table`='user_info'  " +
                "and `type`='insert'  ");
        tableEnv.createTemporaryView("user_info",user_info);

        //写入kafka
        String topic ="Dwd_User_Info";
        tableEnv.executeSql(" CREATE TABLE adduser ( " +
                "`id` STRING, " +
                "`login_name` STRING, " +
                "`nick_name` STRING, " +
                "`passwd` STRING, " +
                "`name` STRING, " +
                "`phone_num` STRING, " +
                "`email` STRING, " +
                "`head_img` STRING, " +
                "`user_level` STRING, " +
                "`birthday` STRING, " +
                "`gender` STRING, " +
                "`create_time` STRING, " +
                "`operate_time` STRING, " +
                "`status` STRING, " +
                "`pt` STRING " +
                ") " +KafkaUtils.getKafkaDDL(topic));
        tableEnv.executeSql(" insert into adduser select * from user_info ");
    }
}

