package com.atguigu.App.dwd.db;

import com.atguigu.Utils.KafkaUtils;
import com.atguigu.Utils.MysqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:Dwd_CommentAdd
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/26 0:02
 */


public class Dwd_CommentAdd {
    public static void main(String[] args) {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //消费kafka db数据，取出 favor_info 表数据
        String Topic="topic_db";
        String GroupId ="Dwd_CommentAdd_0212";
        tableEnv.executeSql(" CREATE TABLE db_table (  " +
                "  `database` STRING,  " +
                "  `table` STRING,  " +
                "  `type` STRING,  " +
                "  `ts` BIGINT,  " +
                "  `pt` as PROCTIME(),  " +
                "  `data` Map<STRING,STRING>,  " +
                "  `old` Map<STRING,STRING>  " +
                ") " + KafkaUtils.getKafkaDDL(Topic,GroupId));

        Table comment_info = tableEnv.sqlQuery(" SELECT  " +
                "`data`['id'] id, " +
                "`data`['user_id'] user_id, " +
                "`data`['nick_name'] nick_name, " +
                "`data`['head_img'] head_img, " +
                "`data`['sku_id'] sku_id, " +
                "`data`['spu_id'] spu_id, " +
                "`data`['order_id'] order_id, " +
                "`data`['appraise'] appraise, " +
                "`data`['comment_txt'] comment_txt, " +
                "`data`['create_time'] create_time, " +
                "`data`['operate_time'] operate_time, " +
                "`pt` " +
                "from  db_table " +
                "where `database`='gmall'  " +
                "and `table`='comment_info'  " +
                "and `type`='insert' ");
        tableEnv.createTemporaryView("comment_info",comment_info);

        //lookup 功能从mysql 读取字典表
        Table base_dic = tableEnv.sqlQuery(MysqlUtils.getTable_frommysql());
        tableEnv.createTemporaryView("base_dic",base_dic);


        //两表关联
        Table jointable = tableEnv.sqlQuery(" select " +
                "ci.id " +
                "ci.user_id, " +
                "ci.nick_name, " +
                "ci.head_img, " +
                "ci.sku_id, " +
                "ci.spu_id, " +
                "ci.order_id, " +
                "ci.appraise, " +
                "ci.comment_txt, " +
                "ci.create_time, " +
                "ci.operate_time, " +
                "bd.dic_name appraise_name " +
                "from comment_info ci " +
                "join base_dic  " +
                "FOR SYSTEM_TIME AS OF ci.pt AS bd " +
                "on ci.appraise=bd.dic_code ");
        tableEnv.createTemporaryView("jointable",jointable);

        //写入kafka
        String topic="Dwd_CommentAdd";
        tableEnv.executeSql(" CREATE TABLE addcomment ( " +
                "`id` STRING, " +
                "`user_id` STRING, " +
                "`nick_name` STRING, " +
                "`head_img` STRING, " +
                "`sku_id` STRING, " +
                "`spu_id` STRING, " +
                "`order_id` STRING, " +
                "`appraise` STRING, " +
                "`comment_txt` STRING, " +
                "`create_time` STRING, " +
                "`operate_time` STRING, " +
                "`appraise_name` STRING " +
                ") " +KafkaUtils.getKafkaDDL(topic));
        tableEnv.executeSql(" insert into addcomment select * from  jointable");
    }
}
