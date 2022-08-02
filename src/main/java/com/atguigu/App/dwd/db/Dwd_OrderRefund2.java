package com.atguigu.App.dwd.db;
import com.atguigu.Utils.KafkaUtils;
import com.atguigu.Utils.MysqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:Dwd_OrderRefund2
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/25 21:25
 */


public class Dwd_OrderRefund2 {
    public static void main(String[] args) throws Exception {

        //  创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //  从 Kafka读取主题db数据
        String Topic="topic_db";
        String Groupid="Dwd_OrderRefund2_0212";
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` map<string, string>, " +
                "`pt` as PROCTIME(), " +
                "`ts` string " +
                ")" + KafkaUtils.getKafkaDDL(Topic,Groupid));

        // 过滤退单表数据
        Table orderRefundInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['sku_id'] sku_id, " +
                "data['refund_type'] refund_type, " +
                "data['refund_num'] refund_num, " +
                "data['refund_amount'] refund_amount, " +
                "data['refund_reason_type'] refund_reason_type, " +
                "data['refund_reason_txt'] refund_reason_txt, " +
                "data['create_time'] create_time, " +
                "pt, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'order_refund_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_refund", orderRefundInfo);



        //  读取db主题数据，筛选出order_info数据，并过滤出退单的部分
        Table orderInfo_Refund = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['province_id'] province_id, " +
                "`old` " +
                "from topic_db " +
                "where `table` = 'order_info' " +
                "and `type` = 'update' " +
                "and data['order_status']='1005' " +
                "and `old`['order_status'] is not null");

        tableEnv.createTemporaryView("order_info", orderInfo_Refund);

        // lookup 功能提取字典表
        tableEnv.executeSql(MysqlUtils.getTable_frommysql());

        // 关联表
        Table resultTable = tableEnv.sqlQuery("select  " +
                "or.id, " +
                "or.user_id, " +
                "or.order_id, " +
                "or.sku_id, " +
                "or.province_id, " +
                "or.create_time, " +
                "or.refund_type, " +
                "base_dic.dic_name refund_type_name , " +
                "or.refund_reason_type, " +
                "base_dic2.dic_name refund_reason_type_name, " +
                "or.refund_reason_txt, " +
                "or.refund_num, " +
                "or.refund_amount, " +
                "or.ts, " +
                "from order_refund or " +
                "join  " +
                "order_info oi " +
                "on or.order_id = oi.id " +
                "join  " +
                "base_dic for system_time as of or.pt  " +
                "on or.refund_type = base_dic.dic_code " +
                "join " +
                "base_dic for system_time as of or.pt as base_dic2 " +
                "on or.refund_reason_type=base_dic2.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // 写入kafka相应主题
        String topic="Dwd_OrderRefund2";
        tableEnv.executeSql("create table dwd_trade_order_refund( " +
                "id string, " +
                "user_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "date_id string, " +
                "create_time string, " +
                "refund_type_code string, " +
                "refund_type_name string, " +
                "refund_reason_type_code string, " +
                "refund_reason_type_name string, " +
                "refund_reason_txt string, " +
                "refund_num string, " +
                "refund_amount string, " +
                "ts string, " +
                ")" +KafkaUtils.getKafkaDDL(topic));
        tableEnv.executeSql(" insert into dwd_trade_order_refund  select * from result_table ");
    }
}
