package com.atguigu.App.dwd.db;

import com.atguigu.Utils.KafkaUtils;
import com.atguigu.Utils.MysqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:Dwd_RefundSucess2
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/25 21:25
 */


public class Dwd_RefundSucess2 {
    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 从Kafka读取db主题数据
        String Topic="topic_db";
        String GroupId="Dwd_RefundSucess2_0212";
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` map<string, string>, " +
                "`pt` as PROCTIME(), " +
                "`ts` string " +
                ")" + KafkaUtils.getKafkaDDL(Topic, GroupId));

        //  读取退款表数据，并筛选退款成功数据
        Table refund_payment_table = tableEnv.sqlQuery("select " +
                        "data['id'] id, " +
                        "data['order_id'] order_id, " +
                        "data['sku_id'] sku_id, " +
                        "data['payment_type'] payment_type, " +
                        "data['callback_time'] callback_time, " +
                        "data['total_amount'] total_amount, " +
                        "pt, " +
                        "ts " +
                        "from topic_db " +
                        "where `table` = 'refund_payment' "
                + "and `type` = 'update' " +
                "and data['refund_status'] = '0705' " +
                "and `old`['refund_status'] is not null"
        );

        tableEnv.createTemporaryView("refund_payment", refund_payment_table);

        // 读取订单表数据，过滤退款成功数据
        Table order_info_table = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['province_id'] province_id, " +
                "`old` " +
                "from topic_db " +
                "where `table` = 'order_info' " +
                "and `type` = 'update' " +
                "and data['order_status']='1006' " +
                "and `old`['order_status'] is not null"
        );

        tableEnv.createTemporaryView("order_info", order_info_table);

        //  读取退单表数据，过滤退款成功数据
        Table order_refund_info_table = tableEnv.sqlQuery("select " +
                        "data['order_id'] order_id, " +
                        "data['sku_id'] sku_id, " +
                        "data['refund_num'] refund_num, " +
                        "`old` " +
                        "from topic_db " +
                        "where `table` = 'order_refund_info' " +
                        "and `type` = 'update' " +
                        "and data['refund_status']='0705' " +
                        "and `old`['refund_status'] is not null"
        );
        tableEnv.createTemporaryView("order_refund_info", order_refund_info_table);

        //  lookup 从mysql获取字典表
        tableEnv.executeSql(MysqlUtils.getTable_frommysql());


        //  四表关联
        Table resultTable = tableEnv.sqlQuery("select " +
                "rp.id, " +
                "rp.order_id, " +
                "rp.sku_id, " +
                "rp.payment_type, " +
                "rp.callback_time, " +
                "rp.callback_time, " +
                "rp.total_amount, " +
                "rp.ts, " +
                "ri.refund_num, " +
                "oi.user_id, " +
                "oi.province_id, " +
                "dic.dic_name payment_type_name " +
                "from refund_payment rp  " +
                "join  " +
                "order_info oi " +
                "on rp.order_id = oi.id " +
                "join " +
                "order_refund_info ri " +
                "on rp.order_id = ri.order_id " +
                "and rp.sku_id = ri.sku_id " +
                "join  " +
                "base_dic for system_time as of rp.pt as dic " +
                "on rp.payment_type = dic.dic_code ");
        tableEnv.createTemporaryView("result_table", resultTable);

        //  写入kafka相应主题
        String topic="Dwd_RefundSucess2";
        tableEnv.executeSql("create table dwd_trade_refund_pay_suc( " +
                "id string, " +
                "user_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "payment_type_code string, " +
                "payment_type_name string, " +
                "date_id string, " +
                "callback_time string, " +
                "refund_num string, " +
                "refund_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + KafkaUtils.getKafkaDDL(topic));

        tableEnv.executeSql(" insert into dwd_trade_refund_pay_suc select * from result_table ");
    }


}
