package com.atguigu.App.dwd.db;

import com.atguigu.Utils.KafkaUtils;
import com.atguigu.Utils.MysqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:Dwd_RefundSucess
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/24 23:50
 */


public class Dwd_RefundSucess {
    public static void main(String[] args) {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 消费kafka db数据，过滤出 refund_payment表数据
        String topic ="topic_db";
        String groupid="Dwd_RefundSucess";
        tableEnv.executeSql(" CREATE TABLE db_table ( " +
                "  `database` STRING, " +
                "  `table` STRING, " +
                "  `type` STRING, " +
                "  `ts` BIGINT, " +
                "  `pt` as PROCTIME(), " +
                "  `data` Map<STRING,STRING>, " +
                "  `old` Map<STRING,STRING> " +
                ") " + KafkaUtils.getKafkaDDL(topic,groupid));

        Table refund_pay = tableEnv.sqlQuery(" SELECT  " +
                "`data`['id'] id, " +
                "`data`['out_trade_no'] out_trade_no, " +
                "`data`['order_id'] order_id, " +
                "`data`['sku_id'] sku_id, " +
                "`data`['payment_type'] payment_type, " +
                "`data`['trade_no'] trade_no, " +
                "`data`['total_amount'] total_amount, " +
                "`data`['subject'] subject, " +
                "`data`['refund_status'] refund_status, " +
                "`data`['create_time'] create_time, " +
                "`data`['callback_time'] callback_time, " +
                "`data`['callback_content'] callback_content " +
                "`pt`" +
                "from  db_table " +
                "where `database`='gmall'  " +
                "and `table`='refund_payment'  " +
                "and `type`='update' " +
                "and `data`['refund_status']='0705' " +
                "and `old`['refund_status'] is  not null  ");
        tableEnv.createTemporaryView("refund_pay",refund_pay);



        //从kafka Dwd_OrderRefund主题数据
        String Topic="Dwd_OrderRefund";
        String GroupId="Dwd_RefundSucess";
        tableEnv.executeSql( " CREATE TABLE refundordertable (  " +
                "`user_id` STRING,  " +
                "`order_id` STRING,  " +
                "`sku_id` STRING,  " +
                "`refund_type` STRING,  " +
                "`refund_num` STRING,  " +
                "`refund_amount` STRING,  " +
                "`refund_reason_type` STRING,  " +
                "`refund_reason_txt` STRING,  " +
                "`refund_status` STRING,  " +
                "`create_time` STRING,  " +
                "`type` STRING,  " +
                "`old` MAP<STRING,STRING>,  " +
                "`id` STRING,  " +
                "`order_id` STRING,  " +
                "`sku_id` STRING,  " +
                "`sku_name` STRING,  " +
                "`order_price` STRING,  " +
                "`sku_num` STRING,  " +
                "`create_time` STRING,  " +
                "`source_type` STRING,  " +
                "`source_id` STRING,  " +
                "`split_total_amount` STRING,  " +
                "`split_activity_amount` STRING,  " +
                "`split_coupon_amount` STRING,  " +
                "`consignee` STRING,  " +
                "`consignee_tel` STRING,  " +
                "`total_amount` STRING,  " +
                "`order_status` STRING,  " +
                "`user_id` STRING,  " +
                "`payment_way` STRING,  " +
                "`delivery_address` STRING,  " +
                "`order_comment` STRING,  " +
                "`out_trade_no` STRING,  " +
                "`trade_body` STRING,  " +
                "`operate_time` STRING,  " +
                "`expire_time` STRING,  " +
                "`process_status` STRING,  " +
                "`tracking_no` STRING,  " +
                "`parent_order_id` STRING,  " +
                "`province_id` STRING,  " +
                "`activity_reduce_amount` STRING,  " +
                "`coupon_reduce_amount` STRING,  " +
                "`original_total_amount` STRING,  " +
                "`feight_fee` STRING,  " +
                "`feight_fee_reduce` STRING,  " +
                "`refundable_time` STRING,  " +
                "`activity_id` STRING,  " +
                "`activity_rule_id` STRING,  " +
                "`coupon_id` STRING,  " +
                "`coupon_use_id` STRING,  " +
                "`sourcename` STRING,  " +
                "`refund_status_name` STRING  " +
                " primary key(id) not enforced " +
                ") " +KafkaUtils.getKafkaDDL(Topic,GroupId));


        //lookup 功能获取字典表
        Table base_dic = tableEnv.sqlQuery(MysqlUtils.getTable_frommysql());
        tableEnv.createTemporaryView("base_dic",base_dic);

        // 关联表
        Table RefundSucess = tableEnv.sqlQuery(" select " +
                "ro.user_id " +
                "ro.order_id, " +
                "ro.sku_id, " +
                "ro.refund_type, " +
                "ro.refund_num, " +
                "ro.refund_amount, " +
                "ro.refund_reason_type, " +
                "ro.refund_reason_txt, " +
                "ro.create_time, " +
                "ro.id, " +
                "ro.order_id, " +
                "ro.sku_id, " +
                "ro.sku_name, " +
                "ro.order_price, " +
                "ro.sku_num, " +
                "ro.create_time, " +
                "ro.source_type, " +
                "ro.source_id, " +
                "ro.split_total_amount, " +
                "ro.split_activity_amount, " +
                "ro.split_coupon_amount, " +
                "ro.consignee, " +
                "ro.consignee_tel, " +
                "ro.total_amount, " +
                "ro.order_status, " +
                "ro.user_id, " +
                "ro.payment_way, " +
                "ro.delivery_address, " +
                "ro.order_comment, " +
                "ro.out_trade_no, " +
                "ro.trade_body, " +
                "ro.operate_time, " +
                "ro.expire_time, " +
                "ro.process_status, " +
                "ro.tracking_no, " +
                "ro.parent_order_id, " +
                "ro.province_id, " +
                "ro.activity_reduce_amount, " +
                "ro.coupon_reduce_amount, " +
                "ro.original_total_amount, " +
                "ro.feight_fee, " +
                "ro.feight_fee_reduce, " +
                "ro.refundable_time, " +
                "ro.activity_id, " +
                "ro.activity_rule_id, " +
                "ro.coupon_id, " +
                "ro.coupon_use_id, " +
                "ro.sourcename, " +
                "ro.refund_status_name, " +
                "rp.out_trade_no, " +
                "rp.payment_type, " +
                "rp.trade_no, " +
                "rp.total_amount, " +
                "rp.subject, " +
                "rp.refund_status, " +
                "rp.callback_time, " +
                "rp.pt, " +
                "rp.callback_content, " +
                "base_dic.dic_name payment_type_name " +
                "from refund_pay rp " +
                "join refundordertable ro " +
                "on (rp.order_id=ro.order_id and rp.sku_id=ro.sku_id) " +
                "join base_dic " +
                "for system_time as of rp.pt " +
                "on rp.payment_type=base_dic.dic_code ");
        tableEnv.createTemporaryView("RefundSucess",RefundSucess);

        //写入kafka
        String SinkTopic="Dwd_RefundSucess";
        tableEnv.executeSql(" CREATE TABLE refundordersucess_table ( " +
                "`user_id` STRING, " +
                "`order_id` STRING, " +
                "`sku_id` STRING, " +
                "`refund_type` STRING, " +
                "`refund_num` STRING, " +
                "`refund_amount` STRING, " +
                "`refund_reason_type` STRING, " +
                "`refund_reason_txt` STRING, " +
                "`create_time` STRING, " +
                "`id` STRING, " +
                "`order_id` STRING, " +
                "`sku_id` STRING, " +
                "`sku_name` STRING, " +
                "`order_price` STRING, " +
                "`sku_num` STRING, " +
                "`create_time` STRING, " +
                "`source_type` STRING, " +
                "`source_id` STRING, " +
                "`split_total_amount` STRING, " +
                "`split_activity_amount` STRING, " +
                "`split_coupon_amount` STRING, " +
                "`consignee` STRING, " +
                "`consignee_tel` STRING, " +
                "`total_amount` STRING, " +
                "`order_status` STRING, " +
                "`user_id` STRING, " +
                "`payment_way` STRING, " +
                "`delivery_address` STRING, " +
                "`order_comment` STRING, " +
                "`out_trade_no` STRING, " +
                "`trade_body` STRING, " +
                "`operate_time` STRING, " +
                "`expire_time` STRING, " +
                "`process_status` STRING, " +
                "`tracking_no` STRING, " +
                "`parent_order_id` STRING, " +
                "`province_id` STRING, " +
                "`activity_reduce_amount` STRING, " +
                "`coupon_reduce_amount` STRING, " +
                "`original_total_amount` STRING, " +
                "`feight_fee` STRING, " +
                "`feight_fee_reduce` STRING, " +
                "`refundable_time` STRING, " +
                "`activity_id` STRING, " +
                "`activity_rule_id` STRING, " +
                "`coupon_id` STRING, " +
                "`coupon_use_id` STRING, " +
                "`sourcename` STRING, " +
                "`refund_status_name` STRING, " +
                "`out_trade_no` STRING, " +
                "`payment_type` STRING, " +
                "`trade_no` STRING, " +
                "`total_amount` STRING, " +
                "`subject` STRING, " +
                "`refund_status` STRING, " +
                "`callback_time` STRING, " +
                "`callback_content` STRING, " +
                "`payment_type_name` STRING " +
                " primary key(id) not enforced " +
                ") " +KafkaUtils.getKafkaDDL(SinkTopic));

        tableEnv.executeSql(" insert into refundordersucess_table select * from RefundSucess ");
    }
}
