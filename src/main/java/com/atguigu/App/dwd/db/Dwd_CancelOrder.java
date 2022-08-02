package com.atguigu.App.dwd.db;

import com.atguigu.Utils.KafkaUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:Dwd_CancelOrder
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/24 19:20
 */


public class Dwd_CancelOrder {
    public static void main(String[] args) {
        //创建执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取kafka中预处理表
        String Topic="Dwd_OrderPre_Precess";
        String GroupId="Dwd_CancelOrder_0212";
        tableEnv.executeSql(" CREATE TABLE Pre_precess_table ( " +
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
                "`pt` STRING, " +
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
                "`type` STRING, " +
                "`old` Map<STRING,STRING>, " +
                "`activity_id` STRING, " +
                "`activity_rule_id` STRING, " +
                "`coupon_id` STRING, " +
                "`coupon_use_id` STRING, " +
                "`sourcename` STRING " +
                ") " + KafkaUtils.getKafkaSourceDDL(Topic,GroupId));


        //过滤出取消订单数据
        Table CancelOrder = tableEnv.sqlQuery(" select  " +
                "id,   " +
                "order_id,   " +
                "sku_id,   " +
                "sku_name,   " +
                "order_price,   " +
                "sku_num,   " +
                "create_time,   " +
                "source_type,   " +
                "source_id,   " +
                "split_total_amount,   " +
                "split_activity_amount,   " +
                "split_coupon_amount,   " +
                "consignee,   " +
                "consignee_tel,   " +
                "total_amount,   " +
                "order_status,   " +
                "user_id,   " +
                "payment_way,   " +
                "delivery_address,   " +
                "order_comment,   " +
                "out_trade_no,   " +
                "trade_body,   " +
                "operate_time,   " +
                "expire_time,   " +
                "process_status,   " +
                "tracking_no,   " +
                "parent_order_id,   " +
                "province_id,   " +
                "activity_reduce_amount,   " +
                "coupon_reduce_amount,   " +
                "original_total_amount,   " +
                "feight_fee,   " +
                "feight_fee_reduce,   " +
                "refundable_time,   " +
                "activity_id,   " +
                "activity_rule_id,   " +
                "coupon_id,   " +
                "coupon_use_id,   " +
                "sourcename  " +
                "from   " +
                "Pre_precess_table  " +
                "where `type`='update' and `order_status`='1003' and `old`['order_status'] is not null ");
        tableEnv.createTemporaryView("CancelOrder",CancelOrder);


        //将取消订单数据写入kafka的对应主题
        String SinkTopic ="Dwd_CancelOrder";
        tableEnv.executeSql(" CREATE TABLE CancalOrder_ka (  " +
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
                "`sourcename` STRING  " +
                " primary key(id) not enforced " +
                ") " +KafkaUtils.getKafkaDDL(SinkTopic));
        tableEnv.executeSql(" insert into  CancalOrder_ka select * from CancelOrder ");

    }
}
