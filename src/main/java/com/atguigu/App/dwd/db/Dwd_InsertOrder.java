package com.atguigu.App.dwd.db;

import com.atguigu.Utils.KafkaUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:InsertOrderTable
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/24 18:52
 */


public class Dwd_InsertOrder {
    public static void main(String[] args) {

        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //从Kafka中消费订单预处理表
        String Topic="Dwd_OrderPre_Precess";
        String GroupId="Dwd_OrderPre_Precess_0212";
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
                ") " + KafkaUtils.getKafkaDDL(Topic,GroupId));


        //过滤下单数据
        Table insert_order = tableEnv.sqlQuery(" select " +
                "id,  " +
                "order_id,  " +
                "sku_id,  " +
                "sku_name,  " +
                "order_price,  " +
                "sku_num,  " +
                "create_time,  " +
                "source_type,  " +
                "source_id,  " +
                "split_total_amount,  " +
                "split_activity_amount,  " +
                "split_coupon_amount,  " +
                "consignee,  " +
                "consignee_tel,  " +
                "total_amount,  " +
                "order_status,  " +
                "user_id,  " +
                "payment_way,  " +
                "delivery_address,  " +
                "order_comment,  " +
                "out_trade_no,  " +
                "trade_body,  " +
                "operate_time,  " +
                "expire_time,  " +
                "process_status,  " +
                "tracking_no,  " +
                "parent_order_id,  " +
                "province_id,  " +
                "activity_reduce_amount,  " +
                "coupon_reduce_amount,  " +
                "original_total_amount,  " +
                "feight_fee,  " +
                "feight_fee_reduce,  " +
                "refundable_time,  " +
                "activity_id,  " +
                "activity_rule_id,  " +
                "coupon_id,  " +
                "coupon_use_id,  " +
                "sourcename  " +
                "from  " +
                "Pre_precess_table " +
                "where `type`='insert' ");
        tableEnv.createTemporaryView("insert_order",insert_order);

        //写入kafka相应主题
        String SinkTopic ="InsertOrderTable";
        tableEnv.executeSql(" CREATE TABLE insert_order_ka ( " +
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
                "`sourcename` STRING " +
                " primary key(id) not enforced " +
                ") " +KafkaUtils.getKafkaDDL(SinkTopic));

        tableEnv.executeSql(" insert into insert_order_ka select * from insert_order ");




    }
}
