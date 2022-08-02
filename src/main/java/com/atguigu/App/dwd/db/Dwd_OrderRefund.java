package com.atguigu.App.dwd.db;

import com.atguigu.Utils.KafkaUtils;
import com.atguigu.Utils.MysqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:Dwd_OrderRefund
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/24 21:16
 *
 */


public class Dwd_OrderRefund {
    public static void main(String[] args) {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

       //从kafka消费 db主题
        String Topic="topic_db";
        String GroupId="Dwd_OrderRefund_0212";
        tableEnv.executeSql(" CREATE TABLE db_table (  " +
                "  `database` STRING,  " +
                "  `table` STRING,  " +
                "  `type` STRING,  " +
                "  `ts` BIGINT,  " +
                "  `pt` as PROCTIME(),  " +
                "  `data` Map<STRING,STRING>,  " +
                "  `old` Map<STRING,STRING>  " +
                ") " + KafkaUtils.getKafkaDDL(Topic,GroupId));



         //过滤出退单数据
        Table refund_order_table = tableEnv.sqlQuery(" select  " +
                "`data`['id'] id,  " +
                "`data`['user_id'] user_id,  " +
                "`data`['order_id'] order_id,  " +
                "`data`['sku_id'] sku_id,  " +
                "`data`['refund_type'] refund_type,  " +
                "`data`['refund_num'] refund_num,  " +
                "`data`['refund_amount'] refund_amount,  " +
                "`data`['refund_reason_type'] refund_reason_type,  " +
                "`data`['refund_reason_txt'] refund_reason_txt,  " +
                "`data`['refund_status'] refund_status,  " +
                "`data`['create_time'] create_time,  " +
                "`type`  " +
                "`old`  " +
                "`pt`  " +
                "from  db_table  " +
                "where `database`='gmall'   " +
                "and `table`='order_refund_info'   " +
                "and `type`='insert' or (`type`='update' and `old`['refund_status'] is not null ) ");
        tableEnv.createTemporaryView("refund_order_table",refund_order_table);


        //消费InsertOrderTable主题数据（下单明细）
        String topic="InsertOrderTable";
        String groupid="Dwd_OrderRefund_0212";
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
                ") " +KafkaUtils.getKafkaDDL(topic,groupid));

       //lookup功能从mysql获取字典表
        Table base_dic = tableEnv.sqlQuery(MysqlUtils.getTable_frommysql());
        tableEnv.createTemporaryView("base_dic",base_dic);



        //三表关联
        Table refund_order = tableEnv.sqlQuery(" select  " +
                "ro.user_id  " +
                "ro.order_id,  " +
                "ro.sku_id,  " +
                "ro.refund_type,  " +
                "ro.refund_num,  " +
                "ro.refund_amount,  " +
                "ro.refund_reason_type,  " +
                "ro.refund_reason_txt,  " +
                "ro.refund_status,  " +
                "ro.create_time,  " +
                "ro.pt,  " +
                "ro.type,  " +
                "ro.old,  " +
                "io.id,  " +
                "io.order_id,  " +
                "io.sku_id,  " +
                "io.sku_name,  " +
                "io.order_price,  " +
                "io.sku_num,  " +
                "io.create_time,  " +
                "io.source_type,  " +
                "io.source_id,  " +
                "io.split_total_amount,  " +
                "io.split_activity_amount,  " +
                "io.split_coupon_amount,  " +
                "io.consignee,  " +
                "io.consignee_tel,  " +
                "io.total_amount,  " +
                "io.order_status,  " +
                "io.user_id,  " +
                "io.payment_way,  " +
                "io.delivery_address,  " +
                "io.order_comment,  " +
                "io.out_trade_no,  " +
                "io.trade_body,  " +
                "io.operate_time,  " +
                "io.expire_time,  " +
                "io.process_status,  " +
                "io.tracking_no,  " +
                "io.parent_order_id,  " +
                "io.province_id,  " +
                "io.activity_reduce_amount,  " +
                "io.coupon_reduce_amount,  " +
                "io.original_total_amount,  " +
                "io.feight_fee,  " +
                "io.feight_fee_reduce,  " +
                "io.refundable_time,  " +
                "io.activity_id,  " +
                "io.activity_rule_id,  " +
                "io.coupon_id,  " +
                "io.coupon_use_id,  " +
                "io.sourcename,  " +
                "base_dic.dic_name refund_status_name  " +
                "from refund_order_table ro   " +
                "join insert_order_ka io   " +
                "on (ro.order_id=io.order_id and ro.sku_id=io.sku_id )   " +
                "join base_dic  " +
                "for system_time as of ro.pt " +
                "on ro.refund_status=base_dic.dic_code ");
        tableEnv.createTemporaryView("refund_order",refund_order);




        //写入kafka相应主题
        String SinkTopic="Dwd_OrderRefund";
        tableEnv.executeSql(" CREATE TABLE refundordertable (  " +
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
                ") " +KafkaUtils.getKafkaDDL(SinkTopic));
        tableEnv.executeSql(" insert into refundordertable select * from refund_order ");

    }
}
