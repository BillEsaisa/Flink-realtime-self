package com.atguigu.App.dwd.db;

import com.atguigu.Utils.KafkaUtils;
import com.atguigu.Utils.MysqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:Dwd_OrderPre_Precess
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/24 17:08
 */


public class Dwd_OrderPre_Precess {
    public static void main(String[] args) {
        //创建执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取kafkaTopic_db主题的数据
        tableEnv.executeSql(" CREATE TABLE db_table ( " +
                "  `database` STRING, " +
                "  `table` STRING, " +
                "  `type` STRING, " +
                "  `ts` BIGINT, " +
                "  `pt` as PROCTIME(), " +
                "  `data` Map<STRING,STRING>, " +
                "  `old` Map<STRING,STRING> " +
                ") WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = 'topic_db', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'properties.group.id' = 'Dwd_OrderPre_Precess_0212', " +
                "  'scan.startup.mode' = 'latest-offset', " +
                "  'format' = 'json' " +
                ") ");

        //过滤订单表
        Table order_info = tableEnv.sqlQuery(" SELECT  " +
                "`data`['id']  id, " +
                "`data`['consignee']  consignee, " +
                "`data`['consignee_tel']  consignee_tel, " +
                "`data`['total_amount']  total_amount, " +
                "`data`['order_status']  order_status, " +
                "`data`['user_id']  user_id, " +
                "`data`['payment_way']  payment_way, " +
                "`data`['delivery_address']  delivery_address, " +
                "`data`['order_comment']  order_comment, " +
                "`data`['out_trade_no']  out_trade_no, " +
                "`data`['trade_body']  trade_body, " +
                "`data`['create_time']  create_time, " +
                "`data`['operate_time']  operate_time, " +
                "`data`['expire_time']  expire_time, " +
                "`data`['process_status']  process_status, " +
                "`data`['tracking_no']  tracking_no, " +
                "`data`['parent_order_id']  parent_order_id, " +
                "`data`['province_id']  province_id, " +
                "`data`['activity_reduce_amount']  activity_reduce_amount, " +
                "`data`['coupon_reduce_amount']  coupon_reduce_amount, " +
                "`data`['original_total_amount']  original_total_amount, " +
                "`data`['feight_fee']  feight_fee, " +
                "`data`['feight_fee_reduce']  feight_fee_reduce, " +
                "`data`['refundable_time']  refundable_time, " +
                "`type`, " +
                "`old` " +
                "from  db_table " +
                "where `database`='gmall'  " +
                "and `table`='order_info'  " +
                "and (`type`='insert' or `type`='update') ");

        tableEnv.createTemporaryView("order_info",order_info);
        //过滤订单明细表
        Table order_detail = tableEnv.sqlQuery(" SELECT  " +
                "`data`['id'] id, " +
                "`data`['order_id'] order_id, " +
                "`data`['sku_id'] sku_id, " +
                "`data`['sku_name'] sku_name, " +
                "`data`['order_price'] order_price, " +
                "`data`['sku_num'] sku_num, " +
                "`data`['create_time'] create_time, " +
                "`data`['source_type'] source_type, " +
                "`data`['source_id'] source_id, " +
                "`data`['split_total_amount'] split_total_amount, " +
                "`data`['split_activity_amount'] split_activity_amount, " +
                "`data`['split_coupon_amount'] split_coupon_amount, " +
                "`pt` " +
                "from  db_table " +
                "where `database`='gmall'  " +
                "and `table`='order_detail'  " +
                "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail",order_detail);


        //过滤活动表
        Table order_detail_activity = tableEnv.sqlQuery(" SELECT  " +
                "`data`['order_id'] order_id, " +
                "`data`['order_detail_id'] order_detail_id, " +
                "`data`['activity_id'] activity_id, " +
                "`data`['activity_rule_id'] activity_rule_id, " +
                "`data`['sku_id'] sku_id, " +
                "`data`['create_time'] create_time " +
                "from  db_table " +
                "where `database`='gmall'  " +
                "and `table`='order_detail_activity'  " +
                "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_activity",order_detail_activity);

        //过滤优惠券表
        Table order_detail_coupon = tableEnv.sqlQuery(" SELECT  " +
                "`data`['order_id'] order_id, " +
                "`data`['order_detail_id'] order_detail_id, " +
                "`data`['coupon_id'] coupon_id, " +
                "`data`['coupon_use_id'] coupon_use_id, " +
                "`data`['sku_id'] sku_id, " +
                "`data`['create_time'] create_time " +
                "from  db_table " +
                "where `database`='gmall'  " +
                "and `table`='order_detail_coupon'  " +
                "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_coupon",order_detail_coupon);


        //lookup功能从mysql读字典表
        Table base_dic = tableEnv.sqlQuery(MysqlUtils.getTable_frommysql());

        //五表join
        Table order_pre_precess = tableEnv.sqlQuery(" select  " +
                "od.id , " +
                "od.order_id, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "od.order_price, " +
                "od.sku_num, " +
                "od.create_time, " +
                "od.source_type, " +
                "od.source_id, " +
                "od.split_total_amount, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.pt, " +
                "oi.consignee, " +
                "oi.consignee_tel, " +
                "oi.total_amount, " +
                "oi.order_status, " +
                "oi.user_id, " +
                "oi.payment_way, " +
                "oi.delivery_address, " +
                "oi.order_comment, " +
                "oi.out_trade_no, " +
                "oi.trade_body, " +
                "oi.operate_time, " +
                "oi.expire_time, " +
                "oi.process_status, " +
                "oi.tracking_no, " +
                "oi.parent_order_id, " +
                "oi.province_id, " +
                "oi.activity_reduce_amount, " +
                "oi.coupon_reduce_amount, " +
                "oi.original_total_amount, " +
                "oi.feight_fee, " +
                "oi.feight_fee_reduce, " +
                "oi.refundable_time, " +
                "oi.type, " +
                "oi.old, " +
                "oa.activity_id, " +
                "oa.activity_rule_id, " +
                "oc.coupon_id, " +
                "oc.coupon_use_id, " +
                "base_dic.dic_name  sourcename" +
                "from order_detail od " +
                "join order_info oi " +
                "on od.order_id=oi.id " +
                "left join order_detail_activity oa " +
                "on od.id=oa.order_detail_id " +
                "left join order_detail_coupon oc " +
                "on od.id=oc.order_detail_id " +
                "join base_dic " +
                "for system_time as of od.pt " +
                "on od.source_id=base_dic.dic_code " );

        tableEnv.createTemporaryView("order_pre_precess",order_pre_precess);

        //将宽表写入kafka,(upsert-kafka，使用到了外连接)
        String topic="Dwd_OrderPre_Precess";
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
                "`old` Map,<STRING,STRING> " +
                "`activity_id` STRING, " +
                "`activity_rule_id` STRING, " +
                "`coupon_id` STRING, " +
                "`coupon_use_id` STRING, " +
                "`sourcename` STRING " +
                " primary key(id) not enforced " +
                ") " +KafkaUtils.getUpsertKafkaDDL(topic));

        tableEnv.executeSql(" insert into  Pre_precess_table select * from order_pre_precess ");



    }
}
