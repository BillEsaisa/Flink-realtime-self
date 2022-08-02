package com.atguigu.App.dwd.db;

import com.atguigu.Utils.KafkaUtils;
import com.atguigu.Utils.MysqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:Dwd_OrderPaySucessed
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/24 19:38
 */


public class Dwd_OrderPaySucessed {
    public static void main(String[] args) {
        //创建流处理执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(905));

        //消费kafka db主题过滤出支付成功的数据
        String Topic="topic_db";
        String GroupId ="Dwd_OrderPaySucessed_0212";
       tableEnv.executeSql(" CREATE TABLE db_table (  " +
               "  `database` STRING,  " +
               "  `table` STRING,  " +
               "  `type` STRING,  " +
               "  `ts` BIGINT,  " +
               "  `pt` as PROCTIME(),  " +
               "  `data` Map<STRING,STRING>,  " +
               "  `old` Map<STRING,STRING>  " +
               ") " + KafkaUtils.getKafkaDDL(Topic,GroupId));


        Table payment_info = tableEnv.sqlQuery(" SELECT   " +
                "`data`['out_trade_no'] out_trade_no,  " +
                "`data`['order_id'] order_id,  " +
                "`data`['user_id'] user_id,  " +
                "`data`['payment_type'] payment_type,  " +
                "`data`['trade_no'] trade_no,  " +
                "`data`['total_amount'] total_amount,  " +
                "`data`['subject'] subject,  " +
                "`data`['payment_status'] payment_status,  " +
                "`data`['create_time'] create_time,  " +
                "`data`['callback_time'] callback_time,  " +
                "`data`['callback_content'] callback_content,  " +
                "`data`['callback_content'] callback_content,  " +
                "`pt,  " +
                "`type,  " +
                "from  db_table  " +
                "where `database`='gmall'   " +
                "and `table`='payment_info'   " +
                "and `type`='update'  " +
                "and `payment_status`='1002'  " +
                "and `old`['payment_status'] is not null ");
        tableEnv.createTemporaryView("payment_info",payment_info);


        //消费kafka InsertOrderTable主题数据（下单明细数据）
        String topic ="InsertOrderTable";
        tableEnv.executeSql(" CREATE TABLE insert_order (  " +
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
                ") " + KafkaUtils.getKafkaDDL(topic));



        //lookup 功能从mysql读取字典表
        Table base_dic = tableEnv.sqlQuery(MysqlUtils.getTable_frommysql());
        tableEnv.createTemporaryView("base_dic",base_dic);


        //三表关联
        Table PaySucessedTable = tableEnv.sqlQuery(" select  " +
                "pi.out_trade_no,  " +
                "pi.order_id,  " +
                "pi.user_id,  " +
                "pi.payment_type,  " +
                "pi.trade_no,  " +
                "pi.total_amount,  " +
                "pi.subject,  " +
                "pi.payment_status,  " +
                "pi.create_time,  " +
                "pi.callback_time,  " +
                "pi.callback_content,  " +
                "pi.pt,  " +      //后续去重
                "io.id,  " +
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
                "bd.dic_name payment_status_name,  " +
                "bd2.dic_name payment_type_name  " +
                "from payment_info pi  " +
                "join insert_order io  " +
                "on pi.order_id=oi.order_id  " +
                "join base_dic bd  " +
                "on pi.payment_status=bd.dic_code  " +
                "join base_dic bd2  " +
                "on pi.payment_type=bd2.dic_code ");
        tableEnv.createTemporaryView("PaySucessedTable",PaySucessedTable);

        //写入kafka相应主题中
        String Sinktopic="Dwd_OrderPaySucessed";
        tableEnv.executeSql(" CREATE TABLE paysucess (  " +
                "`out_trade_no` STRING,  " +
                "`order_id` STRING,  " +
                "`user_id` STRING,  " +
                "`payment_type` STRING,  " +
                "`trade_no` STRING,  " +
                "`total_amount` STRING,  " +
                "`subject` STRING,  " +
                "`payment_status` STRING,  " +
                "`create_time` STRING,  " +
                "`callback_time` STRING,  " +
                "`callback_content` STRING,  " +
                "`pt` STRING,  " +
                "`id` STRING,  " +
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
                "`payment_status_name` STRING,  " +
                "`payment_type_name` STRING  " +
                " primary key(id) not enforced " +
                ") " +KafkaUtils.getKafkaDDL(Sinktopic)) ;

        tableEnv.executeSql(" insert into paysucess select * from PaySucessedTable ");
    }
}
