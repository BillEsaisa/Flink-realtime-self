package com.atguigu.App.dwd.db;

import com.atguigu.Utils.KafkaUtils;
import com.atguigu.Utils.MysqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PackageName:com.atguigu.App.dwd.db
 * @ClassNmae:Dwd_TradeCartAdd
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/22 21:54
 */


public class Dwd_TradeCartAdd {
    public static void main(String[] args) {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //消费kafkadb主题的数据
        String topic ="topic_db";
        String GroupId="Dwd_TradeCartAdd_0212";
        tableEnv.executeSql(KafkaUtils.getKafkaSourceDDL(topic,GroupId));

        //过滤出属于gmall库，base_dic 表的数据(insert,update),封装成表
        Table cart_info = tableEnv.sqlQuery(" SELECT  " +
                "`data`['id'] id, " +
                "`data`['user_id'] user_id, " +
                "`data`['sku_id'] sku_id, " +
                "`data`['cart_price'] cart_price, " +
                "if(`type`='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num, " +
                "`data`['sku_name'] sku_name, " +
                "`data`['is_checked'] is_checked, " +
                "`data`['create_time'] create_time, " +
                "`data`['operate_time'] operate_time, " +
                "`data`['is_ordered'] is_ordered, " +
                "`data`['order_time'] order_time, " +
                "`data`['source_type'] source_type, " +
                "`data`['source_id'] source_id, " +
                "`pt` " +
                "from  db_table " +
                "where `database`='gmall'  " +
                "and `table`='cart_info'  " +
                "and ((`type`='update' and `old`['sku_num'] is not null and (cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int))) or `type`='insert'  ) ");
        tableEnv.createTemporaryView("cart_info",cart_info);

        //关联维表
         tableEnv.executeSql(MysqlUtils.getTable_frommysql());

        Table resulttable = tableEnv.sqlQuery(" SELECT " +
                "c.id, " +
                "c.user_id, " +
                "c.sku_id, " +
                "c.cart_price, " +
                "c.sku_name, " +
                "c.is_checked, " +
                "c.create_time, " +
                "c.operate_time, " +
                "c.is_ordered, " +
                "c.order_time, " +
                "c.source_type, " +
                "c.source_id, " +
                "b.dic_name " +
                "FROM cart_info AS c " +
                "JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS b " +
                "ON c.source_type=b.dic_code ");
        tableEnv.createTemporaryView("cart_join_dic",resulttable);


        //将结果表写到kafka相应主题
        String Topic ="Dwd_TradeCartAdd";
        tableEnv.executeSql(KafkaUtils.getKafkaSinkDDL(Topic));
        tableEnv.executeSql(" insert into  jointable select * from cart_join_dic ");

    }
}
