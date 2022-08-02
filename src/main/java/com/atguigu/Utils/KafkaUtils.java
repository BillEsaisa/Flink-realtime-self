package com.atguigu.Utils;

/**
 * @PackageName:com.atguigu.Utils
 * @ClassNmae:KafkaUtils
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/22 21:58
 *
 *
 * //message: { "database": "gmall", "table": "comment_info", "type": "insert", "ts": 1.592226993E9, "xid": 224.0, "commit": true,
 * "data": { "id": 1.5490203011561472E18, "user_id": 1144.0, "sku_id": 17.0, "spu_id": 5.0, "order_id": 5609.0, "appraise": "1204",
 *  "comment_txt": "评论内容：87799864442689383865962134613835725445268943915661", "create_time": "2020-06-15 21:16:33" } }
 */


public class KafkaUtils {

public static String getKafkaSourceDDL(String topic,String groupid){
    return "CREATE TABLE db_table ( " +
            "  `database` STRING, " +
            "  `table` STRING, " +
            "  `type` STRING, " +
            "  `ts` BIGINT, " +
            "  `pt` as PROCTIME(), " +
            "  `data` Map<STRING,STRING>, " +
            "  `old` Map<STRING,STRING> " +
            ")" +getKafkaDDL(topic,groupid);

}

public static String getKafkaDDL(String topic,String groupid){
    return " WITH ( " +
            "  'connector' = 'kafka', " +
            "  'topic' = '"+ topic +"', " +
            "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
            "  'properties.group.id' = '"+ groupid +"', " +
            "  'scan.startup.mode' = 'latest-offset', " +
            "  'format' = 'json' " +
            ") ";

}
    public static String getKafkaDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'format' = 'json' " +
                ") ";

    }
    public static String getUpsertKafkaDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ") ";

    }

public static String getKafkaSinkDDL(String topic){
    return " CREATE TABLE jointable ( " +
            "`id` STRING, " +
            "`user_id` STRING, " +
            "`sku_id` STRING, " +
            "`cart_price` STRING, " +
            "`sku_name` STRING, " +
            "`is_checked` STRING, " +
            "`create_time` STRING, " +
            "`operate_time` STRING, " +
            "`is_ordered` STRING, " +
            "`order_time` STRING, " +
            "`source_type` STRING, " +
            "`source_id` STRING, " +
            "`dic_name` STRING " +
            ") " +getKafkaDDL(topic);



}


}
