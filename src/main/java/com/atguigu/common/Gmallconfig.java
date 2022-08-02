package com.atguigu.common;

/**
 * @PackageName:com.atguigu.common
 * @ClassNmae:Gmallconfig
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/19 19:44
 */


public class Gmallconfig {
    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL220212_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    public static final String BROKE_LIST = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/gmall_realtime";


}
