package com.atguigu.Utils;

/**
 * @PackageName:com.atguigu.Utils
 * @ClassNmae:MysqlUtils
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/22 23:12
 */


public class MysqlUtils {
    public static String getTable_frommysql(){
        return "  CREATE TEMPORARY TABLE base_dic ( " +
                "  `dic_code` STRING, " +
                "  `dic_name` STRING, " +
                "  `parent_code` STRING, " +
                "  `create_time` STRING, " +
                "  `operate_time` STRING, " +
                "  PRIMARY KEY (`dic_code`) NOT ENFORCED " +
                ") " +JdbcConnect("base_dic");

    }
    public static String JdbcConnect(String TN){
        return " WITH ( " +
                "  'connector' = 'jdbc', " +
                "  'url' = 'jdbc:mysql://hadoop102:3306/gmall', " +
                "  'username' = 'root', " +
                "  'password' = '123456', " +
                "  'table-name' = '"+TN+"', " +
                "  'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ") ";
    }
}
