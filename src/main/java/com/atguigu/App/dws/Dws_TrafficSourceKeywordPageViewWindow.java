package com.atguigu.App.dws;

import com.atguigu.App.func.DWS_SplitKeyWordFunc;
import com.atguigu.Utils.ClickHouseUtil;
import com.atguigu.Utils.KafkaUtils;
import com.atguigu.bean.KeyWordBean;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PackageName:com.atguigu.App.dws
 * @ClassNmae:Dws_TrafficSourceKeywordPageViewWindow
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/25 18:46
 */


public class Dws_TrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //消费kafka page 主题数据生成表，提取wm（dwd_traffic_page_log）
        String Topic ="dwd_traffic_page_log";
        String Groupid="Dws_TrafficSourceKeywordPageViewWindow_0212";
        tableEnv.executeSql(" CREATE TABLE page_log ( " +
                "`page` MAP<STRING,STRING>, " +
                "`ts` BIGINT, " +
                "`rt` AS TO_TIMESTAMP_LTZ(ts, 3), " +
                "WATERMARK FOR rt AS rt - INTERVAL '2' SECOND  " +
                ") " + KafkaUtils.getKafkaDDL(Topic,Groupid));

        //过滤搜索数据
        Table splittable = tableEnv.sqlQuery(" select  " +
                "`page`['item'] words, " +
                "rt " +
                "from page_log " +
                "where `page`['item'] is not null " +
                "and  `page`['item_type']='keyword' " +
                "and `page`['last_page_id']='search' ");
        tableEnv.createTemporaryView("resplittable",splittable);

        //注册函数，切词
        tableEnv.createTemporarySystemFunction("splitwords_function", DWS_SplitKeyWordFunc.class);
        Table splitedtable = tableEnv.sqlQuery(" SELECT  " +
                "rt, " +
                "word  " +
                "FROM resplittable, " +
                "LATERAL TABLE(SplitFunction(words)) ");


        //分组，开窗，聚合
        Table groupwindowtable = tableEnv.sqlQuery(" select " +
                "DATE_FORMAT(TUMBLE_START(rt,  INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rt,  INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "UNIX_TIMESTAMP() ts, " +
                "'search' source, " +
                "word  keyword, " +
                "count(*)  keyword_count " +
                "from splitedtable " +
                "GROUP BY " +
                "TUMBLE(rt, INTERVAL '10' SECOND), " +
                "word ");
        tableEnv.createTemporaryView("groupwindowtable",groupwindowtable);


        //转换成流写入clickhouse (暂时不支持DDL方式连接clickhouse)
        String sql="insert into KeywordPageView_0212(stt,edt,ts,source,keyword,keyword_count) values(?,?,?,?,?,?)";
        DataStream<KeyWordBean> keyWordBeanDataStream = tableEnv.toAppendStream(groupwindowtable, KeyWordBean.class);
        keyWordBeanDataStream.addSink(ClickHouseUtil.getJdbcSink(sql));

    }
}
