package com.atguigu.App.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.App.func.Dim_BroadcastPrefunc;
import com.atguigu.App.func.PhoenixRichFunc;
import com.atguigu.Utils.MykafkaUtils;
import com.atguigu.bean.TableProcess;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @PackageName:com.atguigu.App.dim
 * @ClassNmae:DimSinkApp
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/18 22:17
 */


public class DimSinkApp {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启ck
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //设置超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60*1000L);
        //设置两次重启的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000L);
        //设置任务关闭时保存最后一次Ck数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //指定ck自动重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1L),Time.minutes(1L)));

        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkrealtime");
        //TODO 2.从kafka消费topic_db的数据
        String Topic ="topic_db";
        String Groupid ="0212flinkreal";
        DataStreamSource<String> fromKafkaDS = env.addSource(MykafkaUtils.getKafkaConsumer(Topic, Groupid));


        //TODO 3.过滤脏数据，并转换成JSON字符串
        SingleOutputStreamOperator<JSONObject> jsonstrDS = fromKafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {

                }
            }
        });

        SingleOutputStreamOperator<JSONObject> jsonDS = fromKafkaDS.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
                };
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(outputTag, value);
                }
            }
        });



        //TODO 4.FlinkCDC实时读取配置表(mysql中)
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-config")
                .tableList("gmall-config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        //TODO 5.将配置流转换成广播流
        DataStreamSource<String> mysql = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql");
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcast = mysql.broadcast(mapStateDescriptor);

        //TODO 6.将来自kafka的主流connect上配置广播流
        BroadcastConnectedStream<JSONObject, String> connectDS = jsonDS.connect(broadcast);

        //TODO 7.过滤维表
        SingleOutputStreamOperator<JSONObject> DIMDS = connectDS.process(new Dim_BroadcastPrefunc(mapStateDescriptor));

        //TODO 8.写入phoenix(使用自定sink写入phoenix,jdbcsink单表写入，要创建连接，需要继承富函数)

        DIMDS.addSink(new PhoenixRichFunc());


        //TODO 9.执行程序
        env.execute();


    }
}
