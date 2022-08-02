package com.atguigu.App.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.MykafkaUtils;
import com.ibm.icu.util.Output;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @PackageName:com.atguigu.App.dwd
 * @ClassNmae:Dwd_JV_Detail
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/20 23:00
 *
 * TODO :过滤用户跳出明细（数据有序的情况下，一条数据上一跳为null,下一条数据的上一跳也为null,就可以判定这条数据为跳出数据）
 * TODO ：考虑到假如下一条数据迟迟不来就无法判断当前数据是不是为跳出，则人为规定超时时间（会话窗口）如果在规定时间内，下一条数据没有来，则这条数据判为跳出（上一跳为null）
 *TODO :状态编程加会话窗口->cep编程
 */


public class Dwd_JV_Detail {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.消费Kafka页面主题数据
        String Topic ="dwd_traffic_page_log";
        String GroupId="Dwd_JV_Detail_0212";
        DataStreamSource<String> pagelogDS = env.addSource(MykafkaUtils.getKafkaConsumer(Topic, GroupId));


        //TODO 3.转换数据格式为JSONobject
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = pagelogDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //TODO 按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 4.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .next("next").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .within(Time.seconds(10L));

        //TODO 5.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 6.提取事件
        OutputTag<String> outputTag = new OutputTag<String>("TimeOut") {
        };
        SingleOutputStreamOperator<String> select = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, String>() {
            @Override
            public String timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                JSONObject start = pattern.get("start").get(0);
                String jsonString = start.toJSONString();
                return jsonString;
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> pattern) throws Exception {
                JSONObject start = pattern.get("start").get(0);
                String jsonString = start.toJSONString();
                return jsonString;
            }
        });

        DataStream<String> sideOutput = select.getSideOutput(outputTag);
        //合并两个流
        DataStream<String> finallyDS = select.union(sideOutput);
        //TODO 7.将最终的数据写入kafka
        String JV_Topic="Dwd_JV_Detail";
        finallyDS.addSink(MykafkaUtils.getKafkaProducer(JV_Topic));

        sideOutput.print(">>>>>>>>sideoutput");
        select.print(">>>>>>>>>>>>>selct");

        //TODO 8.执行任务
        env.execute();
    }
}
