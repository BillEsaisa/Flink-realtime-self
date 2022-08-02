package com.atguigu.App.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @PackageName:com.atguigu.App.dwd
 * @ClassNmae:BaseLogApp
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/19 22:20
 */


public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费kafka数据（topic_log）
        String Topic="topic_log";
        String Groupid="220212log";
        DataStreamSource<String> kafkaSourceDS = env.addSource(MykafkaUtils.getKafkaConsumer(Topic, Groupid));
        

        //过滤脏数据并将脏数据写进侧输出流
        OutputTag<String> DirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaSourceDS.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(DirtyTag, value);
                }
            }
        });
        //打印脏数据
        jsonDS.getSideOutput(DirtyTag).print();

        //根据mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //对数据进行新老用户校验(状态编程)
        SingleOutputStreamOperator<JSONObject> map = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastState = getRuntimeContext().getState(new ValueStateDescriptor<String>("laststate", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                JSONObject common = value.getJSONObject("common");
                String is_new = common.getString("is_new");
                String state = lastState.value();
                Long ts = value.getLong("ts");
                String date = DateFormatUtil.toDate(ts);
                if ("1".equals(is_new)) {
                    if (state == null) {
                        //更新日期到状态
                        lastState.update(date);
                    } else if (!state.equals(date)) {
                        common.put("is_new", "0");
                    }

                } else if (state == null) {
                    //更新状态为昨天
                    lastState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }
                return value;


            }

        });


        //根据数据的类型进行分流写入不同的topic (侧输出流)
        OutputTag<String> start = new OutputTag<String>("start") {
        };
        OutputTag<String> err = new OutputTag<String>("err") {
        };
        OutputTag<String> display = new OutputTag<String>("diaplay") {
        };
        OutputTag<String> action = new OutputTag<String>("action") {
        };

        SingleOutputStreamOperator<String> pageDS = map.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {


                //获取err日志
                if (value.getJSONObject("err") != null) {
                    ctx.output(err, value.toJSONString());
                    value.remove("err");
                }
                //获取start日志
                if (value.getJSONObject("start") != null) {
                    ctx.output(start, value.toJSONString());

                } else {

                    //获取display日志
                    JSONArray displays = value.getJSONArray("display");
                    if (displays != null) {
                        //遍历写出跑侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject jsonObject = displays.getJSONObject(i);
                            jsonObject.put("common", value.getJSONObject("common"));
                            jsonObject.put("page", value.getJSONObject("page"));
                            jsonObject.put("ts", value.getJSONObject("ts"));
                            ctx.output(display, jsonObject.toJSONString());
                        }

                    }
                    //获取action日志
                    JSONArray actions = value.getJSONArray("action");
                    //遍历写入侧输出流
                    for (int i = 0; i < actions.size(); i++) {
                        JSONObject jsonObject = actions.getJSONObject(i);
                        jsonObject.put("common", value.getJSONObject("common"));
                        jsonObject.put("page", value.getJSONObject("page"));
                        ctx.output(action, jsonObject.toJSONString());

                    }

                    value.remove("display");
                    value.remove("action");
                    out.collect(value.toJSONString());


                }


            }
        });

        //获取各个侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(start);
        DataStream<String> errDS = pageDS.getSideOutput(err);
        DataStream<String> actionDS = pageDS.getSideOutput(action);
        DataStream<String> displayDS = pageDS.getSideOutput(display);
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        //将分好的流写入各个主题
        pageDS.addSink(MykafkaUtils.getKafkaProducer(page_topic));
        startDS.addSink(MykafkaUtils.getKafkaProducer(start_topic));
        errDS.addSink(MykafkaUtils.getKafkaProducer(error_topic));
        actionDS.addSink(MykafkaUtils.getKafkaProducer(action_topic));
        displayDS.addSink(MykafkaUtils.getKafkaProducer(display_topic));

        //启动任务
        env.execute("logapp");


    }
}
