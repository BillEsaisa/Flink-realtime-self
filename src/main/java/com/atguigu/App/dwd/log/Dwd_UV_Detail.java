package com.atguigu.App.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @PackageName:com.atguigu.App.dwd
 * @ClassNmae:Dwd_UV_Dteail
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/20 21:56
 */


public class Dwd_UV_Detail {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.消费Kafka页面主题数据
        String Topic ="dwd_traffic_page_log";
        String GroupId="Dwd_UV_Dteail_0212";
        DataStreamSource<String> pagelogDS = env.addSource(MykafkaUtils.getKafkaConsumer(Topic, GroupId));

        //TODO 3.转换数据格式为JSONobject
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = pagelogDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        //TODO 过滤出上一跳为null的数据
       // {"common":{"ar":"370000","ba":"Xiaomi","ch":"web","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_2190279","os":"Android 11.0","uid":"688","vc":"v2.1.134"},
        // "page":{"during_time":8465,"last_page_id":"good_detail","page_id":"login"},"ts":1651303987000}

        SingleOutputStreamOperator<JSONObject> filter = jsonObjectDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        });

        //TODO 4.按照mid进行分组
        KeyedStream<JSONObject, String> KeyDS = filter.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.过滤数据（状态编程）
        SingleOutputStreamOperator<JSONObject> finallydata = KeyDS.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastdate;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("lastdate", String.class);

                lastdate = getRuntimeContext().getState(stateDescriptor);

                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                stateDescriptor.enableTimeToLive(stateTtlConfig);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出状态值等需要的值
                String statevalue = lastdate.value();
                String last_page = value.getJSONObject("page").getString("last_page_id");
                Long ts = value.getLong("ts");
                String todaydate = DateFormatUtil.toDate(ts);

                //情况一：上一跳和状态值都为null,保留
                if (statevalue == null) {
                    lastdate.update(todaydate);
                    return true;

                }
                //情况二：上一跳为null，但是状态不为null,将今天日期和状态值对比，不相同保留(更新状态)，相同过滤掉
                if ( statevalue != null && statevalue != todaydate) {
                    lastdate.update(todaydate);
                    return true;
                }

                return false;

            }
        });

        //TODO 6.将每日独立访客日志数据写入kafka
        String uvtopic="Dwd_UV_Dteail";
        finallydata.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject value) throws Exception {
                return value.toJSONString();
            }
        }).addSink(MykafkaUtils.getKafkaProducer(uvtopic));


        //TODO 7.执行任务
        env.execute();




    }


}
