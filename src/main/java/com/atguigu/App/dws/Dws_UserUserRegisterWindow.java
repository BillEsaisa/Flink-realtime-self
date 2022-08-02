package com.atguigu.App.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.ClickHouseUtil;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import com.atguigu.bean.UserRegisterBean;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @PackageName:com.atguigu.App.dws
 * @ClassNmae:Dws_UserUserRegisterWindow
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/28 19:48
 *
 */


public class Dws_UserUserRegisterWindow {
    public static void main(String[] args) throws Exception {

        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费Dwd_User_Info主题数据，转换数据格式
        String topic="Dwd_User_Info";
        String Groupid="Dws_UserUserRegisterWindow_0212";
        DataStreamSource<String> streamSource = env.addSource(MykafkaUtils.getKafkaConsumer(topic, Groupid));

        SingleOutputStreamOperator<JSONObject> mapDS = streamSource.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject;
            }
        });

        //设置wm
        mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return DateFormatUtil.toTs(element.getString("create_time"),true);
            }
        }));
        SingleOutputStreamOperator<UserRegisterBean> mapBeanDS = mapDS.map(new MapFunction<JSONObject, UserRegisterBean>() {
            Long registerCt = 1L;

            @Override
            public UserRegisterBean map(JSONObject value) throws Exception {
                return new UserRegisterBean("", "", registerCt, null);
            }
        });


        //开窗聚合
        AllWindowedStream<UserRegisterBean, TimeWindow> allWindowedStream = mapBeanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));
        SingleOutputStreamOperator<UserRegisterBean> resultDS = allWindowedStream.reduce(new ReduceFunction<UserRegisterBean>() {
            @Override
            public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                return value1;
            }
        }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                UserRegisterBean next = values.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setTs(System.currentTimeMillis());
            }
        });


        //写入外部数据库
        String sql=" insert into Dws_UserUserRegisterWindow (stt,edt,registerCt,ts) values(?,?,?,?) ";
        resultDS.addSink(ClickHouseUtil.getJdbcSink(sql));


        //执行job
        env.execute();


    }
}
