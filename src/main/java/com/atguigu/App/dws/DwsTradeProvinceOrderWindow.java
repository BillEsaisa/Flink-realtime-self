package com.atguigu.App.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.App.func.DiminfoAsyncFunction;
import com.atguigu.Utils.ClickHouseUtil;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import com.atguigu.bean.TradeProvinceOrderWindow;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @PackageName:com.atguigu.App.dws
 * @ClassNmae:DwsTradeProvinceOrderWindow
 * @Dscription
 * @author:Esaisa
 * @date:2022/8/1 14:16
 */


public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费kafka db主题数据 封装数据,过滤出下单数据
        String Topic="topic_db";
        String GroupId="DwsTradeProvinceOrderWindow_0212";
        DataStreamSource<String> streamSource = env.addSource(MykafkaUtils.getKafkaConsumer(Topic, GroupId));

        SingleOutputStreamOperator<JSONObject> flatMap = streamSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                if ("insert".equals(jsonObject.getString("type")) && "order_info".equals(jsonObject.getJSONObject("data").getString("table"))) {
                    out.collect(jsonObject);

                }
            }
        });


        //封装成javaBean
        SingleOutputStreamOperator<TradeProvinceOrderWindow> map = flatMap.map(new MapFunction<JSONObject, TradeProvinceOrderWindow>() {
            @Override
            public TradeProvinceOrderWindow map(JSONObject value) throws Exception {

                return TradeProvinceOrderWindow.builder()
                        .provinceId(value.getJSONObject("data").getString("province_id"))
                        .orderAmount(value.getJSONObject("data").getDouble("total_amount"))
                        .orderCount(1L)
                        .ts(value.getLong("ts"))
                        .build();
            }
        });


        //设置WM
        map.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
            @Override
            public long extractTimestamp(TradeProvinceOrderWindow element, long recordTimestamp) {
                
                return element.getTs();
            }
        }));

        //按照省份id 进行分组 开窗 聚合
        KeyedStream<TradeProvinceOrderWindow, String> keyedStream = map.keyBy(new KeySelector<TradeProvinceOrderWindow, String>() {
            @Override
            public String getKey(TradeProvinceOrderWindow value) throws Exception {

                return value.getProvinceId();
            }
        });

        WindowedStream<TradeProvinceOrderWindow, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reduce = window.reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
            @Override
            public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());

                return value1;
            }
        }, new WindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindow> input, Collector<TradeProvinceOrderWindow> out) throws Exception {
                TradeProvinceOrderWindow next = input.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setTs(System.currentTimeMillis());
            }
        });

        //关联维表
        SingleOutputStreamOperator<TradeProvinceOrderWindow> resultDS = AsyncDataStream.unorderedWait(reduce, new DiminfoAsyncFunction<TradeProvinceOrderWindow>("base_province") {
            @Override
            public void join(TradeProvinceOrderWindow input, JSONObject dimInfo) {
                input.setProvinceName(dimInfo.getString("NAME"));

            }

            @Override
            public String getid(TradeProvinceOrderWindow input) {
                String provinceId = input.getProvinceId();
                return provinceId;
            }
        }, 100L, TimeUnit.SECONDS);


        //将数据写入外部数据库
        String Sql=" insert into DwsTradeProvinceOrderWindow values(?,?,?,?,?,?) ";
        resultDS.addSink(ClickHouseUtil.getJdbcSink(Sql));

        //提交job
        env.execute();


    }
}
