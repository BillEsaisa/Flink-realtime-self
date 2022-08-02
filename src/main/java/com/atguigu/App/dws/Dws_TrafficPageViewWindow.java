package com.atguigu.App.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.ClickHouseUtil;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import com.atguigu.bean.TrafficHomeDetailPageViewBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @PackageName:com.atguigu.App.dws
 * @ClassNmae:Dws_TrafficPageViewWindow
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/26 22:39
 *
 * 维度字段：无
 * 度量值：nv
 *当日的首页和商品详情页独立访客数
 */


public class Dws_TrafficPageViewWindow {
    public static void main(String[] args) throws Exception {

        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费kafka log主题的数据,转换数据结构,设置WM
        String topic="dwd_traffic_page_log";
        String Groupid="Dws_TrafficPageViewWindow_0212";
        DataStreamSource<String> sourceDS = env.addSource(MykafkaUtils.getKafkaConsumer(topic, Groupid));

        SingleOutputStreamOperator<JSONObject> JSONwithWMDS = sourceDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {

                return element.getLong("ts");
            }
        }));


        //过滤数据
        SingleOutputStreamOperator<JSONObject> filterDS = JSONwithWMDS.flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
            @Override
            public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
                String firstpage = value.getJSONObject("page").getString("page_id");
                String order_detail = value.getJSONObject("page").getString("page_id");
                if ("home".equals(firstpage) || "good_detail".equals(order_detail)) {
                    out.collect(value);

                }
            }
        });


        //根据mid 分组
        KeyedStream<JSONObject, String> keyedDS = filterDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        //统计当日的首页和商品详情页独立访客数，
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> flatMap = keyedDS.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            //设置状态
           private ValueState<String> homestate;
             private ValueState<String> gooddetailstate;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                ValueStateDescriptor<String> homestatedesc = new ValueStateDescriptor<>("homestate", String.class);
                ValueStateDescriptor<String> good_detail_state_desc = new ValueStateDescriptor<>("good_detail_state", String.class);
                homestate = getRuntimeContext().getState(homestatedesc);
                gooddetailstate = getRuntimeContext().getState(good_detail_state_desc);
                //启用状态ttl(存活时间24h，状态更新存活时间更新)
                homestatedesc.enableTimeToLive(new StateTtlConfig.Builder(Time.days(1L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                good_detail_state_desc.enableTimeToLive(new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());

            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                //获取状态值，数据中的日期时间（状态中保存日期）

                String pageId = value.getJSONObject("page").getString("page_id");

                String homestateValue = homestate.value();

                String gooddetailValue = gooddetailstate.value();

                Long ts = value.getLong("ts");

                String date = DateFormatUtil.toDate(ts);

                long homeUvCt = 0L;
                long goodDetailUvCt = 0L;

                if ("home".equals(pageId)) {
                    if (homestateValue == null || (homestateValue != null && homestateValue != date)) {
                        //更新今日日期到状态中
                        homestate.update(date);
                        //一条UV记录
                        homeUvCt = 1L;
                    }

                } else {
                    if (gooddetailValue == null || (gooddetailValue != null && gooddetailValue != date)) {
                        //更新今日日期到状态中
                        gooddetailstate.update(date);
                        //一条UV记录
                        goodDetailUvCt = 1L;

                    }

                }
                if (homeUvCt==1 ||goodDetailUvCt==1 ){
                    out.collect(new TrafficHomeDetailPageViewBean("", "", homeUvCt, goodDetailUvCt, value.getLong("ts")));
                }



            }
        });


        //开窗聚合
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowedStream = flatMap.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDS = windowedStream.reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                return value1;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                TrafficHomeDetailPageViewBean pageViewBean = values.iterator().next();
                pageViewBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                pageViewBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                pageViewBean.setTs(System.currentTimeMillis());
                out.collect(pageViewBean);
            }
        });

        //写入外部数据库clickhouse
        String sql=" insert into Dws_TrafficPageViewWindow_0212 (stt,edt,homeUvCt,goodDetailUvCt,ts)  values(?,?,?,?,?) ";
        resultDS.addSink(ClickHouseUtil.getJdbcSink(sql));

        //执行任务
        env.execute();



    }

}
