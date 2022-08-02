package com.atguigu.App.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.ClickHouseUtil;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import com.atguigu.bean.TrafficPageViewBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @PackageName:com.atguigu.App.dws
 * @ClassNmae:Dws_TrafficVcChArIsNewPageViewWindow
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/26 20:45
 *
 * TODO:维度字段：版本，新老用户，渠道，地区（vc,is_new,ch,ar）
 * TODO:度量：会话数，页面浏览数，页面浏览时长，独立访客数，跳出会话数()
 * TODO:写入clickhouse，引擎：replacemergetree,带有去重功能，添加字段:窗口时间，结合维度字段去重
 */


public class Dws_TrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {

        //创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //从kafka pagelog主题消费日志数据，封装数据
        String topic="dwd_traffic_page_log";
        String Groupid="Dws_TrafficVcChArIsNewPageViewWindow_0212";
        DataStreamSource<String> sourceDS = env.addSource(MykafkaUtils.getKafkaConsumer(topic, Groupid));

        SingleOutputStreamOperator<TrafficPageViewBean> PVBean_DS = sourceDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject;
            }
        }).map(new MapFunction<JSONObject, TrafficPageViewBean>() {

            @Override
            public TrafficPageViewBean map(JSONObject value) throws Exception {
                Long uvCt = 0L;
                Long svCt = 0L;
                Long pvCt = 1L;
                Long durSum = 0L;
                Long ujCt = 0L;
                if (value.getJSONObject("page").getString("last_page_id") == null) {
                    svCt = 1L;
                }
                durSum = value.getJSONObject("page").getLong("during_time");

                return new TrafficPageViewBean("", "", value.getJSONObject("common").getString("vc"), value.getJSONObject("common").getString("ch"),
                        value.getJSONObject("common").getString("ar"), value.getJSONObject("common").getString("is_new"),
                        uvCt, svCt, pvCt, durSum, ujCt, value.getLong("ts"));
            }
        });


        //从kafka UV主题消费独立访客日志数据，封装数据
        String Topic="Dwd_UV_Dteail";
        DataStreamSource<String> UVDs = env.addSource(MykafkaUtils.getKafkaConsumer(Topic, Groupid));
        SingleOutputStreamOperator<TrafficPageViewBean> UVBeanDS = UVDs.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject;
            }
        }).map(new MapFunction<JSONObject, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(JSONObject value) throws Exception {
                Long uvCt = 0L;
                Long svCt = 1L;
                Long pvCt = 0L;
                Long durSum = 0L;
                Long ujCt = 0L;


                return new TrafficPageViewBean("", "", value.getJSONObject("common").getString("vc"), value.getJSONObject("common").getString("ch"),
                        value.getJSONObject("common").getString("ar"), value.getJSONObject("common").getString("is_new"), uvCt, svCt, pvCt, durSum, ujCt, value.getLong("ts"));
            }
        });






        //从kafka JV主题消费跳出会话日志数据，封装数据
        String TOPIC="Dwd_JV_Detail";
        DataStreamSource<String> JVDS = env.addSource(MykafkaUtils.getKafkaConsumer(TOPIC, Groupid));
        SingleOutputStreamOperator<TrafficPageViewBean> JVBeanDS = JVDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject;
            }
        }).map(new MapFunction<JSONObject, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(JSONObject value) throws Exception {
                Long uvCt = 0L;
                Long svCt = 0L;
                Long pvCt = 0L;
                Long durSum = 0L;
                Long ujCt = 1L;

                return new TrafficPageViewBean("", "", value.getJSONObject("common").getString("vc"), value.getJSONObject("common").getString("ch"),
                        value.getJSONObject("common").getString("ar"), value.getJSONObject("common").getString("is_new"), uvCt, svCt, pvCt, durSum, ujCt, value.getLong("ts"));
            }
        });


        //将三条流union起来
        DataStream<TrafficPageViewBean> unionDS = PVBean_DS.union(UVBeanDS, JVBeanDS);


        //提取wm(等待时间在原有的处理乱序的时间基础上要加上 判定为（跳出会话时间+等待乱序数据时间）)，让窗口延时关闭确保jv数据能进入窗口（睡着没听）
        unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(14)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {

                return element.getTs();
            }
        }));


        //按维度进行分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedDS = unionDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {

                return new Tuple4<>(value.getIsNew(), value.getAr(), value.getCh(), value.getVc());
            }
        });




        //开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> rt_windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));




        //聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> resultDS = rt_windowDS.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setSvCt(value1.getSvCt() + value1.getSvCt());
                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());

                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                TrafficPageViewBean trafficPageViewBean = input.iterator().next();
                trafficPageViewBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                trafficPageViewBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                trafficPageViewBean.setTs(System.currentTimeMillis());
                out.collect(trafficPageViewBean);

            }
        });


        //写入clickhouse
        String sql=" insert into  Dws_TrafficVcChArIsNewPageViewWindow_0212 (stt,edt,vc,ch,ar,isNew,uvCt,svCt,pvCt,durSum,ujCt,ts) values(?,?,?,?,?,?,?,?,?,?,?,?)";
        resultDS.addSink(ClickHouseUtil.getJdbcSink(sql));

        //执行任务
        env.execute();

    }

}
