package com.atguigu.App.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.ClickHouseUtil;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import com.atguigu.bean.UserLoginBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
 * @ClassNmae:Dws_UserUserLoginWindow
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/28 18:45
 *
 * 统计七日回流用户，和当日的独立用户数
 *
 *
 */


public class Dws_UserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费kafka page 主题的数据，转换数据格式，过滤用户 登录的数据
        String topic="dwd_traffic_page_log";
        String Groupid="Dws_UserUserLoginWindow_0212";
        DataStreamSource<String> sourceDS = env.addSource(MykafkaUtils.getKafkaConsumer(topic, Groupid));

        SingleOutputStreamOperator<JSONObject> flatMapDS = sourceDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                if (jsonObject.getJSONObject("common").getString("uid") != null && (jsonObject.getJSONObject("page").getString("last_page_id") == null ||
                        "login".equals(jsonObject.getJSONObject("page").getString("last_page_id")))) {
                    out.collect(jsonObject);
                }
            }
        });


        //设置wm
        flatMapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {

                return element.getLong("ts");
            }
        }));

        //分组
        KeyedStream<JSONObject, String> keyedStream = flatMapDS.keyBy(json -> json.getJSONObject("common").getString("uid"));


        //统计七日回流用户，和当日的独立用户数
        SingleOutputStreamOperator<UserLoginBean> flatMapBean = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {
            //设置状态
            private ValueState<String> lastlogindatestate;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                ValueStateDescriptor<String> last_login_dateDESC = new ValueStateDescriptor<>("last_login_date", String.class);
                lastlogindatestate = getRuntimeContext().getState(last_login_dateDESC);
                //
            }

            @Override
            public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {
                //取出状态值
                String last_login_dt = lastlogindatestate.value();
                Long statetots = DateFormatUtil.toTs(last_login_dt);
                //取出数据中的登录时间
                Long ts = value.getLong("ts");
                String DT = DateFormatUtil.toDate(ts);
                Long DTtots = DateFormatUtil.toTs(DT);
                //初始化度量
                long backCt = 0L;
                long uuCt = 0L;

                if (last_login_dt == null) {
                    //更新状态
                    lastlogindatestate.update(DT);
                    //uuCt=1
                    uuCt = 1L;
                } else if ((DTtots - statetots) / (24 * 60 * 60 * 1000L) > 7) {
                    //更新状态
                    lastlogindatestate.update(DT);
                    uuCt = 1L;
                    backCt = 1L;

                }
                if (uuCt == 1L) {
                    out.collect(new UserLoginBean("", "", backCt, uuCt, null));

                }


            }
        });


        // 开窗 聚合
        AllWindowedStream<UserLoginBean, TimeWindow> allWindowedStream = flatMapBean.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));
        SingleOutputStreamOperator<UserLoginBean> resultDS = allWindowedStream.reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                value2.setUuCt(value1.getUuCt() + value2.getUuCt());
                return value1;
            }
        }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                UserLoginBean next = values.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setTs(System.currentTimeMillis());
            }
        });


        //写入外部数据库（clickhouse）
        String Sql=" insert into Dws_UserUserLoginWindow_0212 (stt,edt,backCt,uuCt,ts) values(?,?,?,?,?) ";
        resultDS.addSink(ClickHouseUtil.getJdbcSink(Sql));


        //提交job
        env.execute();
    }



}
