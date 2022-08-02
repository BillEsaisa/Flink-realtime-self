package com.atguigu.App.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.ClickHouseUtil;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import com.atguigu.bean.TradePaymentWindowBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @PackageName:com.atguigu.App.dws
 * @ClassNmae:Dws_TradePaymentSucWindow
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/28 21:21
 *
 *统计独立支付人数和新增支付人数
 */


public class Dws_TradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费 Dwd_OrderPaySucessed 主题数据，封装数据
        String topic="Dwd_OrderPaySucessed";
        String Groupid="Dws_TradePaymentSucWindow_0212";
        DataStreamSource<String> sourceDS = env.addSource(MykafkaUtils.getKafkaConsumer(topic, Groupid));

        SingleOutputStreamOperator<JSONObject> mapDS = sourceDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {

                }
            }
        });


        //设置wm
        mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                Long callback_time = DateFormatUtil.toTs(element.getString("callback_time"), true);
                return callback_time;
            }
        }));

        //按UID进行分组
        KeyedStream<JSONObject, String> keyedStream = mapDS.keyBy(json -> json.getString("user_id"));

        //过滤独立支付人数和新增支付人数
        SingleOutputStreamOperator<TradePaymentWindowBean> flatMapDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {
            //定义状态
            private ValueState<String> firstpay_dtstate;
            private ValueState<String> pay_uv_dtstate;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("firstpay_dtstate", String.class);
                ValueStateDescriptor<String> valueStateDescriptor2 = new ValueStateDescriptor<>("pay_uv_dtstate", String.class);
                valueStateDescriptor2.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                firstpay_dtstate = getRuntimeContext().getState(valueStateDescriptor);
                pay_uv_dtstate = getRuntimeContext().getState(valueStateDescriptor2);

            }

            @Override
            public void flatMap(JSONObject value, Collector<TradePaymentWindowBean> out) throws Exception {
                //取出状态值
                String first_pay = firstpay_dtstate.value();
                String pay_uv = pay_uv_dtstate.value();
                Long uv_pay_ts = DateFormatUtil.toTs(pay_uv, true);
                String state_dt = DateFormatUtil.toDate(uv_pay_ts);
                //取出数据中的日期
                String create_time = value.getString("create_time");
                Long create_time_ts = DateFormatUtil.toTs(create_time, true);
                String value_dt = DateFormatUtil.toDate(create_time_ts);
                //初始化度量
                long paymentSucNewUserCount = 0L;
                long paymentSucUniqueUserCount = 0L;

                if (first_pay == null) {
                    //更新状态
                    firstpay_dtstate.update(create_time);
                    //
                    paymentSucNewUserCount = 1L;

                }

                if (pay_uv == null) {
                    //更新状态
                    pay_uv_dtstate.update(create_time);
                    //
                    paymentSucUniqueUserCount = 1L;
                } else if (!state_dt.equals(value_dt)) {
                    //更新状态
                    pay_uv_dtstate.update(create_time);
                    //
                    paymentSucUniqueUserCount = 1L;

                }

                if (paymentSucNewUserCount == 1L || paymentSucUniqueUserCount == 1L) {
                    out.collect(new TradePaymentWindowBean("", "", paymentSucUniqueUserCount, paymentSucNewUserCount, null));
                }


            }
        });

        //开窗聚合
        AllWindowedStream<TradePaymentWindowBean, TimeWindow> allWindowedStream = flatMapDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(2L)));
        SingleOutputStreamOperator<TradePaymentWindowBean> resultDS = allWindowedStream.reduce(new ReduceFunction<TradePaymentWindowBean>() {
            @Override
            public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                return value1;
            }
        }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> values, Collector<TradePaymentWindowBean> out) throws Exception {
                TradePaymentWindowBean next = values.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setTs(System.currentTimeMillis());
            }
        });

        //写入外部数据库
        String sql="  insert into Dws_TradePaymentSucWindow (stt,edt,paymentSucUniqueUserCount,paymentSucNewUserCount,ts) values (?,?,?,?,?) ";
        resultDS.addSink(ClickHouseUtil.getJdbcSink(sql));

        //提交job
        env.execute();




    }

}
