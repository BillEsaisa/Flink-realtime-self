package com.atguigu.App.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.ClickHouseUtil;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import com.atguigu.bean.TradeOrderBean;
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
import org.apache.kafka.common.protocol.types.Field;

import java.time.Duration;

/**
 * @PackageName:com.atguigu.App.dws
 * @ClassNmae:Dws_TradeOrderWindow
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/28 23:28
 *当日下单独立用户数和新增下单用户数
 *
 */


public class Dws_TradeOrderWindow {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费 kafka InsertOrderTable 主题数据，转换数据格式
        String topic="InsertOrderTable";
        String Groupid="Dws_TradeOrderWindow_0212";
        DataStreamSource<String> sourceDS = env.addSource(MykafkaUtils.getKafkaConsumer(topic, Groupid));

        SingleOutputStreamOperator<JSONObject> mapDS = sourceDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {

                }
            }
        });

        //设置wm
        mapDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                Long create_time = element.getLong("create_time");
                return create_time;
            }
        }));


        //按照uid 进行分组
        KeyedStream<JSONObject, String> keyedStream = mapDS.keyBy(json -> json.getString("user_id"));


        //过滤当日下单独立用户和新增下单用户
        SingleOutputStreamOperator<TradeOrderBean> FlatmapBean = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {
            //定义状态
            private ValueState<String> first_order_state;
            private ValueState<String> order_uv_state;


            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                ValueStateDescriptor<String> first_order_desc = new ValueStateDescriptor<String>("first_order", String.class);
                ValueStateDescriptor<String> uv_order_desc = new ValueStateDescriptor<>("uv_order", String.class);
                uv_order_desc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                first_order_state = getRuntimeContext().getState(first_order_desc);
                order_uv_state = getRuntimeContext().getState(uv_order_desc);

            }

            @Override
            public void flatMap(JSONObject value, Collector<TradeOrderBean> out) throws Exception {
                //取出状态
                String state_first_order = first_order_state.value();
                String uv_order_state = order_uv_state.value();
                String create_time = value.getString("create_time");
                Long state_ts = DateFormatUtil.toTs(uv_order_state, true);
                Long create_ts = DateFormatUtil.toTs(create_time, true);
                String state_dt = DateFormatUtil.toDate(state_ts);
                String create_dt = DateFormatUtil.toDate(create_ts);

                //初始化度量
                long orderUniqueUserCount = 0L;
                long orderNewUserCount = 0L;

                if (state_first_order == null) {
                    //更新状态
                    first_order_state.update(create_time);
                    //
                    orderNewUserCount = 1L;
                }

                if (uv_order_state == null) {
                    //更新状态
                    order_uv_state.update(create_time);
                    //
                    orderUniqueUserCount = 1L;

                } else if (!state_dt.equals(create_dt)) {
                    //更新状态
                    order_uv_state.update(create_time);
                    //
                    orderUniqueUserCount = 1L;

                }
                if (orderUniqueUserCount == 1L || orderNewUserCount == 1L) {
                    out.collect(new TradeOrderBean("", "", orderUniqueUserCount, orderNewUserCount, null));

                }


            }
        });

        //开窗聚合
        AllWindowedStream<TradeOrderBean, TimeWindow> allWindowedStream = FlatmapBean.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(2L)));
        SingleOutputStreamOperator<TradeOrderBean> resultDS = allWindowedStream.reduce(new ReduceFunction<TradeOrderBean>() {
            @Override
            public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                return value1;
            }
        }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                TradeOrderBean next = values.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setTs(System.currentTimeMillis());
            }
        });
        //写入外部数据库（clickhouse）
        String sql=" insert into Dws_TradeOrderWindow (stt,edt,orderUniqueUserCount,orderNewUserCount,ts) values(?,?,?,?,?)";
        resultDS.addSink(ClickHouseUtil.getJdbcSink(sql));

        //提交job
        env.execute();

    }
}
