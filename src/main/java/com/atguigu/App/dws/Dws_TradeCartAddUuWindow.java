package com.atguigu.App.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.Utils.ClickHouseUtil;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import com.atguigu.bean.CartAddUuBean;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @PackageName:com.atguigu.App.dws
 * @ClassNmae:Dws_TradeCartAddUuWindow
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/28 20:09
 *
 * 消费kafka加购明细主题
 * 加购独立用户数
 */


public class Dws_TradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费kafka  Dwd_TradeCartAdd 主题数据，封装数据
        String topic="Dwd_TradeCartAdd";
        String Groupid="Dws_TradeCartAddUuWindow_0212";
        DataStreamSource<String> sourceDS = env.addSource(MykafkaUtils.getKafkaConsumer(topic, Groupid));

        SingleOutputStreamOperator<JSONObject> mapDS = sourceDS.map(new MapFunction<String, JSONObject>() {
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
                if (element.getString("operate_time")!=null){
                    return DateFormatUtil.toTs(element.getString("operate_time"),true);

                }
                return DateFormatUtil.toTs(element.getString("create_time"),true);
            }
        }));


        //按照uid分组
        KeyedStream<JSONObject, String> keyedStream = mapDS.keyBy(json -> json.getString("user_id"));

        //过滤加购独立用户数，封装成javaBean
        SingleOutputStreamOperator<CartAddUuBean> flatMapDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
            //定义状态
            private ValueState<String> last_addcart_Dtstate;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last_add_dt", String.class);

                new ValueStateDescriptor<String>("last_add_dt", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                //初始化状态
                last_addcart_Dtstate = getRuntimeContext().getState(valueStateDescriptor);

            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {
                //取出状态值
                String statevalue = last_addcart_Dtstate.value();

                //取出数据中末次加购日期
                String operate_time = value.getString("operate_time");
                String create_time = value.getString("create_time");
                String last_addcart_dt;
                if (operate_time == null) {
                    last_addcart_dt = create_time;
                } else {
                    last_addcart_dt = operate_time;
                }
                Long value_ts = DateFormatUtil.toTs(last_addcart_dt, true);
                Long state_ts = DateFormatUtil.toTs(statevalue, true);

                String value_dt = DateFormatUtil.toDate(value_ts);
                String state_dt = DateFormatUtil.toDate(state_ts);


                //初始化度量
                long cartAddUuCt = 0L;

                if (statevalue == null) {
                    //更新状态
                    last_addcart_Dtstate.update(last_addcart_dt);
                    //cartAddUuCt=1
                    cartAddUuCt = 1L;
                } else if (!state_dt.equals(value_dt)) {
                    //更新状态
                    last_addcart_Dtstate.update(last_addcart_dt);
                    cartAddUuCt = 1L;
                }

                if (cartAddUuCt == 1) {
                    out.collect(new CartAddUuBean("", "", cartAddUuCt, null));
                }

            }
        });


        //开窗聚合
        AllWindowedStream<CartAddUuBean, TimeWindow> cartAddUuBeanTimeWindowAllWindowedStream = flatMapDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));
        SingleOutputStreamOperator<CartAddUuBean> resultDS = cartAddUuBeanTimeWindowAllWindowedStream.reduce(new ReduceFunction<CartAddUuBean>() {
            @Override
            public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                return value1;
            }
        }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                CartAddUuBean next = values.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setTs(System.currentTimeMillis());
            }
        });
        //写入到外部数据库
        String sql=" insert into Dws_TradeCartAddUuWindow (stt,edt,cartAddUuCt,ts) values(?,?,?,?)  ";
        resultDS.addSink(ClickHouseUtil.getJdbcSink(sql));

        //提交job
        env.execute();



    }
}
