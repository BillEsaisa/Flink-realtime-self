package com.atguigu.App.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.App.func.DiminfoAsyncFunction;
import com.atguigu.Utils.ClickHouseUtil;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import com.atguigu.bean.TradeSkuOrderBean_2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @PackageName:com.atguigu.App.dws
 * @ClassNmae:Dws_TradeSkuOrderWindow_2
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/29 21:24
 */


public class Dws_TradeSkuOrderWindow_2 {
    public static void main(String[] args) throws Exception {
        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //消费Kafka InsertOrderTable 主题数据，转换数据格式
        String topic="InsertOrderTable";
        String Groupid="Dws_TradeSkuOrderWindow_0212";
        DataStreamSource<String> sourceDS = env.addSource(MykafkaUtils.getKafkaConsumer(topic, Groupid));

        SingleOutputStreamOperator<JSONObject> process = sourceDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                }

            }
        });

        //过滤重复数据（来源于外连接写入kafka ）按照order_detail_id进行分组，状态编程
        KeyedStream<JSONObject, String> keyedBydetailIdStream = process.keyBy(json -> json.getString("id"));
        SingleOutputStreamOperator<JSONObject> process1 = keyedBydetailIdStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            //定义状态
            private ValueState<JSONObject> newdatastate;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("new_data", JSONObject.class);
                newdatastate = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                //取出状态
                JSONObject statevalue = newdatastate.value();
                String state_pt = statevalue.getString("pt");
                String pt = value.getString("pt");
                if (statevalue == null) {
                    //更新状态
                    newdatastate.update(value);
                    //设置定时器
                    TimerService timerService = ctx.timerService();
                    long ts = timerService.currentProcessingTime();
                    timerService.registerProcessingTimeTimer(ts + 5000L);
                } else if (pt.compareTo(state_pt) >= 0) {
                    //更新状态
                    newdatastate.update(value);
                }

            }

            @Override
            public void onTimer(long timestamp, ProcessFunction<JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                //取出状态值
                JSONObject statevalue = newdatastate.value();
                //输出状态中的值
                out.collect(statevalue);
                //清空状态
                newdatastate.clear();

            }
        });
        //设置wm
        process1.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                Long create_time = element.getLong("create_time");
                return create_time;
            }
        }));

        //按照order_id， sku_id 进行分组，状态编程进行去重（同一个订单中，两个相同的sku_id 情况）

        KeyedStream<JSONObject, String> keyedStream = process1.keyBy(json -> json.getString("order_id")).keyBy(json -> json.getString("sku_id"));
        SingleOutputStreamOperator<TradeSkuOrderBean_2> flatMap = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TradeSkuOrderBean_2>() {
            private ValueState<String> statevalue;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("statevalue", String.class);
                statevalue = getRuntimeContext().getState(valueStateDescriptor);

            }

            @Override
            public void flatMap(JSONObject value, Collector<TradeSkuOrderBean_2> out) throws Exception {
                //取出状态值
                String state = statevalue.value();
                Double activity = 0.0;
                Double coupon = 0.0;
                if (value.getString("split_activity_amount") != null) {
                    activity = value.getDouble("split_activity_amount");

                }
                if (value.getString("split_coupon_amount") != null) {
                    coupon = value.getDouble("split_coupon_amount");

                }

                if (state == null) {
                    //更新状态
                    statevalue.update("1");
                    //输出
                    out.collect(TradeSkuOrderBean_2.builder()
                            .Order_Count(1L)
                            .activityAmount(activity)
                            .couponAmount(coupon)
                            .skuId(value.getString("sku_id"))
                            .orderAmount(value.getDouble("split_total_amount"))
                            .originalAmount(value.getDouble("total_amount"))
                            .skuName(value.getString("sku_name"))
                            .build());
                }

            }
        });

        //按照sku_id进行分组，开窗，聚合
        AllWindowedStream<TradeSkuOrderBean_2, TimeWindow> tradeSkuOrderBean_2TimeWindowAllWindowedStream = flatMap.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));
        SingleOutputStreamOperator<TradeSkuOrderBean_2> resultDS = tradeSkuOrderBean_2TimeWindowAllWindowedStream.reduce(new ReduceFunction<TradeSkuOrderBean_2>() {
            @Override
            public TradeSkuOrderBean_2 reduce(TradeSkuOrderBean_2 value1, TradeSkuOrderBean_2 value2) throws Exception {
                value1.setOrder_Count(value1.getOrder_Count() + value2.getOrder_Count());
                value1.setActivityAmount(value1.getActivityAmount() + value2.getActivityAmount());
                value1.setCouponAmount(value1.getCouponAmount() + value2.getCouponAmount());

                return value1;
            }
        }, new AllWindowFunction<TradeSkuOrderBean_2, TradeSkuOrderBean_2, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TradeSkuOrderBean_2> values, Collector<TradeSkuOrderBean_2> out) throws Exception {
                TradeSkuOrderBean_2 next = values.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setTs(System.currentTimeMillis());

            }
        });

        //关联sku_info维表
        SingleOutputStreamOperator<TradeSkuOrderBean_2> tradeSkuOrderBean_2SingleOutputStreamOperator = AsyncDataStream.unorderedWait(resultDS, new DiminfoAsyncFunction<TradeSkuOrderBean_2>("SKU_INFO") {
            @Override
            public void join(TradeSkuOrderBean_2 input, JSONObject dimInfo) {
                input.setSkuName(dimInfo.getString("SKU_NAME"));
                input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                input.setTrademarkId(dimInfo.getString("TM_ID"));

            }

            @Override
            public String getid(TradeSkuOrderBean_2 input) {
                String skuId = input.getSkuId();
                return skuId;
            }
        }, 100, TimeUnit.SECONDS);


        //关联spu_info
        SingleOutputStreamOperator<TradeSkuOrderBean_2> spuDS = AsyncDataStream.unorderedWait(tradeSkuOrderBean_2SingleOutputStreamOperator, new DiminfoAsyncFunction<TradeSkuOrderBean_2>("SPU_INFO") {
            @Override
            public void join(TradeSkuOrderBean_2 input, JSONObject dimInfo) {
                input.setSpuName(dimInfo.getString("SPU_NAME"));

            }

            @Override
            public String getid(TradeSkuOrderBean_2 input) {
                String spuid = input.getSpuId();
                return spuid;
            }
        }, 100, TimeUnit.SECONDS);

        //关联Category3表
        SingleOutputStreamOperator<TradeSkuOrderBean_2> category3DS = AsyncDataStream.unorderedWait(spuDS, new DiminfoAsyncFunction<TradeSkuOrderBean_2>("BASE_CATEGORY3") {
            @Override
            public void join(TradeSkuOrderBean_2 input, JSONObject dimInfo) {
                input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                input.setCategory3Name(dimInfo.getString("NAME"));

            }

            @Override
            public String getid(TradeSkuOrderBean_2 input) {
                String category3Id = input.getCategory3Id();
                return category3Id;
            }
        }, 100, TimeUnit.SECONDS);

        //关联Category2表
        SingleOutputStreamOperator<TradeSkuOrderBean_2> category2DS = AsyncDataStream.unorderedWait(category3DS, new DiminfoAsyncFunction<TradeSkuOrderBean_2>("BASE_CATEGORY2") {
            @Override
            public void join(TradeSkuOrderBean_2 input, JSONObject dimInfo) {
                input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                input.setCategory2Name(dimInfo.getString("NAME"));

            }

            @Override
            public String getid(TradeSkuOrderBean_2 input) {
                String category2Id = input.getCategory2Id();
                return category2Id;
            }
        }, 100, TimeUnit.SECONDS);

        //关联Category1表
        SingleOutputStreamOperator<TradeSkuOrderBean_2> category1 = AsyncDataStream.unorderedWait(category2DS, new DiminfoAsyncFunction<TradeSkuOrderBean_2>("BASE_CATEGORY1") {
            @Override
            public void join(TradeSkuOrderBean_2 input, JSONObject dimInfo) {
                input.setCategory1Name(dimInfo.getString("NAME"));

            }

            @Override
            public String getid(TradeSkuOrderBean_2 input) {
                String category1Id = input.getCategory1Id();
                return category1Id;
            }
        }, 100, TimeUnit.SECONDS);


        //关联base_trademark
        SingleOutputStreamOperator<TradeSkuOrderBean_2> finishDS = AsyncDataStream.unorderedWait(category1, new DiminfoAsyncFunction<TradeSkuOrderBean_2>("BASE_TRADEMARK") {
            @Override
            public void join(TradeSkuOrderBean_2 input, JSONObject dimInfo) {
                input.setTrademarkName(dimInfo.getString("TM_NAME"));

            }

            @Override
            public String getid(TradeSkuOrderBean_2 input) {
                String trademarkId = input.getTrademarkId();
                return trademarkId;
            }
        }, 100, TimeUnit.SECONDS);


        //写入外部数据库
        String Sql=" insert into Dws_TradeSkuOrderWindow_2 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
        finishDS.addSink(ClickHouseUtil.getJdbcSink(Sql));


        //提交job
        env.execute();



    }
}
