package com.atguigu.App.dws;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.App.func.DiminfoAsyncFunction;
import com.atguigu.Utils.ClickHouseUtil;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import com.atguigu.bean.TradeSkuOrderBean;
import com.atguigu.bean.TradeSkuOrderBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;


/**
 * @PackageName:com.atguigu.App.dws
 * @ClassNmae:Dws_TradeSkuOrderWindow
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/29 18:16
 *
 *
 * 统计sku维度各窗口的订单数、原始金额、活动减免金额、优惠券减免金额和订单金额，
 */


public class Dws_TradeSkuOrderWindow {
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

        //转换格式javabean
        SingleOutputStreamOperator<TradeSkuOrderBean> map = process1.map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(JSONObject value) throws Exception {
                Double coupon = 0.0;
                if (value.getString("split_coupon_amount") != null) {
                    coupon = value.getDouble("split_coupon_amount");
                }

                Double activity = 0.0;
                if (value.getString("split_activity_amount") != null) {
                    activity = value.getDouble("split_activity_amount");

                }
                HashSet<String> order_idset = new HashSet<>();
                order_idset.add(value.getString("order_id"));

                return TradeSkuOrderBean.builder()
                        .ts(value.getLong("create_time"))
                        .skuId(value.getString("sku_id"))
                        .skuName(value.getString("sku_name"))
                        .couponAmount(coupon)
                        .activityAmount(activity)
                        .Order_idset(order_idset)
                        .originalAmount(value.getDouble("total_amount"))
                        .orderAmount(value.getDouble("split_total_amount"))
                        .build();
            }
        });


        //设置wm
        map.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<TradeSkuOrderBean>() {
            @Override
            public long extractTimestamp(TradeSkuOrderBean element, long recordTimestamp) {
                Long ts = element.getTs();
                return ts;
            }
        }));

        //根据sku_id 进行分组，开窗，聚合
        KeyedStream<TradeSkuOrderBean, String> keyedStream = map.keyBy(new KeySelector<TradeSkuOrderBean, String>() {
            @Override
            public String getKey(TradeSkuOrderBean value) throws Exception {
                String skuId = value.getSkuId();
                return skuId;
            }
        });
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));
        SingleOutputStreamOperator<TradeSkuOrderBean> resultDS = window.reduce(new ReduceFunction<TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                value1.getOrder_idset().addAll(value2.getOrder_idset());
                value1.setActivityAmount(value1.getActivityAmount() + value2.getActivityAmount());
                value1.setCouponAmount(value1.getCouponAmount() + value2.getCouponAmount());
                return value1;
            }
        }, new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                TradeSkuOrderBean next = input.iterator().next();
                next.setOrder_Count((long) next.getOrder_idset().size());
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setTs(System.currentTimeMillis());
            }
        });


        //关联sku_info维表
        SingleOutputStreamOperator<TradeSkuOrderBean> TradeSkuOrderBeanSingleOutputStreamOperator = AsyncDataStream.unorderedWait(resultDS, new DiminfoAsyncFunction<TradeSkuOrderBean>("SKU_INFO") {
            @Override
            public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                input.setSkuName(dimInfo.getString("SKU_NAME"));
                input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                input.setTrademarkId(dimInfo.getString("TM_ID"));

            }

            @Override
            public String getid(TradeSkuOrderBean input) {
                String skuId = input.getSkuId();
                return skuId;
            }
        }, 100, TimeUnit.SECONDS);


        //关联spu_info
        SingleOutputStreamOperator<TradeSkuOrderBean> spuDS = AsyncDataStream.unorderedWait(TradeSkuOrderBeanSingleOutputStreamOperator, new DiminfoAsyncFunction<TradeSkuOrderBean>("SPU_INFO") {
            @Override
            public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                input.setSpuName(dimInfo.getString("SPU_NAME"));

            }

            @Override
            public String getid(TradeSkuOrderBean input) {
                String spuid = input.getSpuId();
                return spuid;
            }
        }, 100, TimeUnit.SECONDS);

        //关联Category3表
        SingleOutputStreamOperator<TradeSkuOrderBean> category3DS = AsyncDataStream.unorderedWait(spuDS, new DiminfoAsyncFunction<TradeSkuOrderBean>("BASE_CATEGORY3") {
            @Override
            public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                input.setCategory3Name(dimInfo.getString("NAME"));

            }

            @Override
            public String getid(TradeSkuOrderBean input) {
                String category3Id = input.getCategory3Id();
                return category3Id;
            }
        }, 100, TimeUnit.SECONDS);

        //关联Category2表
        SingleOutputStreamOperator<TradeSkuOrderBean> category2DS = AsyncDataStream.unorderedWait(category3DS, new DiminfoAsyncFunction<TradeSkuOrderBean>("BASE_CATEGORY2") {
            @Override
            public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                input.setCategory2Name(dimInfo.getString("NAME"));

            }

            @Override
            public String getid(TradeSkuOrderBean input) {
                String category2Id = input.getCategory2Id();
                return category2Id;
            }
        }, 100, TimeUnit.SECONDS);

        //关联Category1表
        SingleOutputStreamOperator<TradeSkuOrderBean> category1 = AsyncDataStream.unorderedWait(category2DS, new DiminfoAsyncFunction<TradeSkuOrderBean>("BASE_CATEGORY1") {
            @Override
            public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                input.setCategory1Name(dimInfo.getString("NAME"));

            }

            @Override
            public String getid(TradeSkuOrderBean input) {
                String category1Id = input.getCategory1Id();
                return category1Id;
            }
        }, 100, TimeUnit.SECONDS);


        //关联base_trademark
        SingleOutputStreamOperator<TradeSkuOrderBean> finishDS = AsyncDataStream.unorderedWait(category1, new DiminfoAsyncFunction<TradeSkuOrderBean>("BASE_TRADEMARK") {
            @Override
            public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                input.setTrademarkName(dimInfo.getString("TM_NAME"));

            }

            @Override
            public String getid(TradeSkuOrderBean input) {
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
