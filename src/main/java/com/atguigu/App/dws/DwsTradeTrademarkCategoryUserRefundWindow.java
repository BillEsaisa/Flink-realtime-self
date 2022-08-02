package com.atguigu.App.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.App.func.DiminfoAsyncFunction;
import com.atguigu.Utils.ClickHouseUtil;
import com.atguigu.Utils.DateFormatUtil;
import com.atguigu.Utils.MykafkaUtils;
import com.atguigu.bean.TradeTrademarkCategoryUserRefundBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
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
 * @ClassNmae:DwsTradeTrademarkCategoryUserRefundWindow
 * @Dscription
 * @author:Esaisa
 * @date:2022/8/1 15:03
 *
 * 从 Kafka 读取退单明细数据，关联与分组相关的维度信息后分组，统计各分组各窗口的订单数和订单金额，补充与分组无关的维度信息
 * 品牌，品类，用户
 *
 */


public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费kafka 退单明细数据 封装数据
        String Topic="Dwd_OrderRefund";
        String Groupid="DwsTradeTrademarkCategoryUserRefundWindow_0212";
        DataStreamSource<String> stringDataStreamSource = env.addSource(MykafkaUtils.getKafkaConsumer(Topic, Groupid));
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> map = stringDataStreamSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                }
            }
        }).map(new MapFunction<JSONObject, TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public TradeTrademarkCategoryUserRefundBean map(JSONObject value) throws Exception {

                return TradeTrademarkCategoryUserRefundBean.builder()
                        .userId(value.getString("user_id"))
                        .refundCount(1L)
                        .ts(value.getLong("create_time"))
                        .refundAmount(value.getDouble("refund_amount"))
                        .build();
            }
        });


        //设置wm
        map.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                Long ts = element.getTs();
                return ts;
            }
        }));
        //关联sku_info 表补充分组字段
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> sku_infoDS = AsyncDataStream.unorderedWait(map, new DiminfoAsyncFunction<TradeTrademarkCategoryUserRefundBean>("SKU_INFO") {
            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                input.setTrademarkId(dimInfo.getString("TM_ID"));


            }


            @Override
            public String getid(TradeTrademarkCategoryUserRefundBean input) {
                String skuId = input.getSkuId();

                return skuId;
            }
        }, 100, TimeUnit.SECONDS);


        //按照tm,cate3,userid 进行分组，开窗聚合
        KeyedStream<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>> keyedStream = sku_infoDS.keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(TradeTrademarkCategoryUserRefundBean value) throws Exception {

                return new Tuple3<>(value.getCategory3Id(), value.getTrademarkId(), value.getUserId());
            }
        });

        WindowedStream<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduce = window.reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                value1.setRefundAmount(value1.getRefundAmount() + value2.getRefundAmount());
                value1.setRefundCount(value1.getRefundCount() + value2.getRefundCount());

                return value1;
            }
        }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple3<String, String, String> stringStringStringTuple3, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefundBean> input, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                TradeTrademarkCategoryUserRefundBean next = input.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setTs(System.currentTimeMillis());
            }
        });


        //关联维表补充剩余字段
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> cate3DS = AsyncDataStream.unorderedWait(reduce, new DiminfoAsyncFunction<TradeTrademarkCategoryUserRefundBean>("BASE_CATEGORY3") {
            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                input.setCategory3Name(dimInfo.getString("NAME"));
                input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));

            }

            @Override
            public String getid(TradeTrademarkCategoryUserRefundBean input) {
                String category3Id = input.getCategory3Id();
                return category3Id;
            }
        }, 100, TimeUnit.SECONDS);


        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> cate2 = AsyncDataStream.unorderedWait(cate3DS, new DiminfoAsyncFunction<TradeTrademarkCategoryUserRefundBean>("BASE_CATEGORY2") {
            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                input.setCategory2Name(dimInfo.getString("NAME"));
                input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));

            }

            @Override
            public String getid(TradeTrademarkCategoryUserRefundBean input) {
                String category2Id = input.getCategory2Id();
                return category2Id;
            }
        }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> cate1 = AsyncDataStream.unorderedWait(cate2, new DiminfoAsyncFunction<TradeTrademarkCategoryUserRefundBean>("BASE_CATEGORY1") {
            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                input.setCategory1Name(dimInfo.getString("NAME"));


            }

            @Override
            public String getid(TradeTrademarkCategoryUserRefundBean input) {
                String category1Id = input.getCategory1Id();
                return category1Id;
            }
        }, 100, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultDS = AsyncDataStream.unorderedWait(cate1, new DiminfoAsyncFunction<TradeTrademarkCategoryUserRefundBean>("BASE_TRADEMARK") {
            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
               input.setTrademarkName(dimInfo.getString("TM_NAME"));


            }

            @Override
            public String getid(TradeTrademarkCategoryUserRefundBean input) {
                String trademarkId = input.getTrademarkId();
                return trademarkId;
            }
        }, 100, TimeUnit.SECONDS);




        //写入外部数据库
        String Sql=" insert into DwsTradeTrademarkCategoryUserRefundWindow values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
        resultDS.addSink(ClickHouseUtil.getJdbcSink(Sql));


        //提交job
        env.execute();

    }
}
