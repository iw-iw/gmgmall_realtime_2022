package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.app.dws
 * @date 2022/8/30 16:30
 */
public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */
        // TODO 3 从Kafka dwd退单表主题 读取数据
        String topicName = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window";
        DataStreamSource<String> refundStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupId));
        // TODO 4 转换结构 JsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjStream = refundStream.map(JSON::parseObject);

        // TODO 5 转换为Bean 且将字段信息传入 （需要传入字段 才能去关联维度）
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanStream = jsonObjStream.map(new MapFunction<JSONObject, TradeTrademarkCategoryUserRefundBean>() {
            //  HashSet<String> strings = null;
            @Override
            public TradeTrademarkCategoryUserRefundBean map(JSONObject value) throws Exception {
                String userId = value.getString("user_id");
                String skuId = value.getString("sku_id");
                String orderId = value.getString("order_id");
                long ts = value.getLong("ts") * 1000L;
                HashSet<String> strings = new HashSet<>();
                strings.add(orderId);

                return TradeTrademarkCategoryUserRefundBean.builder()
                        .skuId(skuId)
                        .userId(userId)
                        //  .orderIdSet(new HashSet<String>(Collections.singleton(orderId)))
                        .orderIdSet(strings)
                        .ts(ts)
                        .build();

            }
        });
        // TODO 6 关联sku_info维度 拿取 需要的分组字段 tm_id category3Id
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> skuBeanStream = AsyncDataStream.unorderedWait(beanStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getSkuId();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject obj) {
                input.setTrademarkName(obj.getString("tmId"));
                input.setCategory3Id(obj.getString("category3Id"));
            }
        }, 100, TimeUnit.SECONDS);

        // TODO 7 设置水位线  分组
        KeyedStream<TradeTrademarkCategoryUserRefundBean, String> keyedStream = skuBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })).keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, String>() {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean value) throws Exception {
                return value.getTrademarkId() + value.getCategory3Id() + value.getUserId();
            }
        });

        // TODO 8 开窗聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                return value1;
            }
        }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefundBean> input, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                TradeTrademarkCategoryUserRefundBean next = input.iterator().next();
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                // 给退单次数
                // 将int型转化为long型,这里的int型是基础类型: int a = 10;long b = (long)a;
                next.setRefundCount((long) (next.getOrderIdSet().size()));
                next.setTs(System.currentTimeMillis());
                out.collect(next);
            }
        });
        // TODO 9 关联其他需要的维度信息
// 关联品牌表
        //{"tmName":"三星","id":"1"}
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmBeanStream = AsyncDataStream.unorderedWait(windowStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getTrademarkId();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject obj) {
                input.setTrademarkName(obj.getString("tmName"));
            }
        }, 100, TimeUnit.SECONDS);
        tmBeanStream.print("tm>>>");

        // 关联3级标签
        //{"name":"电子书","category2Id":"1","id":"1"}
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category3BeanStream = AsyncDataStream.unorderedWait(tmBeanStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getCategory3Id();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject obj) {
                input.setCategory3Name(obj.getString("name"));
                input.setCategory2Id(obj.getString("category2Id"));
            }
        }, 100, TimeUnit.SECONDS);
        category3BeanStream.print("c3>>>");

        // 关联2级标签
        //{"name":"电子书刊","category1Id":"1","id":"1"}
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category2BeanStream = AsyncDataStream.unorderedWait(category3BeanStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY2") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getCategory2Id();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject obj) {
                input.setCategory2Name(obj.getString("name"));
                input.setCategory1Id(obj.getString("category1Id"));
            }
        }, 100, TimeUnit.SECONDS);
        category2BeanStream.print("c2>>>");

        // 关联1级标签
        //{"name":"图书、音像、电子书刊","id":"1"}
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultBeanStream = AsyncDataStream.unorderedWait(category2BeanStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY1") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getCategory1Id();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject obj) {
                input.setCategory1Name(obj.getString("name"));
            }
        }, 100, TimeUnit.SECONDS);

        resultBeanStream.print("result>>>");


        // TODO 10 写入Clickhouse
        resultBeanStream.addSink(ClickHouseUtil.getJdbcSink("insert into dws_trade_trademark_category_user_refund_window " +
                "values(?,?,?,?,?," +
                "?,?,?,?,?" +
                ",?,?,?)"));

        // TODO 11 执行环境
        env.execute();
    }
}
