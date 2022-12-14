package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.util.*;
import com.sun.corba.se.spi.ior.IdentifiableFactory;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.app.dws
 * @date 2022/8/28 14:30
 */
public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 ????????????
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2 ??????????????????
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // TODO 3 ??????kafka?????????????????????dwd_trade_order_detail
        String topicName = "dwd_trade_order_detail";
        String groupID = "dws_trade_sku_order_window";
        DataStreamSource<String> orderDetailDStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));
        // TODO 4 ????????????
        // TODO 5 ???????????????????????????
        //"user_id" "source_type"
        SingleOutputStreamOperator<JSONObject> jsonObjStream = orderDetailDStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String userId = jsonObject.getString("user_id");
                String sourceType = jsonObject.getString("source_type");
                if (userId != null && sourceType != null) {
                    out.collect(jsonObject);
                }
            }
        });
        // TODO 6 ????????????????????????
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("id");
            }
        });

        // TODO 7 ????????????left join?????????????????????  (??????????????????null?????????????????????????????????)
        SingleOutputStreamOperator<JSONObject> processStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<JSONObject> lastOrderJsonObjState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderJsonObjState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("last_order_jsonObj", JSONObject.class));
            }


            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastOrderJsonObj = lastOrderJsonObjState.value();
                // ??????id??????????????????
                if (lastOrderJsonObj == null) {
                    lastOrderJsonObjState.update(value);
                    // ??????
                    // ???????????????????????? ??????5S
                    long processingTime = ctx.timerService().currentProcessingTime();
                    ctx.timerService().registerProcessingTimeTimer(processingTime + 5000L);
                } else {
                    // ???????????????
                    // ??????join?????????  2022-08-26 07:47:30.513Z
                    String rowOpTs = value.getString("row_op_ts");
                    String lastTs = lastOrderJsonObj.getString("row_op_ts");
                    if (rowOpTs.compareTo(lastTs) >= 0) {
                        lastOrderJsonObjState.update(value);
                    }
                }
            }


            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                // ?????????????????????  ????????????????????? ???????????????
                JSONObject jsonObject = lastOrderJsonObjState.value();
                out.collect(jsonObject);
                // ???????????? ????????????????????? ????????????
                lastOrderJsonObjState.clear();
            }
        });

        // TODO 8 ??????sku??????????????????????????????
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = processStream.map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(JSONObject value) throws Exception {
                return TradeSkuOrderBean.builder()
                        .skuId(value.getString("sku_id"))
                        .skuName(value.getString("sku_name"))
                        .originalAmount(value.getDouble("split_original_amount") == null ? 0.0 : value.getDouble("split_original_amount"))
                        .activityAmount(value.getDouble("split_activity_amount") == null ? 0.0 : value.getDouble("split_activity_amount"))
                        .couponAmount(value.getDouble("split_coupon_amount") == null ? 0.0 : value.getDouble("split_coupon_amount"))
                        .orderAmount(value.getDouble("split_total_amount") == null ? 0.0 : value.getDouble("split_total_amount"))
                        .ts(value.getLong("od_ts") * 1000L)
                        .build();
            }
        });
        // ???????????????
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceBeanStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<TradeSkuOrderBean>() {
            @Override
            public long extractTimestamp(TradeSkuOrderBean element, long recordTimestamp) {
                return element.getTs();
            }
        })).keyBy(new KeySelector<TradeSkuOrderBean, String>() {
            @Override
            public String getKey(TradeSkuOrderBean value) throws Exception {
                return value.getSkuId();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                value1.setOriginalAmount(value1.getOriginalAmount() + value2.getOriginalAmount());
                value1.setActivityAmount(value1.getActivityAmount() + value2.getActivityAmount());
                value1.setCouponAmount(value1.getCouponAmount() + value2.getCouponAmount());
                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                return value1;
            }
        }, new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                TradeSkuOrderBean tradeSkuOrderBean = input.iterator().next();
                tradeSkuOrderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                tradeSkuOrderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                tradeSkuOrderBean.setTs(System.currentTimeMillis());
                out.collect(tradeSkuOrderBean);
            }
        });

        // TODO 9 ???DIM???????????????????????????
      /*  reduceBeanStream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
            DruidDataSource dataSource = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                // ??????hbase
                dataSource = DruidPhoenixDSUtil.getDataSource();
            }

            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean value) throws Exception {

                // select * from t where id = 'v.id'
                // ??????sku?????????
                JSONObject skuInfo = DimUtil.getDimInfo(dataSource.getConnection(), "DIM_SKU_INFO", value.getSkuId());

                value.setSkuName(skuInfo.getString("skuName"));
                value.setSpuId(skuInfo.getString("spuId"));
                value.setCategory3Id(skuInfo.getString("category3Id"));

                // ??????spu?????????
                JSONObject dimSpuInfo = DimUtil.getDimInfo(dataSource.getConnection(), "DIM_SPU_INFO", value.getSpuId());
                value.setSpuName(dimSpuInfo.getString("spuName"));

                // ??????3???????????????
                JSONObject dimBaseCategory3 = DimUtil.getDimInfo(dataSource.getConnection(), "DIM_BASE_CATEGORY3", value.getCategory3Id());
                value.setCategory3Name(dimBaseCategory3.getString("name"));
                value.setCategory2Id(dimBaseCategory3.getString("category2Id"));

                return value;

            }
        });*/
        // ??????????????? ?????????join ????????????
        //{"skuName":"??????10 ??????????????? ??????","tmId":"1","createTime":"2020-11-11 14:07:35","price":"59","category3Id":"61","weight":"100.00","skuDefaultImg":"http://47.93.148.192:8080/group1/M00/00/01/rBHu8l-rfvmAIpgZAAIvrX6L9fo612.jpg","isSale":"1","spuId":"1","skuDesc":"??????10 ??????????????? ??????5G ??????865 120HZ???????????? 120??????????????? 120W?????? 12GB+256GB ????????? ????????????","id":"1"}
        SingleOutputStreamOperator<TradeSkuOrderBean> skuBeanStream = AsyncDataStream.unorderedWait(reduceBeanStream, new DimAsyncFunction<TradeSkuOrderBean>("DIM_SKU_INFO") {
            @Override
            public String getKey(TradeSkuOrderBean input) {
                return input.getSkuId();
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject obj) {
                input.setSkuName(obj.getString("skuName"));
                input.setTrademarkId(obj.getString("tmId"));
                input.setSpuId(obj.getString("spuId"));
                input.setCategory3Name(obj.getString("category3Id"));
            }
        }, 100, TimeUnit.SECONDS);

        skuBeanStream.print("sku>>>>");
        // ??????spu
        // {"spuName":"??????10","tmId":"1","category3Id":"61","description":"??????10","id":"1"}
        SingleOutputStreamOperator<TradeSkuOrderBean> spuBeanStream = AsyncDataStream.unorderedWait(skuBeanStream, new DimAsyncFunction<TradeSkuOrderBean>("DIM_SPU_INFO") {

            @Override
            public String getKey(TradeSkuOrderBean input) {
                return input.getSpuId();
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject obj) {
                input.setSpuName(obj.getString("spuName"));
            }
        }, 100, TimeUnit.SECONDS);
        spuBeanStream.print("spu>>>");
        // ???????????????
        //{"tmName":"??????","id":"1"}
        SingleOutputStreamOperator<TradeSkuOrderBean> tmBeanStream = AsyncDataStream.unorderedWait(spuBeanStream, new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(TradeSkuOrderBean input) {
                return input.getTrademarkId();
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject obj) {
                input.setTrademarkName(obj.getString("tmName"));
            }
        }, 100, TimeUnit.SECONDS);
        tmBeanStream.print("tm>>>");

        // ??????3?????????
        //{"name":"?????????","category2Id":"1","id":"1"}
        SingleOutputStreamOperator<TradeSkuOrderBean> category3BeanStream = AsyncDataStream.unorderedWait(tmBeanStream, new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(TradeSkuOrderBean input) {
                return input.getCategory3Id();
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject obj) {
                input.setCategory3Name(obj.getString("name"));
                input.setCategory2Id(obj.getString("category2Id"));
            }
        }, 100, TimeUnit.SECONDS);
        category3BeanStream.print("c3>>>");

        // ??????2?????????
        //{"name":"????????????","category1Id":"1","id":"1"}
        SingleOutputStreamOperator<TradeSkuOrderBean> category2BeanStream = AsyncDataStream.unorderedWait(category3BeanStream, new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_CATEGORY2") {
            @Override
            public String getKey(TradeSkuOrderBean input) {
                return input.getCategory2Id();
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject obj) {
                input.setCategory2Name(obj.getString("name"));
                input.setCategory1Id(obj.getString("category1Id"));
            }
        }, 100, TimeUnit.SECONDS);
        category2BeanStream.print("c2>>>");

        // ??????1?????????
        //{"name":"??????????????????????????????","id":"1"}
        SingleOutputStreamOperator<TradeSkuOrderBean> resultBeanStream = AsyncDataStream.unorderedWait(category2BeanStream, new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_CATEGORY1") {
            @Override
            public String getKey(TradeSkuOrderBean input) {
                return input.getCategory1Id();
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject obj) {
                input.setCategory1Name(obj.getString("name"));
            }
        }, 100, TimeUnit.SECONDS);

        resultBeanStream.print("result>>>");
        // TODO ??????clickhouse
        resultBeanStream.addSink(ClickHouseUtil.getJdbcSink("insert into dws_trade_sku_order_window values(?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?)"));
        // TODO ????????????
        env.execute();

    }

}
