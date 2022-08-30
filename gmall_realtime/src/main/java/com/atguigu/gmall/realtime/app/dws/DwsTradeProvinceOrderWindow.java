package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.TradeProvinceOrderWindowBean;
import com.atguigu.gmall.realtime.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
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
import java.util.concurrent.TimeUnit;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.app.dws
 * @date 2022/8/29 18:34
 */
// DWS交易域省份粒度下单各窗口汇总表
public class DwsTradeProvinceOrderWindow {
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
        // TODO 3 从Kafka topic_db 读取数据
        String topic = "topic_db";
        String groupId = "dws_trade_province_order_window";
        DataStreamSource<String> dbStream = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 4 过滤出订单表的数据并转换为jsonObject 过滤条件 table = order_info and type = insert
        SingleOutputStreamOperator<JSONObject> orderInfoStream = dbStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String table = jsonObject.getString("table");
                String type = jsonObject.getString("type");
                if ("order_info".equals(table) && "insert".equals(type)) {
                    out.collect(jsonObject);
                }
            }
        });
        // TODO 5 转换为javaBean结构  需要结构注解 只需要部分字段
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> benaStream = orderInfoStream.map(new MapFunction<JSONObject, TradeProvinceOrderWindowBean>() {
            @Override
            public TradeProvinceOrderWindowBean map(JSONObject value) throws Exception {
                JSONObject data = value.getJSONObject("data");
                Long ts = value.getLong("ts") * 1000L;
                String provinceId = data.getString("province_id");
                Double orderAmount = data.getDouble("order_amount");
                return TradeProvinceOrderWindowBean.builder()
                        .provinceId(provinceId)
                        .orderAmount(orderAmount)
                        .orderCount(1L)
                        .ts(ts)
                        .build();
            }
        });
        // TODO 6  设置水位线 按省份分组
        KeyedStream<TradeProvinceOrderWindowBean, String> keyedStream = benaStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TradeProvinceOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindowBean>() {
                    @Override
                    public long extractTimestamp(TradeProvinceOrderWindowBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })).keyBy(new KeySelector<TradeProvinceOrderWindowBean, String>() {
            @Override
            public String getKey(TradeProvinceOrderWindowBean value) throws Exception {
                return value.getProvinceId();
            }
        });

        // TODO 7  开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> resultStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<TradeProvinceOrderWindowBean>() {
            @Override
            public TradeProvinceOrderWindowBean reduce(TradeProvinceOrderWindowBean value1, TradeProvinceOrderWindowBean value2) throws Exception {
                value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                return value1;
            }
        }, new WindowFunction<TradeProvinceOrderWindowBean, TradeProvinceOrderWindowBean, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindowBean> input, Collector<TradeProvinceOrderWindowBean> out) throws Exception {
                TradeProvinceOrderWindowBean next = input.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setTs(System.currentTimeMillis());
                out.collect(next);
            }
        });
        // TODO 8 和DIM层的维度表进行关联  省份维度
        /*SingleOutputStreamOperator<TradeProvinceOrderWindowBean> map = resultStream.map(new RichMapFunction<TradeProvinceOrderWindowBean, TradeProvinceOrderWindowBean>() {
            DruidDataSource dataSource = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 连接hbase
                dataSource = DruidPhoenixDSUtil.getDataSource();
            }

            @Override
            public TradeProvinceOrderWindowBean map(TradeProvinceOrderWindowBean value) throws Exception {
                JSONObject dimBaseProvince = DimUtil.getDimInfo(dataSource.getConnection(), "DIM_BASE_PROVINCE", value.getProvinceId());
                value.setProvinceName(dimBaseProvince.getString("name"));
                return value;
            }
        });
*/
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> result = AsyncDataStream.unorderedWait(resultStream, new DimAsyncFunction<TradeProvinceOrderWindowBean>("DIM_BASE_PROVINCE") {
            @Override
            public String getKey(TradeProvinceOrderWindowBean input) {
                return input.getProvinceId();
            }

            @Override
            public void join(TradeProvinceOrderWindowBean input, JSONObject obj) {
                input.setProvinceName(obj.getString("name"));
            }
        }, 100, TimeUnit.SECONDS);
        // TODO 9 写入clickhouse
        result.addSink(ClickHouseUtil.getJdbcSink("insert into t values(?,?,?,?,?,?,?)"));
        // TODO 10 执行
        env.execute();

    }
}
