package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TradeOrderBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
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
 * @author iw
 * @Package com.atguigu.gmall.realtime.app.dws
 * @date 2022/8/27 11:35
 */
public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */
        // TODO 3 读取kafka 明细主题数据 dwd_trade_order_detail
        String topic = "dwd_trade_order_detail";
        String groupID = "dws_trade_order_window";
        DataStreamSource<String> orderStream = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupID));
        // TODO 4 转换格式和user_id分组
        KeyedStream<JSONObject, String> keyedStream = orderStream.map(JSON::parseObject).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("user_id");
            }
        });
        // TODO 5 过滤数据 独立下单用户 和当日新增用户
        SingleOutputStreamOperator<TradeOrderBean> BeanStream = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {
            ValueState<String> lastOrderDt = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderDt = getRuntimeContext()
                        .getState(new ValueStateDescriptor<String>("last_order_Dt", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradeOrderBean> out) throws Exception {
                //od_ts
                String orderDt = lastOrderDt.value();
                Long ts = value.getLong("od_ts") * 1000L; // 13位 毫秒级
                String Dt = DateFormatUtil.toDate(ts);
                if (orderDt == null) {
                    lastOrderDt.update(Dt);
                    out.collect(new TradeOrderBean("", "", 1L, 1L, ts));
                } else if (!orderDt.equals(Dt)) {
                    lastOrderDt.update(Dt);
                    out.collect(new TradeOrderBean("", "", 1L, 0L, ts));
                }
            }
        });
        // TODO 6 设置水位线 开窗聚合
        SingleOutputStreamOperator<TradeOrderBean> resultStream = BeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<TradeOrderBean>() {
            @Override
            public long extractTimestamp(TradeOrderBean element, long recordTimestamp) {
                return element.getTs();
            }
        })).windowAll(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<TradeOrderBean>() {
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
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setTs(System.currentTimeMillis());
                out.collect(next);
            }
        });
        resultStream.print();
        // TODO 7 写入clickhouse
        resultStream.addSink(ClickHouseUtil.getJdbcSink("insert into dws_trade_order_window values(?,?,?,?,?)"));
        // 执行
        env.execute("dws_trade_order_window");

    }
}
