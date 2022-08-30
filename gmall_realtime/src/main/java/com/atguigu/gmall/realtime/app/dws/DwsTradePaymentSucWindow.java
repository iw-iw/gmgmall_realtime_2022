package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TradePaymentWindowBean;
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
 * @date 2022/8/26 21:30
 */
public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        // 获取环境
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

        // TODO 3 从kafka读取数据
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window";
        DataStreamSource<String> payStream = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));
        // TODO 4 转换格式
        SingleOutputStreamOperator<JSONObject> jsonObjStream = payStream.map(JSON::parseObject);
        // TODO 5 按user_id分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("user_id");
            }
        });
        // TODO 6 过滤当日支付成功数据和独立支付人数
        SingleOutputStreamOperator<TradePaymentWindowBean> beanStream = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {
            ValueState<String> lastPayDt = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastPayDt = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_pay_dt", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradePaymentWindowBean> out) throws Exception {
                Long payTime = DateFormatUtil.toTs(value.getString("pay_time"),true);
                String date = DateFormatUtil.toDate(payTime);
                String payDt = lastPayDt.value();
                if (payDt == null) {
                    lastPayDt.update(date);
                    out.collect(new TradePaymentWindowBean("", "", 1L, 1L, payTime));
                } else if (!payDt.equals(date)) {
                    lastPayDt.update(date);
                    out.collect(new TradePaymentWindowBean("", "", 1L, 0L, payTime));
                }
            }
        });
        // TODO 7 设置水位线 开窗聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> reduce = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TradePaymentWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<TradePaymentWindowBean>() {
                    @Override
                    public long extractTimestamp(TradePaymentWindowBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })).windowAll(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<TradePaymentWindowBean>() {
            @Override
            public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());

                return value1;
            }
        }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> values, Collector<TradePaymentWindowBean> out) throws Exception {
                TradePaymentWindowBean next = values.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setTs(System.currentTimeMillis());
                out.collect(next);
            }
        });
        //  写入clickhouse
        reduce.addSink(ClickHouseUtil.getJdbcSink("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));
        // 执行
        env.execute();
    }
}
