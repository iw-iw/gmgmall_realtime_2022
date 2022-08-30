package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.CartAddUuBean;
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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.app.dws
 * @date 2022/8/26 19:19
 */
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建连接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
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

        // TODO 3 读取kafka 主题的数据 转为为javaBean
        String dwdTradeCartAdd = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        DataStreamSource<String> cartStream = env.addSource(KafkaUtil.getKafkaConsumer(dwdTradeCartAdd, groupId));
        SingleOutputStreamOperator<JSONObject> jsonObject = cartStream.map(JSON::parseObject);

        // TODO 4 按照用户分组
        KeyedStream<JSONObject, String> keyedStream = jsonObject.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("user_id");
            }
        });
        // keyedStream.print("key");
        // TODO 5 过滤当日加购数据
        SingleOutputStreamOperator<CartAddUuBean> filterStream = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
            // 维护末次加购日期
            ValueState<String> lastCartDt = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastCartDt = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_cart_dt", String.class));

            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {
                String cartDt = lastCartDt.value();
                // 换算为毫秒 db里面的ts是秒级 需要换算
                Long ts = value.getLong("ts") * 1000L;
                String visitDt = DateFormatUtil.toDate(ts);

                if (cartDt == null || !cartDt.equals(visitDt)) {
                    lastCartDt.update(visitDt);
                    out.collect(new CartAddUuBean("", "", 1L, ts));
                }
            }
        });
      //   filterStream.print();
        // TODO 6 设置水位线 开窗聚合
        SingleOutputStreamOperator<CartAddUuBean> resultStream = filterStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<CartAddUuBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<CartAddUuBean>() {
                    @Override
                    public long extractTimestamp(CartAddUuBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })).windowAll(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<CartAddUuBean>() {
            @Override
            public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                return value1;
            }
        }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                CartAddUuBean next = values.iterator().next();
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setEnt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setTs(System.currentTimeMillis());
                out.collect(next);
            }
        });
        // TODO 7 将数据写入到clickhouse
       //  resultStream.print();
        resultStream.addSink(ClickHouseUtil.getJdbcSink("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));
        // 建议写上表名 这样方便排除job问题
        env.execute("dws_trade_cart_add_uu_window");

    }
}
