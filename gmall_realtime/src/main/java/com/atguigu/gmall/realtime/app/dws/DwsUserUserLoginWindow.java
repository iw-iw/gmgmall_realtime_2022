package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.UserLoginBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
 * @date 2022/8/26 11:24
 */
public class DwsUserUserLoginWindow {
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
        // TODO 3 读取kafka 页面主题数据
        String page_topic = "dwd_traffic_page_log";
        String groupID = "dws_user_user_login_window";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getKafkaConsumer(page_topic, groupID));
        // TODO 4 过滤出登录数据并转换结构 jsonobject
        SingleOutputStreamOperator<JSONObject> jsonOBJStream = pageStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                if (common.getString("uid") != null &&
                        (page.getString("last_page_id") == null || "login".equals(page.getString("last_page_id")))) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 5 按mid 分组
        KeyedStream<JSONObject, String> keyedStream = jsonOBJStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("uid");
            }
        });

        // TODO 6 统计登录人数和7日回流数  并转换为javabean
        SingleOutputStreamOperator<UserLoginBean> loginBeanStream = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {
            // 涉及状态需要富函数 open 提前加载状态
            ValueState<String> lastLoginDt = null;  // 如果从来没有出现过则为空，给一个默认空值

            @Override
            public void open(Configuration parameters) throws Exception {
                lastLoginDt = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_login_Dt", String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {
                String loginDt = lastLoginDt.value(); // 原来记录的状态
                Long ts = value.getLong("ts");
                String visitDate = DateFormatUtil.toDate(ts); // 当前流的时间状态
                // 过滤掉一天重复登录用户
                if (loginDt == null || !loginDt.equals(visitDate)) {
                    if (loginDt != null && ts - DateFormatUtil.toTs(loginDt) > 1000 * 60 * 60 * 24 * 7L) {
                        // 7日回流
                        out.collect(new UserLoginBean("", "", 1L, 1L, ts));
                        lastLoginDt.update(loginDt);
                    } else {
                        // 当日独立
                        out.collect(new UserLoginBean("", "", 0L, 1L, ts));
                        lastLoginDt.update(loginDt);
                    }
                }
            }
        });
        // TODO 7 开窗聚合
        // 设置水位线
        SingleOutputStreamOperator<UserLoginBean> reduce = loginBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<UserLoginBean>() {
                    @Override
                    public long extractTimestamp(UserLoginBean element, long recordTimestamp) {

                        return element.getTs();
                    }
                })
        ).windowAll(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                return value1;
            }
        }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                UserLoginBean next = values.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                next.setTs(System.currentTimeMillis());
                out.collect(next);
            }
        });

        // TODO 8 写入clickhouse
        reduce.addSink(ClickHouseUtil.getJdbcSink("insert into dws_user_user_login_window values(?,?,?,?,?)"));
        env.execute();
    }
}
