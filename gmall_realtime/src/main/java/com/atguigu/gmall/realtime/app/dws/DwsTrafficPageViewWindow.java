package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
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

public class DwsTrafficPageViewWindow {
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
        // TODO 3 读取数据 转为JSONObject
        String page_topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getKafkaConsumer(page_topic, groupId));
        // TODO 4 过滤出首页和详情页的数据
        SingleOutputStreamOperator<JSONObject> pageObject = pageStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String pageID = jsonObject.getJSONObject("page").getString("page_id");
                if ("home".equals(pageID) || "good_detail".equals(pageID)) {
                    out.collect(jsonObject);
                }
            }
        });
        // TODO 5 设置水位线
        SingleOutputStreamOperator<JSONObject> pageWatermarks = pageObject.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));
        // TODO 6 按mid分组
        KeyedStream<JSONObject, String> keyedStream = pageWatermarks.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        // TODO 7 去重 统计首页和商品详情页独立访客数，转换数据结构  通过状态记录
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> pageBean = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeLastVisitDt = null;
            private ValueState<String> detailLastVisitDt = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastVisitDt = new ValueStateDescriptor<>("home_last_visit_dt", String.class);
                lastVisitDt.enableTimeToLive(StateTtlConfig
                        .newBuilder(org.apache.flink.api.common.time.Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                homeLastVisitDt = getRuntimeContext().getState(lastVisitDt);

                ValueStateDescriptor<String> detailLastVisitDtTime = new ValueStateDescriptor<>("detail_last_visit_dt", String.class);
                detailLastVisitDtTime.enableTimeToLive(StateTtlConfig
                        .newBuilder(org.apache.flink.api.common.time.Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                detailLastVisitDt = getRuntimeContext().getState(detailLastVisitDtTime);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                // 判断数据为首页还是详情页
                String pageID = value.getJSONObject("page").getString("page_id");
                // 因为业务是要一天的
                String visitDt = DateFormatUtil.toDate(value.getLong("ts"));
                if ("home".equals(pageID)) {
                    String value1 = homeLastVisitDt.value();
                    if (value1 == null || !value1.equals(visitDt)) {
                        homeLastVisitDt.update(visitDt);
                        // 进入这个判断才是新的数据 判断之外的是重复数据
                        out.collect(new TrafficHomeDetailPageViewBean("", "", 1L, 0L, value.getLong("ts")));
                    }
                } else {
                    String value2 = detailLastVisitDt.value();
                    if (value2 == null || !value2.equals(visitDt)) {
                        detailLastVisitDt.update(visitDt);
                        out.collect(new TrafficHomeDetailPageViewBean("", "", 0L, 1L, value.getLong("ts")));
                    }
                }

            }
        });

        // TODO 8 开窗聚合
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowedStream = pageBean.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        // reduce 写聚合逻辑
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> result = windowedStream.reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                value2.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                return value1;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                long end = window.getEnd();
                long start = window.getStart();
                TrafficHomeDetailPageViewBean next = values.iterator().next();
                next.setEdt(DateFormatUtil.toYmdHms(end));
                next.setStt(DateFormatUtil.toYmdHms(start));
                // 将时间戳换为当前系统时间
                next.setTs(System.currentTimeMillis());
                out.collect(next);
            }
        });
        // TODO 9 将数据写出到 ClickHouse
        String sql = "insert into dws_traffic_page_view_window values(?,?,?,?,?)";
        result.addSink(ClickHouseUtil.getJdbcSink(sql));
        result.print(">>>>>>>");
        // 执行
        env.execute(groupId);

    }
}
