package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

public class DwsTrafficVcChArIsNewPageViewWindow {
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
        // TODO 3 读取页面主题数据，封装成流
        String page_topic = "dwd_traffic_page_log";
        String groupID = "dws_traffic_vc_ch_ar_is_new_page_view_window";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getKafkaConsumer(page_topic, groupID));
        // TODO 4 统计页面浏览时长、页面浏览数、会话数，转换数据结构
        SingleOutputStreamOperator<TrafficPageViewBean> pageBean = pageStream.map(new RichMapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                Long svCt = 0L;
                if (common.getString("last_page_id") == null) {
                    svCt = 1L;
                }
                return new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"), common.getString("ar"),
                        common.getString("is_new"), 0L, svCt, 1L, page.getLong("during_time"), 0L, jsonObject.getLong("ts"));
            }
        });

        // TODO 5 读取用户跳出明细
        String targetTopic = "dwd_traffic_user_jump_detail";
        DataStreamSource<String> ujStream = env.addSource(KafkaUtil.getKafkaConsumer(targetTopic, groupID));
        // TODO 6 转换用户跳出流数据结构
        SingleOutputStreamOperator<TrafficPageViewBean> jumpBean = ujStream.map(new MapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                return new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"),
                        common.getString("ar"), common.getString("is_new"),
                        0L, 0L, 0L, 0L, 1L, jsonObject.getLong("ts"));
            }
        });
        // TODO 7 读取独立访客明细数据
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        DataStreamSource<String> uvStream = env.addSource(KafkaUtil.getKafkaConsumer(uvTopic, groupID));

        // TODO 8 转换独立访客明细流结构
        SingleOutputStreamOperator<TrafficPageViewBean> uvBean = uvStream.map(new MapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                return new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"),
                        common.getString("ar"), common.getString("is_new"),
                        1L, 0L, 0L, 0L, 0L, jsonObject.getLong("ts"));
            }
        });
        // TODO 9 union合并三条流
        DataStream<TrafficPageViewBean> union = uvBean.union(jumpBean).union(pageBean);
        // TODO 10 设置水位线   相当于乱序时间
        // 这里的时间是根据跳出数据的延迟时间 相当于乱序 ，也可以设置在窗口迟到，
        // 但是窗口迟到还需要再次计算,所以建议设置在水位线这里
        // 延迟关窗  需要设置水位线以什么为基准
        SingleOutputStreamOperator<TrafficPageViewBean> outputStreamOperator = union.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(15)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));
        // TODO 11 按字段分组   所以这里多字段分组选用Tuple类型
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedStream = outputStreamOperator.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return new Tuple4<>(value.getVc(), value.getCh(), value.getIsNew(), value.getAr());
            }
        });
        // TODO 开窗聚合 开窗等同于按窗口分组
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10))).allowedLateness(Time.seconds(2));
        // 窗口计算逻辑
        SingleOutputStreamOperator<TrafficPageViewBean> reduce = windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            // 一般默认前面的是返回结果
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                TrafficPageViewBean next = input.iterator().next();
                // 转换一下时间戳
                // 获取数据
                long start = window.getStart();
                long end = window.getEnd();
                // 添加参数
                next.setStt(DateFormatUtil.toYmdHms(start));
                next.setEdt(DateFormatUtil.toYmdHms(end));
                // 当前系统时间 为了clickhouse的时间标识 相同数据聚合时,其他字段保留最新时间的
                next.setTs(System.currentTimeMillis());
                //输出结果
                out.collect(next);

            }
        });
        // TODO 12 将数据写入ClickHouse
        String sql = "insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)";
        reduce.addSink(ClickHouseUtil.getJdbcSink(sql));
        reduce.print(">>>>>>>>>");
        // 执行
        env.execute("dws_traffic_vc_ch_ar_is_new_page_view_window");

    }
}
