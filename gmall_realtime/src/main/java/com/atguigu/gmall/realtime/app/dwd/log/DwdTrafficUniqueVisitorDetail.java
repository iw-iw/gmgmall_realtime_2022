package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //  TODO 2 设置状态后端
           /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */
        // TODO 3 读取kafka对应主题page的数据
        String page_topic = "dwd_traffic_page_log";
        String groupID = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(page_topic, groupID);
        DataStreamSource<String> pageStream = env.addSource(kafkaConsumer);

        // TODO 4 清洗掉last_page_id != null 加转换
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String lastPageID = jsonObject.getJSONObject("page").getString("last_page_id");
                if (lastPageID == null) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 5 根据mid去重
        // 先根据mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        // 过滤
        SingleOutputStreamOperator<JSONObject> filterStream = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            ValueState<String> state = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastVisitDt = new ValueStateDescriptor<>("last_visit_dt", String.class);
                // 给状态设置存活时间
                lastVisitDt.enableTimeToLive(
                        StateTtlConfig
                                // 设置TTL时间为1天
                                .newBuilder(Time.days(1))
                                // 设置更新一天时间的条件为第一次创建和修改的时间
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());
                state = getRuntimeContext().getState(lastVisitDt);
            }


            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastVisitDt = state.value();
                String visitDt = DateFormatUtil.toDate(value.getLong("ts"));
                if (lastVisitDt == null || !lastVisitDt.equals(visitDt)) {
                    return true;
                }
                state.update(visitDt);
                return false;
            }
        });


        // TODO 6 写出到新的kafka主题中
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        filterStream.print("filter>>>>>>>>");
        filterStream.map(JSONAware::toJSONString).addSink(KafkaUtil.getKafkaProducer(targetTopic));
        // 执行
        env.execute(groupID);
    }
}
