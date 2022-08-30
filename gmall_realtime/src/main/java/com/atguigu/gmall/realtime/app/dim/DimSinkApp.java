package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.MyBroadcastFunction;
import com.atguigu.gmall.realtime.app.func.MyPhoenixSink;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度  正常情况 kafka有多少个分区设置多少并行度
        env.setParallelism(1);
      /*  // TODO 设置检查点和状态后端
        env.enableCheckpointing(5*60*1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(3*60*1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME","atguigu");
        env.setStateBackend(new HashMapStateBackend());*/
        // TODO 3 读取kafka对应的数据
        String topicName = "topic_db";
        String groupID = "dim_sink_app";
        DataStreamSource<String> topicDStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));
        // topicDStream.print("topic_db>>>>>>>>>>>>>>>>>");
        // TODO 4 转换格式和清洗过滤脏数据
        //不是json的数据；
        //type类型为bootstrap-start和bootstrap-complete

        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
        };
        // 将脏数据写入到侧输出流中
        SingleOutputStreamOperator<JSONObject> jsonObjStream = topicDStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String type = jsonObject.getString("type");
                    if (!"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)) {
                        out.collect(jsonObject);
                    } else {
                        ctx.output(outputTag, value);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                    // 不为json写到侧输出流
                    ctx.output(outputTag, value);
                }
            }
        });

        // 获取脏数据的流
        DataStream<String> drityStream = jsonObjStream.getSideOutput(outputTag);
        //  drityStream.print("dirty>>>>>>>>>>");
        // TODO 5 使用flinkCDC读取配置表数据
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_config")
                // 注意库
                .tableList("gmall_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 数据输出格式
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> tableConfig = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "table_config");

        // TODO 6 将配置流转换为广播流和主流进行连接
        // String(表名)  判断当前表是否为维度表
        // V(后面的数据)

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table_process", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableConfig.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjStream.connect(broadcastStream);
        // TODO 7 处理连接流 根据配置流的信息 过滤出主流的维度表内容
        SingleOutputStreamOperator<JSONObject> filterTableStream = connectedStream.process(new MyBroadcastFunction(mapStateDescriptor));
        filterTableStream.print("filterTable>>>>>>>>");
        // TODO 8 将数据写入到phoenix中
        filterTableStream.addSink(new MyPhoenixSink());
        // TODO 执行任务
        env.execute(groupID);

    }
}
