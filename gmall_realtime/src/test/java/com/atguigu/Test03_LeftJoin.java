package com.atguigu;

import com.bean.NameBean;
import com.bean.SexBean;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

public class Test03_LeftJoin {
    public static void main(String[] args) {
        // TODO 1 获取连接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        // TODO 3 读取节点数据

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> dataStreamSource1 = env.socketTextStream("hadoop102", 9999);

        // TODO 4 过滤清洗数据
        SingleOutputStreamOperator<NameBean> flatMap = dataStreamSource.flatMap(new FlatMapFunction<String, NameBean>() {
            @Override
            public void flatMap(String value, Collector<NameBean> out) throws Exception {
                try {
                    String[] split = value.split(",");
                    out.collect(new NameBean(split[0], split[1], Long.parseLong(split[2])));
                } catch (Exception e) {
                    System.out.println("数据错误");
                }
            }
        });
        SingleOutputStreamOperator<SexBean> flatMap1 = dataStreamSource1.flatMap(new FlatMapFunction<String, SexBean>() {
            @Override
            public void flatMap(String value, Collector<SexBean> out) throws Exception {
                try {
                    String[] split = value.split(",");
                    out.collect(new SexBean(split[0], split[1], Long.parseLong(split[2])));
                } catch (Exception e) {
                    System.out.println("数据错误");
                }
            }
        });

        // TODO 5 转换流为表
        tableEnv.createTemporaryView("name", flatMap);
        tableEnv.createTemporaryView("sex", flatMap1);
        tableEnv.executeSql("select * from name n \n" +
                "left join sex s \n" +
                "on n.id = s.id ").print();


    }
}
