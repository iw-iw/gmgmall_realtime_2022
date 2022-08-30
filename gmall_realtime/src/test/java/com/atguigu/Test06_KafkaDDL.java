package com.atguigu;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test06_KafkaDDL {
    public static void main(String[] args) {
        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置后端状态
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // 从kafka中读取数据
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                " `database` String,\n" +
                " `table` String,\n" +
                " `type` String,\n" +
                " `ts` String,\n" +
                " `xid` String,\n" +
                " `commit` String,\n" +
                " `data` map<String,String>,\n" +
                " `old` map<String,String>\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = '" + KafkaUtil.BOOTSTRAP_SERVERS + "',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'format' = 'json'\n" +
                ")");
        //   查询topic_db表的数据
        tableEnv.sqlQuery("select * from topic_db").execute().print();
    }
}
