package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test05_LookupJoin {
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
        // 创建表
        tableEnv.executeSql("CREATE TEMPORARY TABLE base_dic (\n" +
                "dic_code String,\n" +
                "dic_name String,\n" +
                "parent_code String,\n" +
                "create_time String,\n" +
                "operate_time String\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "  'table-name' = 'base_dic'\n" +
                ")");
        tableEnv.sqlQuery(" select * from base_dic ").execute().print();
    }
}