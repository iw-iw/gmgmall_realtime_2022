package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.app.dwd.db
 * @date 2022/8/25 21:28
 */
public class DwdInteractionFavorAdd {
    public static void main(String[] args) {
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

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_interaction_favor_add"));

        // TODO 4. 读取收藏表数据
        Table favorInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['create_time'] create_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'favor_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("favor_info", favorInfo);

        // TODO 5. 创建 Kafka-Connector dwd_interaction_favor_add 表
        tableEnv.executeSql("create table dwd_interaction_favor_add (\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "create_time string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_interaction_favor_add"));

        // TODO 6. 将数据写入 Kafka-Connector 表
        tableEnv.executeSql("insert into dwd_interaction_favor_add select * from favor_info");

    }
}
