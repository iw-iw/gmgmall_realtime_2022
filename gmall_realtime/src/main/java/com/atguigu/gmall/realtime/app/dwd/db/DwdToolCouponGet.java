package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.app.dwd.db
 * @date 2022/8/25 18:24
 */
public class DwdToolCouponGet {
    public static void main(String[] args) {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置存活时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(905L));

        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */
        // 设置存活时间
        // tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(2L));
        // TODO 3 从Kafka读数据
        String topic = "topic_db";
        String groupID = "dwd_tool_coupon_get";
        tableEnv.executeSql("create table `topic_db`(\n" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`data` map<string, string>,\n" +
                "`type` string,\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL(topic, groupID));
        // TODO 4  读取优惠券领用数据，封装为表
        Table table = tableEnv.sqlQuery("select\n" +
                "data['id'],\n" +
                "data['coupon_id'],\n" +
                "data['user_id'],\n" +
                "data['get_time'],\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'coupon_use'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("result_table", table);
        // TODO 5 写入kafka
        tableEnv.executeSql("create table dwd_tool_coupon_get (\n" +
                "id string,\n" +
                "coupon_id string,\n" +
                "user_id string,\n" +
                "get_time string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_tool_coupon_get"));
        tableEnv.executeSql("insert into dwd_tool_coupon_get select * from result_table");
    }
}
