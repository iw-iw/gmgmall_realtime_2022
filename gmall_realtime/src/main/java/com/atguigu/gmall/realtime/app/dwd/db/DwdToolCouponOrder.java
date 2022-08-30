package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.app.dwd.db
 * @date 2022/8/25 19:52
 */
public class DwdToolCouponOrder {
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
        //TODO 3 从kafka topic_db 中读取数据
        String topic = "topic_db";
        String groupId = "dwd_tool_coupon_order";

        tableEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                " `database` String,\n" +
                " `table` String,\n" +
                " `type` String,\n" +
                " `ts` String,\n" +
                " `xid` String,\n" +
                " `commit` String,\n" +
                " `data` map<String,String>,\n" +
                " `old` map<String,String>\n" +
                ")" + KafkaUtil.getKafkaDDL(topic, groupId));
        // TODO 4 过滤 coupon_use 的数据
        Table table = tableEnv.sqlQuery("select \n" +
                "`data`['id'] id,\n" +
                "`data`['coupon_id'] coupon_id,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['order_id'] order_id,\n" +
                "`data`['using_time'] using_time,\n" +
                "`ts`\n" +
                "from KafkaTable\n" +
                "where `type` = 'update'\n" +
                "and `data`['coupon_status'] = '1402'\n" +
                "and `old`['coupon_status'] is not null ");
        tableEnv.createTemporaryView("result_table", table);
        //写入kafka 主题
        tableEnv.executeSql("create table dwd_tool_coupon_order(\n" +
                "id string,\n" +
                "coupon_id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "order_time string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_tool_coupon_order"));
        // TODO 6. 将数据写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_order select " +
                "id,\n" +
                "coupon_id,\n" +
                "user_id,\n" +
                "order_id,\n" +
                "using_time order_time,\n" +
                "ts from result_table");
    }
}
