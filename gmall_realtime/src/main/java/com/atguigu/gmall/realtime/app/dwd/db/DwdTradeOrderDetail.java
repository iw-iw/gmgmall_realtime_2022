package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.time.Duration;

public class DwdTradeOrderDetail {
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
        // TODO 3 读取Kafka   dwd_trade_order_pre_process 主题的数据
        String topic_Name = "dwd_trade_order_pre_process";
        String groupID = "dwd_trade_order_detail";
        tableEnv.executeSql("create TABLE order_pre(\n" +
                "  id String,\n" +
                "  order_id String,\n" +
                "  sku_id String,\n" +
                "  sku_name String,\n" +
                "  order_price String,\n" +
                "  sku_num String,\n" +
                "  create_time String,\n" +
                "  source_type String,\n" +
                "  source_id String,\n" +
                "  split_total_amount String,\n" +
                "  split_activity_amount String,\n" +
                "  split_coupon_amount String,\n" +
                "  split_original_amount String,\n" +
                "  pt TIMESTAMP_LTZ(3),\n" +
                "  od_ts String,\n" +
                "  order_status String,\n" +
                "  user_id String,\n" +
                "  operate_time String,\n" +
                "  province_id String,\n" +
                "  type String,\n" +
                "  `old` MAP<STRING, STRING>,\n" +
                "  oi_ts String,\n" +
                "  activity_id String,\n" +
                "  activity_rule_id String,\n" +
                "  coupon_id String,\n" +
                "  coupon_use_id String,\n" +
                "  row_op_ts TIMESTAMP_LTZ(3)\n" +
                ")" + KafkaUtil.getKafkaDDL(topic_Name, groupID));
        // TODO 4 筛选订单数据 类型为insert 即为下单数据 无需考虑存活时间
        Table ordertable = tableEnv.sqlQuery("select\n" +
                "id,\n" +
                "order_id,\n" +
                "user_id,\n" +
                "sku_id,\n" +
                "sku_name,\n" +
                "sku_num,\n" +
                "order_status,\n" +
                "order_price,\n" +
                "source_type,\n" +
                "source_id,\n" +
                "activity_id,\n" +
                "activity_rule_id,\n" +
                "coupon_id,\n" +
                "coupon_use_id,\n" +
                "create_time,\n" +
                "province_id,\n" +
                "split_total_amount,\n" +
                "split_activity_amount,\n" +
                "split_coupon_amount,\n" +
                "split_original_amount,\n" +
                "od_ts ts, \n" +
                "row_op_ts \n" +
                "from order_pre\n" +
                "where `type` = 'insert'");
        tableEnv.createTemporaryView("order", ordertable);

        // TODO 5 写入Kafka 新的主题中 dwd_trade_order_detail
        tableEnv.executeSql("create table kafka_sink (\n" +
                "id String,\n" +
                "order_id String,\n" +
                "user_id String,\n" +
                "sku_id String,\n" +
                "sku_name String,\n" +
                "sku_num String,\n" +
                "order_status String,\n" +
                "order_price String,\n" +
                "source_type String,\n" +
                "source_id String,\n" +
                "activity_id String,\n" +
                "activity_rule_id String,\n" +
                "coupon_id String,\n" +
                "coupon_use_id String,\n" +
                "create_time String,\n" +
                "province_id String,\n" +
                "split_total_amount String,\n" +
                "split_activity_amount String,\n" +
                "split_coupon_amount String,\n" +
                "split_original_amount String,\n" +
                "od_ts String, \n" +
                "row_op_ts TIMESTAMP_LTZ(3),\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + KafkaUtil.getUpsertKafkaSinkDDL("dwd_trade_order_detail"));
        // 数据装载
        tableEnv.executeSql("insert into kafka_sink select * from `order`");
    }
}
