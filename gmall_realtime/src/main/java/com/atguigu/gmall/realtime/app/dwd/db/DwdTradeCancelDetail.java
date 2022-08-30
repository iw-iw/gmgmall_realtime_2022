package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCancelDetail {
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

        // TODO 3 读取kafka的主题数据订单预处理  主题名 dwd_trade_order_pre_process
        String topicName = "dwd_trade_order_pre_process";
        String groupID = "dwd_trade_cancel_detail";
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
                ")" + KafkaUtil.getKafkaDDL(topicName, groupID));

        // TODO 4 过滤出取消订单的数据 类型 update order_status字段为1003且old中的order_status一定不null
        Table filterTable = tableEnv.sqlQuery("select \n" +
                "  id,\n" +
                "  order_id,\n" +
                "  sku_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  create_time,\n" +
                "  source_type,\n" +
                "  source_id,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  split_original_amount,\n" +
                "  od_ts,\n" +
                "  order_status,\n" +
                "  user_id,\n" +
                "  operate_time,\n" +
                "  province_id,\n" +
                "  oi_ts,\n" +
                "  pt,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  coupon_use_id,\n" +
                "  row_op_ts\n" +
                "from order_pre\n" +
                "where type = 'update'\n" +
                "and order_status='1003'" +
                "and `old`['order_status'] is not null");
        tableEnv.createTemporaryView("filter_table", filterTable);

        // TODO 5 写入到kafka主题
        tableEnv.executeSql("create table cancel_detail(\n" +
                "  id string,\n" +
                "  order_id string,\n" +
                "  sku_id string,\n" +
                "  sku_name string,\n" +
                "  order_price string,\n" +
                "  sku_num string,\n" +
                "  create_time string,\n" +
                "  source_type string,\n" +
                "  source_id string,\n" +
                "  split_total_amount string,\n" +
                "  split_activity_amount string,\n" +
                "  split_coupon_amount string,\n" +
                "  split_original_amount string,\n" +
                "  od_ts STRING,\n" +
                "  order_status string,\n" +
                "  user_id string,\n" +
                "  operate_time string,\n" +
                "  province_id string,\n" +
                "  oi_ts STRING,\n" +
                "  pt TIMESTAMP_LTZ(3),\n" +
                "  activity_id string,\n" +
                "  activity_rule_id string,\n" +
                "  coupon_id string,\n" +
                "  coupon_use_id string,\n" +
                "  row_op_ts TIMESTAMP_LTZ(3),\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + KafkaUtil.getUpsertKafkaSinkDDL("dwd_trade_cancel_detail"));

        tableEnv.executeSql("insert into cancel_detail select * from filter_table");


    }
}
