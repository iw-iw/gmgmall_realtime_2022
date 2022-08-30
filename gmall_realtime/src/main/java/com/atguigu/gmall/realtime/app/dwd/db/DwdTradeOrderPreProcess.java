package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.atguigu.gmall.realtime.util.MySQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderPreProcess {
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
        // TODO 3 读取kafka中 topic_db的数据
        String topicName = "topic_db";
        String groupID = "dwd_trade_order_pre_process";
        tableEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                " `database` String,\n" +
                " `table` String,\n" +
                " `type` String,\n" +
                " `ts` String,\n" +
                " `xid` String,\n" +
                " `commit` String,\n" +
                " `data` map<String,String>,\n" +
                " `old` map<String,String>,\n" +
                "   pt AS PROCTIME() \n" +
                ")" + KafkaUtil.getKafkaDDL(topicName, groupID));

        // TODO 4 过滤出订单明细表的数据并转换
        Table order_detail = tableEnv.sqlQuery("select \n" +
                " `data`['id'] id,\n" +
                " `data`['order_id'] order_id,\n" +
                " `data`['sku_id'] sku_id,\n" +
                " `data`['sku_name'] sku_name,\n" +
                " `data`['order_price'] order_price,\n" +
                " `data`['sku_num'] sku_num,\n" +
                " `data`['create_time'] create_time,\n" +
                " `data`['source_type'] source_type,\n" +
                " `data`['source_id'] source_id,\n" +
                " `data`['split_total_amount'] split_total_amount,\n" +
                " `data`['split_activity_amount'] split_activity_amount,\n" +
                " `data`['split_coupon_amount'] split_coupon_amount,\n" +
                " cast((cast(`data`['order_price'] as decimal(16,2)) * cast(`data`['sku_num'] as decimal(16,2))) as String) split_original_amount,\n" +
                " pt,\n" +
                " ts od_ts\n" +
                "from KafkaTable\n" +
                "where `table` = 'order_detail'\n" +
                "and `type` = 'insert' \n");
        tableEnv.createTemporaryView("od", order_detail);

        // TODO 5 过滤出订单表的数据并转换
        Table order_info = tableEnv.sqlQuery("select \n" +
                " `data`['id'] id,\n" +
                " `data`['order_status'] order_status,\n" +
                " `data`['user_id'] user_id,\n" +
                " `data`['operate_time'] operate_time,\n" +
                " `data`['province_id'] province_id,\n" +
                " `type`,\n" +
                " `old`,\n" +
                " pt,\n" +
                " ts oi_ts\n" +
                "from KafkaTable\n" +
                "where `table` = 'order_info'\n" +
                "and (`type` = 'insert' or `type` = 'update')");
        tableEnv.createTemporaryView("oi", order_info);
        // TODO 6 过滤出订单明细活动关联表的数据并转换
        Table act = tableEnv.sqlQuery("select \n" +
                " `data`['order_detail_id'] order_detail_id,\n" +
                " `data`['activity_id'] activity_id,\n" +
                " `data`['activity_rule_id'] activity_rule_id,\n" +
                " `pt`,\n" +
                " `ts`\n" +
                "from KafkaTable\n" +
                "where `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("act", act);
        // TODO 7 过滤出订单明细优惠关联表的数据并转换
        Table order_cou = tableEnv.sqlQuery("select \n" +
                " `data`['order_detail_id'] order_detail_id,\n" +
                " `data`['coupon_id'] coupon_id,\n" +
                " `data`['coupon_use_id'] coupon_use_id,\n" +
                " `pt`,\n" +
                " `ts`\n" +
                "from KafkaTable\n" +
                "where `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("cou", order_cou);
        // TODO 8 读取mysql中base_dic表数据
        tableEnv.executeSql(MySQLUtil.getBaseDicDDL());
        // TODO 9 整合数据  join 5张表的数据

        Table result_table = tableEnv.sqlQuery("select\n" +
                "od.id,\n" +
                "od.order_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "od.order_price,\n" +
                "od.sku_num,\n" +
                "od.create_time,\n" +
                "dic_name source_type,\n" +
                "od.source_id,\n" +
                "od.split_total_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_original_amount,\n" +
                "od.pt,\n" +
                "od.od_ts,\n" +
                "oi.order_status,\n" +
                "oi.user_id,\n" +
                "oi.operate_time,\n" +
                "oi.province_id,\n" +
                "oi.type,\n" +
                "oi.`old`,\n" +
                "oi.oi_ts,\n" +
                "activity_id,\n" +
                "activity_rule_id,\n" +
                "coupon_id,\n" +
                "coupon_use_id,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from od join oi " +
                "on od.order_id=oi.id\n" +
                "left join act on act.order_detail_id=od.id\n" +
                "left join cou on cou.order_detail_id=od.id\n" +
                "join base_dic FOR SYSTEM_TIME AS OF od.pt AS dic\n" +
                "on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", result_table);

        // TODO 10 将数据写入到新的Kafka主题  dwd_trade_order_pre_process
        String topic_Name = "dwd_trade_order_pre_process";
        tableEnv.executeSql("create TABLE kafka_sink(\n" +
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
                "  row_op_ts TIMESTAMP_LTZ(3),\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + KafkaUtil.getUpsertKafkaSinkDDL(topic_Name));

        // 写入数据
        tableEnv.executeSql("insert into kafka_sink select * from result_table");
    }
}
