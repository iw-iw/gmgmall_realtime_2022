package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.atguigu.gmall.realtime.util.MySQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.app.dwd.db
 * @date 2022/8/25 15:30
 */
public class DwdTradeRefundPaySuc {
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
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(2L));
        // TODO 3 从kafka topic_db 读取数据
        String topicName = "topic_db";
        String groupID = "dwd_trade_refund_pay_suc";
        tableEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                " `database` String,\n" +
                " `table` String,\n" +
                " `type` String,\n" +
                " `ts` String,\n" +
                " `xid` String,\n" +
                " `commit` String,\n" +
                " `data` map<String,String>,\n" +
                " `old` map<String,String>,\n" +
                "`pt` as PROCTIME(),\n" + // 因为要和mysql join 使用了lookup join
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL(topicName, groupID));

        // TODO 4 过滤出 退单表 退款表 订单表
        // 退单表
        Table orderRefundTable = tableEnv.sqlQuery("select\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['refund_num'] refund_num\n" +
                "from KafkaTable\n" +
                "where `table` = 'order_refund_info'\n" +
                "and `type` = 'update'\n" +
                "and `data`['refund_status'] = '1005'\n" +
                "and `old`['refund_status'] is not null");
        tableEnv.createTemporaryView("order_refund", orderRefundTable);
        // 订单表
        Table orderInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['province_id'] province_id,\n" +
                "`old`\n" +
                "from KafkaTable\n" +
                "where `table` = 'order_info'\n" +
                "and `type` = 'update'\n" +
                "and data['order_status']='1006'\n" +
                "and `old`['order_status'] is not null");
        tableEnv.createTemporaryView("order_info", orderInfo);
        // 退款表
        Table refundPay = tableEnv.sqlQuery("select \n" +
                "`data`['id'] id,\n" +
                "`data`['out_trade_no'] out_trade_no,\n" +
                "`data`['order_id'] order_id,\n" +
                "`data`['sku_id'] sku_id,\n" +
                "`data`['payment_type'] payment_type,\n" +
                "`data`['trade_no'] trade_no,\n" +
                "`data`['total_amount'] total_amount,\n" +
                "`data`['subject'] subject,\n" +
                "`data`['refund_status'] refund_status,\n" +
                "`data`['create_time'] create_time,\n" +
                "`data`['callback_time'] callback_time,\n" +
                "`data`['callback_content'] callback_content,\n" +
                "`pt`,\n" +
                "`ts`\n" +
                "from KafkaTable\n" +
                "where `table` = 'refund_payment'\n" +
                "and `type` = 'update'\n" +
                "and `data`['refund_status'] = '0702'\n" +
                "and `old`['refund_status'] is not null ");
        tableEnv.createTemporaryView("refund_pay", refundPay);
        // TODO 5 读取mysql 中base_dic表
        tableEnv.executeSql(MySQLUtil.getBaseDicDDL());
        // TODO 6 整合数据
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "rp.id,\n" +
                "oi.user_id,\n" +
                "rp.order_id,\n" +
                "rp.sku_id,\n" +
                "oi.province_id,\n" +
                "rp.payment_type,\n" +
                "b.dic_name payment_type_name,\n" +
                "rp.callback_time,\n" +
                "ri.refund_num,\n" +
                "rp.total_amount,\n" +
                "rp.ts,\n" +
                "current_row_timestamp() row_op_ts \n" +
                "from refund_pay rp join order_info oi\n" +
                " on rp.order_id = oi.id\n" +
                " join order_refund ri on rp.order_id = ri.order_id and rp.sku_id = ri.sku_id\n" +
                " join base_dic FOR SYSTEM_TIME AS OF rp.pt AS b\n" +
                " on rp.payment_type = b.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);
        // TODO 7 写入kafka新的主题
        tableEnv.executeSql("create table dwd_trade_refund_pay_suc(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "payment_type_code string,\n" +
                "payment_type_name string,\n" +
                "callback_time string,\n" +
                "refund_num string,\n" +
                "refund_amount string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3)\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_refund_pay_suc"));
        tableEnv.executeSql("insert into dwd_trade_refund_pay_suc select * from result_table");
    }
}
