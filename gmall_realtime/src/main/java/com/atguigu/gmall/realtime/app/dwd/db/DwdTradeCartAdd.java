package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.atguigu.gmall.realtime.util.MySQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kerby.kerberos.kerb.admin.kadmin.Kadmin;

public class DwdTradeCartAdd {
    public static void main(String[] args) {
        // TODO 1 创建连接
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

        // TODO 3 从kafka读取topic_db 的数据
        String topicName = "topic_db";
        String groupID = "dwd_trade_cart_add";
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                " `database` String,\n" +
                " `table` String,\n" +
                " `type` String,\n" +
                " `ts` String,\n" +
                " `xid` String,\n" +
                " `commit` String,\n" +
                " `data` map<String,String>,\n" +
                " `old` map<String,String>,\n" +
                " pt AS PROCTIME() \n" +
                ")" + KafkaUtil.getKafkaDDL(topicName, groupID));
        // TODO 4 过滤出cart_info的数据
        Table cart_info = tableEnv.sqlQuery("select \n" +
                " `data`['id'] id,\n" +
                " `data`['user_id'] user_id,\n" +
                " `data`['sku_id'] sku_id,\n" +
                " `data`['cart_price'] cart_price,\n" +
                " if(`type` = 'insert',cast(`data`['sku_num'] as bigint),cast(`data`['sku_num'] as bigint)- cast(`old`['sku_num'] as bigint)) sku_num,\n" +
                " `data`['sku_name'] sku_name,\n" +
                " `data`['is_checked'] is_checked,\n" +
                " `data`['create_time'] create_time,\n" +
                " `data`['operate_time'] operate_time,\n" +
                " `data`['is_ordered'] is_ordered,\n" +
                " `data`['order_time'] order_time,\n" +
                " `data`['source_type'] source_type,\n" +
                " `data`['source_id'] source_id,\n" +
                " pt ,\n" +
                " ts \n" +
                "from topic_db\n" +
                "where `table` = 'cart_info'\n" +
                "and (`type` = 'insert' or \n" +
                "(`type` = 'update' and cast(`data`['sku_num'] as bigint)- cast(`old`['sku_num'] as bigint) > 0 ))");
        // TODO 5 注册临时视图
        tableEnv.createTemporaryView("cartAdd", cart_info);
        // TODO 6 拿取mysql 中的base_dic 中的数据
        tableEnv.executeSql(MySQLUtil.getBaseDicDDL());
        // TODO 7 两张表进行lookUp join
        Table joinTable = tableEnv.sqlQuery("SELECT \n" +
                "c.`id`,\n" +
                "c.`user_id`,\n" +
                "c.`sku_id`,\n" +
                "c.`cart_price`,\n" +
                "c.`sku_num`,\n" +
                "c.`sku_name`,\n" +
                "c.`is_checked`,\n" +
                "c.`create_time`,\n" +
                "c.`operate_time`,\n" +
                "c.`is_ordered`,\n" +
                "c.`order_time`,\n" +
                "b.`dic_name`,\n" +
                "c.`source_id`,\n" +
                "ts \n" +
                "FROM cartAdd AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS b\n" +
                "  ON c.source_id = b.dic_code");
        tableEnv.createTemporaryView("result_table", joinTable);
        // TODO 8 写出到kafka中
        tableEnv.executeSql("CREATE TABLE Kafka_Sink (\n" +
                "`id` String,\n" +
                "`user_id` String,\n" +
                "`sku_id` String,\n" +
                "`cart_price` String,\n" +
                "`sku_num` Bigint,\n" +
                "`sku_name` String,\n" +
                "`is_checked` String,\n" +
                "`create_time` String,\n" +
                "`operate_time` String,\n" +
                "`is_ordered` String,\n" +
                "`order_time` String,\n" +
                "`dic_name` String,\n" +
                "`source_id` String,\n" +
                " ts String\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));
        tableEnv.executeSql("insert into Kafka_Sink select * from result_table");

    }
}
