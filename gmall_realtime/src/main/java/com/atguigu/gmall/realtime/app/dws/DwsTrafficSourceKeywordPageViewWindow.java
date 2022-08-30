package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.KeywordUDTF;
import com.atguigu.gmall.realtime.bean.KeywordBean;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
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

        // TODO 3 使用sql的形式读取数据dwd_traffic_page_log
        // {"common":{"ar":"110000","uid":"840","os":"iOS 13.2.9","ch":"Appstore","is_new":"0","md":"iPhone Xs Max","mid":"mid_855298","vc":"v2.1.111","ba":"iPhone"},"page":{"page_id":"trade","item":"8","during_time":13561,"item_type":"sku_ids","last_page_id":"cart"},"ts":1645452180000}
        String page_topic = "dwd_traffic_page_log";
        String groupID = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql("create table page_table (\n" + "`common` map<String,String>,\n" + "`page` map<String,String>,\n" + "`ts` bigint,\n" + "  rt AS TO_TIMESTAMP_LTZ(ts, 3),\n" + "  WATERMARK FOR rt AS rt - INTERVAL '2' SECOND\n" + ")" + KafkaUtil.getKafkaDDL(page_topic, groupID));
        // TODO 4 过滤出搜索行为  过滤条件 item_type = keyword   last_page_id:search  item is  not  null
        Table search_table = tableEnv.sqlQuery("select \n" + "`page`['item'] keyword,\n" + "`rt`\n" + "from  page_table\n" + "where `page`['item_type'] = 'keyword'\n" + "and `page`['last_page_id'] = 'search'\n" + "and `page`['item'] is not null");
        tableEnv.createTemporaryView("filter_table", search_table);
        // TODO 5 调用自定义函数拆分关键词
        // 注册函数
        tableEnv.createTemporarySystemFunction("KeywordUDTF", KeywordUDTF.class);
        // 调用函数拆分关键词
        Table keyWord_table = tableEnv.sqlQuery("select " + "rt , " + "word " + "from filter_table, " + "LATERAL TABLE(KeywordUDTF(keyword))");
        tableEnv.createTemporaryView("wordTable", keyWord_table);
        // TODO 6 开窗聚合函数
      /*  SELECT TUMBLE_START(rt, INTERVAL '10' MINUTE), SELECT TUMBLE_START(rt, INTERVAL '10' MINUTE),word,COUNT(*)
        FROM keyWord_table
        GROUP BY word,TUMBLE(rt, INTERVAL '10' MINUTE);*/
        Table result = tableEnv.sqlQuery("select \n" + "  DATE_FORMAT(TUMBLE_START(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt,\n" + "  DATE_FORMAT(TUMBLE_END(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss')  edt,\n" + "  word keyword,\n" + "  count(*) keyword_count, \n  " + "'" + GmallConstant.KEYWORD_SEARCH + "'  source , \n " + "  UNIX_TIMESTAMP()*1000 ts \n " + " FROM wordTable\n" + " GROUP BY word,TUMBLE(rt, INTERVAL '10' second)");
        // TODO 7 转换为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(result, KeywordBean.class);
        // TODO 8 写入到clickhouse
        String sql = "insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)";
        // 传入类型
        keywordBeanDataStream.addSink(ClickHouseUtil.<KeywordBean>getJdbcSink(sql));
        env.execute();
    }
}
