package com.atguigu;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 使用flinkCDC 读取配置表内容
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_config")
                // 注意库
                .tableList("gmall_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 数据输出格式
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> tableConfig = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "table_config");
        // TODO 输出流信息
        /*
     "before": null,
    "after": {
        "source_table": "cart_info",
        "sink_table": null,
        "sink_columns": null,
        "sink_pk": null,
        "sink_extend": null
    },
    "source": {
        "version": "1.5.4.Final",
        "connector": "mysql",
        "name": "mysql_binlog_source",
        "ts_ms": 1660563022668,
        "snapshot": "false",
        "db": "gmall_config",
        "sequence": null,
        "table": "table_process",
        "server_id": 0,
        "gtid": null,
        "file": "",
        "pos": 0,
        "row": 0,
        "thread": null,
        "query": null
    },
    "op": "r",
    "ts_ms": 1660563022671,
    "transaction": null
}
        * */
        tableConfig.print("table_config>>>>>>>>>>>>");
        env.execute();
    }
}
