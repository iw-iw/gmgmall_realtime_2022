package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidPhoenixDSUtil;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private DruidDataSource druidDataSource;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public MyBroadcastFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidPhoenixDSUtil.getDataSource();
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 1. 判断类型 如果是删除 从状态中去除对应的表
        JSONObject jsonObject = JSON.parseObject(value);
        if ("d".equals(jsonObject.getString("op"))) {
            // 获取 source_table
            String source_table = jsonObject.getJSONObject("before").getString("source_table");
            broadcastState.remove(source_table);
        } else {
            // 2 判断表格是否在phoenix中存在  不存在创建
            TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);
            String sinkTable = tableProcess.getSinkTable();
            String sinkColumns = tableProcess.getSinkColumns();
            String sinkPk = tableProcess.getSinkPk();
            String sinkExtend = tableProcess.getSinkExtend();
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);

            // 3 将内容写入状态
            broadcastState.put(tableProcess.getSourceTable(), tableProcess);
        }
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        // 创建表格不支持复合主键
        // 1. 拼接一个sql语句
        // create table if not exists db.sinkTable ( sinkColumn1 varchar primary key, sinkColumn2 varchar)
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists ").append(GmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append(" (");
        // 判断主键和扩展信息是否为空
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        String[] cols = sinkColumns.split(",");
        for (int i = 0; i < cols.length; i++) {
            if (cols[i].equals(sinkPk)) {
                sql.append(cols[i]).append(" varchar primary key ");

            } else {
                sql.append(cols[i]).append(" varchar ");
            }
            // 如果不是最后一个字段 添加逗号
            if (i < cols.length - 1) {
                sql.append(",");
            }
        }
        sql.append(" )").append(sinkExtend);

        System.out.println(sql.toString());

        // 2. 连接phoenix执行建表语句
        DruidPooledConnection connection = null;
        try {
            connection = druidDataSource.getConnection();
            PhoenixUtil.executeSql(sql.toString(), connection);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("从druid连接池获取连接异常");
        }

    }

    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 1 获取状态  判断当前表是否为维度表  不是删除
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String table = value.getString("table");
        TableProcess tableProcess = broadcastState.get(table);
        if (tableProcess != null) {
            // 当前的主流数据就是维度表数据
            // 2 过滤掉多余的字段  只保留sink_columns字段
            JSONObject data = value.getJSONObject("data");
            filterColumns(data, tableProcess.getSinkColumns());

            // 3 添加sink_table字段
            data.put("sink_table", tableProcess.getSinkTable());
            out.collect(data);
        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        // 保留下来sinkColumns中的字段,没有的删除
        List<String> cols = Arrays.asList(sinkColumns.split(","));
        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!cols.contains(next.getKey())) {
                iterator.remove();
            }

        }
        // data.entrySet().removeIf(next -> !cols.contains(next.getKey()));

    }
}
