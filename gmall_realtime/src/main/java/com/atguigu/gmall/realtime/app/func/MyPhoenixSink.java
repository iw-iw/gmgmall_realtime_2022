package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.DruidPhoenixDSUtil;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import com.sun.org.apache.bcel.internal.generic.I2F;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;


public class MyPhoenixSink extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidPhoenixDSUtil.getDataSource();

    }

    @Override
    public void invoke(JSONObject value, Context context) {
        // 如果维度表的数据类型为update删除掉redis中对应的缓存数据  保持数据一致性
        if (value.getString("type").equals("update")){
            DimUtil.deleteRedisCache(value
                    .getString("sink_table")
                    .toUpperCase(),value.getString("id"));
        }
        value.remove("type");

        // 拼接sql 写入到phoenix
        String sql = createUpsertSQL(value);
        // 使用连接执行sql
        try {
            DruidPooledConnection connection = druidDataSource.getConnection();
            PhoenixUtil.executeSql(sql, connection);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("druid获取连接异常");
        }

    }

    private String createUpsertSQL(JSONObject jsonObject) {
        // upsert into db.sink_table (columns) values (值)
        String sink_table = jsonObject.getString("sink_table");
        StringBuilder sql = new StringBuilder();
        jsonObject.remove("sink_table");
        Set<String> cols = jsonObject.keySet();
        Collection<Object> values = jsonObject.values();
        sql.append("upsert into")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sink_table)
                .append("(")
                .append(StringUtils.join(cols, ","))
                .append(") values ('")
                .append(StringUtils.join(values, "','"))
                .append("')");

        return sql.toString();

    }
}
