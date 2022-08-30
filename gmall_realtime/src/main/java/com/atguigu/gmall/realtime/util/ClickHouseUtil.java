package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
                    @SneakyThrows  // 这个注解是告诉程序这里会涉及权限问题
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        // 反射
                        Class<?> aClass = t.getClass();
                        //stt,edt,ts,source,keyword,keywordCount
                        int offset = 0;
                        Field[] fields = aClass.getDeclaredFields();
                        for (int i = 0; i < fields.length; i++) {
                            // 正常调用属性
                            // String s = t.属性

                            fields[i].setAccessible(true);
                            TransientSink annotation = fields[i].getAnnotation(TransientSink.class);
                            // 如果调用到注解 则证明这个字段需要排除  排除之后需要考虑索引变化
                            if (annotation != null) {
                                offset++;
                                continue;
                            }
                            Object o = fields[i].get(t);
                            preparedStatement.setObject(i + 1 - offset, o);
                        }
                    }
                }, JdbcExecutionOptions.builder()
                        // 批大小
                        .withBatchSize(10)
                        // 最小等待时间
                        .withBatchIntervalMs(100)
                        //重试次数
                        .withMaxRetries(3)
                        .build(), new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .build());
    }
}
