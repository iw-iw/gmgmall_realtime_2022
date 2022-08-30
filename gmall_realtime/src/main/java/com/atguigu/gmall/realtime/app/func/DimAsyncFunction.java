package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.DruidPhoenixDSUtil;
import com.atguigu.gmall.realtime.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.app.func
 * @date 2022/8/30 13:58
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    private DruidDataSource dataSource = null;
    private String tableName = null;
    ThreadPoolExecutor poolExecutor = null;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidPhoenixDSUtil.getDataSource();
        poolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        poolExecutor.submit(new Runnable() {
            @Override
            public void run() {

                try {
                    // 获取连接
                    DruidPooledConnection connection = dataSource.getConnection();
                    // 拼接sql
                    // select * from t where id = key
                    String id = getKey(input);
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                    // 读取对应维度表数据
                    // 合并数据到input
                    if (dimInfo != null) {
                        join(input, dimInfo);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.out.println("关联维度表出错");
                }
                // 输出input
                resultFuture.complete(Collections.singleton(input));
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        resultFuture.complete(Collections.singleton(input));
    }
}
