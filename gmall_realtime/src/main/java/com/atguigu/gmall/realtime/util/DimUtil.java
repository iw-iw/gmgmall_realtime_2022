package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.util
 * @date 2022/8/28 16:03
 */
public class DimUtil {
    // select * from t where id = 'v.id' and user_id = '10'
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) {
        // 优先读取redis
        Jedis jedis = null;
        String redisKey = "Dim:" + tableName + ":" + id;
        String value = null;
        // 获取jedis连接
        try {
            jedis = JedisUtil.getJedis();
            value = jedis.get(redisKey);
        } catch (Exception e) {
            System.out.println("jedis异常");
        }
        // 判断redis 中是否存在redisKey 如果不存在，则去查询phoenix 写入redis
        // 如果存在 则直接返回查询的数据
        JSONObject result = null;
        if (value != null) {
            result = JSON.parseObject(value);
        } else {
            List<JSONObject> dimInfo = getDimInfo(connection, tableName, new Tuple2<>("ID", id));
            if (dimInfo.size() > 0) {
                result = dimInfo.get(0);
                // 写入redis
                jedis.setex(redisKey, 3600 * 24, result.toJSONString()); // 主键 存活时间TTL 数据

            }
        }
        if (jedis != null) {
            jedis.close();
        }
        try {
            if (connection != null ){
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static List<JSONObject> getDimInfo(Connection conn, String tableName, Tuple2<String, String>... tuple2s) {
        StringBuilder sql = new StringBuilder();
        sql.append("select * from ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(tableName)
                .append(" where ");
        for (int i = 0; i < tuple2s.length; i++) {
            sql.append(tuple2s[i].f0)
                    .append(" = '")
                    .append(tuple2s[i].f1)
                    .append("'");
            if (i < tuple2s.length - 1) {
                sql.append(" and ");
            }
        }


        // System.out.println(sql);
        return PhoenixUtil.sqlQuery(conn, sql.toString(), JSONObject.class);

    }

    public static void deleteRedisCache(String tableName, String id) {
        String redisKey = "DIM:" + tableName + ":" + id;
        try {
            Jedis jedis = JedisUtil.getJedis();
            jedis.del(redisKey);
            jedis.close();
        } catch (Exception e) {
            System.out.println("jedis连接异常");
        }
    }
}
