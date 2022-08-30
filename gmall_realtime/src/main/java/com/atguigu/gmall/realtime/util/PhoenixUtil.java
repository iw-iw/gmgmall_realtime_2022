package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {
    public static void executeSql(String sql, Connection conn) {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(preparedStatement, conn);
        }

    }

    private static void close(PreparedStatement preparedStatement, Connection conn) {
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static <T> List<T> sqlQuery(Connection conn, String sql, Class<T> clz) {
        ArrayList<T> result = new ArrayList<>();

        // 如果给泛型的对象添加对应的属性
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            // 获取一行数据
            while (rs.next()) {
                // 获取对应的元数据  即列名
                T t = clz.newInstance();
                ResultSetMetaData metaData = rs.getMetaData();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    String value = rs.getString(i);
                    // 把_分割的大写的列名转换为小驼峰的属性名称
                    String propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                    BeanUtils.setProperty(t, propertyName, value);
                }
                System.out.println(t);
                result.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("读取维度表数据错误");
        } finally {
            close(ps, conn);
        }
        return result;
    }
}
