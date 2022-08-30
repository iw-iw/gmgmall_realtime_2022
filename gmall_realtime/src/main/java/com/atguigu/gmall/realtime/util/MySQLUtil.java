package com.atguigu.gmall.realtime.util;

public class MySQLUtil {

    public static String getBaseDicDDL() {
        return "CREATE TEMPORARY TABLE base_dic (\n" +
                "`dic_code` String,\n" +
                "`dic_name` String,\n" +
                "`parent_code` String,\n" +
                "`create_time` String,\n" +
                "`operate_time` String\n" +
                ")" + getMysqlDDL("base_dic");
    }

    public static String getMysqlDDL(String tableName) {
        return "WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "  'table-name' = '" + tableName + "'\n" +
                ")";
    }
}
