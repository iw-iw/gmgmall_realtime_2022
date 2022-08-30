package com.atguigu.gmall.realtime.app.func;

import com.atguigu.gmall.realtime.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String str) {
        // 调用分词工具类
        List<String> list = KeywordUtil.analyze(str);
        for (String s : list) {
            // collect调用一次就是拆分一行  row里面的多数据就是多列
            collect(Row.of(s));
        }
    }
}
