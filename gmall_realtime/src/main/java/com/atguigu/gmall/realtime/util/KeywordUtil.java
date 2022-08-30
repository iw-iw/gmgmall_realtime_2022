package com.atguigu.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> analyze(String keyword) {
        ArrayList<String> result = new ArrayList<>();
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
        Lexeme next = null;
        try {
            while ((next = ikSegmenter.next()) != null) {
                result.add(next.getLexemeText());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) {
        System.out.println(analyze("大数据学科"));
    }
}
