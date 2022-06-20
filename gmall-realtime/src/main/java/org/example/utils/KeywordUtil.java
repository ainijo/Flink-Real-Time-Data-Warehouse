package org.example.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    // todo 使用 Hanlp 优化分词器
    public static List<String> splitKeyWord(String keyWord) throws IOException {
        // 创建集合存放结果数据
        ArrayList<String> resultList = new ArrayList<>();

        StringReader reader = new StringReader(keyWord);

        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        while (true) {
            Lexeme next = ikSegmenter.next();
            if (next != null) {
                String word = next.getLexemeText();
                resultList.add(word);
            } else {
                break;
            }
        }

        // 返回结果
        return resultList;
    }
}
