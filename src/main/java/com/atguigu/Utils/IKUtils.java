package com.atguigu.Utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import org.wltea.analyzer.lucene.IKAnalyzer;
import org.wltea.analyzer.lucene.IKTokenizer;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @PackageName:com.atguigu.Utils
 * @ClassNmae:IKUtils
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/25 18:47
 */


public class IKUtils {

    public static List<String> getwords(String Keyword) throws IOException {

        ArrayList<String> list = new ArrayList<>();
        StringReader stringReader = new StringReader(Keyword);

        IKSegmenter ikSegmenter = new IKSegmenter(stringReader,true);
        Lexeme next =null ;
        next=ikSegmenter.next();

        while (next!=null){
            String word = next.getLexemeText();
            list.add(word);
            next=ikSegmenter.next();
        }

        return list;
    }
}
