package com.atguigu.App.func;

import com.atguigu.Utils.IKUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @PackageName:com.atguigu.App.func
 * @ClassNmae:DWS_SplitKeyWordFunc
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/25 19:33
 */

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class DWS_SplitKeyWordFunc  extends TableFunction<Row> {
    public void eval(String KeyWord) {
        List<String> words = null;
        try {
            words = IKUtils.getwords(KeyWord);
            for (String word : words) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
          collect(Row.of(KeyWord));
        }

    }

}
