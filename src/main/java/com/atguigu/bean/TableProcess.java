package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.bean
 * @ClassNmae:TableProcess
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/19 0:03
 */
@NoArgsConstructor
@AllArgsConstructor
@Data

public class TableProcess {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;

}
