package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.bean
 * @ClassNmae:KeyWordBean
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/25 21:14
 */

@AllArgsConstructor
@Data@NoArgsConstructor
public class KeyWordBean {
    // 窗口起始时间
    private String stt;

    // 窗口闭合时间
    private String edt;

    // 关键词来源
    private String source;

    // 关键词
    private String keyword;

    // 关键词出现频次
    private Long keyword_count;

    // 时间戳
    private Long ts;


}
