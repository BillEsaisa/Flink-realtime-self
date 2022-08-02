package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.bean
 * @ClassNmae:CartAddUuBean
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/28 20:09
 */

@AllArgsConstructor
@Data
@NoArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;

    // 窗口闭合时间
    String edt;

    // 加购独立用户数
    Long cartAddUuCt;

    // 时间戳
    Long ts;

}
