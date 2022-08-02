package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.bean
 * @ClassNmae:UserLoginBean
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/28 18:45
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLoginBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 回流用户数
    Long backCt;

    // 独立用户数
    Long uuCt;

    // 时间戳
    Long ts;

}
