package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.bean
 * @ClassNmae:TradeOrderBean
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/28 23:37
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TradeOrderBean {
    // 窗口起始时间
    String stt;

    // 窗口关闭时间
    String edt;

    // 下单独立用户数
    Long orderUniqueUserCount;

    // 下单新用户数
    Long orderNewUserCount;

    // 时间戳
    Long ts;

}
