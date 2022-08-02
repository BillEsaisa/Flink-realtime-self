package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.bean
 * @ClassNmae:TradePaymentWindowBean
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/28 21:22
 */

@NoArgsConstructor
@AllArgsConstructor
@Data
public class TradePaymentWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 支付成功独立用户数
    Long paymentSucUniqueUserCount;

    // 支付成功新用户数
    Long paymentSucNewUserCount;

    // 时间戳
    Long ts;

}
