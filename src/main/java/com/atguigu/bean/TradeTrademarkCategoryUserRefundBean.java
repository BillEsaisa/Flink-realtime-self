package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

/**
 * @PackageName:com.atguigu.bean
 * @ClassNmae:TradeTrademarkCategoryUserRefundBean
 * @Dscription
 * @author:Esaisa
 * @date:2022/8/1 15:01
 */

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class TradeTrademarkCategoryUserRefundBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 品牌 ID
    String trademarkId;
    // 品牌名称
    String trademarkName;
    // 一级品类 ID
    String category1Id;
    // 一级品类名称
    String category1Name;
    // 二级品类 ID
    String category2Id;
    // 二级品类名称
    String category2Name;
    // 三级品类 ID
    String category3Id;
    // 三级品类名称
    String category3Name;

    //退单金额
    Double refundAmount;

    // sku_id
    @TransientSink
    String skuId;

    // 用户 ID
    String userId;
    // 退单次数
    Long refundCount;
    // 时间戳
    Long ts;

}
