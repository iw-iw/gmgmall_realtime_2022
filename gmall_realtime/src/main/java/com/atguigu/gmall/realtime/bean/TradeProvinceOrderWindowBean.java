package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.bean
 * @date 2022/8/29 18:53
 */
@Builder
@AllArgsConstructor
@Data
@NoArgsConstructor
public class TradeProvinceOrderWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 省份 ID
    String provinceId;

    // 省份名称 默认为空
    @Builder.Default
    String provinceName = "";

    // 累计下单次数
    Long orderCount;

    // 累计下单金额
    Double orderAmount;

    // 时间戳
    Long ts;

}
