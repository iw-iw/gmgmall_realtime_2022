package com.atguigu.gmall.realtime.bean;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.bean
 * @date 2022/8/26 21:29
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
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
