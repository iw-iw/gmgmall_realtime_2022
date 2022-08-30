package com.atguigu.gmall.realtime.bean;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.bean
 * @date 2022/8/27 11:31
 */

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
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

