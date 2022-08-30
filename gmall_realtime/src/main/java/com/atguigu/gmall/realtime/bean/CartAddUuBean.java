package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.bean
 * @date 2022/8/26 19:17
 */
@NoArgsConstructor
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口开始时间
    String Stt;
    // 窗口结束时间
    String Ent;
    // 加购独立用户数
    Long cartAddUuCt;

    // 时间戳
    Long ts;

}
