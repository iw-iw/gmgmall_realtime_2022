package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.bean
 * @date 2022/8/26 11:23
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
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
