package com.atguigu.gmall.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author iw
 * @Package com.atguigu.gmall.realtime.util
 * @date 2022/8/30 13:51
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor = null;

    static {
        threadPoolExecutor = new ThreadPoolExecutor(5, 200, 5 * 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        return threadPoolExecutor;
    }
}
