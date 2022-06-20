package org.example.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// 自定义一个线程池
public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor = null;
    private ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPool() {

        // 双重校验
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(
                            8,
                            16,
                            1L,
                            TimeUnit.MINUTES,
                            new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }

}
