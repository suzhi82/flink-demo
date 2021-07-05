package com.abc.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Author: Cliff
 * Desc:  线程池工具类
 * 参考《一步一步分析RejectedExecutionException 异常》
 * https://cloud.tencent.com/developer/article/1491233
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor pool;

    /*
    获取单例的线程池对象
    corePoolSize: 指定了线程池中的线程数量
        它的数量决定了添加的任务是开辟新的线程去执行，还是放到workQueue 任务队列中去
    maximumPoolSize: 指定了线程池中的最大线程数量
        这个参数会根据你使用的workQueue 任务队列的类型，决定线程池会开辟的最大线程数量
    keepAliveTime: 当线程池中空闲线程数量超过corePoolSize 时，多余的线程会在多长时间内被销毁
    unit: keepAliveTime 的单位
    workQueue: 任务队列，被添加到线程池中，但尚未被执行的任务
    */
    public static ThreadPoolExecutor getInstance() {
        // 如果线程池不为空的话，是没有必要加锁的，所有多加一层判断
        if (pool == null) {
            // 加锁保证线程安全，同一时刻只会被某个线程所调用，确保单例
            // 可以在方法或代码块上加synchronized 锁，但不建议在方法上加
            // synchronized 锁住的是一个对象，由于这里调用的静态方法，所以得锁住类.class
            synchronized (ThreadPoolUtil.class) {
                if (pool == null) {
                    System.out.println("开辟I/O 线程池！！！！！");
                    pool = new ThreadPoolExecutor(
                            5,  // 线程池大小，超过该数值的线程用完后将被销毁
                            20,  // 线程池最大线程数
                            300,  // 超过线程池大小的线程多久销毁
                            TimeUnit.SECONDS,  // 销毁线程的时间单位
                            //new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)  // 无界的队列
                            new LinkedBlockingQueue<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return pool;
    }
}