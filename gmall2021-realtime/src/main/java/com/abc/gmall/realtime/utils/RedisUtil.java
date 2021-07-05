package com.abc.gmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Author: Cliff
 * Desc:  通过连接池获取 Jedis 的工具类
 */
public class RedisUtil {
    private static JedisPool jedisPool = null;

    public static Jedis getJedis() {
        // 如果jedisPool 不为空，则返回该对象，不需要加同步锁
        if (jedisPool == null) {
            // 由于是静态方法返回静态对象，所以只能在类.class 上加同步锁
            // 多线程中，一次只能有一个线程进入以下代码，确保jedisPool 对象为单例
            synchronized (RedisUtil.class) {
                if (jedisPool == null) {
                    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
                    jedisPoolConfig.setMaxTotal(100);  // 最大可用连接数
                    jedisPoolConfig.setBlockWhenExhausted(true);  // 连接耗尽是否等待
                    jedisPoolConfig.setMaxWaitMillis(2000);  // 等待时间
                    jedisPoolConfig.setMaxIdle(5);  // 最大闲置连接数
                    jedisPoolConfig.setMinIdle(5);  // 最小闲置连接数
                    jedisPoolConfig.setTestOnBorrow(true);  // 取连接的时候进行一下测试 ping pong
                    jedisPool = new JedisPool(jedisPoolConfig, "hadoop114", 6379, 1000);
                    System.out.println("开辟Redis 连接池");
                }
            }
        } else {
            System.out.println("Redis 连接池：" + jedisPool.getNumActive());
        }
        return jedisPool.getResource();
    }
}