package com.github.caffeine.cache;

import java.util.concurrent.TimeUnit;

public class TimerWheelIntegrationTest {
    public static void main(String[] args) throws InterruptedException {
        // 配置：2秒过期，秒级精度
        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .expireAfterWrite(2, TimeUnit.SECONDS)
                .recordStats()
                .build();

        System.out.println("=== 测试分层时间轮 ===");

        // 测试1：正常过期
        cache.put("key1", "value1");
        System.out.println("Put key1 at " + System.currentTimeMillis());

        Thread.sleep(1000);
        System.out.println("After 1s: " + cache.getIfPresent("key1")); // 应存在

        Thread.sleep(1500); // 总共2.5秒
        System.out.println("After 2.5s: " + cache.getIfPresent("key1")); // 应null（已过期）

        // 测试2：不同过期时间的分层调度
        cache.put("key2", "value2"); // 2秒后过期
        System.out.println("\nPut key2 at " + System.currentTimeMillis());

        // 测试3：主动删除取消时间轮任务
        cache.put("key3", "value3");
        cache.invalidate("key3");
        Thread.sleep(2500);
        System.out.println("After invalidate: " + cache.getIfPresent("key3")); // 应null（不是过期）

        System.out.println("\nStats: " + cache.stats().hitRate());

        ((BoundedLocalCache<String, String>) cache).shutdownTimerWheel(); // 关闭调度线程
    }
}
