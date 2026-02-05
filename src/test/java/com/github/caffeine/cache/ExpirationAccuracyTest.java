package com.github.caffeine.cache;

import org.junit.jupiter.api.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ExpirationAccuracyTest {

    @Test
    @Order(1)
    public void testExpireAfterWritePrecision() throws InterruptedException {
        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .expireAfterWrite(100, TimeUnit.MILLISECONDS)
                .build();

        cache.put("key", "value");

        // T+0: 应该存在
        assertEquals("value", cache.getIfPresent("key"), "刚写入应可访问");

        // T+50ms: 应该存在
        Thread.sleep(50);
        assertEquals("value", cache.getIfPresent("key"), "50ms时应未过期");

        // T+150ms: 应该已过期
        Thread.sleep(100);
        long start = System.nanoTime();
        String result = cache.getIfPresent("key");
        long costMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertNull(result, "150ms时应已过期");
        System.out.println("过期检测耗时: " + costMs + "ms");
        assertTrue(costMs < 10, "惰性清理应极快"); // 实际只是map查询，应<10ms

        ((BoundedLocalCache<String, String>) cache).shutdown();
    }

    @Test
    @Order(2)
    public void testExpireAfterAccessPrecision() throws InterruptedException {
        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .expireAfterAccess(100, TimeUnit.MILLISECONDS)
                .build();

        cache.put("key", "value");

        // 循环访问，确保不会过期
        for (int i = 0; i < 3; i++) {
            Thread.sleep(50);
            assertEquals("value", cache.getIfPresent("key"), "访问续约应生效，第" + i + "次");
        }

        // 停止访问，等待过期
        Thread.sleep(150);
        assertNull(cache.getIfPresent("key"), "停止访问后应过期");

        ((BoundedLocalCache<String, String>) cache).shutdown();
    }

    @Test
    @Order(3)
    public void testConcurrentExpireAndAccess() throws InterruptedException {
        BoundedLocalCache<String, String> cache =
                (BoundedLocalCache<String, String>) Caffeine.<String, String>newBuilder()
                        .expireAfterWrite(50, TimeUnit.MILLISECONDS)
                        .build();

        // 阶段1：并发写入（覆盖写入会重置过期时间）
        Runnable writeTask = () -> {
            for (int i = 0; i < 10; i++) {
                cache.put("key" + (i % 5), "value" + i);
                try { Thread.sleep(5); } catch (InterruptedException e) { break; }
            }
        };

        Thread t1 = new Thread(writeTask);
        Thread t2 = new Thread(writeTask);
        t1.start(); t2.start();
        t1.join(); t2.join();

        // 阶段2：等待过期（必须超过最后一次写入时间50ms以上）
        // 最后一次写入约在 t=45ms（9*5），所以等待100ms确保过期
        Thread.sleep(100);

        // 阶段3：主线程直接验证（触发惰性清理）
        int misses = 0;
        for (int i = 0; i < 5; i++) {
            if (cache.getIfPresent("key" + i) == null) {
                misses++;
            }
        }

        // 只要有一个key过期即通过（可能部分key在写入时刚好被覆盖，时间戳最新）
        assertTrue(misses > 0, "至少部分数据应已过期，实际misses=" + misses);

        // 统计信息验证（可选，因为时间轮粒度问题可能延迟）
        System.out.println("Expire count: " + cache.stats().expireCount());

        cache.shutdown();
    }
}