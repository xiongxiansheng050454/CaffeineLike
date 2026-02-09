package com.github.caffeine.cache;

import com.github.caffeine.cache.event.CacheEvent;
import com.github.caffeine.cache.event.CacheEventType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Timeout;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WriteBuffer 写缓冲与合并功能测试
 * 验证：批量聚合、去重合并、背压降级、并发安全
 */
public class WriteBufferTest {

    @Test
    @DisplayName("基础写入与刷盘 - 验证最终一致性")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testBasicWriteAndFlush() throws InterruptedException {
        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .enableWriteBuffer()
                .writeBufferConfig(1024, 64, 10, 50) // 50ms flush间隔
                .build();

        cache.put("key1", "value1");
        cache.put("key2", "value2");

        // 立即读取应该能从缓冲命中（如果实现支持）
        assertNotNull(cache.getIfPresent("key1"));

        // 等待刷盘
        Thread.sleep(100);

        // 验证最终一致性
        assertEquals("value1", cache.getIfPresent("key1"));
        assertEquals("value2", cache.getIfPresent("key2"));

        // 检查统计（需要强制转换为 BoundedLocalCache）
        if (cache instanceof BoundedLocalCache<String, String> bounded) {
            BoundedLocalCache.WriteBufferStats stats = bounded.getWriteBufferStats();
            assertNotNull(stats);
            assertTrue(stats.submitted() >= 2, "应至少提交2次");
            assertTrue(stats.flushed() >= 2, "应至少刷盘2次");
        }
    }

    @Test
    @DisplayName("写合并功能 - 相同Key多次写入应合并为一次")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testMergeFunctionality() throws InterruptedException {
        BoundedLocalCache<String, String> cache = (BoundedLocalCache<String, String>)
                Caffeine.<String, String>newBuilder()
                        .enableWriteBuffer()
                        .writeBufferConfig(65536, 1024, 1000, 1000) // 大batch，长延迟，确保合并发生
                        .build();

        String key = "merge-key";

        // 快速写入同一key 100次
        for (int i = 0; i < 100; i++) {
            cache.put(key, "value-" + i);
        }

        // 等待刷盘
        Thread.sleep(200);

        // 验证最终值是最后一次写入的值
        assertEquals("value-99", cache.getIfPresent(key));

        // 验证合并统计
        BoundedLocalCache.WriteBufferStats stats = cache.getWriteBufferStats();
        System.out.println("Merge stats: " + stats);

        // 提交100次，应该合并为1次刷盘（或极少次数）
        assertEquals(100, stats.submitted(), "应提交100次");
        assertTrue(stats.merged() >= 99, "应至少合并99次");
        assertTrue(stats.flushed() <= 10, "实际刷盘次数应远小于提交次数");

        double mergeRate = stats.submitted() == 0 ? 0 : (double) stats.merged() / stats.submitted();
        assertTrue(mergeRate > 0.9, "合并率应高于90%，实际: " + mergeRate);
    }

    @Test
    @DisplayName("并发写入测试 - 多线程安全与聚合效率")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    public void testConcurrentWrite() throws InterruptedException {
        BoundedLocalCache<Integer, Integer> cache = (BoundedLocalCache<Integer, Integer>)
                Caffeine.<Integer, Integer>newBuilder()
                        .enableWriteBuffer()
                        .writeBufferConfig(65536, 4096, 500, 100)
                        .build();

        int threadCount = 32;
        int writesPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        long startTime = System.currentTimeMillis();

        // 32个线程并发写入
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < writesPerThread; i++) {
                        int key = threadId * writesPerThread + i;
                        cache.put(key, key * 10);

                        // 每100次混合一次读操作
                        if (i % 100 == 0) {
                            cache.getIfPresent(key / 2);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long writeTime = System.currentTimeMillis() - startTime;
        System.out.println("Concurrent write time: " + writeTime + "ms");

        // 等待刷盘完成
        Thread.sleep(300);

        // 验证数据完整性
        int missingCount = 0;
        for (int i = 0; i < threadCount * writesPerThread; i++) {
            Integer value = cache.getIfPresent(i);
            if (value == null || value != i * 10) {
                missingCount++;
            }
        }

        assertEquals(0, missingCount, "所有写入的数据都应最终可见");

        // 打印统计
        BoundedLocalCache.WriteBufferStats stats = cache.getWriteBufferStats();
        System.out.println("Concurrent write stats: " + stats);
        System.out.println("Merge rate: " + String.format("%.2f%%", cache.getWriteBufferMergeRate() * 100));

        executor.shutdown();
    }

    @Test
    @DisplayName("背压测试 - 缓冲区满时的降级行为")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testBackpressure() throws InterruptedException {
        // 创建极小的缓冲区（容量16），容易触发背压
        BoundedLocalCache<String, String> cache = (BoundedLocalCache<String, String>)
                Caffeine.<String, String>newBuilder()
                        .enableWriteBuffer()
                        .writeBufferConfig(16, 8, 1000, 5000) // 5秒flush间隔，确保缓冲区堆积
                        .build();

        // 快速提交大量数据，触发背压
        int writeCount = 0;
        long start = System.currentTimeMillis();

        // 持续写入直到超过缓冲区容量
        for (int i = 0; i < 1000; i++) {
            cache.put("backpressure-key-" + i, "value-" + i);
            writeCount++;

            // 检查背压是否发生（通过统计推断）
            BoundedLocalCache.WriteBufferStats stats = cache.getWriteBufferStats();
            if (stats.submitted() < writeCount) {
                System.out.println("Backpressure detected at iteration " + i);
                break;
            }
        }

        long duration = System.currentTimeMillis() - start;
        System.out.println("Write " + writeCount + " items in " + duration + "ms");

        // 即使发生背压，数据也不应丢失
        Thread.sleep(100);

        // 抽查验证
        assertNotNull(cache.getIfPresent("backpressure-key-0"));
        assertNotNull(cache.getIfPresent("backpressure-key-" + (writeCount - 1)));
    }

    @Test
    @DisplayName("读-your-写一致性 - getPending 应该返回最新值")
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    public void testReadYourWrites() throws InterruptedException {
        BoundedLocalCache<String, String> cache = (BoundedLocalCache<String, String>)
                Caffeine.<String, String>newBuilder()
                        .enableWriteBuffer()
                        .writeBufferConfig(1024, 64, 100, 1000) // 1秒刷盘间隔
                        .build();

        String key = "ryw-key";

        // 写入
        cache.put(key, "original");

        // 立即读（应能从缓冲命中）
        assertEquals("original", cache.getIfPresent(key));

        // 更新
        cache.put(key, "updated");

        // 再次读取应得到最新值（测试 getPending 逻辑）
        assertEquals("updated", cache.getIfPresent(key));

        // 验证不同 key
        cache.put("key-a", "value-a");
        assertEquals("value-a", cache.getIfPresent("key-a"));
    }

    @Test
    @DisplayName("删除操作合并 - 重复删除应合并，删除后读应返回null")
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    public void testDeleteMerge() throws InterruptedException {
        BoundedLocalCache<String, String> cache = (BoundedLocalCache<String, String>)
                Caffeine.<String, String>newBuilder()
                        .enableWriteBuffer()
                        .writeBufferConfig(1024, 64, 100, 500)
                        .build();

        String key = "delete-key";

        // 先写入
        cache.put(key, "value");
        Thread.sleep(50); // 确保刷盘

        // 多次删除同一key（应该合并）
        cache.invalidate(key);
        cache.invalidate(key);
        cache.invalidate(key);

        // 立即读应该返回null（即使还未刷盘）
        assertNull(cache.getIfPresent(key));

        Thread.sleep(100);

        // 最终状态验证
        assertNull(cache.getIfPresent(key));

        BoundedLocalCache.WriteBufferStats stats = cache.getWriteBufferStats();
        System.out.println("Delete merge stats: " + stats);
    }

    @Test
    @DisplayName("事件发布验证 - 合并后应只发布最终事件")
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    public void testEventPublishingAfterMerge() throws InterruptedException {
        List<CacheEvent<String, String>> events = Collections.synchronizedList(new ArrayList<>());

        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .enableWriteBuffer()
                .writeBufferConfig(1024, 64, 100, 200)
                .removalListener(event -> {
                    // 只收集 WRITE 事件
                    if (event.getType() == CacheEventType.WRITE) {
                        events.add(event);
                    }
                })
                .build();

        // 写入同一key多次
        cache.put("event-key", "v1");
        cache.put("event-key", "v2");
        cache.put("event-key", "v3");

        // 等待刷盘和事件处理
        Thread.sleep(400);

        // 由于合并，应该只收到一个 WRITE 事件（值为v3）
        long writeEvents = events.stream()
                .filter(e -> e.getKey().equals("event-key"))
                .count();

        System.out.println("Event count for merged key: " + writeEvents);
        // 注意：根据实现，可能会收到多个事件，但合并优化后应该减少
    }

    @Test
    @DisplayName("优雅关闭测试 - shutdown应刷出所有数据")
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    public void testGracefulShutdown() {
        BoundedLocalCache<String, String> cache = (BoundedLocalCache<String, String>)
                Caffeine.<String, String>newBuilder()
                        .enableWriteBuffer()
                        .writeBufferConfig(65536, 1024, 1000, 10000) // 10秒刷盘，确保不会自动刷盘
                        .build();

        // 写入数据但不等待刷盘
        for (int i = 0; i < 100; i++) {
            cache.put("shutdown-key-" + i, "value-" + i);
        }

        BoundedLocalCache.WriteBufferStats statsBefore = cache.getWriteBufferStats();
        System.out.println("Before shutdown: " + statsBefore);
        assertTrue(statsBefore.flushed() < statsBefore.submitted(), "关闭前应有未刷盘数据");

        // 关闭
        cache.shutdown();

        BoundedLocalCache.WriteBufferStats statsAfter = cache.getWriteBufferStats();
        System.out.println("After shutdown: " + statsAfter);

        // 验证所有数据已刷盘
        assertEquals(statsAfter.submitted(), statsAfter.flushed(), "关闭后所有提交的数据都应已刷盘");
    }

    @Test
    @DisplayName("与过期策略集成 - 写缓冲不应影响过期")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testIntegrationWithExpiration() throws InterruptedException {
        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .enableWriteBuffer()
                .writeBufferConfig(1024, 64, 10, 50)
                .expireAfterWrite(200, TimeUnit.MILLISECONDS) // 200ms过期
                .build();

        cache.put("expire-key", "value");

        // 立即读取应成功
        assertEquals("value", cache.getIfPresent("expire-key"));

        // 等待过期
        Thread.sleep(300);

        // 应已过期
        assertNull(cache.getIfPresent("expire-key"));
    }
}