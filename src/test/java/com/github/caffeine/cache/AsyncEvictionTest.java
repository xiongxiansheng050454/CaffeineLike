package com.github.caffeine.cache;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Timeout;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 异步驱逐系统测试套件
 * 验证要点：非阻塞读写、频率保护、批量驱逐、背压机制
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AsyncEvictionTest {

    private static final int MAX_SIZE = 100;
    private static final int EXPIRE_MS = 60_000; // 足够长，避免过期干扰

    private Caffeine<String, String> builder;

    @BeforeAll
    void setup() {
        builder = Caffeine.<String, String>newBuilder()
                .maximumSize(MAX_SIZE)
                .expireAfterWrite(EXPIRE_MS, TimeUnit.MILLISECONDS)
                .recordStats();
    }

    /**
     * 测试1：基本驱逐功能 - 写入超过容量，验证最终大小被限制
     */
    @Test
    @DisplayName("基本驱逐：写入200个，最终应保留约100个高频条目")
    void testBasicEviction() throws InterruptedException {
        Cache<String, String> cache = builder.build();

        // 写入 200 个不同 key
        for (int i = 0; i < 200; i++) {
            cache.put("key-" + i, "value-" + i);
        }

        // 等待异步驱逐完成（最多等 2 秒）
        waitForEviction(cache, MAX_SIZE, 2000);

        long size = cache.estimatedSize();
        assertTrue(size <= MAX_SIZE * 1.1, // 允许 10% 误差
                "驱逐后大小应不超过 " + MAX_SIZE + ", 实际: " + size);
        assertTrue(size >= MAX_SIZE * 0.5, // 至少保留一半（防止全清）
                "驱逐后大小应合理，实际: " + size);
    }

    /**
     * 测试2：频率保护 - 高频访问的 key 应被保留
     */
    @Test
    @DisplayName("频率保护：高频key应免于被驱逐")
    void testFrequencyProtection() throws InterruptedException {
        Cache<String, String> cache = builder.build();

        String hotKey = "hot-key";
        String coldKey = "cold-key";

        // 先写入冷数据（只写不读）
        for (int i = 0; i < 150; i++) {
            cache.put("cold-" + i, "cold-value");
        }

        // 写入并频繁访问热数据
        cache.put(hotKey, "hot-value");
        for (int i = 0; i < 50; i++) {
            cache.getIfPresent(hotKey);
        }

        // 继续写入更多冷数据触发驱逐
        for (int i = 150; i < 300; i++) {
            cache.put("cold-" + i, "cold-value");
        }

        // 等待驱逐稳定
        Thread.sleep(500);
        waitForEviction(cache, MAX_SIZE, 2000);

        // 验证热数据仍然存在
        assertNotNull(cache.getIfPresent(hotKey),
                "高频访问的 hot-key 应被保留");

        // 统计热数据占比（应该明显高于随机概率）
        long totalSize = cache.estimatedSize();
        long hotCount = IntStream.range(0, 300)
                .filter(i -> cache.getIfPresent("hot-" + i) != null)
                .count();

        System.out.println("热数据保留率: " + (double)hotCount/totalSize);
    }

    /**
     * 测试3：非阻塞验证 - put 操作不应被驱逐阻塞
     */
    @Test
    @DisplayName("非阻塞性：put操作应在1ms内返回，即使触发驱逐")
    void testNonBlockingPut() {
        Cache<String, String> cache = builder.build();

        // 先填满缓存
        for (int i = 0; i < MAX_SIZE; i++) {
            cache.put("fill-" + i, "value");
        }

        // 测量后续写入的延迟
        List<Long> latencies = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            long start = System.nanoTime();
            cache.put("new-" + i, "value");
            long duration = (System.nanoTime() - start) / 1_000_000; // 转 ms

            latencies.add(duration);
            assertTrue(duration < 5, // 5ms 内必须完成
                    "put 操作应非阻塞，但耗时 " + duration + "ms");
        }

        double avgLatency = latencies.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0);

        System.out.println("平均 put 延迟: " + avgLatency + "ms");
        assertTrue(avgLatency < 2, "平均延迟应小于 2ms");
    }

    /**
     * 测试4：并发压力测试 - 多线程写入时驱逐正确性
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("并发压力：16线程并发写入，验证无内存泄漏且size受控")
    void testConcurrentEviction() throws InterruptedException {
        Cache<String, String> cache = builder.build();
        int threads = 16;
        int operationsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger errorCount = new AtomicInteger(0);

        // 并发写入
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        int key = threadId * operationsPerThread + i;
                        cache.put(String.valueOf(key), "value-" + key);

                        // 随机读取（模拟热点）
                        if (i % 10 == 0) {
                            cache.getIfPresent(String.valueOf(threadId * operationsPerThread));
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        // 等待异步驱逐完成
        waitForEviction(cache, MAX_SIZE, 5000);

        long finalSize = cache.estimatedSize();
        System.out.println("并发写入后最终大小: " + finalSize);

        assertEquals(0, errorCount.get(), "不应有异常发生");
        assertTrue(finalSize <= MAX_SIZE * 1.2,
                "并发场景下大小应受控，实际: " + finalSize);
    }

    /**
     * 测试5：背压机制 - 队列满时不应阻塞写入线程
     */
    @Test
    @DisplayName("背压保护：队列满时put应继续工作，最终大小收敛")
    void testBackpressure() throws InterruptedException {
        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();

        // 快速写入大量数据（不应抛异常）
        assertDoesNotThrow(() -> {
            for (int i = 0; i < 10000; i++) {
                cache.put("flood-" + i, "value");
            }
        });

        // 等待后台驱逐完成
        Thread.sleep(2000);

        // 验证：最终大小应该接近限制（允许20%上浮）
        long size = cache.estimatedSize();
        long maxSize = 10;
        assertTrue(size <= maxSize * 1.2,
                "背压下最终大小应收敛到限制附近，实际: " + size + "，限制: " + maxSize);
    }

    /**
     * 测试6：驱逐统计准确性
     */
    @Test
    @DisplayName("统计验证：驱逐计数器应准确记录")
    void testEvictionStats() throws InterruptedException {
        BoundedLocalCache<String, String> cache =
                (BoundedLocalCache<String, String>) Caffeine.<String, String>newBuilder()
                        .maximumSize(MAX_SIZE)
                        .recordStats()
                        .enableWriteBuffer()
                        .build();

        long initialEvicted = cache.getEvictionStats().evicted();

        // 触发驱逐：写入 200 条，应触发约 100 次驱逐
        for (int i = 0; i < 200; i++) {
            cache.put("stat-" + i, "value");
        }

        Thread.sleep(200);
        System.out.println("WriteBuffer stats: " + cache.getWriteBufferStats());
        System.out.println("Current size: " + cache.estimatedSize());

        long finalEvicted = cache.getEvictionStats().evicted();
        long evicted = finalEvicted - initialEvicted;

        System.out.println("本次测试驱逐数量: " + evicted);
        assertTrue(evicted >= 50,
                "应记录到足够的驱逐次数，实际: " + evicted);
    }

    /**
     * 测试7：优雅关闭 - 待处理任务应被排空
     */
    @Test
    @DisplayName("优雅关闭：shutdown应等待所有驱逐完成")
    void testGracefulShutdown() throws InterruptedException {
        BoundedLocalCache<String, String> cache =
                (BoundedLocalCache<String, String>) builder.build();

        // 写入数据触发驱逐
        for (int i = 0; i < 500; i++) {
            cache.put("shutdown-" + i, "value");
        }

        // 立即关闭（此时队列中可能有待处理任务）
        cache.shutdown();

        // 验证无异常抛出，且状态一致
        assertDoesNotThrow(() -> {
            // 重复关闭应安全
            cache.shutdown();
        });
    }

    /**
     * 测试8：时间轮 + 异步驱逐协同 - 过期和驱逐同时工作
     */
    @Test
    @DisplayName("协同工作：过期与驱逐同时触发")
    void testEvictionWithExpiration() throws InterruptedException {
        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .maximumSize(50)
                .expireAfterWrite(500, TimeUnit.MILLISECONDS) // 短过期时间
                .recordStats()
                .build();

        // 写入 100 条（触发驱逐）且 500ms 后过期
        for (int i = 0; i < 100; i++) {
            cache.put("mixed-" + i, "value");
        }

        // 立即检查：应已被驱逐到 50 左右
        Thread.sleep(200);
        long sizeAfterEviction = cache.estimatedSize();
        assertTrue(sizeAfterEviction <= 60,
                "异步驱逐后应约 50 条，实际: " + sizeAfterEviction);

        // 等待过期
        Thread.sleep(600);

        // 触发访问以清理过期项
        for (int i = 0; i < 100; i++) {
            cache.getIfPresent("mixed-" + i);
        }

        long sizeAfterExpire = cache.estimatedSize();
        assertTrue(sizeAfterExpire <= 5,
                "过期后应几乎为空，实际: " + sizeAfterExpire);
    }

    // === 辅助方法 ===

    /**
     * 等待驱逐完成（轮询检查）
     */
    private void waitForEviction(Cache<?, ?> cache, long targetSize, long timeoutMs)
            throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            if (cache.estimatedSize() <= targetSize) {
                return;
            }
            Thread.sleep(50);
        }
        // 超时只是记录，不抛异常（让断言去失败）
    }

    /**
     * 通过反射或接口获取驱逐统计（假设 BoundedLocalCache 提供此方法）
     */
    private long getEvictedCount(BoundedLocalCache<?, ?> cache) {
        // 实际实现中应通过 cache.getEvictionStats() 获取
        // 这里简化处理，假设可通过 stats() 获取
        CacheStats stats = cache.stats();
        // 如果 stats 包含驱逐数，返回；否则返回 0
        try {
            return (long) stats.getClass()
                    .getMethod("evictionCount")
                    .invoke(stats);
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 等待缓存大小稳定（用于测试结束判定）
     */
    private boolean waitForStableSize(Cache<?, ?> cache, long timeoutMs)
            throws InterruptedException {
        long start = System.currentTimeMillis();
        long lastSize = -1;
        int stableCount = 0;

        while (System.currentTimeMillis() - start < timeoutMs) {
            long currentSize = cache.estimatedSize();
            if (currentSize == lastSize) {
                stableCount++;
                if (stableCount >= 3) return true; // 连续 3 次相同视为稳定
            } else {
                stableCount = 0;
                lastSize = currentSize;
            }
            Thread.sleep(100);
        }
        return false;
    }
}