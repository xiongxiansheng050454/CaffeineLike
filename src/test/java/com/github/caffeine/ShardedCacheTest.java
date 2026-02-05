package com.github.caffeine;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ShardedCacheTest {
    public static void main(String[] args) throws InterruptedException {
        // 根据CPU核心数创建缓存（Caffeine默认也是基于可用处理器）
        int nCpu = Runtime.getRuntime().availableProcessors();
        ShardedLocalCache<String, String> cache = new ShardedLocalCache<>(nCpu);

        System.out.println("CPU Cores: " + nCpu + ", Segments: " + cache.getSegmentCount());

        // 验证哈希定位：打印key的分布
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            int hash = CacheUtils.spread(key.hashCode());
            int segIndex = hash & (cache.getSegmentCount() - 1);
            System.out.printf("Key[%s] hash=%d → Segment[%d]%n", key, hash, segIndex);
        }

        // Step 5: 并发安全测试
        System.out.println("\n=== 并发测试 ===");
        int threads = 100;
        int opsPerThread = 1000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger successCount = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "concurrent-key-" + (threadId * opsPerThread + i);
                        cache.put(key, "value-" + i);
                        String val = cache.get(key);
                        if (val != null) successCount.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();

        System.out.println("Total operations: " + (threads * opsPerThread));
        System.out.println("Success reads: " + successCount.get());
        System.out.println("Cache size: " + cache.size());

        // 验证无数据竞争：所有写入都应能读出
        assert successCount.get() == threads * opsPerThread : "并发数据丢失！";
        assert cache.size() == threads * opsPerThread : "Size计算错误！";

        System.out.println("✅ 并发安全验证通过");
    }
}
