package com.github.caffeine.cache;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.CountDownLatch;
import java.util.Random;

public class StripedLockTest {

    @Test
    public void test64ThreadsContentionRate() throws InterruptedException {
        // 1024个锁
        InstrumentedStripedLock lock = new InstrumentedStripedLock(1024);
        int threadCount = 64;
        int opsPerThread = 1000;
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            new Thread(() -> {
                // 使用ThreadLocalRandom，避免Random的线程竞争
                java.util.concurrent.ThreadLocalRandom rand =
                        java.util.concurrent.ThreadLocalRandom.current();

                for (int j = 0; j < opsPerThread; j++) {
                    // 完全随机key，覆盖1024个锁的槽位
                    String key = "key_" + rand.nextInt(100000) + "_t" + threadId;

                    var ctx = lock.acquireLock(key);
                    try {
                        // 关键：零延迟操作，仅自增计数器
                        // 去掉Thread.sleep，避免系统调度开销
                        int dummy = 0;
                        dummy++;
                    } finally {
                        lock.releaseLock(ctx);
                    }
                }
                latch.countDown();
            }).start();
        }

        latch.await();

        double rate = lock.getContentionRate();
        System.out.println("锁数量: 1024");
        System.out.println("总操作数: " + lock.getTotalOps());
        System.out.println("真实冲突率: " + String.format("%.2f%%", rate));

        // 验收标准：<5%
        assertTrue(rate < 5.0,
                String.format("冲突率 %.2f%% 超过阈值 5%%", rate));
    }

    @Test
    public void testIntegrationWithCache() throws InterruptedException {
        Caffeine<String, String> builder = Caffeine.<String, String>newBuilder()
                .initialCapacity(1000)
                .maximumSize(10000);

        BoundedLocalCache<String, String> cache =
                new BoundedLocalCache<>(builder);

        int threads = 64;
        CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            int idx = i;
            new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    cache.put("key_" + idx + "_" + j, "value_" + j);
                }
                latch.countDown();
            }).start();
        }

        latch.await();

        // 验证：6400条数据应全部写入
        long size = cache.estimatedSize();
        System.out.println("实际写入: " + size);
        assertEquals(6400, size, "数据应全部写入，无丢失");
    }
}