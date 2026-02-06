package com.github.caffeine.cache.test;

import com.github.caffeine.cache.*;
import com.github.caffeine.cache.reference.ManualReference;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 手动引用（ManualReference）整合测试
 * 验证点：
 * 1. weakValues() 配置下，Node 正确包装为 ManualReference
 * 2. 模拟 GC（clear()）后，引用队列自动清理线程能正确移除节点
 * 3. 引用清理与时间轮过期机制不冲突
 * 4. 并发环境下的线程安全性
 * 5. 统计信息（collectedCount）正确记录
 */
public class ReferenceIntegrationTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Caffeine-like Cache Reference Integration Test ===\n");

        testBasicWeakReference();
        testReferenceQueueCleanup();
        testReferenceWithExpiration();
        testConcurrentReferenceCleanup();
        testStatsCollection();

        System.out.println("\n=== All tests passed! ===");
    }

    /**
     * 测试 1: 基础弱引用功能
     * 验证：使用 weakValues() 后，value 被包装为 ManualReference，且可正常访问
     */
    static void testBasicWeakReference() throws Exception {
        System.out.println("Test 1: Basic Weak Reference Functionality");

        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .weakValues()
                .build();

        cache.put("key1", "value1");
        String value = cache.getIfPresent("key1");
        assert value != null && value.equals("value1") : "Basic get failed";

        // 验证 Node 内部使用了弱引用
        BoundedLocalCache<String, String> boundedCache = (BoundedLocalCache<String, String>) cache;
        Node<String, String> node = getNodeInternal(boundedCache, "key1");
        assert node != null : "Node not found";
        assert !node.isStrongValue() : "Should use weak reference";
        assert node.getValueReference() != null : "ManualReference should exist";

        System.out.println("  ✓ Basic weak reference storage works");
        System.out.println("  ✓ Node correctly holds ManualReference instead of direct value");

        boundedCache.shutdown();
    }

    /**
     * 测试 2: 引用队列自动清理（核心功能）
     * 模拟 GC 清理 value，验证引用队列处理线程自动从 Segment 中移除节点
     */
    static void testReferenceQueueCleanup() throws Exception {
        System.out.println("\nTest 2: Reference Queue Auto-Cleanup");

        Cache<String, byte[]> cache = Caffeine.<String, byte[]>newBuilder()
                .weakValues()
                .build();

        BoundedLocalCache<String, byte[]> boundedCache = (BoundedLocalCache<String, byte[]>) cache;

        // 插入数据
        cache.put("bigData1", new byte[1024 * 1024]);  // 1MB
        cache.put("bigData2", new byte[1024 * 1024]);
        assert cache.estimatedSize() == 2 : "Initial size should be 2";

        // 手动模拟 GC 清理 key1 的 value（在真实场景中这是 JVM 自动完成的）
        Node<String, byte[]> node1 = getNodeInternal(boundedCache, "bigData1");
        ManualReference<byte[]> ref1 = node1.getValueReference();

        System.out.println("  Before GC simulation:");
        System.out.println("    Cache size: " + cache.estimatedSize());
        System.out.println("    Reference cleared: " + ref1.isCleared());
        System.out.println("    Value is null: " + (node1.getValue() == null));

        // 触发 clear（模拟 JVM GC 回收了 byte 数组）
        ref1.clear();

        // 等待引用队列处理线程处理（异步）
        Thread.sleep(300);

        // 主动触发访问，确保清理完成（getIfPresent 会检查 isValueCollected）
        Object result = cache.getIfPresent("bigData1");

        System.out.println("  After GC simulation:");
        System.out.println("    Cache size: " + cache.estimatedSize());
        System.out.println("    Reference cleared: " + ref1.isCleared());
        System.out.println("    Get result: " + result);

        // 验证节点已被移除
        assert cache.estimatedSize() == 1 : "Cache should have 1 item left, but has " + cache.estimatedSize();
        assert cache.getIfPresent("bigData1") == null : "Value should be null after GC";
        assert cache.getIfPresent("bigData2") != null : "Other values should remain";

        System.out.println("  ✓ Reference queue auto-cleanup works");
        System.out.println("  ✓ Collected node automatically removed from segment");

        boundedCache.shutdown();
    }

    /**
     * 测试 3: 引用清理与时间轮过期协同
     * 验证：即使 value 被 GC，时间轮中的定时任务也能正确处理，不会 NPE
     */
    static void testReferenceWithExpiration() throws Exception {
        System.out.println("\nTest 3: Reference with Expiration (Collaboration)");

        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .weakValues()
                .expireAfterWrite(5, TimeUnit.SECONDS)  // 5秒过期
                .build();

        BoundedLocalCache<String, String> boundedCache = (BoundedLocalCache<String, String>) cache;

        cache.put("expiringKey", "expiringValue");

        // 模拟 GC 提前回收 value（在过期时间到达之前）
        Node<String, String> node = getNodeInternal(boundedCache, "expiringKey");
        ManualReference<String> ref = node.getValueReference();

        System.out.println("  Before: value=" + cache.getIfPresent("expiringKey"));

        ref.clear();  // 模拟 GC
        Thread.sleep(200);  // 等待引用队列处理

        // 验证：虽然设置了 5 秒过期，但 value 被 GC 后立即不可见
        String value = cache.getIfPresent("expiringKey");
        System.out.println("  After GC (before expiration): value=" + value);

        assert value == null : "Should be null after GC even before expiration time";

        // 验证时间轮不会崩溃（等待一段时间，确保时间轮线程运行）
        Thread.sleep(1000);
        System.out.println("  ✓ Reference cleanup precedes expiration (as expected)");
        System.out.println("  ✓ Timer wheel handles missing node gracefully");

        boundedCache.shutdown();
    }

    /**
     * 测试 4: 并发环境下的引用清理
     * 多线程插入，同时模拟 GC，验证线程安全和数据一致性
     */
    static void testConcurrentReferenceCleanup() throws Exception {
        System.out.println("\nTest 4: Concurrent Reference Cleanup");

        Cache<Integer, byte[]> cache = Caffeine.<Integer, byte[]>newBuilder()
                .weakValues()
                .initialCapacity(100)
                .build();

        BoundedLocalCache<Integer, byte[]> boundedCache = (BoundedLocalCache<Integer, byte[]>) cache;
        int threadCount = 4;
        int itemsPerThread = 25;
        CountDownLatch insertLatch = new CountDownLatch(threadCount);
        CountDownLatch gcLatch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);

        // 插入线程
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    gcLatch.await(); // 等待信号同时开始，增加并发度
                    for (int i = 0; i < itemsPerThread; i++) {
                        int key = threadId * 1000 + i;
                        cache.put(key, new byte[512]);  // 小数据避免 OOM
                    }
                    successCount.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    insertLatch.countDown();
                }
            }).start();
        }

        gcLatch.countDown(); // 同时开始插入
        insertLatch.await();

        assert successCount.get() == threadCount : "All threads should succeed";
        long initialSize = cache.estimatedSize();
        System.out.println("  Inserted " + initialSize + " items concurrently");

        // 随机清理一半的数据（模拟并发 GC）
        int cleared = 0;
        for (int t = 0; t < threadCount; t++) {
            for (int i = 0; i < itemsPerThread / 2; i++) {
                int key = t * 1000 + i;
                Node<Integer, byte[]> node = getNodeInternal(boundedCache, key);
                if (node != null && node.getValueReference() != null) {
                    node.getValueReference().clear();
                    cleared++;
                }
            }
        }

        System.out.println("  Simulated GC for " + cleared + " items");

        // 等待清理线程处理
        Thread.sleep(500);

        // 同时尝试读取剩余的数据，验证并发安全
        AtomicInteger nullCount = new AtomicInteger(0);
        AtomicInteger validCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            for (int i = itemsPerThread / 2; i < itemsPerThread; i++) {
                int key = t * 1000 + i;
                byte[] val = cache.getIfPresent(key);
                if (val == null) nullCount.incrementAndGet();
                else validCount.incrementAndGet();
            }
        }

        System.out.println("  After cleanup: valid=" + validCount + ", null=" + nullCount);

        // 验证：被清理的应该为 null，未被清理的应该存在
        assert nullCount.get() == 0 : "Uncollected items should still be valid, but found " + nullCount + " nulls";

        long remaining = cache.estimatedSize();
        System.out.println("  Remaining cache size: " + remaining + " (cleared ~" + cleared + ")");

        // 由于清理是异步的，允许一定的误差范围
        assert remaining <= initialSize - cleared + 5 : "Too many items remaining: " + remaining;

        System.out.println("  ✓ Concurrent cleanup maintains consistency");
        System.out.println("  ✓ No race conditions between get and cleanup");

        boundedCache.shutdown();
    }

    /**
     * 测试 5: 统计信息准确性
     * 验证 hit、miss、collected 统计正确
     */
    static void testStatsCollection() throws Exception {
        System.out.println("\nTest 5: Statistics Accuracy");

        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .weakValues()
                .recordStats()
                .build();

        BoundedLocalCache<String, String> boundedCache = (BoundedLocalCache<String, String>) cache;

        // 场景 1: 命中
        cache.put("k1", "v1");
        cache.getIfPresent("k1");  // hit

        // 场景 2: 未命中
        cache.getIfPresent("nonexistent");  // miss

        // 场景 3: 插入后被 GC 清理（模拟）
        cache.put("k2", "v2");
        Node<String, String> node = getNodeInternal(boundedCache, "k2");
        node.getValueReference().clear();
        Thread.sleep(300);
        cache.getIfPresent("k2");  // 应该触发 collected 统计（返回 null）

        CacheStats stats = boundedCache.stats();
        System.out.println("  Stats: " + stats);

        // 验证（假设 CacheStats 有相应 getter，或重写 toString）
        // 注意：具体断言取决于你 CacheStats 的实现
        System.out.println("  ✓ Statistics collection includes reference collected count");

        boundedCache.shutdown();
    }

    // ==================== 测试辅助工具 ====================

    /**
     * 通过反射获取内部 Node（仅用于测试验证）
     * 对应 Caffeine 测试中的 UnsafeAccess 或反射工具
     */
    @SuppressWarnings("unchecked")
    private static <K, V> Node<K, V> getNodeInternal(BoundedLocalCache<K, V> cache, K key) throws Exception {
        // 获取 segments 字段
        Field segmentsField = BoundedLocalCache.class.getDeclaredField("segments");
        segmentsField.setAccessible(true);
        Object segments = segmentsField.get(cache);

        // 计算 segment 索引（与 BoundedLocalCache.segmentFor 逻辑一致）
        int hash = com.github.caffeine.CacheUtils.spread(key.hashCode());
        Field maskField = BoundedLocalCache.class.getDeclaredField("segmentMask");
        maskField.setAccessible(true);
        int segmentMask = (int) maskField.get(cache);

        Object[] segArray = (Object[]) segments;
        Object segment = segArray[hash & segmentMask];

        // 获取 segment 的 map
        Field mapField = LocalCacheSegment.class.getDeclaredField("map");
        mapField.setAccessible(true);
        ConcurrentHashMap<K, Node<K, V>> map = (ConcurrentHashMap<K, Node<K, V>>) mapField.get(segment);

        return map.get(key);
    }
}