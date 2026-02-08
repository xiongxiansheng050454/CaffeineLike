package com.github.caffeine.cache;

import com.github.caffeine.cache.event.CacheEventType;
import com.github.caffeine.cache.reference.ManualReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.DisplayName;

import java.lang.reflect.Method;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ManualReference Key 存储与快速清理路径测试
 * 所有测试都设置超时，防止死锁导致测试挂起
 */
public class KeyedReferenceIntegrationTest {

    private BoundedLocalCache<String, byte[]> cache;
    private AtomicInteger collectedEventCount;
    private ConcurrentLinkedQueue<String> collectedKeys;

    @BeforeEach
    void setUp() {
        collectedEventCount = new AtomicInteger(0);
        collectedKeys = new ConcurrentLinkedQueue<>();

        cache = (BoundedLocalCache<String, byte[]>) Caffeine.<String, byte[]>newBuilder()
                .weakValues()
                .recordStats()
                .removalListener(event -> {
                    if (event.getType() == CacheEventType.COLLECTED) {
                        collectedEventCount.incrementAndGet();
                        collectedKeys.offer(event.getKey());
                        System.out.printf("[Event] Key=%s 被GC回收并清理%n", event.getKey());
                    }
                })
                .build();
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @DisplayName("1. ManualReference 正确存储 Key")
    void testManualReferenceStoresKey() throws Exception {
        String key = "test-key-1";
        byte[] value = new byte[1024];

        cache.put(key, value);

        ManualReference<byte[]> ref = getReferenceForKey(cache, key);

        assertNotNull(ref, "弱引用模式下应该存在 ManualReference");
        assertEquals(key, ref.getKey(), "ManualReference 应该存储正确的 key");
        assertFalse(ref.isCleared(), "引用应该未被清理");
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @DisplayName("2. O(1) 快速清理路径")
    void testFastCleanupPath() throws Exception {
        // 准备数据
        String[] keys = {"key-0", "key-1", "key-2", "key-3", "key-4"};
        ManualReference<byte[]>[] refs = new ManualReference[keys.length];

        for (int i = 0; i < keys.length; i++) {
            cache.put(keys[i], new byte[1024 * (i + 1)]);
            refs[i] = getReferenceForKey(cache, keys[i]);
            assertEquals(keys[i], refs[i].getKey(), "Key-" + i + " 应该正确存储");
        }

        assertEquals(5, cache.estimatedSize(), "初始应该有5个条目");

        // 模拟 GC 回收 key-2 和 key-4
        ManualReference.simulateGC(refs[2]);
        ManualReference.simulateGC(refs[4]);

        assertTrue(refs[2].isCleared() && refs[4].isCleared(), "引用应该被标记为已清理");

        // 调用清理（O(1) 路径）
        long startTime = System.nanoTime();
        invokeDrainReferenceQueue(cache);
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        // 验证清理结果
        assertEquals(3, cache.estimatedSize(), "应该清理掉2个条目，剩余3个");
        assertNull(cache.getIfPresent(keys[2]), "key-2 应该已被清理");
        assertNull(cache.getIfPresent(keys[4]), "key-4 应该已被清理");
        assertNotNull(cache.getIfPresent(keys[0]), "key-0 应该仍然存在");
        assertNotNull(cache.getIfPresent(keys[1]), "key-1 应该仍然存在");

        // 验证统计
        assertEquals(2, cache.stats().collectedCount(), "统计应该记录2次 GC 回收");

        System.out.printf("[性能] 清理2个条目耗时: %d ms%n", durationMs);
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @DisplayName("3. GC 回收事件正确触发")
    void testCollectedEventFired() throws Exception {
        String key = "event-test-key";
        cache.put(key, new byte[2048]);

        ManualReference<byte[]> ref = getReferenceForKey(cache, key);
        assertEquals(key, ref.getKey());

        // 模拟 GC
        ManualReference.simulateGC(ref);
        assertTrue(ref.isCleared());

        // 触发清理
        invokeDrainReferenceQueue(cache);

        // 等待事件处理（稍微等待异步处理）
        Thread.sleep(50);

        assertEquals(1, collectedEventCount.get(), "应该触发一次 COLLECTED 事件");
        assertEquals(key, collectedKeys.poll(), "事件应该携带正确的 key");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("4. 并发清理不阻塞读操作")
    void testConcurrentCleanupDoesNotBlockReads() throws Exception {
        int itemCount = 50; // 减少数量，降低竞争

        // 准备数据和引用
        String[] keys = new String[itemCount];
        ManualReference<byte[]>[] refs = new ManualReference[itemCount];

        for (int i = 0; i < itemCount; i++) {
            keys[i] = "concurrent-" + i;
            cache.put(keys[i], new byte[512]);
            refs[i] = getReferenceForKey(cache, keys[i]);
        }

        assertEquals(itemCount, cache.estimatedSize());

        // 标记偶数 key 为已回收（但先不调用清理）
        for (int i = 0; i < itemCount; i += 2) {
            ManualReference.simulateGC(refs[i]);
        }

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch cleanupDone = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        // 清理线程
        Thread cleanupThread = new Thread(() -> {
            try {
                startLatch.await(); // 等待信号同时开始
                invokeDrainReferenceQueue(cache);
            } catch (Exception e) {
                error.set(e);
                e.printStackTrace();
            } finally {
                cleanupDone.countDown();
            }
        }, "cleanup-thread");

        // 读取线程 - 持续读取奇数 key
        Thread readThread = new Thread(() -> {
            try {
                startLatch.await();
                for (int round = 0; round < 50; round++) { // 减少轮数
                    for (int i = 1; i < itemCount; i += 2) {
                        byte[] val = cache.getIfPresent(keys[i]);
                        if (val == null) {
                            throw new IllegalStateException("不应该清理奇数 key: " + i);
                        }
                    }
                }
            } catch (Exception e) {
                error.set(e);
                e.printStackTrace();
            }
        }, "read-thread");

        cleanupThread.start();
        readThread.start();

        // 同时开始
        startLatch.countDown();

        // 等待完成（设置超时）
        boolean cleanupFinished = cleanupDone.await(3, TimeUnit.SECONDS);
        readThread.join(3000); // 等待读取线程最多3秒

        assertTrue(cleanupFinished, "清理应该在3秒内完成，可能发生死锁");
        assertNull(error.get(), "不应该有异常: " + error.get());

        // 验证结果：偶数被清理，奇数保留
        assertEquals(itemCount / 2, cache.estimatedSize(), "应该只剩一半");
        for (int i = 0; i < itemCount; i++) {
            if (i % 2 == 0) {
                assertNull(cache.getIfPresent(keys[i]), "偶数 key-" + i + " 应该被清理");
            } else {
                assertNotNull(cache.getIfPresent(keys[i]), "奇数 key-" + i + " 应该保留");
            }
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @DisplayName("5. 批量并发清理测试")
    void testBatchConcurrentCleanup() throws Exception {
        // 模拟高并发下大量引用被同时回收
        int count = 100;
        CountDownLatch latch = new CountDownLatch(count);
        ExecutorService executor = Executors.newFixedThreadPool(10);

        for (int i = 0; i < count; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    String key = "batch-" + idx;
                    cache.put(key, new byte[256]);
                    ManualReference<byte[]> ref = getReferenceForKey(cache, key);
                    ManualReference.simulateGC(ref);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(3, TimeUnit.SECONDS), "准备数据应该完成");
        assertEquals(count, cache.estimatedSize());

        // 批量清理
        invokeDrainReferenceQueue(cache);

        // 验证全部清理
        assertEquals(0, cache.estimatedSize(), "所有条目应该被清理");
        assertEquals(count, cache.stats().collectedCount(), "统计应该显示100次回收");
    }

    // ============ 辅助方法 ============

    @SuppressWarnings("unchecked")
    private <K, V> LocalCacheSegment<K, V> getSegmentFor(BoundedLocalCache<K, V> cache, K key)
            throws Exception {
        Method method = BoundedLocalCache.class.getDeclaredMethod("segmentFor", Object.class);
        method.setAccessible(true);
        return (LocalCacheSegment<K, V>) method.invoke(cache, key);
    }

    @SuppressWarnings("unchecked")
    private <K, V> ManualReference<V> getReferenceForKey(BoundedLocalCache<K, V> cache, K key)
            throws Exception {
        LocalCacheSegment<K, V> segment = getSegmentFor(cache, key);
        // 使用反射直接访问 map，避免触发额外的锁逻辑
        java.lang.reflect.Field mapField = LocalCacheSegment.class.getDeclaredField("map");
        mapField.setAccessible(true);
        java.util.HashMap<K, Node<K, V>> map = (java.util.HashMap<K, Node<K, V>>) mapField.get(segment);

        Node<K, V> node = map.get(key);
        assertNotNull(node, "Key " + key + " 对应的 Node 不存在");

        ManualReference<V> ref = node.getValueReference();
        assertNotNull(ref, "应该是引用类型");
        return ref;
    }

    private void invokeDrainReferenceQueue(BoundedLocalCache<?, ?> cache) throws Exception {
        Method method = BoundedLocalCache.class.getDeclaredMethod("drainReferenceQueue");
        method.setAccessible(true);
        method.invoke(cache);
    }
}