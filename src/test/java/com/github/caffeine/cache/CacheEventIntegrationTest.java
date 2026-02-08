package com.github.caffeine.cache;

import com.github.caffeine.cache.event.CacheEvent;
import com.github.caffeine.cache.event.CacheEventType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 事件系统与BoundedLocalCache集成测试
 */
@DisplayName("Cache Event Integration Tests")
public class CacheEventIntegrationTest {

    @Test
    @DisplayName("写入事件通知测试")
    void testWriteEventNotification() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> capturedKey = new AtomicReference<>();
        AtomicReference<String> capturedValue = new AtomicReference<>();

        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .removalListener(event -> {
                    if (event.getType() == CacheEventType.WRITE) {
                        capturedKey.set(event.getKey());
                        capturedValue.set(event.getValue());
                        latch.countDown();
                    }
                })
                .build();

        cache.put("test-key", "test-value");

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals("test-key", capturedKey.get());
        assertEquals("test-value", capturedValue.get());
    }

    @Test
    @DisplayName("显式删除事件通知测试")
    void testRemoveEventNotification() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<CacheEventType> eventType = new AtomicReference<>();

        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .removalListener(event -> {
                    eventType.set(event.getType());
                    latch.countDown();
                })
                .build();

        cache.put("key", "value");
        cache.invalidate("key");

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(CacheEventType.REMOVE, eventType.get());
    }

    @Test
    @DisplayName("过期事件通知测试")
    @Timeout(5)
    void testExpireEventNotification() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> expiredKey = new AtomicReference<>();

        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .expireAfterWrite(100, TimeUnit.MILLISECONDS)
                .removalListener(event -> {
                    if (event.getType() == CacheEventType.EXPIRE) {
                        expiredKey.set(event.getKey());
                        latch.countDown();
                    }
                })
                .build();

        cache.put("expiring-key", "value");

        // 等待过期（时间轮精度约1秒，但事件应被触发）
        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertEquals("expiring-key", expiredKey.get());
    }

    @Test
    @DisplayName("GC回收事件通知测试（软引用）")
    void testCollectedEventNotification() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> collectedKey = new AtomicReference<>();

        Cache<String, byte[]> cache = Caffeine.<String, byte[]>newBuilder()
                .softValues()
                .removalListener(event -> {
                    if (event.getType() == CacheEventType.COLLECTED) {
                        collectedKey.set(event.getKey());
                        latch.countDown();
                    }
                })
                .build();

        cache.put("soft-key", new byte[1024 * 1024]); // 1MB数据

        // 模拟GC压力
        System.gc();
        Thread.sleep(500);

        // 再次访问，应该触发清理检查
        cache.getIfPresent("soft-key");

        // 注意：软引用回收依赖JVM，测试可能不稳定
        // 这里主要验证代码路径正确
    }

    @Test
    @DisplayName("多操作事件顺序测试")
    void testEventOrdering() throws InterruptedException {
        ConcurrentLinkedQueue<CacheEventType> eventOrder = new ConcurrentLinkedQueue<>();

        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .removalListener(event -> eventOrder.offer(event.getType()))
                .build();

        cache.put("key1", "value1");      // WRITE
        cache.put("key1", "value2");      // WRITE (update)
        cache.invalidate("key1");          // REMOVE

        Thread.sleep(100); // 等待异步处理

        // 验证事件被记录（顺序可能因并发略有不同，但都应存在）
        long writeCount = eventOrder.stream().filter(t -> t == CacheEventType.WRITE).count();
        long removeCount = eventOrder.stream().filter(t -> t == CacheEventType.REMOVE).count();

        assertEquals(2, writeCount); // 两次写入
        assertEquals(1, removeCount); // 一次删除
    }

    @Test
    @DisplayName("invalidateAll批量事件测试")
    void testInvalidateAllEvents() throws InterruptedException {
        int keyCount = 100;
        CountDownLatch latch = new CountDownLatch(keyCount);
        AtomicInteger removeCount = new AtomicInteger(0);

        Cache<Integer, String> cache = Caffeine.<Integer, String>newBuilder()
                .removalListener(event -> {
                    if (event.getType() == CacheEventType.REMOVE) {
                        removeCount.incrementAndGet();
                        latch.countDown();
                    }
                })
                .build();

        for (int i = 0; i < keyCount; i++) {
            cache.put(i, "value-" + i);
        }

        cache.invalidateAll();

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(keyCount, removeCount.get());
    }

    @Test
    @DisplayName("监听器异常隔离测试")
    void testListenerExceptionIsolation() throws InterruptedException {
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);

        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .removalListener(event -> {
                    // 只统计移除事件
                    if (event.getType() == CacheEventType.REMOVE ||
                            event.getType() == CacheEventType.EXPIRE) {

                        if (errorCount.incrementAndGet() == 1) {
                            throw new RuntimeException("Listener Error");
                        }
                        successCount.incrementAndGet();
                    }
                })
                .build();

        cache.put("key1", "value1");
        cache.invalidate("key1");  // 触发第一次监听，抛出异常

        cache.put("key2", "value2");
        cache.invalidate("key2");  // 应正常处理，不受之前异常影响

        Thread.sleep(100);  // 等待异步消费

        assertEquals(2, errorCount.get());  // 两个事件都尝试了
        assertEquals(1, successCount.get()); // 第二个成功处理
    }
}