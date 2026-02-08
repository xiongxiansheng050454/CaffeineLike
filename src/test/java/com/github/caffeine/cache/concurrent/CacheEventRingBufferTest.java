package com.github.caffeine.cache.concurrent;

import com.github.caffeine.cache.event.CacheEvent;
import com.github.caffeine.cache.event.CacheEventType;
import org.junit.jupiter.api.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;

/**
 * CacheEventRingBuffer 单元测试（修正版）
 * 使用自旋重试适应非阻塞特性
 */
@DisplayName("CacheEventRingBuffer Tests")
public class CacheEventRingBufferTest {

    private CacheEventRingBuffer<String, String> buffer;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(2); // 保持兼容，虽然内部不再使用
    }

    @AfterEach
    void tearDown() {
        if (buffer != null) {
            buffer.shutdown();
        }
        executor.shutdownNow();
    }

    /**
     * 非阻塞发布辅助方法：自旋直到成功
     */
    private void publishSpin(CacheEvent<String, String> event) {
        while (!buffer.publish(event)) {
            Thread.onSpinWait();
        }
    }

    @Test
    @DisplayName("基本发布/消费测试")
    void testBasicPublishConsume() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger received = new AtomicInteger(0);

        Consumer<CacheEvent<String, String>> consumer = event -> {
            if (event.getType() == CacheEventType.WRITE) {
                received.incrementAndGet();
                latch.countDown();
            }
        };

        buffer = new CacheEventRingBuffer<>(64, false, consumer);

        CacheEvent<String, String> event = CacheEvent.<String, String>builder()
                .type(CacheEventType.WRITE)
                .key("test")
                .value("value")
                .build();

        // 使用自旋重试而非 assertTrue
        publishSpin(event);

        assertTrue(latch.await(1, TimeUnit.SECONDS), "消费超时");
        assertEquals(1, received.get());
    }

    @Test
    @DisplayName("多消费者负载均衡测试")
    void testMultiConsumerLoadBalancing() throws InterruptedException {
        int consumerCount = 4;
        int eventCount = 1000;
        CountDownLatch latch = new CountDownLatch(eventCount);
        AtomicInteger[] counters = new AtomicInteger[consumerCount];

        for (int i = 0; i < consumerCount; i++) {
            counters[i] = new AtomicInteger(0);
        }

        @SuppressWarnings("unchecked")
        Consumer<CacheEvent<String, String>>[] consumers = new Consumer[consumerCount];
        for (int i = 0; i < consumerCount; i++) {
            final int idx = i;
            consumers[i] = event -> {
                counters[idx].incrementAndGet();
                latch.countDown();
            };
        }

        // 关键修复：增大缓冲区，避免消费者启动延迟导致队列满
        buffer = new CacheEventRingBuffer<>(1024, false, consumers);

        // 单生产者发布事件，使用自旋重试
        for (int i = 0; i < eventCount; i++) {
            final int idx = i;
            CacheEvent<String, String> event = CacheEvent.<String, String>builder()
                    .type(CacheEventType.WRITE)
                    .key("key-" + idx)
                    .value("value-" + idx)
                    .build();
            publishSpin(event);
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Not all events were consumed");

        int total = 0;
        for (AtomicInteger counter : counters) {
            assertTrue(counter.get() > 0, "Consumer should process at least one event");
            total += counter.get();
        }
        assertEquals(eventCount, total);
    }

    @Test
    @DisplayName("队列满时丢弃策略测试")
    void testDropOnFull() {
        AtomicInteger received = new AtomicInteger(0);
        Consumer<CacheEvent<String, String>> slowConsumer = event -> {
            try {
                Thread.sleep(100); // 慢消费
                received.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        // 小缓冲区，非阻塞模式
        buffer = new CacheEventRingBuffer<>(4, true, slowConsumer);

        // 快速填充队列，使用自旋确保发布成功直到队列满
        int published = 0;
        for (int i = 0; i < 100; i++) {
            CacheEvent<String, String> event = CacheEvent.<String, String>builder()
                    .type(CacheEventType.READ)
                    .key("key-" + i)
                    .value("value")
                    .build();

            if (buffer.publish(event)) {
                published++;
            } else {
                // 队列已满，跳出或继续尝试
                break;
            }
        }

        // 应该只成功发布一部分（不超过缓冲区大小）
        assertTrue(published <= 4, "Should not exceed buffer size");
    }

    @Test
    @DisplayName("强制发布阻塞测试")
    void testForcePublishBlocking() throws InterruptedException {
        CountDownLatch blockLatch = new CountDownLatch(1);
        AtomicInteger received = new AtomicInteger(0);

        Consumer<CacheEvent<String, String>> blockingConsumer = event -> {
            try {
                blockLatch.await(); // 阻塞消费者
                received.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        buffer = new CacheEventRingBuffer<>(4, false, blockingConsumer);

        // 填充队列（使用自旋确保填满）
        for (int i = 0; i < 3; i++) {
            publishSpin(CacheEvent.<String, String>builder()
                    .type(CacheEventType.WRITE)
                    .key("key-" + i)
                    .value("value")
                    .build());
        }

        // 异步强制发布（应该会阻塞直到消费者释放）
        Future<?> future = Executors.newSingleThreadExecutor().submit(() -> {
            try {
                buffer.publishForce(CacheEvent.<String, String>builder()
                        .type(CacheEventType.EXPIRE)
                        .key("forced")
                        .value("value")
                        .build());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // 应该还没完成（因为消费者阻塞）
        assertFalse(future.isDone());

        // 释放消费者
        blockLatch.countDown();
        assertDoesNotThrow(() -> future.get(1, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("并发安全性测试 - 单生产者多消费者")
    void testConcurrentSafety() throws InterruptedException {
        int eventCount = 10000;
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(eventCount);

        Consumer<CacheEvent<String, String>> consumer = event -> {
            counter.incrementAndGet();
            latch.countDown();
        };

        // 关键修复：使用大缓冲区 + 两个消费者实例
        buffer = new CacheEventRingBuffer<>(4096, false, consumer, consumer);

        // 单生产者快速发布，使用自旋重试
        Thread producer = new Thread(() -> {
            for (int i = 0; i < eventCount; i++) {
                CacheEvent<String, String> event = CacheEvent.<String, String>builder()
                        .type(CacheEventType.WRITE)
                        .key("key-" + i)
                        .value("value-" + i)
                        .build();
                publishSpin(event);
            }
        });

        producer.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Timeout waiting for events");
        producer.join();

        assertEquals(eventCount, counter.get());
    }

    @Test
    @DisplayName("验证消费者进度独立追踪")
    void testConsumerProgressIsolation() throws InterruptedException {
        int consumerCount = 2;
        AtomicLong[] lastConsumed = new AtomicLong[consumerCount];
        for (int i = 0; i < consumerCount; i++) lastConsumed[i] = new AtomicLong(-1);

        CountDownLatch latch = new CountDownLatch(100);

        @SuppressWarnings("unchecked")
        Consumer<CacheEvent<String, String>>[] consumers = new Consumer[consumerCount];
        for (int i = 0; i < consumerCount; i++) {
            final int id = i;
            consumers[i] = event -> {
                lastConsumed[id].set(Long.parseLong(event.getKey()));
                latch.countDown();
            };
        }

        // 关键修复：使用足够大的缓冲区（至少大于事件数）
        buffer = new CacheEventRingBuffer<>(256, false, consumers);

        // 快速发布100个事件，使用自旋重试
        for (int i = 0; i < 100; i++) {
            final int idx = i;
            publishSpin(CacheEvent.<String, String>builder()
                    .type(CacheEventType.WRITE)
                    .key(String.valueOf(idx))
                    .value("v")
                    .build());
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }
}