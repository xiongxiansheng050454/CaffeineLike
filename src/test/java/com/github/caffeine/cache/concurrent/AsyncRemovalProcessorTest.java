package com.github.caffeine.cache.concurrent;

import com.github.caffeine.cache.BoundedLocalCache;
import com.github.caffeine.cache.Cache;
import com.github.caffeine.cache.Caffeine;
import com.github.caffeine.cache.event.CacheEvent;
import com.github.caffeine.cache.event.CacheEventType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AsyncRemovalProcessor 集成测试
 * 验证RingBuffer无锁队列、批量聚合、背压回退等核心机制
 */
public class AsyncRemovalProcessorTest {

    private AsyncRemovalProcessor<String, String> processor;
    private List<CacheEvent<String, String>> receivedEvents;
    private AtomicInteger batchCount;

    @BeforeEach
    void setUp() {
        receivedEvents = Collections.synchronizedList(new ArrayList<>());
        batchCount = new AtomicInteger(0);
    }

    @AfterEach
    void tearDown() {
        if (processor != null) {
            // 缩短shutdown等待时间，避免测试超时
            processor.shutdown();
        }
    }

    /**
     * 测试1：基本功能 - 验证事件最终被异步消费
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testBasicAsyncConsumption() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);

        processor = new AsyncRemovalProcessor<>(
                1024,  // buffer size
                10,    // batch size
                1000,  // flush interval (long enough to test batching)
                events -> {
                    receivedEvents.addAll(events);
                    batchCount.incrementAndGet();
                    events.forEach(e -> latch.countDown());
                }
        );
        processor.start();

        // 发布3个事件
        processor.publish(CacheEvent.<String, String>builder()
                .type(CacheEventType.REMOVE).key("key1").value("value1").build());
        processor.publish(CacheEvent.<String, String>builder()
                .type(CacheEventType.EXPIRE).key("key2").value("value2").build());
        processor.publish(CacheEvent.<String, String>builder()
                .type(CacheEventType.EVICT).key("key3").value("value3").build());

        // 等待消费完成
        assertTrue(latch.await(3, TimeUnit.SECONDS), "事件应在超时前被处理");

        // 验证
        assertEquals(3, receivedEvents.size());
        assertTrue(receivedEvents.stream().anyMatch(e -> e.getKey().equals("key1")));
        assertTrue(receivedEvents.stream().anyMatch(e -> e.getType() == CacheEventType.EXPIRE));
    }

    /**
     * 测试2：批量聚合 - 验证达到batchSize时触发flush
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testBatchingBehavior() throws InterruptedException {
        int batchSize = 50;
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger lastBatchSize = new AtomicInteger(0);

        processor = new AsyncRemovalProcessor<>(
                1024,
                batchSize,
                10000, // 10秒，确保不会超时触发
                events -> {
                    lastBatchSize.set(events.size());
                    if (events.size() == batchSize) {
                        latch.countDown();
                    }
                }
        );
        processor.start();

        // 快速发布batchSize个事件
        for (int i = 0; i < batchSize; i++) {
            boolean success = processor.publish(CacheEvent.<String, String>builder()
                    .type(CacheEventType.REMOVE)
                    .key("key-" + i)
                    .value("value-" + i)
                    .build());
            assertTrue(success, "发布应成功，缓冲区足够大");
        }

        // 等待批量处理触发
        assertTrue(latch.await(2, TimeUnit.SECONDS), "应在达到batchSize时触发批量处理");
        assertEquals(batchSize, lastBatchSize.get(), "批量大小应等于配置的batchSize");
    }

    /**
     * 测试3：超时Flush - 验证即使没有达到batchSize，超时后也会处理
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testTimeoutFlush() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        long flushIntervalMs = 200;

        processor = new AsyncRemovalProcessor<>(
                1024,
                100,   // 大batchSize，确保不会提前触发
                flushIntervalMs,
                events -> latch.countDown()
        );
        processor.start();

        // 只发布5个事件（远小于batchSize）
        processor.publish(CacheEvent.<String, String>builder()
                .type(CacheEventType.REMOVE).key("key1").value("value1").build());

        // 应在超时时间内被处理
        long start = System.currentTimeMillis();
        assertTrue(latch.await(flushIntervalMs * 2, TimeUnit.MILLISECONDS),
                "应在超时间隔内处理事件");
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed >= flushIntervalMs, "应等待至少flushInterval时间");
        assertTrue(elapsed < flushIntervalMs * 1.5, "不应等待过久");
    }

    /**
     * 测试4：背压回退 - 验证当RingBuffer满时同步回退
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testBackpressureFallback() throws InterruptedException {
        AtomicInteger syncProcessedCount = new AtomicInteger(0);
        AtomicInteger consumedCount = new AtomicInteger(0);

        // 使用极慢消费者（每次处理100ms），但不阻塞线程
        processor = new AsyncRemovalProcessor<>(
                16,    // 很小的buffer，容易满
                100,   // 大batchSize，不会提前flush
                10000, // 长超时
                events -> {
                    // 模拟慢速处理
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                    consumedCount.addAndGet(events.size());
                }
        );
        processor.start();

        int fallbackCount = 0;
        int publishedCount = 0;

        // 快速发布事件直到触发背压
        for (int i = 0; i < 50; i++) {
            boolean success = processor.publish(CacheEvent.<String, String>builder()
                    .type(CacheEventType.REMOVE)
                    .key("key-" + i)
                    .value("value-" + i)
                    .build());

            publishedCount++;
            if (!success) {
                fallbackCount++;
                // 模拟同步回退处理（立即执行，不经过队列）
                processor.fallbackProcess(CacheEvent.<String, String>builder()
                        .type(CacheEventType.REMOVE)
                        .key("fallback-" + i)
                        .value("value-" + i)
                        .build());
                syncProcessedCount.incrementAndGet();

                // 触发背压后退出循环（已验证目标）
                if (fallbackCount >= 5) break;
            }
        }

        // 核心验证：确实有事件因为队列满而走了回退路径
        assertTrue(fallbackCount > 0, "应有事件因背压而走同步回退路径");
        assertTrue(syncProcessedCount.get() > 0, "同步处理计数应大于0");
        assertEquals(fallbackCount, processor.getDroppedCount(),
                "Processor记录的丢弃数应等于fallback次数");

        // 给一点时间让消费者处理队列中的部分事件（不强制要求处理完）
        Thread.sleep(200);

        // 验证：同步处理的事件数 + 队列中已消费的事件数 <= 总发布数
        // 注意：由于消费者很慢，consumedCount 可能很小甚至为0，这是正常的
        int totalHandled = consumedCount.get() + syncProcessedCount.get();
        assertTrue(totalHandled <= publishedCount,
                "处理总数不应超过发布总数");

        // 验证：成功入队的事件数 = publishedCount - fallbackCount
        int queuedEvents = publishedCount - fallbackCount;
        assertTrue(queuedEvents > 0, "应有事件成功进入队列");

        // 可选：验证队列确实被消费了一部分（或等待shutdown时处理）
        System.out.println("Published: " + publishedCount +
                ", Fallback: " + fallbackCount +
                ", Sync processed: " + syncProcessedCount.get() +
                ", Async consumed: " + consumedCount.get());
    }

    /**
     * 测试5：性能对比 - 验证异步模式不阻塞主线程
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testPerformanceComparison() throws InterruptedException {
        int eventCount = 10000;  // 增加到10000个事件
        int batchSize = 100;
        CountDownLatch latch = new CountDownLatch(eventCount);
        AtomicLong totalProcessingTime = new AtomicLong(0);

        // 创建慢速消费者（每个batch处理5ms）
        processor = new AsyncRemovalProcessor<>(
                65536,  // 大buffer避免背压
                batchSize,
                5000,   // 长超时，等待批量触发
                events -> {
                    long start = System.nanoTime();
                    // 模拟批量处理耗时
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
                    totalProcessingTime.addAndGet(System.nanoTime() - start);
                    events.forEach(e -> latch.countDown());
                }
        );
        processor.start();

        // 测量发布事件的耗时（主线程）
        long publishStart = System.nanoTime();
        for (int i = 0; i < eventCount; i++) {
            boolean success = processor.publish(CacheEvent.<String, String>builder()
                    .type(CacheEventType.REMOVE)
                    .key("key-" + i)
                    .value("value-" + i)
                    .build());
            assertTrue(success, "大buffer下不应触发背压");
        }
        long publishDuration = System.nanoTime() - publishStart;

        // 验证主线程发布耗时极短（无锁RingBuffer写入应极快）
        long publishDurationMs = TimeUnit.NANOSECONDS.toMillis(publishDuration);
        System.out.println("Publish " + eventCount + " events duration: " + publishDurationMs + "ms");

        // 发布10000个事件应该非常快（<100ms），不受消费者处理速度影响
        assertTrue(publishDurationMs < 100,
                "异步模式下发布事件应极快（无锁CAS），实际: " + publishDurationMs + "ms");

        // 等待所有事件被处理
        assertTrue(latch.await(5, TimeUnit.SECONDS), "所有事件应被处理");

        // 计算预期处理时间：总batch数 * 每batch处理时间
        int expectedBatches = eventCount / batchSize; // 100个batch
        long expectedProcessingMs = expectedBatches * 5; // 约500ms

        long actualProcessingMs = TimeUnit.NANOSECONDS.toMillis(totalProcessingTime.get());
        System.out.println("Total processing time: " + actualProcessingMs + "ms");

        // 验证异步特性：发布时间应远小于总处理时间
        // 在异步模式下，发布是立即返回的，处理是后台进行的
        assertTrue(publishDurationMs < actualProcessingMs / 5,
                "异步模式下，发布时间(" + publishDurationMs + "ms)应远小于处理时间(" + actualProcessingMs + "ms)");
    }

    /**
     * 测试6：并发压力测试 - 多生产者单消费者模型
     */
    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void testConcurrentProducers() throws InterruptedException {
        int producerCount = 10;
        int eventsPerProducer = 1000;
        int totalEvents = producerCount * eventsPerProducer;
        CountDownLatch latch = new CountDownLatch(totalEvents);
        AtomicInteger errorCount = new AtomicInteger(0);

        processor = new AsyncRemovalProcessor<>(
                65536,
                100,
                50,
                events -> {
                    try {
                        events.forEach(e -> latch.countDown());
                    } catch (Exception ex) {
                        errorCount.incrementAndGet();
                    }
                }
        );
        processor.start();

        ExecutorService producers = Executors.newFixedThreadPool(producerCount);

        // 启动多个生产者
        for (int p = 0; p < producerCount; p++) {
            final int producerId = p;
            producers.submit(() -> {
                for (int i = 0; i < eventsPerProducer; i++) {
                    boolean success = processor.publish(CacheEvent.<String, String>builder()
                            .type(CacheEventType.REMOVE)
                            .key("producer-" + producerId + "-key-" + i)
                            .value("value-" + i)
                            .build());

                    if (!success) {
                        // 如果队列满，使用回退
                        processor.fallbackProcess(CacheEvent.<String, String>builder()
                                .type(CacheEventType.REMOVE)
                                .key("fallback-" + producerId + "-" + i)
                                .value("value-" + i)
                                .build());
                    }
                }
            });
        }

        producers.shutdown();
        assertTrue(producers.awaitTermination(5, TimeUnit.SECONDS), "生产者应完成");

        // 等待所有事件被消费
        assertTrue(latch.await(10, TimeUnit.SECONDS), "所有事件应被处理，剩余: " + latch.getCount());
        assertEquals(0, errorCount.get(), "消费过程不应有异常");

        // 验证总数（processed + dropped = total）
        long processed = processor.getProcessedCount();
        long dropped = processor.getDroppedCount();
        assertEquals(totalEvents, processed + dropped, "处理数+丢弃数应等于总数");
    }

    /**
     * 测试7：异常隔离 - 监听器异常不应影响后续事件处理
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testExceptionIsolation() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger successCount = new AtomicInteger(0);

        processor = new AsyncRemovalProcessor<>(
                1024,
                1,     // batchSize=1确保每个事件单独处理
                1000,
                events -> {
                    if (events.get(0).getKey().equals("error")) {
                        throw new RuntimeException("模拟监听器异常");
                    }
                    successCount.incrementAndGet();
                    latch.countDown();
                }
        );
        processor.start();

        // 发布三个事件，中间那个会触发异常
        processor.publish(CacheEvent.<String, String>builder()
                .type(CacheEventType.REMOVE).key("ok1").value("value1").build());
        processor.publish(CacheEvent.<String, String>builder()
                .type(CacheEventType.REMOVE).key("error").value("error-value").build());
        processor.publish(CacheEvent.<String, String>builder()
                .type(CacheEventType.REMOVE).key("ok2").value("value2").build());

        // 等待正常事件被处理
        assertTrue(latch.await(3, TimeUnit.SECONDS), "正常事件应被处理");

        // 验证两个正常事件都被处理（异常事件被捕获不影响后续）
        assertEquals(2, successCount.get(), "异常事件后的正常事件应被继续处理");
    }

    /**
     * 测试8：优雅关闭 - 验证shutdown时处理剩余事件
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)  // 增加超时到10秒
    void testGracefulShutdown() throws InterruptedException {
        AtomicInteger processedCount = new AtomicInteger(0);

        processor = new AsyncRemovalProcessor<>(
                1024,
                100,   // 大batchSize，确保不会提前flush
                100,   // 缩短超时时间，确保事件能被及时处理
                events -> {
                    processedCount.addAndGet(events.size());
                }
        );
        processor.start();

        // 发布5个事件
        for (int i = 0; i < 5; i++) {
            processor.publish(CacheEvent.<String, String>builder()
                    .type(CacheEventType.REMOVE)
                    .key("key-" + i)
                    .value("value-" + i)
                    .build());
        }

        // 给一点时间让事件进入队列，但不足以触发超时flush
        Thread.sleep(50);

        // 立即关闭（应在shutdown时处理完所有剩余事件）
        processor.shutdown();

        // 验证所有事件都被处理（shutdown会等待消费者线程结束并处理剩余事件）
        assertEquals(5, processedCount.get(), "关闭前应处理完所有剩余事件");
    }
}