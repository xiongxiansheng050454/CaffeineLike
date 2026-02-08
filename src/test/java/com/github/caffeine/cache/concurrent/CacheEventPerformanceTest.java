package com.github.caffeine.cache.concurrent;

import com.github.caffeine.cache.event.CacheEvent;
import com.github.caffeine.cache.event.CacheEventType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 性能基准测试 - 验证 >100万事件/秒吞吐量
 */
@DisplayName("CacheEventRingBuffer Performance Tests")
@EnabledIfSystemProperty(named = "performanceTest", matches = "true")
public class CacheEventPerformanceTest {

    @Test
    @DisplayName("单生产者单消费者吞吐量测试")
    void testSingleProducerSingleConsumerThroughput() throws InterruptedException {
        int eventCount = 10_000_000;
        CountDownLatch latch = new CountDownLatch(eventCount);
        AtomicLong received = new AtomicLong(0);

        Consumer<CacheEvent<String, String>> consumer = event -> {
            received.incrementAndGet();
            latch.countDown();
        };

        // 注意：不需要ExecutorService，RingBuffer内部管理线程
        CacheEventRingBuffer<String, String> buffer = new CacheEventRingBuffer<>(
                1024 * 64,  // 64K buffer
                false,      // 不丢弃
                consumer
        );

        // 预热
        for (int i = 0; i < 10000; i++) {
            buffer.publish(CacheEvent.<String, String>builder()
                    .type(CacheEventType.WRITE)
                    .key("warmup")
                    .value("value")
                    .build());
        }
        Thread.sleep(100);

        // 正式测试
        long start = System.nanoTime();

        for (int i = 0; i < eventCount; i++) {
            CacheEvent<String, String> event = CacheEvent.<String, String>builder()
                    .type(CacheEventType.WRITE)
                    .key("key-" + i)
                    .value("value-" + i)
                    .build();

            while (!buffer.publish(event)) {
                Thread.onSpinWait(); // 自旋等待
            }
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS),
                String.format("Only received %d/%d events", received.get(), eventCount));
        long duration = System.nanoTime() - start;

        double throughput = (eventCount * 1_000_000_000.0) / duration;
        System.out.printf("Single-Producer-Single-Consumer: %,.0f events/sec (%,d events in %.2f ms)%n",
                throughput, eventCount, duration / 1_000_000.0);

        assertTrue(throughput > 1_000_000,
                String.format("Throughput %.0f should be > 1,000,000 events/sec", throughput));

        buffer.shutdown();
    }

    @Test
    @DisplayName("单生产者多消费者轮询吞吐量测试")
    void testSingleProducerMultiConsumerThroughput() throws InterruptedException {
        int eventCount = 10_000_000;
        int consumerCount = 4;

        // 关键修正：轮询分发，每个事件只被一个消费者处理
        // 所以每个消费者应收到 eventCount/consumerCount 个事件
        CountDownLatch latch = new CountDownLatch(eventCount);
        AtomicLong[] received = new AtomicLong[consumerCount];
        for (int i = 0; i < consumerCount; i++) {
            received[i] = new AtomicLong(0);
        }

        @SuppressWarnings("unchecked")
        Consumer<CacheEvent<String, String>>[] consumers = new Consumer[consumerCount];
        for (int i = 0; i < consumerCount; i++) {
            final int id = i;
            consumers[i] = event -> {
                received[id].incrementAndGet();
                latch.countDown();
            };
        }

        CacheEventRingBuffer<String, String> buffer = new CacheEventRingBuffer<>(
                1024 * 128, // 128K buffer
                false,
                consumers
        );

        // 预热
        for (int i = 0; i < 100000; i++) {
            buffer.publish(CacheEvent.<String, String>builder()
                    .type(CacheEventType.READ)
                    .key("warmup")
                    .value("value")
                    .build());
        }
        Thread.sleep(100);

        long start = System.nanoTime();

        for (int i = 0; i < eventCount; i++) {
            while (!buffer.publish(CacheEvent.<String, String>builder()
                    .type(CacheEventType.WRITE)
                    .key("key-" + i)
                    .value("value")
                    .build())) {
                Thread.onSpinWait();
            }
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS),
                String.format("Timeout, remaining: %d", latch.getCount()));
        long duration = System.nanoTime() - start;

        double throughput = (eventCount * 1_000_000_000.0) / duration;
        System.out.printf("Single-Producer-%d-Consumers: %,.0f events/sec%n",
                consumerCount, throughput);

        // 打印每个消费者的负载
        for (int i = 0; i < consumerCount; i++) {
            System.out.printf("  Consumer[%d]: %,d events%n", i, received[i].get());
        }

        assertTrue(throughput > 1_000_000,
                String.format("Throughput %.0f should be > 1,000,000 events/sec", throughput));

        buffer.shutdown();
    }

    @Test
    @DisplayName("多生产者单消费者并发写入测试")
    void testMultiProducerSingleConsumer() throws InterruptedException {
        int producerCount = 8;
        int eventsPerProducer = 1_000_000;
        int totalEvents = producerCount * eventsPerProducer;

        CountDownLatch latch = new CountDownLatch(totalEvents);
        AtomicLong received = new AtomicLong(0);

        Consumer<CacheEvent<String, String>> consumer = event -> {
            received.incrementAndGet();
            latch.countDown();
        };

        CacheEventRingBuffer<String, String> buffer = new CacheEventRingBuffer<>(
                1024 * 128, false, consumer);

        ExecutorService producers = Executors.newFixedThreadPool(producerCount);

        // 预热
        Thread.sleep(50);

        long start = System.nanoTime();

        // 启动多个生产者并发写入
        for (int p = 0; p < producerCount; p++) {
            final int producerId = p;
            producers.submit(() -> {
                for (int i = 0; i < eventsPerProducer; i++) {
                    CacheEvent<String, String> event = CacheEvent.<String, String>builder()
                            .type(CacheEventType.WRITE)
                            .key("producer-" + producerId + "-key-" + i)
                            .value("value")
                            .build();

                    while (!buffer.publish(event)) {
                        Thread.onSpinWait();
                    }
                }
            });
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS),
                String.format("Timeout, received %d/%d", received.get(), totalEvents));
        long duration = System.nanoTime() - start;

        double throughput = (totalEvents * 1_000_000_000.0) / duration;
        System.out.printf("%d-Producers-Single-Consumer: %,.0f events/sec%n",
                producerCount, throughput);

        assertTrue(throughput > 1_000_000,
                String.format("Throughput %.0f should be > 1,000,000 events/sec", throughput));

        producers.shutdown();
        buffer.shutdown();
    }

    @Test
    @DisplayName("延迟测试（P99）")
    void testLatency() throws InterruptedException {
        int sampleCount = 1_000_000;
        // 使用LongAdder避免竞争
        LongAdder[] latencyBuckets = new LongAdder[100]; // 0-10us, 10-20us...
        for (int i = 0; i < latencyBuckets.length; i++) {
            latencyBuckets[i] = new LongAdder();
        }

        AtomicLong totalLatency = new AtomicLong(0);
        AtomicLong maxLatency = new AtomicLong(0);

        Consumer<CacheEvent<String, String>> consumer = event -> {
            // 注意：CacheEvent.timestamp是在构造时设置的System.nanoTime()
            long latency = System.nanoTime() - event.getTimestamp();
            totalLatency.addAndGet(latency);

            // 更新max
            long currentMax;
            do {
                currentMax = maxLatency.get();
            } while (latency > currentMax && !maxLatency.compareAndSet(currentMax, latency));

            // 分桶统计（每10us一个桶）
            int bucket = (int) (latency / 10_000);
            if (bucket < latencyBuckets.length) {
                latencyBuckets[bucket].increment();
            }
        };

        CacheEventRingBuffer<String, String> buffer = new CacheEventRingBuffer<>(
                1024 * 16, false, consumer);

        // 发送事件
        long start = System.nanoTime();
        for (int i = 0; i < sampleCount; i++) {
            CacheEvent<String, String> event = CacheEvent.<String, String>builder()
                    .type(CacheEventType.WRITE)
                    .key("latency-test")
                    .value("value")
                    .build();
            buffer.publish(event);
        }

        // 等待所有事件处理完成
        Thread.sleep(2000);

        long duration = System.nanoTime() - start;
        double avgLatency = (double) totalLatency.get() / sampleCount;

        // 计算P99
        long p99Threshold = (long) (sampleCount * 0.99);
        long cumulative = 0;
        long p99Latency = 0;
        for (int i = 0; i < latencyBuckets.length; i++) {
            cumulative += latencyBuckets[i].sum();
            if (cumulative >= p99Threshold) {
                p99Latency = (i + 1) * 10_000; // 纳秒
                break;
            }
        }

        System.out.printf("Latency - Avg: %.0f ns, Max: %,d ns, P99: %,d ns (%.2f us)%n",
                avgLatency, maxLatency.get(), p99Latency, p99Latency / 1000.0);
        System.out.printf("Throughput: %,.0f events/sec%n",
                (sampleCount * 1_000_000_000.0) / duration);

        // P99应小于50微秒（50000纳秒）- 放宽要求以适应CI环境
        assertTrue(p99Latency < 50_000,
                String.format("P99 latency %d ns should be < 50000 ns (50 us)", p99Latency));

        buffer.shutdown();
    }

    @Test
    @DisplayName("突发流量处理能力测试")
    void testBurstTraffic() throws InterruptedException {
        int burstSize = 5_000_000;
        AtomicLong received = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(burstSize);

        Consumer<CacheEvent<String, String>> consumer = event -> {
            received.incrementAndGet();
            latch.countDown();
        };

        // 小缓冲区，测试反压能力
        CacheEventRingBuffer<String, String> buffer = new CacheEventRingBuffer<>(
                1024, false, consumer);

        long start = System.nanoTime();

        // 突发写入
        for (int i = 0; i < burstSize; i++) {
            while (!buffer.publish(CacheEvent.<String, String>builder()
                    .type(CacheEventType.WRITE)
                    .key("burst-" + i)
                    .value("value")
                    .build())) {
                // 缓冲区满时短暂自旋
                Thread.onSpinWait();
            }
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS));
        long duration = System.nanoTime() - start;

        double throughput = (burstSize * 1_000_000_000.0) / duration;
        System.out.printf("Burst Traffic (5M events): %,.0f events/sec%n", throughput);

        buffer.shutdown();
    }

    @Test
    @DisplayName("内存稳定性测试（无泄漏）")
    void testMemoryStability() throws InterruptedException {
        int iterations = 100;
        int eventsPerIteration = 100_000;

        AtomicLong processed = new AtomicLong(0);
        Consumer<CacheEvent<String, byte[]>> consumer = event -> {
            processed.incrementAndGet();
            // 模拟处理时间，制造消费滞后
            if (processed.get() % 1000 == 0) {
                Thread.yield();
            }
        };

        // 小缓冲区（1024），强制触发反压/丢弃
        CacheEventRingBuffer<String, byte[]> buffer = new CacheEventRingBuffer<>(
                1024, true, consumer); // dropOnFull = true

        Runtime runtime = Runtime.getRuntime();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();

        for (int i = 0; i < iterations; i++) {
            // 发送大对象事件
            int published = 0;
            for (int j = 0; j < eventsPerIteration; j++) {
                boolean success = buffer.publish(CacheEvent.<String, byte[]>builder()
                        .type(CacheEventType.WRITE)
                        .key("key-" + j)
                        .value(new byte[1024]) // 1KB payload
                        .build());
                if (success) published++;
            }

            // 每轮等待消费完成
            Thread.sleep(50);

            // 强制GC
            if (i % 10 == 0) {
                System.gc();
                Thread.sleep(100);
            }
        }

        // 最终等待
        Thread.sleep(1000);
        System.gc();
        Thread.sleep(500);

        long memAfter = runtime.totalMemory() - runtime.freeMemory();
        long memDiff = (memAfter - memBefore) / 1024 / 1024; // MB

        System.out.printf("Memory delta: %d MB (processed: %,d events)%n",
                memDiff, processed.get());
        System.out.printf("Drop rate: %.2f%%%n",
                (1.0 - (double) processed.get() / (iterations * eventsPerIteration)) * 100);

        // 内存增长应小于100MB（允许一定波动）
        assertTrue(memDiff < 100,
                String.format("Memory leak detected: %d MB increase", memDiff));

        buffer.shutdown();
    }
}