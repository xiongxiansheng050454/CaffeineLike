package com.github.caffeine.cache.concurrent;

import com.github.caffeine.cache.Node;
import jdk.internal.vm.annotation.Contended;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

/**
 * 专用后台驱逐调度器 - 异步处理缓存驱逐
 * 特性：MPSC任务队列、批量驱逐、自适应频率
 */
public class EvictionScheduler<K, V> {

    // MPSC 任务队列：读写线程提交，单消费者线程处理
    private final ArrayBlockingQueue<EvictionTask<K, V>> taskQueue;
    private final int batchSize;
    private final long evictionIntervalMs;

    // 执行器
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    // 统计信息
    @Contended
    private final LongAdder submittedTasks = new LongAdder();
    @Contended
    private final LongAdder evictedCount = new LongAdder();
    @Contended
    private final LongAdder rejectedTasks = new LongAdder();

    // 驱逐函数引用：实际执行删除的回调（由 BoundedLocalCache 提供）
    private final BiConsumer<K, Node<K, V>> evictionExecutor;
    private final BiConsumer<K, V> eventPublisher;

    public EvictionScheduler(
            int queueCapacity,
            int batchSize,
            long evictionIntervalMs,
            BiConsumer<K, Node<K, V>> evictionExecutor,
            BiConsumer<K, V> eventPublisher
    ) {
        this.taskQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.batchSize = batchSize;
        this.evictionIntervalMs = evictionIntervalMs;
        this.evictionExecutor = evictionExecutor;
        this.eventPublisher = eventPublisher;

        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "async-eviction-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动后台线程
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            executor.submit(this::evictionLoop);
        }
    }

    /**
     * 提交驱逐候选（非阻塞，读写线程调用）
     * @return true=提交成功, false=队列满（背压）
     */
    public boolean submitCandidate(K key, Node<K, V> node, int frequency, long accessTime) {
        if (shutdown.get()) return false;

        submittedTasks.increment();

        // 构造轻量级任务（不持有 heavy reference）
        EvictionTask<K, V> task = new EvictionTask<>(key, node, frequency, accessTime);

        if (!taskQueue.offer(task)) {
            rejectedTasks.increment();
            return false; // 背压信号：调用方应同步处理或丢弃
        }
        return true;
    }

    /**
     * 批量提交候选（用于定期扫描）
     */
    public int submitCandidates(List<EvictionCandidate<K, V>> candidates) {
        int submitted = 0;
        for (EvictionCandidate<K, V> c : candidates) {
            if (submitCandidate(c.key(), c.node(), c.frequency(), c.accessTime())) {
                submitted++;
            } else {
                break; // 队列满，停止提交
            }
        }
        return submitted;
    }

    /**
     * 触发立即驱逐（用于紧急内存释放）
     */
    public void triggerEmergencyEviction() {
        taskQueue.offer(new EvictionTask<>(null, null, -1, -1, true));
    }

    /**
     * 驱逐主循环 - 单线程消费
     */
    private void evictionLoop() {
        List<EvictionTask<K, V>> batch = new ArrayList<>(batchSize);
        long lastEviction = System.currentTimeMillis();

        while (running.get() && !Thread.interrupted()) {
            batch.clear();

            // 1. 批量拉取任务（阻塞或超时）
            try {
                long timeout = evictionIntervalMs - (System.currentTimeMillis() - lastEviction);
                if (timeout > 0) {
                    EvictionTask<K, V> first = taskQueue.poll(timeout, TimeUnit.MILLISECONDS);
                    if (first != null) {
                        batch.add(first);
                        taskQueue.drainTo(batch, batchSize - 1);
                    }
                } else {
                    // 超时，立即处理
                    taskQueue.drainTo(batch, batchSize);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            // 2. 处理批量驱逐
            if (!batch.isEmpty()) {
                processBatch(batch);
                lastEviction = System.currentTimeMillis();
            }

            // 3. 定期检查是否需主动扫描（防止任务遗漏）
            if (System.currentTimeMillis() - lastEviction > evictionIntervalMs * 2) {
                // 发送扫描信号给缓存主类（通过回调或标志位）
                // 实际实现中可触发 BoundedLocalCache 的扫描逻辑
            }
        }

        // 4. 处理剩余任务（优雅关闭）
        drainRemaining();
    }

    /**
     * 批量处理驱逐
     */
    private void processBatch(List<EvictionTask<K, V>> batch) {
        // 按频率+时间排序（低的优先驱逐）
        batch.sort((a, b) -> {
            if (a.emergency()) return -1;
            if (b.emergency()) return 1;

            int freqCmp = Integer.compare(a.frequency(), b.frequency());
            return freqCmp != 0 ? freqCmp : Long.compare(a.accessTime(), b.accessTime());
        });

        int success = 0;
        for (EvictionTask<K, V> task : batch) {
            if (task.emergency()) {
                // 紧急驱逐：调用方已准备好候选列表
                continue;
            }

            try {
                // 执行实际驱逐（回调 BoundedLocalCache 的方法）
                evictionExecutor.accept(task.key(), task.node());

                // 发布事件
                if (eventPublisher != null && task.node() != null) {
                    V value = task.node().getValue();
                    if (value != null) {
                        eventPublisher.accept(task.key(), value);
                    }
                }

                success++;
            } catch (Exception e) {
                System.err.println("[EvictionScheduler] 驱逐失败: " + e.getMessage());
            }
        }

        evictedCount.add(success);
    }

    /**
     * 排空剩余任务
     */
    private void drainRemaining() {
        List<EvictionTask<K, V>> remaining = new ArrayList<>();
        taskQueue.drainTo(remaining);
        processBatch(remaining);
    }

    /**
     * 优雅关闭
     */
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            running.set(false);
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    // === 统计数据 ===
    public long getSubmittedCount() { return submittedTasks.sum(); }
    public long getEvictedCount() { return evictedCount.sum(); }
    public long getRejectedCount() { return rejectedTasks.sum(); }
    public int getPendingCount() { return taskQueue.size(); }

    /**
     * 驱逐任务内部类（轻量级不可变对象）
     */
    private record EvictionTask<K, V>(
            K key,
            Node<K, V> node,
            int frequency,
            long accessTime,
            boolean emergency
    ) {
        EvictionTask(K key, Node<K, V> node, int frequency, long accessTime) {
            this(key, node, frequency, accessTime, false);
        }
    }

    /**
     * 驱逐候选记录（用于批量提交接口）
     */
    public record EvictionCandidate<K, V>(K key, Node<K, V> node, int frequency, long accessTime) {}
}