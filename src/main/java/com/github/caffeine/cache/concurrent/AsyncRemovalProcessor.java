package com.github.caffeine.cache.concurrent;

import com.github.caffeine.cache.event.CacheEvent;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * 异步RemovalListener处理器 - 基于无锁RingBuffer
 * 特性：批量聚合、超时_flush、背压回退（同步执行）
 *
 * 参考Caffeine的AsyncEvictionListener设计模式
 */
public class AsyncRemovalProcessor<K, V> {

    // ========== RingBuffer 无锁队列 ==========
    private final Object[] buffer;
    private final int mask;
    private final int capacity;

    // 序列号控制（使用VarHandle实现无锁并发）
    private volatile long writeSeq = 0;
    private volatile long readSeq = 0;

    private static final VarHandle WRITE_SEQ;
    private static final VarHandle READ_SEQ;
    private static final VarHandle BUFFER_ELEM;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            WRITE_SEQ = lookup.findVarHandle(AsyncRemovalProcessor.class, "writeSeq", long.class);
            READ_SEQ = lookup.findVarHandle(AsyncRemovalProcessor.class, "readSeq", long.class);
            BUFFER_ELEM = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ========== 批量处理配置 ==========
    private final int batchSize;
    private final long flushIntervalNanos;
    private final Consumer<List<CacheEvent<K, V>>> batchListener;

    // ========== 线程控制 ==========
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    // 统计信息（使用LongAdder避免伪共享，但这里简化用volatile）
    private volatile long droppedEvents = 0;
    private volatile long processedEvents = 0;

    public AsyncRemovalProcessor(
            int bufferSize,           // RingBuffer容量（2的幂）
            int batchSize,            // 批量触发阈值
            long flushIntervalMs,     // 超时flush间隔
            Consumer<List<CacheEvent<K, V>>> batchListener  // 批量消费者
    ) {
        // 确保2的幂
        int size = 1;
        while (size < bufferSize) size <<= 1;
        this.capacity = size;
        this.mask = size - 1;
        this.buffer = new Object[size];

        this.batchSize = batchSize;
        this.flushIntervalNanos = TimeUnit.MILLISECONDS.toNanos(flushIntervalMs);
        this.batchListener = batchListener;

        // 单线程消费者（保证事件顺序性）
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "async-removal-processor");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动消费者线程
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            executor.submit(this::processLoop);
        }
    }

    /**
     * 优雅关闭
     */
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            running.set(false);
            // 等待剩余事件处理
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

    /**
     * 发布事件 - 非阻塞，支持背压
     * @return true=成功入队, false=队列满（需回退处理）
     */
    public boolean publish(CacheEvent<K, V> event) {
        if (shutdown.get()) return false;

        long currentWrite;
        long nextWrite;

        // CAS自旋写入
        do {
            currentWrite = (long) WRITE_SEQ.getAcquire(this);
            nextWrite = currentWrite + 1;

            // 检查队列满（保留一个槽位区分空/满）
            if (nextWrite - (long) READ_SEQ.getAcquire(this) > capacity - 1) {
                return false; // 背压信号
            }
        } while (!WRITE_SEQ.compareAndSet(this, currentWrite, nextWrite));

        int index = (int) (currentWrite & mask);
        BUFFER_ELEM.setRelease(buffer, index, event);
        return true;
    }

    /**
     * 强制发布 - 阻塞直到成功（用于关键事件）
     */
    public void publishForce(CacheEvent<K, V> event) throws InterruptedException {
        while (!publish(event)) {
            if (Thread.interrupted()) throw new InterruptedException();
            LockSupport.parkNanos(1000); // 1微秒自旋
        }
    }

    /**
     * 消费者主循环 - 批量聚合逻辑
     */
    private void processLoop() {
        List<CacheEvent<K, V>> batch = new ArrayList<>(batchSize);
        long lastFlush = System.nanoTime();

        while (running.get()) {
            // 1. 批量拉取（非阻塞）
            int drained = drainTo(batch, batchSize - batch.size());

            long now = System.nanoTime();
            boolean timeout = (now - lastFlush) >= flushIntervalNanos;

            // 2. 触发条件：攒够批量 或 超时 或 队列满风险
            if (!batch.isEmpty() && (batch.size() >= batchSize || timeout)) {
                flush(batch);
                batch = new ArrayList<>(batchSize);
                lastFlush = now;
            }

            // 3. 无事件时休眠（避免CPU空转）
            if (drained == 0) {
                LockSupport.parkNanos(100_000); // 100微秒
            }
        }

        // 4. 处理剩余事件
        drainTo(batch, Integer.MAX_VALUE);
        if (!batch.isEmpty()) {
            flush(batch);
        }
    }

    /**
     * 从RingBuffer拉取数据
     */
    @SuppressWarnings("unchecked")
    private int drainTo(List<CacheEvent<K, V>> container, int maxElements) {
        long currentRead = (long) READ_SEQ.getAcquire(this);
        long available = (long) WRITE_SEQ.getAcquire(this) - currentRead;
        int toRead = (int) Math.min(available, maxElements);

        for (int i = 0; i < toRead; i++) {
            int index = (int) ((currentRead + i) & mask);
            CacheEvent<K, V> event = (CacheEvent<K, V>) BUFFER_ELEM.getAcquire(buffer, index);
            container.add(event);
            BUFFER_ELEM.setRelease(buffer, index, null); // 帮助GC
        }

        if (toRead > 0) {
            READ_SEQ.setRelease(this, currentRead + toRead);
        }
        return toRead;
    }

    /**
     * 执行批量回调（异常隔离）
     */
    private void flush(List<CacheEvent<K, V>> events) {
        try {
            batchListener.accept(new ArrayList<>(events)); // 防御性拷贝
            processedEvents += events.size();
        } catch (Exception e) {
            // 监听器异常不应影响缓存主流程
            System.err.println("[AsyncRemoval] Listener error: " + e.getMessage());
            e.printStackTrace();
        }
        events.clear();
    }

    /**
     * 同步回退处理 - 当RingBuffer满时直接执行（避免事件丢失）
     */
    public void fallbackProcess(CacheEvent<K, V> event) {
        droppedEvents++;
        List<CacheEvent<K, V>> single = new ArrayList<>(1);
        single.add(event);
        try {
            batchListener.accept(single);
        } catch (Exception e) {
            System.err.println("[AsyncRemoval] Fallback error: " + e.getMessage());
        }
    }

    public long getProcessedCount() { return processedEvents; }
    public long getDroppedCount() { return droppedEvents; }
}