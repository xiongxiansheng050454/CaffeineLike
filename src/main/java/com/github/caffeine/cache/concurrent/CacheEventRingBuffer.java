package com.github.caffeine.cache.concurrent;

import com.github.caffeine.cache.event.CacheEvent;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 无锁RingBuffer - 专为缓存事件优化（线程独立版）
 * 单生产者多消费者，每个消费者运行在独立守护线程中
 */
public final class CacheEventRingBuffer<K, V> {

    private final int capacity;
    private final int mask;
    private final CacheEvent<K, V>[] buffer;

    // 写序列号（单生产者）
    private volatile long writeSeq = 0;

    // 每个消费者的进度（下一个要消费的序列号）
    private final AtomicLong[] consumerProgress;
    private final int consumerCount;
    private final Thread[] consumerThreads;

    // VarHandle
    private static final VarHandle WRITE_SEQ;
    private static final VarHandle BUFFER;

    private final Consumer<CacheEvent<K, V>>[] consumers;
    private final boolean dropOnFull;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            WRITE_SEQ = lookup.findVarHandle(CacheEventRingBuffer.class, "writeSeq", long.class);
            BUFFER = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unchecked")
    public CacheEventRingBuffer(int capacity, boolean dropOnFull,
                                Consumer<CacheEvent<K, V>>... consumers) {
        if (consumers == null || consumers.length == 0) {
            throw new IllegalArgumentException("At least one consumer required");
        }

        this.consumerCount = consumers.length;
        this.consumerProgress = new AtomicLong[consumerCount];
        this.consumerThreads = new Thread[consumerCount];
        this.consumers = consumers;
        this.dropOnFull = dropOnFull;

        // 初始值：消费者i从序列号i开始消费，步长为consumerCount
        for (int i = 0; i < consumerCount; i++) {
            this.consumerProgress[i] = new AtomicLong(i);
        }

        // 确保2的幂次方
        int size = 1;
        while (size < capacity) size <<= 1;
        this.capacity = size;
        this.mask = size - 1;
        this.buffer = new CacheEvent[size];

        startConsumers();
    }

    /**
     * 发布事件 - 非阻塞
     * 检查所有消费者最小进度，确保不覆盖未消费数据
     */
    public boolean publish(CacheEvent<K, V> event) {
        long current = (long) WRITE_SEQ.getAcquire(this);
        long next = current + 1;

        // 计算所有消费者中的最小进度
        long minSeq = Long.MAX_VALUE;
        for (AtomicLong progress : consumerProgress) {
            long p = progress.get();
            if (p < minSeq) minSeq = p;
        }

        // 检查是否会覆盖未消费数据
        if (current - minSeq >= capacity) {
            return false;
        }

        // CAS更新写序列号
        if (WRITE_SEQ.compareAndSet(this, current, next)) {
            int index = (int) (current & mask);
            BUFFER.setRelease(buffer, index, event);
            return true;
        }
        return false;
    }

    /**
     * 强制发布 - 阻塞直到成功
     */
    public void publishForce(CacheEvent<K, V> event) throws InterruptedException {
        while (!publish(event)) {
            Thread.onSpinWait();
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
    }

    /**
     * 启动消费者线程 - 每个消费者一个独立守护线程
     */
    private void startConsumers() {
        for (int i = 0; i < consumers.length; i++) {
            final int consumerId = i;
            Thread t = new Thread(() -> consumeLoop(consumerId), "ringbuffer-consumer-" + i);
            t.setDaemon(true);
            t.start();
            consumerThreads[i] = t;
        }
    }

    /**
     * 消费者循环 - 每个消费者消费特定序列号
     * 消费者i消费: i, i+consumerCount, i+2*consumerCount...
     */
    private void consumeLoop(int consumerId) {
        long nextSeq = consumerId;

        while (!Thread.currentThread().isInterrupted()) {
            CacheEvent<K, V> event = null;
            int index = -1;

            try {
                // 等待数据写入
                while ((long) WRITE_SEQ.getAcquire(this) <= nextSeq) {
                    Thread.onSpinWait();
                }

                index = (int) (nextSeq & mask);

                // 自旋等待数据可用
                while ((event = (CacheEvent<K, V>) BUFFER.getAcquire(buffer, index)) == null) {
                    Thread.onSpinWait();
                }

                // 消费事件（可能抛出异常）
                consumers[consumerId].accept(event);

            } catch (Exception e) {
                // 监听器异常隔离：记录但不传播
                System.err.println("CacheEvent listener error: " + e.getMessage());
            } finally {
                // 关键：无论成功与否，都要推进进度，避免死循环
                if (index >= 0) {
                    BUFFER.setRelease(buffer, index, null);
                }
                consumerProgress[consumerId].set(nextSeq);
                nextSeq += consumerCount;
            }
        }
    }

    /**
     * 关闭所有消费者线程
     */
    public void shutdown() {
        for (Thread t : consumerThreads) {
            if (t != null) {
                t.interrupt();
            }
        }
    }
}