package com.github.caffeine.cache.concurrent;

import com.github.caffeine.cache.event.CacheEvent;
import com.github.caffeine.cache.event.CacheEventType;
import jdk.internal.vm.annotation.Contended;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * MPSC无锁写缓冲与合并器 - 修复版
 */
public class WriteBuffer<K, V> {

    private final Object[] buffer;
    private final int mask;
    private final int capacity;

    @Contended
    private volatile long writeSeq = 0;
    private static final VarHandle WRITE_SEQ;

    @Contended
    private volatile long readSeq = 0;
    private static final VarHandle READ_SEQ;
    private static final VarHandle BUFFER_ELEM;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            WRITE_SEQ = lookup.findVarHandle(WriteBuffer.class, "writeSeq", long.class);
            READ_SEQ = lookup.findVarHandle(WriteBuffer.class, "readSeq", long.class);
            BUFFER_ELEM = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final ConcurrentHashMap<K, WriteTask<K, V>> mergeWindow;
    private final int mergeWindowSize;

    private final int batchSize;
    private final long flushIntervalNanos;
    private volatile Thread flushThread;
    private final BiConsumer<K, V> writeFunction;
    private final Consumer<CacheEvent<K, V>> eventPublisher;

    @Contended
    private final LongAdder submittedCount = new LongAdder();
    @Contended
    private final LongAdder mergedCount = new LongAdder();
    @Contended
    private final LongAdder flushedCount = new LongAdder();

    public WriteBuffer(int bufferSize, int mergeSize, int batchSize,
                       long flushIntervalMs,
                       BiConsumer<K, V> writeFunction,
                       Consumer<CacheEvent<K, V>> eventPublisher) {
        int size = 1;
        while (size < bufferSize) size <<= 1;
        this.capacity = size;
        this.mask = size - 1;
        this.buffer = new Object[size];

        this.mergeWindowSize = mergeSize;
        this.mergeWindow = new ConcurrentHashMap<>(mergeSize);

        this.batchSize = batchSize;
        this.flushIntervalNanos = TimeUnit.MILLISECONDS.toNanos(flushIntervalMs);
        this.writeFunction = writeFunction;
        this.eventPublisher = eventPublisher;

        startFlushThread();
    }

    public boolean submit(K key, V value, CacheEventType type, long expireAt) {
        submittedCount.increment();

        long currentWrite;
        long nextWrite;

        do {
            currentWrite = (long) WRITE_SEQ.getAcquire(this);
            nextWrite = currentWrite + 1;

            long currentRead = (long) READ_SEQ.getAcquire(this);
            if (nextWrite - currentRead > capacity - 1) {
                return false;
            }
        } while (!WRITE_SEQ.compareAndSet(this, currentWrite, nextWrite));

        int index = (int) (currentWrite & mask);
        WriteTask<K, V> task = new WriteTask<>(key, value, type, expireAt);
        BUFFER_ELEM.setRelease(buffer, index, task);
        return true;
    }

    public void submitForce(K key, V value, CacheEventType type, long expireAt) {
        while (!submit(key, value, type, expireAt)) {
            LockSupport.parkNanos(1000);
            Thread.yield();
        }
    }

    // 修复：同时查 mergeWindow 和 RingBuffer
    public V getPending(K key, long now) {
        // 1. 查 mergeWindow
        WriteTask<K, V> task = mergeWindow.get(key);
        if (task != null) {
            return task.isExpired(now) ? null : task.value;
        }

        // 2. 查 RingBuffer（从 readSeq 到 writeSeq）
        long currentRead = (long) READ_SEQ.getAcquire(this);
        long currentWrite = (long) WRITE_SEQ.getAcquire(this);

        // 关键修复：从后往前扫描，确保读到同一 key 的最后一次写入
        for (long seq = currentWrite - 1; seq >= currentRead; seq--) {
            int index = (int) (seq & mask);
            @SuppressWarnings("unchecked")
            WriteTask<K, V> pending = (WriteTask<K, V>) BUFFER_ELEM.getAcquire(buffer, index);
            if (pending != null && Objects.equals(pending.key, key)) {
                return pending.isExpired(now) ? null : pending.value;
            }
        }

        // 短暂自旋等待
        for (int i = 0; i < 1000; i++) {
            task = mergeWindow.get(key);
            if (task != null) {
                return task.isExpired(now) ? null : task.value;
            }
            Thread.onSpinWait();
        }

        return null;
    }

    // WriteBuffer.java 中的 isPendingDelete 方法
    public boolean isPendingDelete(K key) {
        // 增加到 10 次，并添加自旋
        int retries = 10;
        while (retries-- > 0) {
            long writeSeqBefore = (long) WRITE_SEQ.getAcquire(this);

            // 1. 检查 mergeWindow
            WriteTask<K, V> task = mergeWindow.get(key);
            if (task != null) {
                return task.value == null && task.type == CacheEventType.REMOVE;
            }

            // 2. 检查 RingBuffer（从后往前找最新的该key操作）
            long currentRead = (long) READ_SEQ.getAcquire(this);
            long currentWrite = (long) WRITE_SEQ.getAcquire(this);

            for (long seq = currentWrite - 1; seq >= currentRead; seq--) {
                int index = (int) (seq & mask);
                @SuppressWarnings("unchecked")
                WriteTask<K, V> pending = (WriteTask<K, V>) BUFFER_ELEM.getAcquire(buffer, index);
                if (pending != null && Objects.equals(pending.key, key)) {
                    return pending.value == null && pending.type == CacheEventType.REMOVE;
                }
            }

            // 3. 再次检查 mergeWindow（可能在检查RingBuffer期间被移入）
            task = mergeWindow.get(key);
            if (task != null) {
                return task.value == null && task.type == CacheEventType.REMOVE;
            }

            // 4. 如果 writeSeq 未变化，说明没有新数据竞争，可以安全返回
            long writeSeqAfter = (long) WRITE_SEQ.getAcquire(this);
            if (writeSeqAfter == writeSeqBefore) {
                return false;
            }

            // 关键修复：短暂自旋，给 flush 线程时间完成数据移动
            if (retries > 0) {
                Thread.onSpinWait();
            }
        }
        return false;
    }

    /**
     * 同步模式下调用：从 mergeWindow 中移除该 key 的待写入任务
     * （防止 put 在缓冲中，invalidate 直接删 segment，随后 put 刷盘导致复活）
     */
    public void removePending(K key) {
        mergeWindow.remove(key);
        // Note: RingBuffer 中的任务无法高效移除，依赖 isPendingDelete 的检测
    }

    private void startFlushThread() {
        this.flushThread = new Thread(this::flushLoop, "write-buffer-flusher");
        flushThread.setDaemon(true);
        flushThread.start();
    }

    @SuppressWarnings("unchecked")
    private void flushLoop() {
        List<WriteTask<K, V>> batch = new ArrayList<>(batchSize);
        long lastFlush = System.nanoTime();

        while (!Thread.interrupted()) {
            batch.clear();
            int drained = drainTo(batch, batchSize);

            // 修复：处理 batch 时如果 mergeWindow 满，先刷盘再继续
            for (WriteTask<K, V> task : batch) {
                // 关键修复：如果 mergeWindow 已满，先强制刷盘腾出空间
                if (mergeWindow.size() >= mergeWindowSize) {
                    flushToMainStorage();
                    lastFlush = System.nanoTime();
                }

                WriteTask<K, V> existing = mergeWindow.put(task.key, task);
                if (existing != null) {
                    mergedCount.increment();
                }
            }

            long now = System.nanoTime();
            boolean timeout = (now - lastFlush) >= flushIntervalNanos;

            // 触发刷盘条件：攒够批量 或 超时 或 mergeWindow满（上面已处理，但这里保留兜底）
            if (!mergeWindow.isEmpty() &&
                    (mergeWindow.size() >= batchSize || timeout)) {
                flushToMainStorage();
                lastFlush = now;
            }

            if (drained == 0 && mergeWindow.isEmpty()) {
                LockSupport.parkNanos(100_000);
            }
        }
    }

    public long getPendingCount() {
        // mergeWindow 大小 + RingBuffer 中未消费的数量
        return mergeWindow.size() + (writeSeq - readSeq);
    }

    @SuppressWarnings("unchecked")
    private int drainTo(List<WriteTask<K, V>> container, int maxElements) {
        long currentRead = (long) READ_SEQ.getAcquire(this);
        long available = (long) WRITE_SEQ.getAcquire(this) - currentRead;
        int toRead = (int) Math.min(available, maxElements);

        for (int i = 0; i < toRead; i++) {
            int index = (int) ((currentRead + i) & mask);
            WriteTask<K, V> task = (WriteTask<K, V>) BUFFER_ELEM.getAcquire(buffer, index);

            // 自旋等待数据就绪
            int spins = 0;
            while (task == null && spins < 100) {
                Thread.onSpinWait();
                task = (WriteTask<K, V>) BUFFER_ELEM.getAcquire(buffer, index);
                spins++;
            }

            if (task != null) {
                container.add(task);
                BUFFER_ELEM.setRelease(buffer, index, null);
            }
        }

        if (toRead > 0) {
            READ_SEQ.setRelease(this, currentRead + toRead);
        }
        return toRead;
    }

    private void flushToMainStorage() {
        long now = System.currentTimeMillis();

        mergeWindow.forEach((key, task) -> {
            try {
                // 跳过已过期数据（避免复活）
                if (task.isExpired(now)) {
                    flushedCount.increment(); // 仍计数为已处理（丢弃）
                    return;
                }

                // 支持删除操作（value == null 且 type == REMOVE）
                writeFunction.accept(key, task.value);
                flushedCount.increment();

                // 发布事件（删除事件也发布）
                if (eventPublisher != null) {
                    eventPublisher.accept(CacheEvent.<K, V>builder()
                            .type(task.type)
                            .key(key)
                            .value(task.value)
                            .build());
                }
            } catch (Exception e) {
                System.err.println("[WriteBuffer] Flush error for key " + key + ": " + e.getMessage());
            }
        });

        mergeWindow.clear();
    }

    public void shutdown() {
        if (flushThread != null) {
            flushThread.interrupt();
            List<WriteTask<K, V>> remaining = new ArrayList<>();
            drainTo(remaining, Integer.MAX_VALUE);
            for (WriteTask<K, V> task : remaining) {
                mergeWindow.put(task.key, task);
            }
            if (!mergeWindow.isEmpty()) {
                flushToMainStorage();
            }
        }
    }

    public long getSubmittedCount() { return submittedCount.sum(); }
    public long getMergedCount() { return mergedCount.sum(); }
    public long getFlushedCount() { return flushedCount.sum(); }

    public double getMergeRate() {
        long submitted = submittedCount.sum();
        long merged = mergedCount.sum();
        return submitted == 0 ? 0.0 : (double) merged / submitted;
    }

    // 在 WriteBuffer.java 中
    record WriteTask<K, V>(K key, V value, CacheEventType type, long expireAt) {
        boolean isExpired(long now) {
            return expireAt > 0 && now >= expireAt;
        }
    }
}