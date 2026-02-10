package com.github.caffeine.cache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * 无锁 Count-Min Sketch - 基于 AtomicLongArray 的 CAS 更新
 * 每个计数器4-bit（0-15），每个 long 存储 16 个计数器
 */
public final class FrequencySketch<K> {
    private static final int COUNTERS_PER_LONG = 16;
    private static final int COUNTER_BITS = 4;
    private static final long COUNTER_MASK = 0x0FL;
    private static final int MAX_COUNT = 15;

    // 4个哈希种子
    private static final long[] SEEDS = {
            0xc3a5c85c97cb3127L, 0xb492b66fbe98f273L,
            0x9ae16a3b2f90404fL, 0xcbf29ce484222325L
    };

    private volatile AtomicLongArray table;
    private volatile int size;
    private int capacity;
    private int mask;

    // 统计 CAS 失败次数（用于性能监控）
    private final LongAdder casFailures = new LongAdder();

    /**
     * 初始化草图容量
     */
    public void ensureCapacity(long maximumSize) {
        int maximum = (int) Math.min(maximumSize * 2, Integer.MAX_VALUE >> 1);
        int tableSize = Math.max(128, ceilingNextPowerOfTwo(maximum));
        this.capacity = tableSize;
        this.mask = tableSize - 1;
        // table 大小 = 容量 / 16
        this.table = new AtomicLongArray(tableSize >>> 4);
        this.size = 0;
    }

    private int ceilingNextPowerOfTwo(int x) {
        return 1 << (32 - Integer.numberOfLeadingZeros(x - 1));
    }

    /**
     * 线程安全 increment - 使用 CAS 无锁更新
     */
    public void increment(K key) {
        if (isNotInitialized()) return;

        int[] hashes = spread(key.hashCode());
        int start = (int) (Thread.currentThread().getId() & 3);

        for (int i = 0; i < 4; i++) {
            int index = hashes[(start + i) & 3] & mask;
            incrementAt(index);
        }

        // 每 256 次检查衰减
        if ((++size & 0xFF) == 0) {
            reset();
        }
    }

    /**
     * CAS 无锁增加指定位置的计数器
     * 关键：读取 -> 修改位域 -> CAS 写回，失败则重试
     */
    private void incrementAt(int index) {
        int longIndex = index >>> 4;      // 除以 16，定位到 AtomicLongArray 的索引
        int shift = (index & 15) << 2;    // 位偏移 (0, 4, 8, ..., 60)
        long mask = COUNTER_MASK << shift;

        AtomicLongArray arr = table;

        while (true) {
            long oldValue = arr.get(longIndex);
            long counter = (oldValue >>> shift) & COUNTER_MASK;

            // 饱和检查：如果已经是 15，不再增加
            if (counter >= MAX_COUNT) {
                return;
            }

            long newValue = oldValue + (1L << shift);

            // CAS 原子更新
            if (arr.compareAndSet(longIndex, oldValue, newValue)) {
                return; // 成功
            }

            // 失败统计（可选，用于监控竞争程度）
            casFailures.increment();

            // 自旋等待（短暂）
            Thread.onSpinWait();
        }
    }

    /**
     * 读取频率（volatile 读，保证可见性）
     */
    public int frequency(K key) {
        if (isNotInitialized()) return 0;

        int[] hashes = spread(key.hashCode());
        int freq = Integer.MAX_VALUE;
        int start = (int) (Thread.currentThread().getId() & 3);
        AtomicLongArray arr = table;

        for (int i = 0; i < 4; i++) {
            int index = hashes[(start + i) & 3] & mask;
            int longIndex = index >>> 4;
            int shift = (index & 15) << 2;

            long value = arr.get(longIndex);
            int counter = (int) ((value >>> shift) & COUNTER_MASK);
            freq = Math.min(freq, counter);
        }
        return freq;
    }

    /**
     * 无锁衰减：CAS 更新每个 long，将所有 4-bit 计数器右移 1 位
     */
    public void reset() {
        if (isNotInitialized()) return;

        size = 0;
        AtomicLongArray arr = table;

        for (int i = 0; i < arr.length(); i++) {
            while (true) {
                long oldValue = arr.get(i);
                if (oldValue == 0) break; // 已经是 0，跳过

                // 对每个 4-bit 段右移 1 位：(0x0F0F0F0F0F0F0F0FL & (oldValue >>> 1))
                long newValue = (oldValue >>> 1) & 0x7777777777777777L;

                if (arr.compareAndSet(i, oldValue, newValue)) {
                    break; // 成功
                }
                // CAS 失败，重试
                Thread.onSpinWait();
            }
        }
    }

    /**
     * 批量增加（用于高吞吐量场景，减少 CAS 竞争）
     * 先本地累加，定期刷入 AtomicLongArray
     */
    public void incrementBatch(K key, int count) {
        if (isNotInitialized() || count <= 0) return;

        int[] hashes = spread(key.hashCode());
        int start = (int) (Thread.currentThread().getId() & 3);

        for (int i = 0; i < 4; i++) {
            int index = hashes[(start + i) & 3] & mask;
            addAt(index, Math.min(count, MAX_COUNT));
        }

        size += count;
        if ((size & 0xFF) == 0) reset();
    }

    /**
     * 批量增加指定数值（带上限检查）
     */
    private void addAt(int index, int delta) {
        int longIndex = index >>> 4;
        int shift = (index & 15) << 2;

        AtomicLongArray arr = table;

        while (true) {
            long oldValue = arr.get(longIndex);
            long current = (oldValue >>> shift) & COUNTER_MASK;

            if (current >= MAX_COUNT) return;

            long newCount = Math.min(current + delta, MAX_COUNT);
            long newValue = (oldValue & ~(COUNTER_MASK << shift)) | (newCount << shift);

            if (arr.compareAndSet(longIndex, oldValue, newValue)) return;

            Thread.onSpinWait();
        }
    }

    private boolean isNotInitialized() {
        return table == null;
    }

    private int[] spread(int hashCode) {
        int[] hashes = new int[4];
        long x = hashCode & 0xFFFFFFFFL;
        for (int i = 0; i < 4; i++) {
            x = x * SEEDS[i];
            hashes[i] = (int) (x >>> 32);
        }
        return hashes;
    }

    /**
     * 获取 CAS 失败率（性能监控用）
     */
    public double getCasFailureRate(long totalOps) {
        return totalOps == 0 ? 0.0 : (double) casFailures.sum() / totalOps;
    }

    /**
     * 获取计数器饱和度（用于调整容量）
     */
    public double getSaturationRate() {
        if (isNotInitialized()) return 0.0;
        AtomicLongArray arr = table;
        long saturated = 0;
        long total = 0;

        for (int i = 0; i < arr.length(); i++) {
            long val = arr.get(i);
            for (int j = 0; j < 64; j += 4) {
                total++;
                if (((val >>> j) & COUNTER_MASK) >= MAX_COUNT) {
                    saturated++;
                }
            }
        }
        return total == 0 ? 0.0 : (double) saturated / total;
    }

    /**
     * 估算内存占用（字节）
     */
    public long estimatedMemoryUsage() {
        if (isNotInitialized()) return 0;
        // AtomicLongArray 对象头 + length 字段 + 引用 + long[] 数组
        return 16 + 4 + 4 + 8 + (table.length() * 8L);
    }
}