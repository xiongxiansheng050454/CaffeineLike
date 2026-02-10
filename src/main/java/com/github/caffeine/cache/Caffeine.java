package com.github.caffeine.cache;

import com.github.caffeine.cache.event.CacheEvent;
import com.github.caffeine.cache.reference.ReferenceStrength;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class Caffeine<K, V> {
    static final int UNSET_INT = -1;
    long maximumSize = UNSET_INT;
    long maximumWeight = UNSET_INT;
    long expireAfterWriteNanos = UNSET_INT;
    long expireAfterAccessNanos = UNSET_INT;
    int initialCapacity = UNSET_INT;

    // 引用类型配置（已存在，但需要添加 getter）
    boolean weakKeys;
    boolean softValues;
    boolean weakValues;
    boolean recordStats;

    // 内存敏感模式（仅对Soft引用有效）
    private boolean memorySensitive = false;
    private double memoryWarningThreshold = 0.75;
    private double memoryEmergencyThreshold = 0.85;

    // 新增：监听器配置
    private Consumer<CacheEvent<K, V>> removalListener;
    private Consumer<CacheEvent<K, V>> statsListener;

    // 新增：异步RemovalListener配置
    private boolean asyncRemovalEnabled = false;
    private int asyncBufferSize = 65536;      // RingBuffer大小
    private int asyncBatchSize = 100;         // 批量阈值
    private long asyncFlushIntervalMs = 50;   // 超时flush（毫秒）

    /**
     * 启用异步RemovalListener（基于RingBuffer）
     * 优势：主线程put/remove零阻塞，批量聚合提升吞吐量
     */
    public Caffeine<K, V> asyncRemoval() {
        this.asyncRemovalEnabled = true;
        return this;
    }

    /**
     * 配置异步处理器参数（可选，使用默认值可不调用）
     */
    public Caffeine<K, V> asyncRemovalConfig(int bufferSize, int batchSize, long flushIntervalMs) {
        this.asyncBufferSize = bufferSize;
        this.asyncBatchSize = batchSize;
        this.asyncFlushIntervalMs = flushIntervalMs;
        this.asyncRemovalEnabled = true;
        return this;
    }



    /**
     * 启用内存敏感模式：当JVM堆内存不足时自动清理Soft引用
     * 仅当使用softValues()时有效
     */
    public Caffeine<K, V> memorySensitive() {
        this.memorySensitive = true;
        return this;
    }

    /**
     * 设置内存阈值（高级配置）
     * @param warning 警告阈值（默认0.75）
     * @param emergency 紧急清理阈值（默认0.85）
     */
    public Caffeine<K, V> memoryThresholds(double warning, double emergency) {
        if (warning <= 0 || emergency <= 0 || warning >= emergency) {
            throw new IllegalArgumentException("Invalid thresholds");
        }
        this.memoryWarningThreshold = warning;
        this.memoryEmergencyThreshold = emergency;
        return this;
    }

    boolean isMemorySensitive() { return memorySensitive; }
    double getWarningThreshold() { return memoryWarningThreshold; }
    double getEmergencyThreshold() { return memoryEmergencyThreshold; }

    private boolean writeBufferEnabled = false;
    private int writeBufferSize = 65536;
    private int writeBufferMergeSize = 1024;
    private int writeBufferBatchSize = 100;
    private long writeBufferFlushMs = 10;

    // 新增字段
    private boolean recordFrequency = false;  // 启用频率统计

    private Caffeine() {}

    public static <K, V> Caffeine<K, V> newBuilder() {
        return new Caffeine<>();
    }

    public Caffeine<K, V> initialCapacity(int initialCapacity) {
        if (initialCapacity < 0) throw new IllegalArgumentException();
        this.initialCapacity = initialCapacity;
        return this;
    }

    // 新增：启用W-TinyLFU频率统计（与maximumSize配合使用）
    public Caffeine<K, V> maximumSize(long maximumSize) {
        if (maximumSize < 0) throw new IllegalArgumentException();
        this.maximumSize = maximumSize;
        this.recordFrequency = true;  // 启用大小限制时自动启用频率统计
        return this;
    }

    public Caffeine<K, V> expireAfterWrite(long duration, TimeUnit unit) {
        this.expireAfterWriteNanos = unit.toNanos(duration);
        return this;
    }

    public Caffeine<K, V> expireAfterAccess(long duration, TimeUnit unit) {
        this.expireAfterAccessNanos = unit.toNanos(duration);
        return this;
    }

    public Caffeine<K, V> weakKeys() {
        this.weakKeys = true;
        return this;
    }

    public Caffeine<K, V> weakValues() {
        this.weakValues = true;
        this.softValues = false;  // 互斥
        return this;
    }

    public Caffeine<K, V> softValues() {
        this.softValues = true;
        this.weakValues = false;  // 互斥
        return this;
    }

    public Caffeine<K, V> recordStats() {
        this.recordStats = true;
        return this;
    }

    /**
     * 配置移除监听器（驱逐、过期、删除时触发）
     */
    public Caffeine<K, V> removalListener(Consumer<CacheEvent<K, V>> listener) {
        this.removalListener = listener;
        return this;
    }

    /**
     * 配置统计监听器（写入、读取时触发，可选）
     */
    public Caffeine<K, V> statsListener(Consumer<CacheEvent<K, V>> listener) {
        this.statsListener = listener;
        return this;
    }

    // 启用写缓冲
    public Caffeine<K, V> enableWriteBuffer() {
        this.writeBufferEnabled = true;
        return this;
    }

    public Caffeine<K, V> writeBufferConfig(int bufferSize, int mergeSize, int batchSize, long flushMs) {
        this.writeBufferSize = bufferSize;
        this.writeBufferMergeSize = mergeSize;
        this.writeBufferBatchSize = batchSize;
        this.writeBufferFlushMs = flushMs;
        return this;
    }

    public <K1 extends K, V1 extends V> Cache<K1, V1> build() {
        return (Cache<K1, V1>)new BoundedLocalCache<>(this);
    }

    // 新增：获取 Value 的引用强度
    public ReferenceStrength valueStrength() {
        if (weakValues) return ReferenceStrength.WEAK;
        if (softValues) return ReferenceStrength.SOFT;
        return ReferenceStrength.STRONG;
    }

    boolean evicts() {
        return maximumSize != UNSET_INT || maximumWeight != UNSET_INT;
    }

    boolean expiresAfterWrite() {
        return expireAfterWriteNanos != UNSET_INT;
    }

    boolean expiresAfterAccess() {
        return expireAfterAccessNanos != UNSET_INT;
    }

    // Package-private getters
    Consumer<CacheEvent<K, V>> getRemovalListener() { return removalListener; }
    Consumer<CacheEvent<K, V>> getStatsListener() { return statsListener; }

    // Package-private getters
    boolean isAsyncRemovalEnabled() { return asyncRemovalEnabled; }
    int getAsyncBufferSize() { return asyncBufferSize; }
    int getAsyncBatchSize() { return asyncBatchSize; }
    long getAsyncFlushIntervalMs() { return asyncFlushIntervalMs; }

    // Package-private getters
    boolean isWriteBufferEnabled() { return writeBufferEnabled; }
    int getWriteBufferSize() { return writeBufferSize; }
    int getWriteBufferMergeSize() { return writeBufferMergeSize; }
    int getWriteBufferBatchSize() { return writeBufferBatchSize; }
    long getWriteBufferFlushMs() { return writeBufferFlushMs; }

    // 包级访问方法
    boolean recordsFrequency() { return recordFrequency; }
    long getMaximumSize() { return maximumSize; }
}