package com.github.caffeine.cache;

import com.github.caffeine.cache.reference.ReferenceStrength;
import java.util.concurrent.TimeUnit;

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

    private Caffeine() {}

    public static <K, V> Caffeine<K, V> newBuilder() {
        return new Caffeine<>();
    }

    public Caffeine<K, V> initialCapacity(int initialCapacity) {
        if (initialCapacity < 0) throw new IllegalArgumentException();
        this.initialCapacity = initialCapacity;
        return this;
    }

    public Caffeine<K, V> maximumSize(long maximumSize) {
        if (maximumSize < 0) throw new IllegalArgumentException();
        this.maximumSize = maximumSize;
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
}