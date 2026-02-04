package com.github.benmanes.caffeine.cache;

import java.util.concurrent.TimeUnit;

public final class Caffeine<K, V> {
    // 配置项（参考源码中的字段命名）
    static final int UNSET_INT = -1;
    long maximumSize = UNSET_INT;
    long maximumWeight = UNSET_INT;
    long expireAfterWriteNanos = UNSET_INT;
    long expireAfterAccessNanos = UNSET_INT;
    int initialCapacity = UNSET_INT;
    boolean weakKeys;
    boolean softValues;
    boolean weakValues;
    boolean recordStats;

    private Caffeine() {}

    public static <K, V> Caffeine<K, V> newBuilder() {
        return new Caffeine<>();
    }

    // 链式配置方法 - 容量控制
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

    // 链式配置方法 - 过期时间
    public Caffeine<K, V> expireAfterWrite(long duration, TimeUnit unit) {
        this.expireAfterWriteNanos = unit.toNanos(duration);
        return this;
    }

    public Caffeine<K, V> expireAfterAccess(long duration, TimeUnit unit) {
        this.expireAfterAccessNanos = unit.toNanos(duration);
        return this;
    }

    // 链式配置方法 - 引用类型（参考源码中的 Strength 概念）
    public Caffeine<K, V> weakKeys() {
        this.weakKeys = true;
        return this;
    }

    public Caffeine<K, V> weakValues() {
        this.weakValues = true;
        this.softValues = false;
        return this;
    }

    public Caffeine<K, V> softValues() {
        this.softValues = true;
        this.weakValues = false;
        return this;
    }

    public Caffeine<K, V> recordStats() {
        this.recordStats = true;
        return this;
    }

    // 构建方法 - 创建缓存实例
    @SuppressWarnings("unchecked")
    public <K1 extends K, V1 extends V> Cache<K1, V1> build() {
        return (Cache<K1, V1>) new BoundedLocalCache<>(this);
    }

    // 内部辅助方法
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