package com.github.caffeine;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓存分片：每个Segment是一个独立的ConcurrentHashMap
 * 仿Caffeine的BoundedLocalCache分段设计
 */
public final class LocalCacheSegment<K, V> {
    // 实际存储（Caffeine底层也是基于ConcurrentHashMap的变体）
    private final ConcurrentHashMap<K, V> map;

    public LocalCacheSegment(int initialCapacity) {
        this.map = new ConcurrentHashMap<>(initialCapacity);
    }

    public V get(K key) {
        return map.get(key);
    }

    public V put(K key, V value) {
        return map.put(key, value);
    }

    public V remove(K key) {
        return map.remove(key);
    }

    public int size() {
        return map.size();
    }

    // 用于统计和清理的辅助方法
    public ConcurrentHashMap<K, V> getMap() {
        return map;
    }
}
