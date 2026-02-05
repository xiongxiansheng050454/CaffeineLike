package com.github.caffeine.cache;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓存分片：每个Segment是一个独立的ConcurrentHashMap
 * 存储的是Node节点而非直接存Value，以支持过期时间、访问时间等元数据
 */
public final class LocalCacheSegment<K, V> {
    // 存储Node而非直接存Value，与Caffeine的Segment设计一致
    private final ConcurrentHashMap<K, BoundedLocalCache.Node<K, V>> map;
    private final int initialCapacity;

    public LocalCacheSegment(int initialCapacity) {
        this.initialCapacity = initialCapacity;
        this.map = new ConcurrentHashMap<>(initialCapacity);
    }

    public BoundedLocalCache.Node<K, V> getNode(K key) {
        return map.get(key);
    }

    public BoundedLocalCache.Node<K, V> putNode(K key, BoundedLocalCache.Node<K, V> node) {
        return map.put(key, node);
    }

    public boolean removeNode(K key, BoundedLocalCache.Node<K, V> node) {
        return map.remove(key, node);
    }

    public BoundedLocalCache.Node<K, V> removeNode(K key) {
        return map.remove(key);
    }

    public int size() {
        return map.size();
    }

    public ConcurrentHashMap<K, BoundedLocalCache.Node<K, V>> getMap() {
        return map;
    }

    public int getInitialCapacity() {
        return initialCapacity;
    }
}