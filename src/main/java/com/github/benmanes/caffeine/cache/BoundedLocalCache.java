package com.github.benmanes.caffeine.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class BoundedLocalCache<K, V> implements Cache<K, V> {
    private final ConcurrentHashMap<K, Node<K, V>> data;
    private final Caffeine<K, V> builder;

    // 统计
    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();

    public BoundedLocalCache(Caffeine<K, V> builder) {
        this.builder = builder;
        int capacity = builder.initialCapacity > 0 ? builder.initialCapacity : 16;
        this.data = new ConcurrentHashMap<>(capacity);
    }

    @Override
    public V getIfPresent(K key) {
        Node<K, V> node = data.get(key);
        if (node == null) {
            missCount.increment();
            return null;
        }

        // 简单过期检查（参考源码中的过期逻辑）
        if (isExpired(node)) {
            data.remove(key, node);
            missCount.increment();
            return null;
        }

        // 更新访问时间（如果配置了 expireAfterAccess）
        if (builder.expiresAfterAccess()) {
            node.setAccessTime(System.nanoTime());
        }

        hitCount.increment();
        return node.getValue();
    }

    @Override
    public void put(K key, V value) {
        long now = System.nanoTime();
        Node<K, V> node = new Node<>(key, value, now);

        // 简单容量检查（超限时拒绝或清理，实际应使用 W-TinyLFU）
        if (builder.maximumSize > 0 && data.size() >= builder.maximumSize) {
            // Day 1 简化处理：随机清理一个（后续替换为淘汰算法）
            if (!data.isEmpty()) {
                K firstKey = data.keySet().iterator().next();
                data.remove(firstKey);
            }
        }

        data.put(key, node);
    }

    @Override
    public void invalidate(K key) {
        data.remove(key);
    }

    @Override
    public void invalidateAll() {
        data.clear();
    }

    @Override
    public long estimatedSize() {
        return data.size();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        // 返回视图，实际应实现转换
        ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>();
        data.forEach((k, node) -> {
            if (!isExpired(node)) {
                map.put(k, node.getValue());
            }
        });
        return map;
    }

    @Override
    public CacheStats stats() {
        return new CacheStats(hitCount.sum(), missCount.sum());
    }

    private boolean isExpired(Node<K, V> node) {
        long now = System.nanoTime();

        if (builder.expiresAfterWrite()) {
            long expireTime = node.getWriteTime() + builder.expireAfterWriteNanos;
            if (now - expireTime >= 0) {
                return true;
            }
        }

        if (builder.expiresAfterAccess()) {
            long expireTime = node.getAccessTime() + builder.expireAfterAccessNanos;
            if (now - expireTime >= 0) {
                return true;
            }
        }

        return false;
    }

    // 内部 Node 类（参考源码中的 Node 设计）
    static class Node<K, V> {
        private final K key;
        private final V value;
        private final long writeTime;
        private volatile long accessTime;

        Node(K key, V value, long writeTime) {
            this.key = key;
            this.value = value;
            this.writeTime = writeTime;
            this.accessTime = writeTime;
        }

        V getValue() { return value; }
        long getWriteTime() { return writeTime; }
        long getAccessTime() { return accessTime; }
        void setAccessTime(long time) { this.accessTime = time; }
    }
}