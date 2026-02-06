package com.github.caffeine.cache;

import java.util.concurrent.ConcurrentHashMap;

public final class LocalCacheSegment<K, V> {
    // 存储增强的 Node 节点
    private final ConcurrentHashMap<K, Node<K, V>> map;
    private final int initialCapacity;

    public LocalCacheSegment(int initialCapacity) {
        this.initialCapacity = initialCapacity;
        this.map = new ConcurrentHashMap<>(initialCapacity);
    }

    public Node<K, V> getNode(K key) {
        return map.get(key);
    }

    public Node<K, V> putNode(K key, Node<K, V> node) {
        return map.put(key, node);
    }

    public boolean removeNode(K key, Node<K, V> node) {
        return map.remove(key, node);
    }

    public Node<K, V> removeNode(K key) {
        return map.remove(key);
    }

    public int size() {
        return map.size();
    }

    public ConcurrentHashMap<K, Node<K, V>> getMap() {
        return map;
    }
}