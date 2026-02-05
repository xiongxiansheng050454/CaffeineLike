package com.github.caffeine.cache;

import java.util.concurrent.ConcurrentMap;

public interface Cache<K, V> {
    V getIfPresent(K key);
    void put(K key, V value);
    void invalidate(K key);
    void invalidateAll();
    long estimatedSize();
    ConcurrentMap<K, V> asMap();
    CacheStats stats();
}