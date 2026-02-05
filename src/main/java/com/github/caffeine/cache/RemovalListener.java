package com.github.caffeine.cache;

@FunctionalInterface
public interface RemovalListener<K, V> {
    void onRemoval(K key, V value, RemovalCause cause);

    enum RemovalCause {
        EXPLICIT, EXPIRED, SIZE
    }
}
