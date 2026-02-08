package com.github.caffeine.cache.event;

/**
 * 缓存事件封装 - 仿Caffeine的RemovalNotification
 */
public final class CacheEvent<K, V> {
    private final CacheEventType type;
    private final K key;
    private final V value;
    private final long timestamp;
    private final Throwable cause;  // 用于异步加载失败场景

    private CacheEvent(Builder<K, V> builder) {
        this.type = builder.type;
        this.key = builder.key;
        this.value = builder.value;
        this.timestamp = System.nanoTime();
        this.cause = builder.cause;
    }

    // Getters...
    public CacheEventType getType() { return type; }
    public K getKey() { return key; }
    public V getValue() { return value; }
    public long getTimestamp() { return timestamp; }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    public static class Builder<K, V> {
        private CacheEventType type;
        private K key;
        private V value;
        private Throwable cause;

        public Builder<K, V> type(CacheEventType type) {
            this.type = type;
            return this;
        }

        public Builder<K, V> key(K key) {
            this.key = key;
            return this;
        }

        public Builder<K, V> value(V value) {
            this.value = value;
            return this;
        }

        public CacheEvent<K, V> build() {
            return new CacheEvent<>(this);
        }
    }
}
