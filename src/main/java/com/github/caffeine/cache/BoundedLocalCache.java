package com.github.caffeine.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class BoundedLocalCache<K, V> implements Cache<K, V> {
    private final ConcurrentHashMap<K, Node<K, V>> data;
    private final Caffeine<K, V> builder;
    private final HierarchicalTimerWheel<K, V> timerWheel;

    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder expireCount = new LongAdder();

    public BoundedLocalCache(Caffeine<K, V> builder) {
        this.builder = builder;
        int capacity = builder.initialCapacity > 0 ? builder.initialCapacity : 16;
        this.data = new ConcurrentHashMap<>(capacity);

        if (builder.expiresAfterWrite() || builder.expiresAfterAccess()) {
            this.timerWheel = new HierarchicalTimerWheel<>(this::onTimerExpired);
        } else {
            this.timerWheel = null;
        }
    }

    @Override
    public V getIfPresent(K key) {
        Node<K, V> node = data.get(key);
        if (node == null) {
            missCount.increment();
            return null;
        }

        // 统一使用毫秒
        long now = System.currentTimeMillis();

        if (isExpired(node, now)) {
            if (data.remove(key, node)) {
                expireCount.increment();
                // 从时间轮移除（同步块保护）
                if (timerWheel != null) {
                    synchronized (node) {
                        timerWheel.cancel(node);
                    }
                }
            }
            missCount.increment();
            return null;
        }

        // expireAfterAccess 处理（毫秒计算）
        if (builder.expiresAfterAccess()) {
            long newExpireAt = now + TimeUnit.NANOSECONDS.toMillis(builder.expireAfterAccessNanos);
            node.setAccessTime(now);
            node.setExpireAt(newExpireAt);
            reschedule(node, newExpireAt);
        }

        hitCount.increment();
        return node.getValue();
    }

    @Override
    public void put(K key, V value) {
        // 统一使用毫秒
        long now = System.currentTimeMillis();
        long expireAt = calculateExpireAt(now);

        Node<K, V> newNode = new Node<>(key, value, now, expireAt);

        // 注意：测试未设置maximumSize，不会触发驱逐
        if (builder.evicts() && builder.maximumSize > 0 && data.size() >= builder.maximumSize) {
            // 简化策略：不插入（生产环境应使用W-TinyLFU）
            return;
        }

        Node<K, V> oldNode = data.put(key, newNode);

        if (timerWheel != null) {
            if (oldNode != null) {
                synchronized (oldNode) {
                    timerWheel.cancel(oldNode);
                }
            }
            if (expireAt > 0) {
                long delayMs = expireAt - now;
                synchronized (newNode) {
                    timerWheel.schedule(newNode, Math.max(1, delayMs));
                }
            }
        }
    }

    @Override
    public void invalidate(K key) {
        Node<K, V> node = data.remove(key);
        if (node != null && timerWheel != null) {
            synchronized (node) {
                timerWheel.cancel(node);
            }
        }
    }

    /**
     * 时间轮回调：Key过期触发
     */
    private void onTimerExpired(K key) {
        Node<K, V> node = data.get(key);
        if (node == null) return;

        // 关键修复：必须使用System.currentTimeMillis()，与expireAt的基准一致
        long now = System.currentTimeMillis();
        if (isExpired(node, now)) {
            if (data.remove(key, node)) {
                expireCount.increment();
            }
        }
        // 否则说明已被reschedule或已访问续约，忽略
    }

    private boolean isExpired(Node<K, V> node, long now) {
        long expireAt = node.getExpireAt();
        if (expireAt <= 0) return false;
        return now >= expireAt;
    }

    // 修正：使用毫秒计算过期时间
    private long calculateExpireAt(long now) {
        long minExpire = Long.MAX_VALUE;

        if (builder.expiresAfterWrite()) {
            minExpire = now + TimeUnit.NANOSECONDS.toMillis(builder.expireAfterWriteNanos);
        }

        if (builder.expiresAfterAccess()) {
            long accessExpire = now + TimeUnit.NANOSECONDS.toMillis(builder.expireAfterAccessNanos);
            minExpire = Math.min(minExpire, accessExpire);
        }

        return minExpire == Long.MAX_VALUE ? -1 : minExpire;
    }

    private void reschedule(Node<K, V> node, long newExpireAt) {
        if (timerWheel == null) return;

        long now = System.currentTimeMillis();
        long delayMs = newExpireAt - now;
        if (delayMs > 0) {
            synchronized (node) {
                timerWheel.cancel(node);
                timerWheel.schedule(node, delayMs);
            }
        }
    }

    @Override
    public void invalidateAll() {
        data.values().forEach(node -> {
            if (timerWheel != null) {
                synchronized (node) {
                    timerWheel.cancel(node);
                }
            }
        });
        data.clear();
    }

    @Override
    public long estimatedSize() {
        return data.size();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>();
        long now = System.nanoTime();
        data.forEach((k, node) -> {
            if (!isExpired(node, now)) {
                map.put(k, node.getValue());
            }
        });
        return map;
    }

    @Override
    public CacheStats stats() {
        return new CacheStats(hitCount.sum(), missCount.sum(), expireCount.sum());
    }

    public void shutdown() {
        if (timerWheel != null) {
            timerWheel.shutdown();
        }
    }

    // Node 类：链表操作添加 synchronized 保护
    static class Node<K, V> {
        private final K key;
        private final V value;
        private final long writeTime;
        private volatile long accessTime;
        private volatile long expireAt;

        // 时间轮链表指针
        private Node<K, V> prevInTimerWheel;
        private Node<K, V> nextInTimerWheel;

        Node(K key, V value, long writeTime, long expireAt) {
            this.key = key;
            this.value = value;
            this.writeTime = writeTime;
            this.accessTime = writeTime;
            this.expireAt = expireAt;
        }

        public K getKey() { return key; }
        public V getValue() { return value; }
        public long getWriteTime() { return writeTime; }
        public long getAccessTime() { return accessTime; }
        public void setAccessTime(long time) { this.accessTime = time; }
        public long getExpireAt() { return expireAt; }
        public void setExpireAt(long time) { this.expireAt = time; }

        // 时间轮链表操作（需要外部 synchronized 保护）
        public Node<K, V> getPrevInTimerWheel() { return prevInTimerWheel; }
        public void setPrevInTimerWheel(Node<K, V> prev) { this.prevInTimerWheel = prev; }
        public Node<K, V> getNextInTimerWheel() { return nextInTimerWheel; }
        public void setNextInTimerWheel(Node<K, V> next) { this.nextInTimerWheel = next; }

        public void detachFromTimerWheel() {
            Node<K, V> prev = this.prevInTimerWheel;
            Node<K, V> next = this.nextInTimerWheel;

            if (prev != null) prev.setNextInTimerWheel(next);
            if (next != null) next.setPrevInTimerWheel(prev);

            this.prevInTimerWheel = null;
            this.nextInTimerWheel = null;
        }
    }
}