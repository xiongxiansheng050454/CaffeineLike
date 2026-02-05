package com.github.caffeine.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

public class BoundedLocalCache<K, V> implements Cache<K, V> {
    private final ConcurrentHashMap<K, Node<K, V>> data;
    private final Caffeine<K, V> builder;
    private final HierarchicalTimerWheel<K, V> timerWheel;

    // 统计
    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();

    public BoundedLocalCache(Caffeine<K, V> builder) {
        this.builder = builder;
        int capacity = builder.initialCapacity > 0 ? builder.initialCapacity : 16;
        this.data = new ConcurrentHashMap<>(capacity);

        // 初始化时间轮（如果配置了过期时间）
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

        // 惰性检查（双保险：时间轮可能稍有延迟）
        if (isExpired(node)) {
            // 主动移除并取消时间轮任务
            removeAndCancel(key, node);
            missCount.increment();
            return null;
        }

        // 更新访问时间（如果配置了expireAfterAccess）
        if (builder.expiresAfterAccess()) {
            node.setAccessTime(System.nanoTime());
            // 重新调度（Caffeine的变长过期策略）
            rescheduleAfterAccess(node);
        }

        hitCount.increment();
        return node.getValue();
    }

    @Override
    public void put(K key, V value) {
        long now = System.nanoTime();
        Node<K, V> newNode = new Node<>(key, value, now);

        // 容量检查（原有逻辑）
        if (builder.maximumSize > 0 && data.size() >= builder.maximumSize) {
            if (!data.isEmpty()) {
                K firstKey = data.keySet().iterator().next();
                removeAndCancel(firstKey, data.get(firstKey));
            }
        }

        Node<K, V> oldNode = data.put(key, newNode);
        if (oldNode != null) {
            // 替换旧值：取消旧节点的时间轮任务
            timerWheel.cancel(oldNode);
        }

        // 注册到时间轮（核心新增）
        if (timerWheel != null) {
            long expireMs = calculateExpirationDelay();
            if (expireMs > 0) {
                timerWheel.schedule(newNode, expireMs);
            }
        }
    }

    @Override
    public void invalidate(K key) {
        Node<K, V> node = data.remove(key);
        if (node != null && timerWheel != null) {
            timerWheel.cancel(node);
        }
    }

    /**
     * 时间轮回调：Key过期触发
     */
    private void onTimerExpired(K key) {
        Node<K, V> node = data.get(key);
        if (node != null && isExpired(node)) {
            // CAS移除，确保一致性
            if (data.remove(key, node) && builder.recordStats) {
                // 可以在这里统计过期数
            }
        }
    }

    /**
     * 辅助：移除并取消时间轮任务
     */
    private void removeAndCancel(K key, Node<K, V> node) {
        if (data.remove(key, node) && timerWheel != null) {
            timerWheel.cancel(node);
        }
    }

    /**
     * 辅助：重新调度（用于expireAfterAccess更新访问时间后）
     */
    private void rescheduleAfterAccess(Node<K, V> node) {
        if (timerWheel == null) return;

        // 取消旧位置
        timerWheel.cancel(node);

        // 计算新的过期时间
        long delayMs = builder.expireAfterAccessNanos / 1_000_000;
        timerWheel.schedule(node, delayMs);
    }

    /**
     * 计算过期延迟（简化版：取write和access中更近的）
     */
    private long calculateExpirationDelay() {
        long minDelay = Long.MAX_VALUE;

        if (builder.expiresAfterWrite()) {
            minDelay = builder.expireAfterWriteNanos / 1_000_000;
        }

        if (builder.expiresAfterAccess()) {
            long delay = builder.expireAfterAccessNanos / 1_000_000;
            minDelay = Math.min(minDelay, delay);
        }

        return minDelay == Long.MAX_VALUE ? -1 : minDelay;
    }

    private boolean isExpired(Node<K, V> node) {
        long now = System.nanoTime();

        if (builder.expiresAfterWrite()) {
            long expireTime = node.getWriteTime() + builder.expireAfterWriteNanos;
            if (now >= expireTime) return true;
        }

        if (builder.expiresAfterAccess()) {
            long expireTime = node.getAccessTime() + builder.expireAfterAccessNanos;
            return now >= expireTime;
        }

        return false;
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

    public void shutdownTimerWheel() {
        timerWheel.shutdown();
    }

    // 内部 Node 类（参考源码中的 Node 设计）
    static class Node<K, V> {
        private final K key;
        private final V value;
        private final long writeTime;
        private volatile long accessTime;

        // 时间轮链表指针（用于分层时间轮的桶内链接）
        private volatile Node<K, V> prevInTimerWheel;
        private volatile Node<K, V> nextInTimerWheel;
        private volatile long expirationTime; // 精确的过期时间戳（ms）

        Node(K key, V value, long writeTime) {
            this.key = key;
            this.value = value;
            this.writeTime = writeTime;
            this.accessTime = writeTime;
            this.expirationTime = -1; // 未设置
        }

        V getValue() { return value; }
        long getWriteTime() { return writeTime; }
        long getAccessTime() { return accessTime; }
        void setAccessTime(long time) { this.accessTime = time; }
        K getKey() { return key; }

        // 时间轮相关方法
        void setExpirationTime(long time) { this.expirationTime = time; }
        long getExpirationTime() { return expirationTime; }

        Node<K, V> getPrevInTimerWheel() { return prevInTimerWheel; }
        void setPrevInTimerWheel(Node<K, V> prev) { this.prevInTimerWheel = prev; }

        Node<K, V> getNextInTimerWheel() { return nextInTimerWheel; }
        void setNextInTimerWheel(Node<K, V> next) { this.nextInTimerWheel = next; }

        // 从时间轮链表中断开（O(1)）
        void detachFromTimerWheel() {
            Node<K, V> prev = this.prevInTimerWheel;
            Node<K, V> next = this.nextInTimerWheel;
            if (prev != null) prev.setNextInTimerWheel(next);
            if (next != null) next.setPrevInTimerWheel(prev);
            this.prevInTimerWheel = null;
            this.nextInTimerWheel = null;
        }
    }
}