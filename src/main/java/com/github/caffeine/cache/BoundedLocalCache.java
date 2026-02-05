package com.github.caffeine.cache;

import com.github.caffeine.CacheUtils;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class BoundedLocalCache<K, V> implements Cache<K, V> {
    // 分片架构替代单ConcurrentHashMap
    private final LocalCacheSegment<K, V>[] segments;
    private final int segmentMask;

    private final Caffeine<K, V> builder;
    private final HierarchicalTimerWheel<K, V> timerWheel;

    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder expireCount = new LongAdder();

    @SuppressWarnings("unchecked")
    public BoundedLocalCache(Caffeine<K, V> builder) {
        this.builder = builder;

        // 计算分片数：基于并发级别（默认CPU核心数）
        int concurrencyLevel = Runtime.getRuntime().availableProcessors();
        int segmentCount = CacheUtils.tableSizeFor(concurrencyLevel);
        this.segmentMask = segmentCount - 1;

        // 计算单分片容量
        int totalCapacity = builder.initialCapacity > 0 ? builder.initialCapacity : 16;
        int segmentCapacity = Math.max(1, totalCapacity / segmentCount);

        // 初始化分片数组
        this.segments = new LocalCacheSegment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new LocalCacheSegment<>(segmentCapacity);
        }

        // 初始化时间轮（全局唯一，与分片无关）
        if (builder.expiresAfterWrite() || builder.expiresAfterAccess()) {
            this.timerWheel = new HierarchicalTimerWheel<>(this::onTimerExpired);
        } else {
            this.timerWheel = null;
        }

        System.out.println("Initialized sharded cache: " + segmentCount +
                " segments, mask=0x" + Integer.toHexString(segmentMask));
    }

    /**
     * 哈希定位：找到key对应的分片（核心优化点，O(1)定位）
     */
    private LocalCacheSegment<K, V> segmentFor(K key) {
        int hash = CacheUtils.spread(key.hashCode());
        // 位运算替代取模：hash % segmentCount == hash & (segmentCount - 1)
        return segments[hash & segmentMask];
    }

    @Override
    public V getIfPresent(K key) {
        LocalCacheSegment<K, V> segment = segmentFor(key);
        Node<K, V> node = segment.getNode(key);

        if (node == null) {
            missCount.increment();
            return null;
        }

        long now = System.currentTimeMillis();

        if (isExpired(node, now)) {
            // 使用removeNode确保原子性（compare-and-remove）
            if (segment.removeNode(key, node)) {
                expireCount.increment();
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
        LocalCacheSegment<K, V> segment = segmentFor(key);
        long now = System.currentTimeMillis();
        long expireAt = calculateExpireAt(now);

        Node<K, V> newNode = new Node<>(key, value, now, expireAt);

        // 注意：最大容量检查现在是基于全局size的近似值，或需要遍历所有segment
        // 简化策略：这里使用全局size检查，实际Caffeine会在每个segment独立触发驱逐
        if (builder.evicts() && builder.maximumSize > 0 && estimatedSize() >= builder.maximumSize) {
            // TODO: 实现W-TinyLFU淘汰，此处暂时跳过插入或简单淘汰
            return;
        }

        Node<K, V> oldNode = segment.putNode(key, newNode);

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
        LocalCacheSegment<K, V> segment = segmentFor(key);
        Node<K, V> node = segment.removeNode(key);
        if (node != null && timerWheel != null) {
            synchronized (node) {
                timerWheel.cancel(node);
            }
        }
    }

    /**
     * 时间轮回调：Key过期触发
     * 注意：过期时需要通过key重新定位segment，因为Node不持有segment引用
     */
    private void onTimerExpired(K key) {
        LocalCacheSegment<K, V> segment = segmentFor(key);
        Node<K, V> node = segment.getNode(key);
        if (node == null) return;

        long now = System.currentTimeMillis();
        if (isExpired(node, now)) {
            // 使用removeNode确保原子性（只有当node未被替换时才删除）
            if (segment.removeNode(key, node)) {
                expireCount.increment();
            }
        }
    }

    private boolean isExpired(Node<K, V> node, long now) {
        long expireAt = node.getExpireAt();
        if (expireAt <= 0) return false;
        return now >= expireAt;
    }

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
        // 遍历所有分片执行clear
        for (LocalCacheSegment<K, V> segment : segments) {
            segment.getMap().values().forEach(node -> {
                if (timerWheel != null) {
                    synchronized (node) {
                        timerWheel.cancel(node);
                    }
                }
            });
            segment.getMap().clear();
        }
    }

    @Override
    public long estimatedSize() {
        long sum = 0;
        for (LocalCacheSegment<K, V> seg : segments) {
            sum += seg.size();
        }
        return sum;
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>();
        long now = System.currentTimeMillis();

        // 遍历所有分片组装视图
        for (LocalCacheSegment<K, V> segment : segments) {
            segment.getMap().forEach((k, node) -> {
                if (!isExpired(node, now)) {
                    map.put(k, node.getValue());
                }
            });
        }
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

    // Node类保持不变（作为静态内部类）
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