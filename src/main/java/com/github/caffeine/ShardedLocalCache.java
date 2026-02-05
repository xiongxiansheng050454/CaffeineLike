package com.github.caffeine;

/**
 * 本地缓存核心：分片ConcurrentHashMap实现
 * 对应Caffeine的BoundedLocalCache基础结构
 */
public class ShardedLocalCache<K, V> {
    // 分片数组（final保证可见性）
    private final LocalCacheSegment<K, V>[] segments;

    // 分片掩码（用于位运算取模，要求segments.length为2^n）
    private final int segmentMask;

    // 单分片初始容量
    private final int segmentCapacity;

    @SuppressWarnings("unchecked")
    public ShardedLocalCache(int concurrencyLevel) {
        // Step 1: 计算分片数（2^n）
        int segmentCount = CacheUtils.tableSizeFor(concurrencyLevel);
        this.segmentMask = segmentCount - 1;
        this.segmentCapacity = 16; // 每个分片初始16个槽位

        // 初始化分片数组
        this.segments = new LocalCacheSegment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new LocalCacheSegment<>(segmentCapacity);
        }

        System.out.println("Initialized cache with " + segmentCount +
                " segments (mask=0x" + Integer.toHexString(segmentMask) + ")");
    }

    /**
     * Step 3: 哈希定位算法
     * 通过位运算定位Segment（比%运算快10倍以上）
     */
    private LocalCacheSegment<K, V> segmentFor(K key) {
        int hash = CacheUtils.spread(key.hashCode());
        // 关键：& segmentMask 等价于 % segmentCount，但要求segmentCount为2^n
        return segments[hash & segmentMask];
    }

    /**
     * 基础CRUD操作：直接路由到对应分片
     */
    public V get(K key) {
        return segmentFor(key).get(key);
    }

    public V put(K key, V value) {
        return segmentFor(key).put(key, value);
    }

    public V remove(K key) {
        return segmentFor(key).remove(key);
    }

    /**
     * 统计所有分片大小（Caffeine的size()也是遍历累加）
     */
    public long size() {
        long sum = 0;
        for (LocalCacheSegment<K, V> seg : segments) {
            sum += seg.size();
        }
        return sum;
    }

    public int getSegmentCount() {
        return segments.length;
    }
}
