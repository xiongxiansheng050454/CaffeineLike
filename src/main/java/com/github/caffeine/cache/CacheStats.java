package com.github.caffeine.cache;

public final class CacheStats {
    private final long hitCount;
    private final long missCount;
    private final long expireCount;
    private final long collectedCount;  // 新增：GC 回收统计

    public CacheStats(long hitCount, long missCount, long expireCount, long collectedCount) {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.expireCount = expireCount;
        this.collectedCount = collectedCount;
    }

    public double hitRate() {
        long total = hitCount + missCount;
        return total == 0 ? 1.0 : (double) hitCount / total;
    }

    public long expireCount() {
        return expireCount;
    }

    public long hitCount() { return hitCount; }
    public long missCount() { return missCount; }
    public long collectedCount() { return collectedCount; }

    @Override
    public String toString() {
        return String.format("CacheStats{hits=%d, misses=%d, expired=%d, collected=%d}",
                hitCount, missCount, expireCount, collectedCount);
    }
}