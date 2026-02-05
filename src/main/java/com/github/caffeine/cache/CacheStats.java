package com.github.caffeine.cache;

public final class CacheStats {
    private final long hitCount;
    private final long missCount;
    private final long expireCount;

    public CacheStats(long hitCount, long missCount) {
        this(hitCount, missCount, 0);
    }

    public CacheStats(long hitCount, long missCount, long expireCount) {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.expireCount = expireCount;
    }

    public double hitRate() {
        long total = hitCount + missCount;
        return total == 0 ? 1.0 : (double) hitCount / total;
    }

    public long expireCount() { return expireCount; }
}