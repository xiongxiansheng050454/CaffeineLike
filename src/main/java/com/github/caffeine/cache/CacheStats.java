package com.github.caffeine.cache;

public final class CacheStats {
    private final long hitCount;
    private final long missCount;

    public CacheStats(long hitCount, long missCount) {
        this.hitCount = hitCount;
        this.missCount = missCount;
    }

    public double hitRate() {
        long total = hitCount + missCount;
        return total == 0 ? 1.0 : (double) hitCount / total;
    }
}
