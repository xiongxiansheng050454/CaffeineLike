package com.github.caffeine;

public final class CacheUtils {
    /**
     * 计算分片数：大于等于n的最小2的幂（参考Caffeine的并发级别设计）
     * 例如：CPU=8核 → 返回8；CPU=12核 → 返回16
     */
    public static int tableSizeFor(int n) {
        int cap = 1;
        while (cap < n) {
            cap <<= 1;
        }
        return cap;
    }

    /**
     * 扰动函数：降低哈希冲突（参考Caffeine的hash算法）
     */
    public static int spread(int h) {
        // 高16位与低16位异或，增加散列性
        return (h ^ (h >>> 16)) & 0x7fffffff;
    }
}
