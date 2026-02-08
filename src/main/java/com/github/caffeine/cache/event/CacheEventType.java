package com.github.caffeine.cache.event;

/**
 * 缓存事件类型 - 对应Caffeine的RemovalCause和StatsCounter
 */
public enum CacheEventType {
    WRITE,      // 写入
    READ,       // 读取（可选，高频操作可能产生大量事件）
    EXPIRE,     // 过期（时间轮触发）
    EVICT,      // 驱逐（LRU/LFU淘汰或内存压力）
    REMOVE,     // 显式删除
    COLLECTED   // 被GC回收（弱/软引用）
}
