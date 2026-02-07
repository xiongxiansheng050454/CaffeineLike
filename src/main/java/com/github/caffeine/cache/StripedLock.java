package com.github.caffeine.cache;

import java.util.concurrent.locks.ReentrantLock;

public class StripedLock {
    // 提升到1024个锁（64线程的16倍，生日悖论碰撞率<1%）
    private static final int DEFAULT_STRIPES = 1024;
    private final int stripes;
    private final ReentrantLock[] locks;

    public StripedLock(int stripes) {
        // 确保是2的幂：1024
        this.stripes = Integer.highestOneBit(Math.max(256, stripes));
        this.locks = new ReentrantLock[this.stripes];
        for (int i = 0; i < this.stripes; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    public ReentrantLock getLock(Object key) {
        // 使用更分散的哈希算法（参考Caffeine的spread）
        int h = key.hashCode();
        int hash = (h ^ (h >>> 16) ^ (h >>> 8) ^ (h >>> 24)) & 0x7fffffff;
        return locks[hash & (stripes - 1)];
    }

    public void lockAll() {
        for (ReentrantLock lock : locks) {
            lock.lock();
        }
    }

    public void unlockAll() {
        for (int i = locks.length - 1; i >= 0; i--) {
            locks[i].unlock();
        }
    }
}