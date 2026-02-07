package com.github.caffeine.cache;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

public class InstrumentedStripedLock extends StripedLock {
    private final LongAdder totalOps = new LongAdder();
    private final LongAdder contentions = new LongAdder();

    public InstrumentedStripedLock(int stripes) {
        super(stripes);
    }

    public LockContext acquireLock(Object key) {
        ReentrantLock lock = getLock(key);
        totalOps.increment();

        // 关键：尝试立即获取（0纳秒超时），失败才算真正冲突
        boolean acquired = false;
        try {
            acquired = lock.tryLock(0, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (!acquired) {
            contentions.increment(); // 真正需要排队
            lock.lock(); // 阻塞获取
        }

        return new LockContext(lock, key);
    }

    public void releaseLock(LockContext ctx) {
        ctx.lock.unlock();
    }

    public double getContentionRate() {
        long ops = totalOps.sum();
        return ops == 0 ? 0.0 : (double) contentions.sum() / ops * 100;
    }

    public long getTotalOps() {
        return totalOps.sum();
    }

    public static class LockContext {
        final ReentrantLock lock;
        final Object key;

        LockContext(ReentrantLock lock, Object key) {
            this.lock = lock;
            this.key = key;
        }
    }
}