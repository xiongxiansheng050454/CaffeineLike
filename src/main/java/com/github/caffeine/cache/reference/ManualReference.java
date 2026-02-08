package com.github.caffeine.cache.reference;

import jdk.internal.vm.annotation.Contended;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * 手动的引用模拟，使用 VarHandle 替代 AtomicReference
 * 达到与 Caffeine WeakValueReference 同等的无锁性能
 */
public class ManualReference<T> {
    private static final VarHandle REFERENT_HANDLE;

    // @Contended 确保与 Node 的其它字段不共享缓存行
    @Contended
    private volatile T referent;

    // 新增：存储 key，用于反向查找
    private final Object key;

    private final ManualReferenceQueue<T> queue;
    private volatile boolean cleared;
    private final ReferenceStrength strength;
    private volatile Runnable cleanupCallback;

    static {
        try {
            REFERENT_HANDLE = MethodHandles.lookup()
                    .findVarHandle(ManualReference.class, "referent", Object.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public ManualReference(Object key, T referent, ManualReferenceQueue<T> queue, ReferenceStrength strength) {
        this.key = key;  // 存储 key
        this.queue = queue;
        this.strength = strength;
        // lazySet 初始化
        REFERENT_HANDLE.setRelease(this, referent);
        this.cleared = false;
    }

    /**
     * 无锁读：getAcquire（volatile 读）
     */
    @SuppressWarnings("unchecked")
    public T get() {
        return (T) REFERENT_HANDLE.getAcquire(this);
    }

    // 新增：获取 key 的方法
    @SuppressWarnings("unchecked")
    public <K> K getKey() {
        return (K) key;
    }

    public ReferenceStrength getStrength() {
        return strength;
    }

    public void setCleanupCallback(Runnable callback) {
        this.cleanupCallback = callback;
    }

    /**
     * 模拟 GC 回收：使用 CAS 确保只清理一次
     */
    public void clear() {
        if (!cleared) {  // 先检查，避免内存屏障
            cleared = true;
            T old = (T) REFERENT_HANDLE.getAndSet(this, null);
            if (queue != null && old != null) {
                queue.enqueue(this);
            }
            if (cleanupCallback != null) {
                cleanupCallback.run();
            }
        }
    }

    public boolean isCleared() {
        return cleared;
    }

    /**
     * 模拟内存压力下的清理（用于测试）
     */
    public static void simulateGC(ManualReference<?> ref) {
        if (ref != null && ref.strength != ReferenceStrength.STRONG) {
            ref.clear();
        }
    }
}