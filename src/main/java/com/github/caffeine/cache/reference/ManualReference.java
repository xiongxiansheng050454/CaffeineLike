package com.github.caffeine.cache.reference;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 手动的引用模拟，替代 JVM 的 WeakReference/SoftReference
 * 与 Caffeine 的 WeakValueReference 对应
 */
public class ManualReference<T> {
    // 使用 AtomicReference 实现线程安全的 referent 存取
    private final AtomicReference<T> referent;
    private final ManualReferenceQueue<T> queue;
    private final AtomicBoolean cleared;
    private final ReferenceStrength strength;

    // 回调：当引用被清除时，通知缓存移除节点（类似 Caffeine 的 onRemoval）
    private volatile Runnable cleanupCallback;

    public ManualReference(T referent, ManualReferenceQueue<T> queue,
                           ReferenceStrength strength) {
        this.referent = new AtomicReference<>(referent);
        this.queue = queue;
        this.strength = strength;
        this.cleared = new AtomicBoolean(false);
    }

    public T get() {
        return referent.get();
    }

    public ReferenceStrength getStrength() {
        return strength;
    }

    public void setCleanupCallback(Runnable callback) {
        this.cleanupCallback = callback;
    }

    /**
     * 模拟 GC 回收对象，触发入队
     * 对应 Caffeine 中 value 被回收后的 die() 逻辑
     */
    public void clear() {
        if (cleared.compareAndSet(false, true)) {
            T old = referent.getAndSet(null);
            if (queue != null && old != null) {
                queue.enqueue(this);
            }
            // 触发清理回调（从 Segment 中移除 Node）
            if (cleanupCallback != null) {
                cleanupCallback.run();
            }
        }
    }

    public boolean isCleared() {
        return cleared.get();
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