package com.github.caffeine.cache;

import com.github.caffeine.cache.reference.ManualReference;
import com.github.caffeine.cache.reference.ManualReferenceQueue;
import com.github.caffeine.cache.reference.ReferenceStrength;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import jdk.internal.vm.annotation.Contended;

/**
 * 增强的 Node，支持 VarHandle 无锁读写
 * 参考 Caffeine 的 Node 设计（AddValue.java + IntermittentNull.java）
 */
public class Node<K, V> implements AccessOrder<Node<K, V>>{
    private static final VarHandle VALUE_HANDLE;
    private static final VarHandle QUEUE_TYPE_HANDLE;

    private final K key;

    // @Contended 确保 value 独占 64 字节缓存行，避免伪共享
    @Contended
    private volatile Object valueHolder;

    @Contended
    private volatile int queueType;  // 0=Window, 1=Probation, 2=Protected


    private final ReferenceStrength valueStrength;
    private final ManualReferenceQueue<V> refQueue;

    // 时间戳字段也隔离，避免与 value 竞争缓存行
    @Contended
    private volatile long accessTime;

    @Contended
    private volatile long expireAt;

    private final long writeTime;

    // 时间轮链表指针
    private Node<K, V> prevInTimerWheel;
    private Node<K, V> nextInTimerWheel;

    @Contended
    private volatile boolean inWindow;  // 标记是否在Window区

    // AccessOrder链表指针（与TimerWheel指针独立）
    private Node<K, V> prevInAccessOrder;
    private Node<K, V> nextInAccessOrder;

    static {
        try {
            VALUE_HANDLE = MethodHandles.lookup()
                    .findVarHandle(Node.class, "valueHolder", Object.class);
            QUEUE_TYPE_HANDLE = MethodHandles.lookup()
                    .findVarHandle(Node.class, "queueType", int.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * 构造方法：根据强度自动选择存储方式
     */
    public Node(K key, V value, long writeTime, long expireAt,
                ReferenceStrength strength, ManualReferenceQueue<V> refQueue) {
        this.key = key;
        this.valueStrength = strength;
        this.refQueue = refQueue;
        this.writeTime = writeTime;
        this.accessTime = writeTime;
        this.expireAt = expireAt;

        if (strength == ReferenceStrength.STRONG || refQueue == null) {
            // 强引用：使用 setRelease（lazySet）初始化，避免立即刷回内存
            VALUE_HANDLE.setRelease(this, value);
        } else {
            // 弱/软引用：包装为 ManualReference
            ManualReference<V> ref = new ManualReference<>(key, value, refQueue, strength);
            VALUE_HANDLE.setRelease(this, ref);
            ref.setCleanupCallback(() -> this.expireAt = 0);
        }

        this.queueType = QueueType.WINDOW.value();  // 默认进入 Window 区
    }

    public int getQueueType() {
        return (int) QUEUE_TYPE_HANDLE.getAcquire(this);
    }

    public void setQueueType(int type) {
        QUEUE_TYPE_HANDLE.setRelease(this, type);
    }

    // 辅助方法
    public boolean isInProbation() { return getQueueType() == QueueType.PROBATION.value(); }
    public boolean isInProtected() { return getQueueType() == QueueType.PROTECTED.value(); }

    public ReferenceStrength getValueStrength() {
        return valueStrength;
    }

    public K getKey() {
        return key;
    }

    /**
     * 无锁读：volatile 语义（getAcquire）
     * 对应 Caffeine 的 getValue 逻辑
     */
    @SuppressWarnings("unchecked")
    public V getValue() {
        if (valueStrength == ReferenceStrength.STRONG) {
            // 强引用：直接 volatile 读
            return (V) VALUE_HANDLE.getAcquire(this);
        } else {
            // 弱/软引用：读取 ManualReference，再读 referent
            ManualReference<V> ref = (ManualReference<V>) VALUE_HANDLE.getAcquire(this);
            if (ref == null) return null;

            V value = ref.get();

            // 防御 stale read：如果读到 null 但引用未被清理，重试一次
            // 参考 Caffeine 的 IntermittentNull.java 模式
            if (value == null && !ref.isCleared()) {
                VarHandle.loadLoadFence();  // 加载屏障
                value = ref.get();
            }
            return value;
        }
    }

    /**
     * 无锁写：lazySet 语义（setRelease）
     * 性能优于 volatile 写，适合缓存更新场景
     */
    public void setValue(V value) {
        if (valueStrength == ReferenceStrength.STRONG) {
            VALUE_HANDLE.setRelease(this, value);
        } else {
            // 软/弱引用：创建新引用，使用 storeStoreFence 确保顺序
            ManualReference<V> newRef = new ManualReference<>(key, value, refQueue, valueStrength);
            ManualReference<V> oldRef = (ManualReference<V>) VALUE_HANDLE.getAcquire(this);

            VALUE_HANDLE.setRelease(this, newRef);
            VarHandle.storeStoreFence();  // 确保新引用可见后再清理旧引用

            if (oldRef != null) oldRef.clear();
        }
    }

    /**
     * CAS 更新（用于复杂并发控制，如 refresh）
     */
    public boolean casValue(V expected, V update) {
        if (valueStrength != ReferenceStrength.STRONG) {
            throw new UnsupportedOperationException("CAS only supported for strong values");
        }
        return VALUE_HANDLE.compareAndSet(this, expected, update);
    }

    @SuppressWarnings("unchecked")
    public ManualReference<V> getValueReference() {
        if (valueStrength == ReferenceStrength.STRONG) return null;
        return (ManualReference<V>) VALUE_HANDLE.getAcquire(this);
    }

    public boolean isStrongValue() {
        return valueStrength == ReferenceStrength.STRONG;
    }

    // === 时间戳访问也使用 VarHandle 优化 ===
    public long getAccessTime() {
        return (long) ACCESS_TIME_HANDLE.getAcquire(this);
    }

    public void setAccessTime(long time) {
        ACCESS_TIME_HANDLE.setRelease(this, time);
    }

    public long getExpireAt() {
        return (long) EXPIRE_AT_HANDLE.getAcquire(this);
    }

    public void setExpireAt(long time) {
        EXPIRE_AT_HANDLE.setRelease(this, time);
    }

    private static final VarHandle ACCESS_TIME_HANDLE;
    private static final VarHandle EXPIRE_AT_HANDLE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            ACCESS_TIME_HANDLE = lookup.findVarHandle(Node.class, "accessTime", long.class);
            EXPIRE_AT_HANDLE = lookup.findVarHandle(Node.class, "expireAt", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public long getWriteTime() {
        return writeTime;
    }

    public boolean isValueCollected() {
        if (valueStrength == ReferenceStrength.STRONG) return false;
        ManualReference<V> ref = getValueReference();
        return ref == null || ref.isCleared() || ref.get() == null;
    }

    // 时间轮链表操作（保持不变）
    public Node<K, V> getPrevInTimerWheel() { return prevInTimerWheel; }
    public void setPrevInTimerWheel(Node<K, V> prev) { this.prevInTimerWheel = prev; }
    public Node<K, V> getNextInTimerWheel() { return nextInTimerWheel; }
    public void setNextInTimerWheel(Node<K, V> next) { this.nextInTimerWheel = next; }

    public void detachFromTimerWheel() {
        Node<K, V> prev = this.prevInTimerWheel;
        Node<K, V> next = this.nextInTimerWheel;

        if (prev != null && next != null && prev != this && next != this) {
            // 标准的双向链表摘除
            prev.setNextInTimerWheel(next);
            next.setPrevInTimerWheel(prev);
        }
        // 清空指针，帮助 GC
        this.prevInTimerWheel = null;
        this.nextInTimerWheel = null;
    }

    public boolean isInWindow() { return inWindow; }
    public void setInWindow(boolean inWindow) { this.inWindow = inWindow; }

    @Override public Node<K, V> getPreviousInAccessOrder() { return prevInAccessOrder; }
    @Override public void setPreviousInAccessOrder(Node<K, V> prev) { this.prevInAccessOrder = prev; }

    @Override public Node<K, V> getNextInAccessOrder() { return nextInAccessOrder; }
    @Override public void setNextInAccessOrder(Node<K, V> next) { this.nextInAccessOrder = next; }
}