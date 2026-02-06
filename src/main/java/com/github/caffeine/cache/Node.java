package com.github.caffeine.cache;

import com.github.caffeine.cache.reference.ManualReference;
import com.github.caffeine.cache.reference.ManualReferenceQueue;
import com.github.caffeine.cache.reference.ReferenceStrength;

/**
 * 增强的 Node，支持强/弱/软引用存储 Value
 * 参考 Caffeine 的 Node 设计（AddValue.java 中的逻辑）
 */
public class Node<K, V> {
    private final K key;

    // 根据强度决定存储方式：强引用直接存，弱/软引用包装
    private final Object valueHolder;
    private final ReferenceStrength valueStrength;
    private final ManualReferenceQueue<V> refQueue; // 仅弱/软引用时使用

    private final long writeTime;
    private volatile long accessTime;
    private volatile long expireAt;

    // 时间轮链表指针（保持与原有设计兼容）
    private Node<K, V> prevInTimerWheel;
    private Node<K, V> nextInTimerWheel;

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
            // 强引用：直接存储（对应 Caffeine 的 strongValues）
            this.valueHolder = value;
        } else {
            // 弱/软引用：包装为 ManualReference（对应 Caffeine 的 weakValues/softValues）
            ManualReference<V> ref = new ManualReference<>(value, refQueue, strength);
            this.valueHolder = ref;

            // 设置清理回调：当引用被 GC 时，标记节点为失效
            // 注意：这里只是标记，实际从 Map 移除由 ReferenceQueue 轮询线程处理
            ref.setCleanupCallback(() -> {
                // 标记过期时间为 0，让下次访问时触发清理
                this.expireAt = 0;
            });
        }
    }

    public ReferenceStrength getValueStrength() {
        return valueStrength;
    }

    public K getKey() {
        return key;
    }

    /**
     * 获取 Value（核心方法，参考 Caffeine 的 getValue 实现）
     * 处理弱/软引用的 null 检查（对象被 GC 的情况）
     */
    @SuppressWarnings("unchecked")
    public V getValue() {
        if (valueStrength == ReferenceStrength.STRONG) {
            return (V) valueHolder;
        } else {
            ManualReference<V> ref = (ManualReference<V>) valueHolder;
            if (ref == null) return null;

            V value = ref.get();
            if (value == null && !ref.isCleared()) {
                // 防御性编程：如果读到 null 但引用未被标记为 cleared，
                // 可能是并发导致的可见性问题，重试一次（参考 AddValue.java）
                value = ref.get();
            }
            return value;
        }
    }

    /**
     * 获取原始引用对象（用于清理时比较）
     */
    @SuppressWarnings("unchecked")
    public ManualReference<V> getValueReference() {
        if (valueStrength == ReferenceStrength.STRONG) {
            return null;
        }
        return (ManualReference<V>) valueHolder;
    }

    public boolean isStrongValue() {
        return valueStrength == ReferenceStrength.STRONG;
    }

    public long getWriteTime() {
        return writeTime;
    }

    public long getAccessTime() {
        return accessTime;
    }

    public void setAccessTime(long time) {
        this.accessTime = time;
    }

    public long getExpireAt() {
        return expireAt;
    }

    public void setExpireAt(long time) {
        this.expireAt = time;
    }

    /**
     * 检查节点是否因引用被清理而失效
     */
    public boolean isValueCollected() {
        if (valueStrength == ReferenceStrength.STRONG) {
            return false;
        }
        @SuppressWarnings("unchecked")
        ManualReference<V> ref = (ManualReference<V>) valueHolder;
        return ref == null || ref.isCleared() || ref.get() == null;
    }

    // 时间轮链表操作（保持不变）
    public Node<K, V> getPrevInTimerWheel() {
        return prevInTimerWheel;
    }

    public void setPrevInTimerWheel(Node<K, V> prev) {
        this.prevInTimerWheel = prev;
    }

    public Node<K, V> getNextInTimerWheel() {
        return nextInTimerWheel;
    }

    public void setNextInTimerWheel(Node<K, V> next) {
        this.nextInTimerWheel = next;
    }

    public void detachFromTimerWheel() {
        Node<K, V> prev = this.prevInTimerWheel;
        Node<K, V> next = this.nextInTimerWheel;

        if (prev != null) prev.setNextInTimerWheel(next);
        if (next != null) next.setPrevInTimerWheel(prev);

        this.prevInTimerWheel = null;
        this.nextInTimerWheel = null;
    }
}