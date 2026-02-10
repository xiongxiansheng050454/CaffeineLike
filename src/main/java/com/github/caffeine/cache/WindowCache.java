package com.github.caffeine.cache;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

/**
 * Window缓存区 - W-TinyLFU的入口层（线程安全版）
 *
 * 设计要点：
 * 1. 容量固定为总容量的1%（至少为1），采用标准LRU维护访问顺序
 * 2. 使用 StampedLock 保证并发安全，所有修改操作独占写锁
 * 3. 准入(admit)操作原子化：检查容量→驱逐LRU→添加新节点
 * 4. promotionHandler 回调在锁内执行，必须确保其不会获取其他锁（避免死锁）
 *
 * 线程安全策略：
 * - admit/remove：写锁保护（阻塞式）
 * - onAccess：先无锁检查，必要时升级写锁（乐观读优化）
 * - size()：最终一致性（LongAdder），可能短暂延迟反映最新值
 */
public final class WindowCache<K, V> {
    private final long capacity;
    private final AccessOrderDeque<K, V> deque;
    private final LongAdder size = new LongAdder();
    private final StampedLock lock = new StampedLock();

    // 晋升回调：当Window满时，将LRU候选节点移到Main区
    // 警告：此回调在写锁内执行，若需获取其他锁（如BoundedLocalCache的分片锁），
    // 必须确保全局锁顺序一致（例如：先WindowCache后Segment），否则可能死锁
    private final Consumer<Node<K, V>> promotionHandler;

    public WindowCache(long totalCapacity, Consumer<Node<K, V>> promotionHandler) {
        // Window默认占1%，至少为1，最大不超过100（避免配置错误）
        this.capacity = Math.min(100, Math.max(1, totalCapacity / 100));
        this.deque = new AccessOrderDeque<>();
        this.promotionHandler = promotionHandler;
    }

    /**
     * 准入：新节点进入Window尾部（MRU位置）
     * 如果Window已满，先驱逐LRU节点到Main区，再添加新节点
     *
     * 并发安全：整个方法被写锁包裹，确保"检查-驱逐-添加"三步原子化
     */
    public void admit(Node<K, V> node) {
        long stamp = lock.writeLock();
        try {
            // 严格容量控制：已满则先驱逐
            if (size.sum() >= capacity) {
                evictToMainLocked();
            }

            deque.add(node);
            size.increment();
            node.setInWindow(true);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * 访问处理：如果在Window中，移到MRU位置（尾部）
     * 实现"移动到新近使用端"的LRU语义
     */
    public void onAccess(Node<K, V> node) {
        // 快速路径：无锁检查（volatile读）
        if (!node.isInWindow()) {
            return;
        }

        // 慢速路径：获取写锁进行链表操作
        long stamp = lock.writeLock();
        try {
            // 双重检查：锁获取后状态可能已被其他线程改变
            if (node.isInWindow() && node.getNextInAccessOrder() != null) {
                deque.moveToTail(node);
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * 从Window区显式移除（例如被invalidate时）
     * 如果节点不在Window中，无副作用
     */
    public void remove(Node<K, V> node) {
        long stamp = lock.writeLock();
        try {
            if (node.isInWindow()) {
                deque.remove(node);
                size.decrement();
                node.setInWindow(false);
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * 驱逐LRU节点到Main Cache（内部方法，需在写锁内调用）
     *
     * 注意：promotionHandler.accept() 在此处调用，若其抛出异常，
     * 节点仍会被移出Window（避免内存泄漏），但可能丢失到Main区的链接
     */
    private void evictToMainLocked() {
        Node<K, V> victim = deque.removeFirst();
        if (victim != null) {
            size.decrement();
            victim.setInWindow(false);

            // 触发晋升回调（进入TinyLFU主区）
            if (promotionHandler != null) {
                try {
                    promotionHandler.accept(victim);
                } catch (Exception e) {
                    // 晋升失败不应影响WindowCache的完整性
                    // 实际项目中应使用日志框架（如SLF4J）记录
                    System.err.println("[WindowCache] Promotion failed for key: "
                            + victim.getKey() + ", error: " + e.getMessage());
                }
            }
        }
    }

    /**
     * 当前Window区条目数（近似值，基于LongAdder）
     */
    public long size() {
        return size.sum();
    }

    /**
     * Window区最大容量（固定为总容量的1%）
     */
    public long capacity() {
        return capacity;
    }

    /**
     * 是否已满（size >= capacity）
     */
    public boolean isFull() {
        return size.sum() >= capacity;
    }

    /**
     * 仅供测试使用：获取当前占用率（0.0 ~ 1.0）
     */
    double occupancyRate() {
        return (double) size.sum() / capacity;
    }
}