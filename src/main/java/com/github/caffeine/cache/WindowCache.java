package com.github.caffeine.cache;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

/**
 * Window缓存区 - 优化版（修正 dummy 引用问题）
 */
public final class WindowCache<K, V> {
    private final long capacity;
    private final AccessOrderDeque<K, V> deque;
    private final LongAdder size = new LongAdder();
    private final StampedLock lock = new StampedLock();
    private final Consumer<Node<K, V>> promotionHandler;

    public WindowCache(long totalCapacity, Consumer<Node<K, V>> promotionHandler) {
        this.capacity = Math.min(100, Math.max(1, totalCapacity / 100));
        this.deque = new AccessOrderDeque<>();
        this.promotionHandler = promotionHandler;
    }

    public void admit(Node<K, V> node) {
        Node<K, V> victimToPromote = null;
        long stamp = lock.writeLock();
        try {
            if (size.sum() >= capacity) {
                victimToPromote = deque.removeFirst();
                if (victimToPromote != null) {
                    size.decrement();
                    victimToPromote.setInWindow(false);
                }
            }

            deque.add(node);
            size.increment();
            node.setInWindow(true);
        } finally {
            lock.unlockWrite(stamp);
        }

        if (victimToPromote != null && promotionHandler != null) {
            try {
                promotionHandler.accept(victimToPromote);
            } catch (Exception e) {
                System.err.println("[WindowCache] Promotion failed: " + e.getMessage());
            }
        }
    }

    public void replace(Node<K, V> oldNode, Node<K, V> newNode) {
        long stamp = lock.writeLock();
        try {
            if (oldNode.isInWindow()) {
                deque.remove(oldNode);
                size.decrement();
                oldNode.setInWindow(false);
            }

            deque.add(newNode);
            size.increment();
            newNode.setInWindow(true);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * 优化后的 onAccess：采样模式，非阻塞尝试获取锁
     * 关键修正：使用 deque.isTail() 替代直接访问 dummy
     */
    public void onAccessSampled(Node<K, V> node) {
        // 快速检查：如果已经在MRU位置（尾部），无需移动
        // 修正：通过 deque.isTail() 方法检查，而非直接访问 dummy
        if (deque.isTail(node)) {
            return;
        }

        // 尝试非阻塞获取写锁（0超时）
        long stamp = lock.tryWriteLock();
        if (stamp == 0) {
            // 锁被占用，放弃本次LRU更新（降低竞争）
            return;
        }

        try {
            // 双重检查：确保节点仍在Window中且需要移动
            if (node.isInWindow() && node.getNextInAccessOrder() != null) {
                // 再次检查是否已经在尾部（可能其他线程已移动）
                if (!deque.isTail(node)) {
                    deque.moveToTail(node);
                }
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

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

    public long size() {
        return size.sum();
    }

    public long capacity() {
        return capacity;
    }

    public boolean isFull() {
        return size.sum() >= capacity;
    }

    double occupancyRate() {
        return (double) size.sum() / capacity;
    }
}