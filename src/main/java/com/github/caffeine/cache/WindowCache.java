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
     * Window区采样访问处理 - 优化版
     * 不仅检查尾部，还检查是否在头部（LRU）附近，如果是则移动到尾部
     */
    public void onAccessSampled(Node<K, V> node) {
        // 优化：如果size很小（<10），每次访问都移动
        // 如果size较大，仅当节点不在尾部附近时移动
        if (size.sum() < 10 || !deque.isTail(node)) {
            long stamp = lock.tryWriteLock();
            if (stamp == 0) return;

            try {
                if (node.isInWindow() && !deque.isTail(node)) {
                    deque.moveToTail(node);
                }
            } finally {
                lock.unlockWrite(stamp);
            }
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