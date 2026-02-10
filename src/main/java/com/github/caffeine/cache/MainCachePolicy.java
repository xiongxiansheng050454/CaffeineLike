package com.github.caffeine.cache;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;

/**
 * Main Cache 策略管理器 - 优化版（采样访问）
 */
public final class MainCachePolicy<K, V> {
    private final AccessOrderDeque<K, V> probationDeque;
    private final AccessOrderDeque<K, V> protectedDeque;

    private final long probationMaximum;
    private final long protectedMaximum;

    private final LongAdder probationWeightedSize = new LongAdder();
    private final LongAdder protectedWeightedSize = new LongAdder();

    private final StampedLock lock = new StampedLock();

    public MainCachePolicy(long mainCacheMaximum) {
        this.probationDeque = new AccessOrderDeque<>();
        this.protectedDeque = new AccessOrderDeque<>();

        this.protectedMaximum = (long) (mainCacheMaximum * 0.8);
        this.probationMaximum = mainCacheMaximum - this.protectedMaximum;
    }

    public Node<K, V> admitToProbation(Node<K, V> node) {
        long stamp = lock.writeLock();
        try {
            if (probationWeightedSize.sum() >= probationMaximum) {
                Node<K, V> lru = probationDeque.peekFirst();
                if (lru != null) {
                    if (protectedWeightedSize.sum() < protectedMaximum) {
                        probationDeque.remove(lru);
                        probationWeightedSize.decrement();

                        protectedDeque.add(lru);
                        lru.setQueueType(QueueType.PROTECTED.value());
                        protectedWeightedSize.increment();
                    } else {
                        Node<K, V> victim = evictFromProbationUnsafe();
                        if (victim != null) {
                            victim.setQueueType(-1);
                            probationDeque.add(node);
                            node.setQueueType(QueueType.PROBATION.value());
                            probationWeightedSize.increment();
                            return victim;
                        }
                    }
                }
            }

            probationDeque.add(node);
            node.setQueueType(QueueType.PROBATION.value());
            probationWeightedSize.increment();
            return null;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public boolean tryPromoteToProtected(Node<K, V> node, int frequency,
                                         int probationHeadFreq) {
        long stamp = lock.writeLock();
        try {
            if (!node.isInProbation()) return false;

            if (protectedWeightedSize.sum() >= protectedMaximum) {
                if (frequency <= probationHeadFreq) {
                    probationDeque.moveToTail(node);
                    return false;
                }
                demoteProtectedToProbationUnsafe();
            }

            probationDeque.remove(node);
            probationWeightedSize.decrement();

            protectedDeque.add(node);
            node.setQueueType(QueueType.PROTECTED.value());
            protectedWeightedSize.increment();
            return true;

        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * 优化：采样模式的Protected访问（非阻塞尝试）
     */
    public void onProtectedAccessSampled(Node<K, V> node) {
        // 尝试非阻塞获取锁
        long stamp = lock.tryWriteLock();
        if (stamp == 0) return; // 放弃本次LRU更新

        try {
            if (node.isInProtected()) {
                protectedDeque.moveToTail(node);
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public Node<K, V> evictFromProbation() {
        long stamp = lock.writeLock();
        try {
            return evictFromProbationUnsafe();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    private Node<K, V> evictFromProbationUnsafe() {
        Node<K, V> victim = probationDeque.removeFirst();
        if (victim != null) {
            probationWeightedSize.decrement();
            victim.setQueueType(-1);
        }
        return victim;
    }

    private void demoteProtectedToProbationUnsafe() {
        Node<K, V> victim = protectedDeque.removeFirst();
        if (victim != null) {
            protectedWeightedSize.decrement();

            probationDeque.add(victim);
            victim.setQueueType(QueueType.PROBATION.value());
            probationWeightedSize.increment();
        }
    }

    public void remove(Node<K, V> node) {
        long stamp = lock.writeLock();
        try {
            if (node.isInProbation()) {
                probationDeque.remove(node);
                probationWeightedSize.decrement();
            } else if (node.isInProtected()) {
                protectedDeque.remove(node);
                protectedWeightedSize.decrement();
            }
            node.setQueueType(-1);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public long probationSize() { return probationWeightedSize.sum(); }
    public long protectedSize() { return protectedWeightedSize.sum(); }
    public long totalSize() { return probationSize() + protectedSize(); }

    public Node<K, V> peekProbationHead() {
        long stamp = lock.tryOptimisticRead();
        Node<K, V> head = probationDeque.peekFirst();
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                head = probationDeque.peekFirst();
            } finally {
                lock.unlockRead(stamp);
            }
        }
        return head;
    }

    public Node<K, V> getProtectedLRU() {
        long stamp = lock.readLock();
        try {
            return protectedDeque.peekFirst();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public Node<K, V> replaceToProbation(Node<K, V> oldNode, Node<K, V> newNode) {
        long stamp = lock.writeLock();
        try {
            if (oldNode.isInProbation()) {
                probationDeque.remove(oldNode);
                probationWeightedSize.decrement();
            } else if (oldNode.isInProtected()) {
                protectedDeque.remove(oldNode);
                protectedWeightedSize.decrement();
            }

            Node<K, V> victim = null;
            if (probationWeightedSize.sum() >= probationMaximum) {
                victim = evictFromProbationUnsafe();
                if (victim != null) {
                    victim.setQueueType(-1);
                }
            }

            probationDeque.add(newNode);
            newNode.setQueueType(QueueType.PROBATION.value());
            probationWeightedSize.increment();

            return victim;
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}