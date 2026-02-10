package com.github.caffeine.cache;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;

/**
 * Main Cache 策略管理器 - 修复版（避免 StampedLock 重入）
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

    /**
     * 将节点从 Window 移入 Probation
     * @return 如果因容量限制需要驱逐节点，返回该节点；否则返回 null
     */
    public Node<K, V> admitToProbation(Node<K, V> node) {
        long stamp = lock.writeLock();
        try {
            // 如果 Probation 已满，先尝试将 LRU 降级到 Protected
            if (probationWeightedSize.sum() >= probationMaximum) {
                Node<K, V> lru = probationDeque.peekFirst();
                if (lru != null) {
                    // 如果 Protected 未满，降级到 Protected
                    if (protectedWeightedSize.sum() < protectedMaximum) {
                        probationDeque.remove(lru);
                        probationWeightedSize.decrement();

                        protectedDeque.add(lru);
                        lru.setQueueType(QueueType.PROTECTED.value());
                        protectedWeightedSize.increment();
                    } else {
                        // Protected 也满，才需要真正驱逐
                        Node<K, V> victim = evictFromProbationUnsafe();
                        if (victim != null) {
                            victim.setQueueType(-1);
                            // 注意：这里只返回victim，不执行实际删除
                            // 让上层调用者（doPutInternal）在锁外处理

                            // 添加新节点前先处理驱逐逻辑会导致计数错误，
                            // 所以先添加新节点，再返回victim
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

    /**
     * 访问 Probation 区节点：尝试晋升到 Protected
     * 修复：避免在 tryPromoteToProtected 内部嵌套获取写锁
     */
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
                // 降级操作，使用 unsafe 版本
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
     * 从 Probation 驱逐（当 Main Cache 满时）- 公共方法，带锁
     */
    public Node<K, V> evictFromProbation() {
        long stamp = lock.writeLock();
        try {
            return evictFromProbationUnsafe();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * 内部驱逐逻辑 - 假设已持有写锁
     */
    private Node<K, V> evictFromProbationUnsafe() {
        Node<K, V> victim = probationDeque.removeFirst();
        if (victim != null) {
            probationWeightedSize.decrement();
            victim.setQueueType(-1);
        }
        return victim;
    }

    /**
     * 降级 Protected 尾部到 Probation 头部 - 假设已持有写锁
     */
    private void demoteProtectedToProbationUnsafe() {
        Node<K, V> victim = protectedDeque.removeFirst();
        if (victim != null) {
            protectedWeightedSize.decrement();

            probationDeque.add(victim);
            victim.setQueueType(QueueType.PROBATION.value());
            probationWeightedSize.increment();
        }
    }

    // 其他方法保持不变...
    public void onProtectedAccess(Node<K, V> node) {
        long stamp = lock.writeLock();
        try {
            if (node.isInProtected()) {
                protectedDeque.moveToTail(node);
            }
        } finally {
            lock.unlockWrite(stamp);
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