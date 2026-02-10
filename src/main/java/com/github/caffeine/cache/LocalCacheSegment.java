package com.github.caffeine.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

public final class LocalCacheSegment<K, V> {
    private final HashMap<K, Node<K, V>> map;
    private final StampedLock lock = new StampedLock();

    public LocalCacheSegment() {
        this.map = new HashMap<>(16);
    }

    /**
     * 优化后的getNode：自旋重试乐观读，提高成功率
     */
    public Node<K, V> getNode(K key) {
        // 优化：最多重试3次，给写操作完成的时间窗口
        for (int i = 0; i < 3; i++) {
            long stamp = lock.tryOptimisticRead();
            Node<K, V> node = map.get(key);

            // 关键：验证前添加内存屏障，确保读到最新值
            if (lock.validate(stamp)) {
                return node;  // 成功，完全无锁返回
            }

            // 短暂自旋，等待写操作释放（减少CPU占用）
            if (i < 2) {
                Thread.onSpinWait();
            }
        }

        // 3次失败后降级到读锁（保护性读取）
        long stamp = lock.readLock();
        try {
            return map.get(key);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    // 写操作保持不变
    public Node<K, V> putNode(K key, Node<K, V> node) {
        long stamp = lock.writeLock();
        try {
            return map.put(key, node);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public boolean removeNode(K key, Node<K, V> node) {
        long stamp = lock.writeLock();
        try {
            if (map.get(key) == node) {
                map.remove(key);
                return true;
            }
            return false;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public Node<K, V> removeNode(K key) {
        long stamp = lock.writeLock();
        try {
            return map.remove(key);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public int size() {
        long stamp = lock.tryOptimisticRead();
        int size = map.size();
        if (lock.validate(stamp)) {
            return size;
        }

        stamp = lock.readLock();
        try {
            return map.size();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public HashMap<K, Node<K, V>> getMap() {
        return map;
    }

    public StampedLock getLock() {
        return lock;
    }
}