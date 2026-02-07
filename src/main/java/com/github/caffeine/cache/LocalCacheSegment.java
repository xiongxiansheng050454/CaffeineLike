package com.github.caffeine.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

public final class LocalCacheSegment<K, V> {
    private final HashMap<K, Node<K, V>> map;
    private final StampedLock lock = new StampedLock(); // 独立锁

    public LocalCacheSegment() {
        this.map = new HashMap<>(16);
    }

    public Node<K, V> getNode(K key) {
        // 乐观读：无锁尝试
        long stamp = lock.tryOptimisticRead();
        Node<K, V> node = map.get(key);
        if (lock.validate(stamp)) {
            return node;  // 无锁成功
        }

        // 回退到读锁
        stamp = lock.readLock();
        try {
            return map.get(key);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    // 写操作使用 writeLock
    public Node<K, V> putNode(K key, Node<K, V> node) {
        long stamp = lock.writeLock();
        try {
            return map.put(key, node);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    // 修正：使用 writeLock 而不是 lock()
    public boolean removeNode(K key, Node<K, V> node) {
        long stamp = lock.writeLock();
        try {
            // 注意：HashMap.remove(key, value) 需要 value 匹配
            if (map.get(key) == node) {
                map.remove(key);
                return true;
            }
            return false;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    // 修正：使用 writeLock
    public Node<K, V> removeNode(K key) {
        long stamp = lock.writeLock();
        try {
            return map.remove(key);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    // 修正：使用 readLock（允许并发读）
    public int size() {
        long stamp = lock.readLock();
        try {
            return map.size();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    // 仅用于遍历（外部需先 lockAll）
    public HashMap<K, Node<K, V>> getMap() {
        return map;
    }

    public StampedLock getLock() {
        return lock;
    }

    // 修正：根据操作类型选择锁
    public void processSafelyWrite(Consumer<Map<K, Node<K, V>>> processor) {
        long stamp = lock.writeLock(); // 或 readLock() 如果只读
        try {
            processor.accept(map);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    // 新增：只读安全处理
    public void processSafelyRead(Consumer<Map<K, Node<K, V>>> processor) {
        long stamp = lock.readLock();
        try {
            processor.accept(map);
        } finally {
            lock.unlockRead(stamp);
        }
    }
}