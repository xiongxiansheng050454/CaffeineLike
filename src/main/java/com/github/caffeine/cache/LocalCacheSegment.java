package com.github.caffeine.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public final class LocalCacheSegment<K, V> {
    private final HashMap<K, Node<K, V>> map;
    private final ReentrantLock lock = new ReentrantLock(); // 独立锁

    public LocalCacheSegment() {
        this.map = new HashMap<>(16);
    }

    public Node<K, V> getNode(K key) {
        lock.lock();
        try {
            return map.get(key);
        } finally {
            lock.unlock();
        }
    }

    public Node<K, V> putNode(K key, Node<K, V> node) {
        lock.lock();
        try {
            return map.put(key, node);
        } finally {
            lock.unlock();
        }
    }

    public boolean removeNode(K key, Node<K, V> node) {
        lock.lock();
        try {
            return map.remove(key, node);
        } finally {
            lock.unlock();
        }
    }

    public Node<K, V> removeNode(K key) {
        lock.lock();
        try {
            return map.remove(key);
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return map.size();
        } finally {
            lock.unlock();
        }
    }

    // 仅用于遍历（外部需先 lockAll）
    public HashMap<K, Node<K, V>> getMap() {
        return map;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public void processSafely(Consumer<Map<K, Node<K, V>>> processor) {
        lock.lock();
        try {
            processor.accept(map);
        } finally {
            lock.unlock();
        }
    }
}