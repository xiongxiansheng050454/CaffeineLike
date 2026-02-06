package com.github.caffeine.cache.reference;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 手动的引用队列，模拟 JVM ReferenceQueue
 */
public class ManualReferenceQueue<T> {
    private final ConcurrentLinkedQueue<ManualReference<T>> queue =
            new ConcurrentLinkedQueue<>();

    void enqueue(ManualReference<? extends T> ref) {
        queue.offer((ManualReference<T>) ref);
    }

    public ManualReference<T> poll() {
        return queue.poll();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }
}

