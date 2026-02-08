package com.github.caffeine.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class HierarchicalTimerWheel<K, V> {

    @FunctionalInterface
    public interface ExpirationCallback<K> {
        void onExpired(K key);
    }

    private static final int[] WHEEL_SIZE = {10, 60, 24};// 10*100ms=1s, 60*1m=1h, 24*1h=1d
    private static final long[] TICK_MS = {100, 60*1000, 60*60*1000};// 100ms, 1min, 1hour

    private final List<TimerBucket<K, V>[]> wheels;
    private volatile long currentTime;
    private final ExpirationCallback<K> callback;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread schedulerThread;
    private final Object lock = new Object(); // 用于保护时间轮状态

    public HierarchicalTimerWheel(ExpirationCallback<K> callback) {
        this.callback = callback;
        this.currentTime = System.currentTimeMillis();
        this.wheels = new ArrayList<>();

        for (int j : WHEEL_SIZE) {
            @SuppressWarnings("unchecked")
            TimerBucket<K, V>[] wheel = new TimerBucket[j];
            for (int i = 0; i < wheel.length; i++) {
                wheel[i] = new TimerBucket<>();
            }
            wheels.add(wheel);
        }

        this.schedulerThread = new Thread(this::runScheduler, "timer-wheel-scheduler");
        this.schedulerThread.setDaemon(true);
        this.schedulerThread.start();
    }

    public void schedule(Node<K, V> node, long delayMs) {
        long expirationTime = currentTime + delayMs;

        synchronized (lock) {
            for (int level = wheels.size() - 1; level >= 0; level--) {
                if (delayMs >= TICK_MS[level]) {
                    int slot = calculateSlot(expirationTime, level);
                    wheels.get(level)[slot].add(node);
                    return;
                }
            }
            wheels.get(0)[calculateSlot(expirationTime, 0)].add(node);
        }
    }

    public void cancel(Node<K, V> node) {
        synchronized (lock) {
            node.detachFromTimerWheel();
        }
    }

    private void runScheduler() {
        while (running.get()) {
            long start = System.currentTimeMillis();

            synchronized (lock) {
                advanceClock();
            }

            long sleepMs = 100 - (System.currentTimeMillis() - start);
            if (sleepMs > 0) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(sleepMs));
            }
        }
    }

    private void advanceClock() {
        currentTime += 100;
        int secondSlot = calculateSlot(currentTime, 0);

        List<Node<K, V>> expired = wheels.get(0)[secondSlot].clearAndGet();

        for (Node<K, V> node : expired) {
            if (node.getExpireAt() <= currentTime) {
                callback.onExpired(node.getKey());
            } else {
                reschedule(node);
            }
        }

        if (secondSlot == 0) {
            promoteLevel(1);
        }
    }

    private void promoteLevel(int level) {
        if (level >= wheels.size()) return;

        int slot = calculateSlot(currentTime, level);
        List<Node<K, V>> nodes = wheels.get(level)[slot].clearAndGet();

        for (Node<K, V> node : nodes) {
            long remaining = node.getExpireAt() - currentTime;

            if (remaining < TICK_MS[level - 1]) {
                int targetSlot = calculateSlot(node.getExpireAt(), level - 1);
                wheels.get(level - 1)[targetSlot].add(node);
            } else {
                int targetSlot = calculateSlot(node.getExpireAt(), level);
                wheels.get(level)[targetSlot].add(node);
            }
        }

        if (slot == 0 && level + 1 < wheels.size()) {
            promoteLevel(level + 1);
        }
    }

    private void reschedule(Node<K, V> node) {
        long delay = node.getExpireAt() - currentTime;
        if (delay > 0) {
            schedule(node, delay);
        }
    }

    private int calculateSlot(long time, int level) {
        return (int) ((time / TICK_MS[level]) % WHEEL_SIZE[level]);
    }

    public void shutdown() {
        running.set(false);
        schedulerThread.interrupt();
    }

    private static class TimerBucket<K, V> {
        private final Node<K, V> sentinel;

        TimerBucket() {
            sentinel = new Node<>(null, null, 0, 0,null,null);
            sentinel.setNextInTimerWheel(sentinel);
            sentinel.setPrevInTimerWheel(sentinel);
        }

        void add(Node<K, V> node) {
            Node<K, V> next = sentinel.getNextInTimerWheel();
            node.setPrevInTimerWheel(sentinel);
            node.setNextInTimerWheel(next);
            sentinel.setNextInTimerWheel(node);
            next.setPrevInTimerWheel(node);
        }

        List<Node<K, V>> clearAndGet() {
            List<Node<K, V>> list = new ArrayList<>();
            Node<K, V> current = sentinel.getNextInTimerWheel();

            while (current != sentinel) {
                list.add(current);
                current = current.getNextInTimerWheel();
            }

            sentinel.setNextInTimerWheel(sentinel);
            sentinel.setPrevInTimerWheel(sentinel);
            return list;
        }
    }
}