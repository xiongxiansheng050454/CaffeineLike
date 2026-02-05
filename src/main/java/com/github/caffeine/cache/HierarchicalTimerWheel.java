package com.github.caffeine.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * 分层时间轮：秒级(60槽) -> 分钟级(60槽) -> 小时级(24槽)
 * 与BoundedLocalCache通过 ExpirationCallback 接口解耦
 */
public class HierarchicalTimerWheel<K, V> {

    @FunctionalInterface
    public interface ExpirationCallback<K> {
        void onExpired(K key);
    }

    // 时间轮层级配置
    private static final int[] WHEEL_SIZE = {60, 60, 24};
    private static final long[] TICK_MS = {1000, 60*1000, 60*60*1000}; // 秒、分、时

    private final List<TimerBucket<K, V>[]> wheels;
    private volatile long currentTime; // 当前时间（ms，对齐到秒）
    private final ExpirationCallback<K> callback;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread schedulerThread;

    public HierarchicalTimerWheel(ExpirationCallback<K> callback) {
        this.callback = callback;
        this.currentTime = System.currentTimeMillis();
        this.wheels = new ArrayList<>();

        // 初始化三层时间轮
        for (int j : WHEEL_SIZE) {
            @SuppressWarnings("unchecked")
            TimerBucket<K, V>[] wheel = new TimerBucket[j];
            for (int i = 0; i < wheel.length; i++) {
                wheel[i] = new TimerBucket<>();
            }
            wheels.add(wheel);
        }

        // 启动调度线程（类似Caffeine的Pacer）
        this.schedulerThread = new Thread(this::runScheduler, "timer-wheel-scheduler");
        this.schedulerThread.setDaemon(true);
        this.schedulerThread.start();
    }

    /**
     * 注册过期任务（在put时调用）
     */
    public void schedule(BoundedLocalCache.Node<K, V> node, long delayMs) {
        long expirationTime = currentTime + delayMs;
        node.setExpirationTime(expirationTime);

        // 计算层级：从高层向低层找合适的槽位
        for (int level = wheels.size() - 1; level >= 0; level--) {
            if (delayMs >= TICK_MS[level]) {
                int slot = calculateSlot(expirationTime, level);
                wheels.get(level)[slot].add(node);
                return;
            }
        }
        // 小于1秒，放入秒级轮
        wheels.get(0)[calculateSlot(expirationTime, 0)].add(node);
    }

    /**
     * 取消任务（在remove时调用）
     */
    public void cancel(BoundedLocalCache.Node<K, V> node) {
        if (node.getPrevInTimerWheel() != null || node.getNextInTimerWheel() != null) {
            node.detachFromTimerWheel();
        }
    }

    /**
     * 调度器主循环（每秒推进）
     */
    private void runScheduler() {
        while (running.get()) {
            long start = System.currentTimeMillis();

            advanceClock();

            // 精确睡眠到下一秒（避免漂移）
            long sleepMs = 1000 - (System.currentTimeMillis() - start);
            if (sleepMs > 0) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(sleepMs));
            }
        }
    }

    /**
     * 推进时间（核心逻辑）
     */
    private void advanceClock() {
        currentTime += 1000;

        // 1. 处理秒级轮当前槽位
        int secondSlot = calculateSlot(currentTime, 0);
        List<BoundedLocalCache.Node<K, V>> expired = wheels.get(0)[secondSlot].clearAndGet();

        for (BoundedLocalCache.Node<K, V> node : expired) {
            // 检查是否真的过期（处理降级过程中的精度问题）
            if (node.getExpirationTime() <= currentTime) {
                callback.onExpired(node.getKey()); // 回调BoundedLocalCache.remove
            } else {
                // 未过期，重新调度（通常是降级到更低层级）
                reschedule(node);
            }
        }

        // 2. 处理层级晋升（每60秒触发一次分钟轮检查）
        if (secondSlot == 0) {
            promoteLevel(1); // 从分钟轮降级
        }
    }

    /**
     * 从高层级向低层级降级任务
     */
    private void promoteLevel(int level) {
        if (level >= wheels.size()) return;

        int slot = calculateSlot(currentTime, level);
        List<BoundedLocalCache.Node<K, V>> nodes = wheels.get(level)[slot].clearAndGet();

        for (BoundedLocalCache.Node<K, V> node : nodes) {
            long remaining = node.getExpirationTime() - currentTime;

            if (remaining < TICK_MS[level - 1]) {
                // 降级到下一层（如从分钟轮降到秒轮）
                int targetSlot = calculateSlot(node.getExpirationTime(), level - 1);
                wheels.get(level - 1)[targetSlot].add(node);
            } else {
                // 还在当前层级有效范围，重新放入当前轮
                int targetSlot = calculateSlot(node.getExpirationTime(), level);
                wheels.get(level)[targetSlot].add(node);
            }
        }

        // 递归检查更高层级（如分钟轮走完一圈，检查小时轮）
        if (slot == 0 && level + 1 < wheels.size()) {
            promoteLevel(level + 1);
        }
    }

    private void reschedule(BoundedLocalCache.Node<K, V> node) {
        long delay = node.getExpirationTime() - currentTime;
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

    /**
     * 时间轮槽位（双向链表管理）
     */
    private static class TimerBucket<K, V> {
        private final BoundedLocalCache.Node<K, V> sentinel; // 哨兵节点

        TimerBucket() {
            sentinel = new BoundedLocalCache.Node<>(null, null, 0);
            sentinel.setNextInTimerWheel(sentinel);
            sentinel.setPrevInTimerWheel(sentinel);
        }

        void add(BoundedLocalCache.Node<K, V> node) {
            // 头插法（O(1)）
            BoundedLocalCache.Node<K, V> next = sentinel.getNextInTimerWheel();
            node.setPrevInTimerWheel(sentinel);
            node.setNextInTimerWheel(next);
            sentinel.setNextInTimerWheel(node);
            next.setPrevInTimerWheel(node);
        }

        List<BoundedLocalCache.Node<K, V>> clearAndGet() {
            List<BoundedLocalCache.Node<K, V>> list = new ArrayList<>();
            BoundedLocalCache.Node<K, V> current = sentinel.getNextInTimerWheel();

            while (current != sentinel) {
                list.add(current);
                current = current.getNextInTimerWheel();
            }

            // 重置链表
            sentinel.setNextInTimerWheel(sentinel);
            sentinel.setPrevInTimerWheel(sentinel);
            return list;
        }
    }
}