package com.github.caffeine.cache;

import java.lang.management.*;
import javax.management.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * JVM内存敏感驱逐器
 * 监听堆内存使用阈值，在OOM前主动清理Soft引用数据
 * 参考Caffeine的异步清理机制设计
 */
public class MemorySensitiveEvictor {
    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private final NotificationEmitter emitter = (NotificationEmitter) memoryMXBean;
    private final AtomicBoolean cleaning = new AtomicBoolean(false);

    private final double emergencyThreshold;  // 紧急清理阈值（默认0.85）
    private final double warningThreshold;    // 警告阈值（默认0.75）

    public MemorySensitiveEvictor(double warningThreshold, double emergencyThreshold) {
        this.warningThreshold = warningThreshold;
        this.emergencyThreshold = emergencyThreshold;
    }

    /**
     * 注册内存监听器，当堆内存超过阈值时触发清理
     * @param softRefCleaner 清理Soft引用条目的回调（通常是BoundedLocalCache的清理方法）
     */
    public void register(Runnable softRefCleaner) {
        // 监听MemoryMXBean通知
        emitter.addNotificationListener((notification, handback) -> {
            String type = notification.getType();
            if (type.equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED) ||
                    type.equals(MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED)) {

                System.out.println("[Memory Alert] 堆内存阈值触发: " + type);
                softRefCleaner.run();
            }
        }, null, null);

        // 设置老年代阈值（如果可用）
        MemoryPoolMXBean oldGen = getOldGenPool();
        if (oldGen != null && oldGen.isUsageThresholdSupported()) {
            long max = oldGen.getUsage().getMax();
            long threshold = (long) (max * emergencyThreshold);
            oldGen.setUsageThreshold(threshold);
            System.out.println("[Memory Monitor] 老年代阈值设置: " + (threshold/1024/1024) + "MB");
        }
    }

    /**
     * 检查内存压力（由维护线程定期调用）
     * @return 压力等级：0=正常, 1=警告, 2=紧急
     */
    public int checkMemoryPressure() {
        MemoryUsage heap = memoryMXBean.getHeapMemoryUsage();
        double ratio = (double) heap.getUsed() / heap.getMax();

        if (ratio > emergencyThreshold) {
            return 2; // 紧急
        } else if (ratio > warningThreshold) {
            return 1; // 警告
        }
        return 0; // 正常
    }

    /**
     * 尝试执行紧急清理（防止OOM）
     * @param emergencyCleanup 清理动作
     * @return 是否执行了清理
     */
    public boolean tryEmergencyCleanup(Runnable emergencyCleanup) {
        if (cleaning.compareAndSet(false, true)) {
            try {
                System.out.println("[Emergency] 内存压力过高，触发Soft引用清理");
                emergencyCleanup.run();

                // 建议JVM进行GC（可选，生产环境慎用）
                System.gc();

                return true;
            } finally {
                cleaning.set(false);
            }
        }
        return false;
    }

    private MemoryPoolMXBean getOldGenPool() {
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP &&
                    (pool.getName().contains("Old") || pool.getName().contains("Tenured"))) {
                return pool;
            }
        }
        return null;
    }
}