package com.github.caffeine.cache;

import com.github.caffeine.CacheUtils;
import com.github.caffeine.cache.reference.ManualReference;
import com.github.caffeine.cache.reference.ManualReferenceQueue;
import com.github.caffeine.cache.reference.ReferenceStrength;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class BoundedLocalCache<K, V> implements Cache<K, V> {
    private final LocalCacheSegment<K, V>[] segments;
    private final int segmentMask;
    private final Caffeine<K, V> builder;
    private final HierarchicalTimerWheel<K, V> timerWheel;

    // 新增：引用队列（仅当使用弱/软引用时初始化）
    private final ManualReferenceQueue<V> referenceQueue;
    private final boolean usesReferences;

    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder expireCount = new LongAdder();
    private final LongAdder collectedCount = new LongAdder();  // 新增：统计被 GC 回收的数量

    // 内存敏感驱逐器（仅当使用Soft引用时启用）
    private final MemorySensitiveEvictor memoryEvictor;
    private final boolean memorySensitive;

    @SuppressWarnings("unchecked")
    public BoundedLocalCache(Caffeine<K, V> builder) {
        this.builder = builder;
        this.usesReferences = builder.valueStrength() != ReferenceStrength.STRONG;

        // 初始化内存敏感层（仅Soft引用模式启用）
        this.memorySensitive = usesReferences && builder.isMemorySensitive();
        if (memorySensitive) {
            this.memoryEvictor = new MemorySensitiveEvictor(0.75, 0.85);
            // 注册监听器：当内存不足时清理Soft引用
            this.memoryEvictor.register(this::cleanupSoftReferences);
        } else {
            this.memoryEvictor = null;
        }

        // 初始化引用队列（如果启用弱/软引用）
        if (usesReferences) {
            this.referenceQueue = new ManualReferenceQueue<>();
        } else {
            this.referenceQueue = null;
        }

        // 分片初始化（保持原有逻辑）
        int concurrencyLevel = Runtime.getRuntime().availableProcessors();
        int segmentCount = CacheUtils.tableSizeFor(concurrencyLevel);
        this.segmentMask = segmentCount - 1;

        int totalCapacity = builder.initialCapacity > 0 ? builder.initialCapacity : 16;
        int segmentCapacity = Math.max(1, totalCapacity / segmentCount);

        this.segments = new LocalCacheSegment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new LocalCacheSegment<>(segmentCapacity);
        }

        // 时间轮初始化
        if (builder.expiresAfterWrite() || builder.expiresAfterAccess()) {
            this.timerWheel = new HierarchicalTimerWheel<>(this::onTimerExpired);
        } else {
            this.timerWheel = null;
        }

        // 关键：确保维护线程启动（合并引用队列+内存检查）
        boolean needMaintenance =usesReferences || memorySensitive;
        if (needMaintenance) {
            startMaintenanceThread();
        }
    }

    /**
     * 清理所有Soft引用条目（内存不足时调用）
     * 直接移除节点，立即释放内存压力
     */
    private void cleanupSoftReferences() {
        if (!usesReferences) return;

        int cleaned = 0;
        for (LocalCacheSegment<K, V> segment : segments) {
            // 使用迭代器安全删除
            var iterator = segment.getMap().entrySet().iterator();
            while (iterator.hasNext()) {
                var entry = iterator.next();
                Node<K, V> node = entry.getValue();

                if (node.getValueStrength() == ReferenceStrength.SOFT) {
                    // 1. 取消时间轮任务
                    if (timerWheel != null) {
                        synchronized (node) {
                            timerWheel.cancel(node);
                        }
                    }

                    // 2. 清理引用（标记为已清理并入队，供后续统计）
                    ManualReference<V> ref = node.getValueReference();
                    if (ref != null && !ref.isCleared()) {
                        ref.clear();
                    }

                    // 3. 立即从Map中移除（关键！）
                    iterator.remove();
                    cleaned++;
                }
            }
        }

        if (cleaned > 0) {
            System.out.println("[Memory] 紧急清理Soft引用: " + cleaned + " 条");
            // 注意：这里清理是内存压力导致的，不是GC回收，所以不计入collectedCount
            // 如需统计驱逐次数，可新增 evictionCount 字段
        }
    }

    /**
     * 渐进式清理：仅清理部分Soft引用（警告级别使用）
     */
    private void cleanupPartialSoftReferences() {
        int limit = 50; // 每次最多清理50条，避免阻塞维护线程
        int count = 0;

        for (LocalCacheSegment<K, V> segment : segments) {
            var iterator = segment.getMap().entrySet().iterator();
            while (iterator.hasNext() && count < limit) {
                var entry = iterator.next();
                Node<K, V> node = entry.getValue();

                if (node.getValueStrength() == ReferenceStrength.SOFT) {
                    // 可选策略：优先清理最久未访问的（这里简化随机清理）
                    if (timerWheel != null) {
                        synchronized (node) {
                            timerWheel.cancel(node);
                        }
                    }

                    ManualReference<V> ref = node.getValueReference();
                    if (ref != null && !ref.isCleared()) {
                        ref.clear();
                    }

                    iterator.remove();
                    count++;
                }
            }
            if (count >= limit) break;
        }

        if (count > 0) {
            System.out.println("[Memory] 警告级别清理Soft引用: " + count + " 条");
        }
    }

    /**
     * 手动触发内存压力清理（仅用于测试验证）
     */
    public void simulateMemoryPressure() {
        if (memorySensitive) {
            System.out.println("[Manual] 模拟内存压力清理");
            cleanupSoftReferences();
        }
    }

    /**
     * 维护线程：合并时间轮推进和内存检查
     */
    private void startMaintenanceThread() {
        Thread maintenance = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // 处理引用队列（现有逻辑）
                    if (usesReferences) {
                        drainReferenceQueue();
                    }

                    // 内存压力检查（新增）
                    if (memorySensitive) {
                        int pressure = memoryEvictor.checkMemoryPressure();
                        if (pressure == 2) { // 紧急
                            memoryEvictor.tryEmergencyCleanup(this::cleanupSoftReferences);
                        } else if (pressure == 1) { // 警告
                            // 可以在这里预热清理部分Soft引用
                            cleanupPartialSoftReferences();
                        }
                    }

                    Thread.sleep(100); // 100ms维护周期
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "cache-maintenance");
        maintenance.setDaemon(true);
        maintenance.start();
    }

    /**
     * 批量处理引用队列（从processReferenceQueue重构）
     */
    private void drainReferenceQueue() {
        ManualReference<V> ref;
        while ((ref = referenceQueue.poll()) != null) {
            cleanupCollectedReference(ref);
        }
    }

    /**
     * 清理被回收的引用对应的节点
     * 注意：实际 Caffeine 通过 Node 反向指针优化，这里简化处理
     */
    private void cleanupCollectedReference(ManualReference<V> ref) {
        // 遍历所有 segment 查找持有该引用的节点
        // 优化：在真实场景中，Node 应持有 key 以便直接定位
        for (LocalCacheSegment<K, V> segment : segments) {
            segment.getMap().values().removeIf(node -> {
                if (node.getValueReference() == ref) {
                    // 从时间轮中取消（如果存在）
                    if (timerWheel != null) {
                        synchronized (node) {
                            timerWheel.cancel(node);
                        }
                    }
                    collectedCount.increment();
                    return true;
                }
                return false;
            });
        }
    }

    private LocalCacheSegment<K, V> segmentFor(K key) {
        int hash = CacheUtils.spread(key.hashCode());
        return segments[hash & segmentMask];
    }

    @Override
    public V getIfPresent(K key) {
        LocalCacheSegment<K, V> segment = segmentFor(key);
        Node<K, V> node = segment.getNode(key);

        if (node == null) {
            missCount.increment();
            return null;
        }

        // 检查引用是否被清理（弱/软引用场景）
        if (node.isValueCollected()) {
            segment.removeNode(key);
            collectedCount.increment();
            missCount.increment();
            return null;
        }

        long now = System.currentTimeMillis();

        if (isExpired(node, now)) {
            if (segment.removeNode(key, node)) {
                expireCount.increment();
                if (timerWheel != null) {
                    synchronized (node) {
                        timerWheel.cancel(node);
                    }
                }
            }
            missCount.increment();
            return null;
        }

        // 处理 expireAfterAccess
        if (builder.expiresAfterAccess()) {
            long newExpireAt = now + TimeUnit.NANOSECONDS.toMillis(builder.expireAfterAccessNanos);
            node.setAccessTime(now);
            node.setExpireAt(newExpireAt);
            reschedule(node, newExpireAt);
        }

        hitCount.increment();
        return node.getValue();
    }

    @Override
    public void put(K key, V value) {
        LocalCacheSegment<K, V> segment = segmentFor(key);
        long now = System.currentTimeMillis();
        long expireAt = calculateExpireAt(now);

        // 根据配置选择引用强度（关键整合点）
        ReferenceStrength strength = builder.valueStrength();

        // 创建节点（传入引用强度）
        Node<K, V> newNode = new Node<>(key, value, now, expireAt, strength,
                usesReferences ? referenceQueue : null);

        // 容量检查（简化版）
        if (builder.evicts() && builder.maximumSize > 0 && estimatedSize() >= builder.maximumSize) {
            // TODO: 实现 W-TinyLFU 淘汰
            return;
        }

        Node<K, V> oldNode = segment.putNode(key, newNode);

        // 处理旧节点的时间轮取消
        if (timerWheel != null) {
            if (oldNode != null) {
                synchronized (oldNode) {
                    timerWheel.cancel(oldNode);
                    // 如果是弱/软引用，触发旧引用的清理（加速 GC）
                    ManualReference<V> oldRef = oldNode.getValueReference();
                    if (oldRef != null) {
                        oldRef.clear();
                    }
                }
            }
            if (expireAt > 0) {
                long delayMs = expireAt - now;
                synchronized (newNode) {
                    timerWheel.schedule(newNode, Math.max(1, delayMs));
                }
            }
        }
    }

    @Override
    public void invalidate(K key) {
        LocalCacheSegment<K, V> segment = segmentFor(key);
        Node<K, V> node = segment.removeNode(key);
        if (node != null) {
            if (timerWheel != null) {
                synchronized (node) {
                    timerWheel.cancel(node);
                }
            }
            // 如果是引用类型，主动清理引用
            ManualReference<V> ref = node.getValueReference();
            if (ref != null) {
                ref.clear();
            }
        }
    }

    private void onTimerExpired(K key) {
        LocalCacheSegment<K, V> segment = segmentFor(key);
        Node<K, V> node = segment.getNode(key);
        if (node == null) return;

        long now = System.currentTimeMillis();
        if (isExpired(node, now)) {
            if (segment.removeNode(key, node)) {
                expireCount.increment();
                // 清理引用
                ManualReference<V> ref = node.getValueReference();
                if (ref != null) ref.clear();
            }
        }
    }

    private boolean isExpired(Node<K, V> node, long now) {
        long expireAt = node.getExpireAt();
        if (expireAt <= 0) return false;
        return now >= expireAt;
    }

    private long calculateExpireAt(long now) {
        long minExpire = Long.MAX_VALUE;

        if (builder.expiresAfterWrite()) {
            minExpire = now + TimeUnit.NANOSECONDS.toMillis(builder.expireAfterWriteNanos);
        }

        if (builder.expiresAfterAccess()) {
            long accessExpire = now + TimeUnit.NANOSECONDS.toMillis(builder.expireAfterAccessNanos);
            minExpire = Math.min(minExpire, accessExpire);
        }

        return minExpire == Long.MAX_VALUE ? -1 : minExpire;
    }

    private void reschedule(Node<K, V> node, long newExpireAt) {
        if (timerWheel == null) return;

        long now = System.currentTimeMillis();
        long delayMs = newExpireAt - now;
        if (delayMs > 0) {
            synchronized (node) {
                timerWheel.cancel(node);
                timerWheel.schedule(node, delayMs);
            }
        }
    }

    @Override
    public void invalidateAll() {
        for (LocalCacheSegment<K, V> segment : segments) {
            segment.getMap().values().forEach(node -> {
                if (timerWheel != null) {
                    synchronized (node) {
                        timerWheel.cancel(node);
                    }
                }
                ManualReference<V> ref = node.getValueReference();
                if (ref != null) ref.clear();
            });
            segment.getMap().clear();
        }
    }

    @Override
    public long estimatedSize() {
        long sum = 0;
        for (LocalCacheSegment<K, V> seg : segments) {
            sum += seg.size();
        }
        return sum;
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>();
        long now = System.currentTimeMillis();

        for (LocalCacheSegment<K, V> segment : segments) {
            segment.getMap().forEach((k, node) -> {
                if (!isExpired(node, now) && !node.isValueCollected()) {
                    V value = node.getValue();
                    if (value != null) {  // 防御性检查（弱引用可能为 null）
                        map.put(k, value);
                    }
                }
            });
        }
        return map;
    }

    @Override
    public CacheStats stats() {
        return new CacheStats(hitCount.sum(), missCount.sum(), expireCount.sum(),collectedCount.sum());
    }

    public void shutdown() {
        if (timerWheel != null) timerWheel.shutdown();
        // 清理Soft引用，避免内存泄漏
        if (memorySensitive) {
            cleanupSoftReferences();
        }
    }
}