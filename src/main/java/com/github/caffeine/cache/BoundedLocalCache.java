package com.github.caffeine.cache;

import com.github.caffeine.CacheUtils;
import com.github.caffeine.cache.concurrent.AsyncRemovalProcessor;
import com.github.caffeine.cache.concurrent.CacheEventRingBuffer;
import com.github.caffeine.cache.event.CacheEvent;
import com.github.caffeine.cache.event.CacheEventType;
import com.github.caffeine.cache.reference.ManualReference;
import com.github.caffeine.cache.reference.ManualReferenceQueue;
import com.github.caffeine.cache.reference.ReferenceStrength;
import jdk.internal.vm.annotation.Contended;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public class BoundedLocalCache<K, V> implements Cache<K, V> {
    private final LocalCacheSegment<K, V>[] segments;
    private final int segmentMask;
    private final Caffeine<K, V> builder;
    private final HierarchicalTimerWheel<K, V> timerWheel;

    // 新增：引用队列（仅当使用弱/软引用时初始化）
    private final ManualReferenceQueue<V> referenceQueue;
    private final boolean usesReferences;

    // === 统计字段：使用 @Contended 避免伪共享 ===
    // LongAdder 已经很好地分散了竞争，但我们可以为极端高并发增加缓存行填充
    @Contended
    private final LongAdder hitCount = new LongAdder();

    @Contended
    private final LongAdder missCount = new LongAdder();

    @Contended
    private final LongAdder expireCount = new LongAdder();

    @Contended
    private final LongAdder collectedCount = new LongAdder();

    // 内存敏感驱逐器（仅当使用Soft引用时启用）
    private final MemorySensitiveEvictor memoryEvictor;
    private final boolean memorySensitive;

    // 新增：分片锁
    private final StripedLock stripedLock;

    // 测试时使用监控版
    private InstrumentedStripedLock instrumentedLock;

    // 缓存时间戳，每 100ms 更新一次（类似 Caffeine 的 ticker）
    private volatile long cachedTime;

    // 新增：事件系统（可选，仅在配置了监听器时初始化）
    private final CacheEventRingBuffer<K, V> eventBuffer;
    private final Consumer<CacheEvent<K, V>> removalListener;
    private final Consumer<CacheEvent<K, V>> statsListener;
    private final boolean enableEvents;

    // 新增：异步RemovalListener处理器（可选）
    private final AsyncRemovalProcessor<K, V> asyncRemovalProcessor;
    private final boolean asyncRemovalEnabled;

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

        // 初始化事件系统
        this.removalListener = builder.getRemovalListener();
        this.statsListener = builder.getStatsListener();
        this.enableEvents = (removalListener != null || statsListener != null);

        if (enableEvents) {
            // 创建消费者数组
            int consumerCount = 0;
            if (removalListener != null) consumerCount++;
            if (statsListener != null) consumerCount++;

            @SuppressWarnings("unchecked")
            Consumer<CacheEvent<K, V>>[] consumers = new Consumer[consumerCount];
            int idx = 0;
            if (removalListener != null) consumers[idx++] = removalListener;
            if (statsListener != null) consumers[idx] = statsListener;

            // RingBuffer大小：65536（2^16），足够缓存短时间爆发的事件
            this.eventBuffer = new CacheEventRingBuffer<>(
                    65536,
                    true,  // 队列满时丢弃非关键事件（如READ）
                    consumers
            );
        } else {
            this.eventBuffer = null;
        }

        // ===== 初始化异步处理器 =====
        if (builder.isAsyncRemovalEnabled() && removalListener != null) {
            this.asyncRemovalEnabled = true;
            // 包装为批量消费者
            this.asyncRemovalProcessor = new AsyncRemovalProcessor<>(
                    builder.getAsyncBufferSize(),      // 默认65536
                    builder.getAsyncBatchSize(),       // 默认100
                    builder.getAsyncFlushIntervalMs(), // 默认50ms
                    events -> {
                        // 批量回调给原始RemovalListener
                        events.forEach(removalListener::accept);
                    }
            );
            this.asyncRemovalProcessor.start();
        } else {
            this.asyncRemovalEnabled = false;
            this.asyncRemovalProcessor = null;
        }
        // ======================================

        // 初始化引用队列（如果启用弱/软引用）
        if (usesReferences) {
            this.referenceQueue = new ManualReferenceQueue<>();
        } else {
            this.referenceQueue = null;
        }

        // 改为1024个Segment，与StripedLock一致
        int segmentCount = 1024;
        this.segmentMask = segmentCount - 1;
        this.segments = new LocalCacheSegment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new LocalCacheSegment<>();
        }

        // StripedLock 仅用于需要全局锁的场景（如 invalidateAll）
        this.stripedLock = new StripedLock(segmentCount); // 用于全局遍历保护

        // 时间轮初始化
        this.timerWheel = (builder.expiresAfterWrite() || builder.expiresAfterAccess())
                ? new HierarchicalTimerWheel<>(this::onTimerExpired)
                : null;

        // 关键：确保维护线程启动（合并引用队列+内存检查）
        if (usesReferences) {
            startMaintenanceThread();
        }

        // 启动时间缓存线程
        if (builder.expiresAfterWrite() || builder.expiresAfterAccess()) {
            Thread timeTicker = new Thread(() -> {
                while (!Thread.interrupted()) {
                    cachedTime = System.currentTimeMillis();
                    LockSupport.parkNanos(100_000_000);  // 100ms
                }
            }, "cache-time-ticker");
            timeTicker.setDaemon(true);
            timeTicker.start();
        }
    }

    /**
     * 发布事件 - 整合异步RemovalListener逻辑
     * 关键修改：Removal事件优先走异步通道，其他事件走原有eventBuffer
     */
    private void publishEvent(CacheEventType type, K key, V value) {

        // 只有Removal相关事件（EVICT/EXPIRE/REMOVE/COLLECTED）走异步通道
        boolean isRemovalEvent = (type == CacheEventType.EVICT ||
                type == CacheEventType.EXPIRE ||
                type == CacheEventType.REMOVE ||
                type == CacheEventType.COLLECTED);

        // 1. 如果启用了异步Removal且是Removal事件
        if (asyncRemovalEnabled && isRemovalEvent && asyncRemovalProcessor != null) {
            CacheEvent<K, V> event = CacheEvent.<K, V>builder()
                    .type(type)
                    .key(key)
                    .value(value)
                    .build();

            // 尝试异步发布（非阻塞）
            boolean success = asyncRemovalProcessor.publish(event);

            if (!success) {
                // 背压回退：同步执行（避免事件丢失）
                asyncRemovalProcessor.fallbackProcess(event);
            }
            return; // 异步路径完成，不再走原有eventBuffer
        }

        // 2. 原有逻辑：同步Removal或其他事件（STATS等）
        if (removalListener != null && isRemovalEvent) {
            // 同步模式
            CacheEvent<K, V> event = CacheEvent.<K, V>builder()
                    .type(type)
                    .key(key)
                    .value(value)
                    .build();
            removalListener.accept(event);
        }

        if (!enableEvents || eventBuffer == null || isRemovalEvent) return;

        // 只有驱逐、过期、删除事件强制确保送达，其他事件可能丢弃
        CacheEvent<K, V> event = CacheEvent.<K, V>builder()
                .type(type)
                .key(key)
                .value(value)
                .build();

        eventBuffer.publish(event);
    }

    /**
     * 清理所有Soft引用条目（内存不足时调用）
     * 修改：添加全局锁保护
     */
    private void cleanupSoftReferences() {
        if (!usesReferences) return;
        int cleaned = 0;
        stripedLock.lockAll();
        try {
            for (LocalCacheSegment<K, V> segment : segments) {
                var iterator = segment.getMap().entrySet().iterator();
                while (iterator.hasNext()) {
                    var entry = iterator.next();
                    Node<K, V> node = entry.getValue();
                    if (node.getValueStrength() == ReferenceStrength.SOFT) {
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
                        cleaned++;
                    }
                }
            }
        } finally {
            stripedLock.unlockAll();
        }
        if (cleaned > 0) {
            System.out.println("[Memory] 紧急清理Soft引用: " + cleaned + " 条");
        }
    }

    /**
     * 测试专用：启用锁冲突监控
     */
    public void enableContentionMonitoring() {
        if (stripedLock instanceof InstrumentedStripedLock) {
            this.instrumentedLock = (InstrumentedStripedLock) stripedLock;
        } else {
            // 运行时替换为监控版（仅测试使用）
            this.instrumentedLock = new InstrumentedStripedLock(64);
            // 注意：实际应重构为构造时注入，此处简化
        }
    }

    public double getContentionRate() {
        return instrumentedLock != null ?
                instrumentedLock.getContentionRate() : -1;
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
     * 清理被回收的引用对应的节点 - 修复版
     * 直接使用写锁，避免乐观读转换的复杂性
     */
    private void cleanupCollectedReference(ManualReference<V> ref) {
        K key = ref.getKey();
        if (key == null) return;

        LocalCacheSegment<K, V> segment = segmentFor(key);

        // 直接使用写锁，确保原子性检查+删除
        long stamp = segment.getLock().writeLock();
        try {
            // 在锁内重新获取节点，确保一致性
            Node<K, V> node = segment.getMap().get(key);

            // 双重检查：确保节点存在且引用匹配（防止误删）
            if (node != null && node.getValueReference() == ref) {
                // 取消时间轮
                if (timerWheel != null) {
                    synchronized (node) {
                        timerWheel.cancel(node);
                    }
                }

                // 移除节点
                segment.getMap().remove(key);
                collectedCount.increment();

                // 发布事件
                publishEvent(CacheEventType.COLLECTED, key, null);
            }
        } finally {
            segment.getLock().unlockWrite(stamp);
        }
    }

    private LocalCacheSegment<K, V> segmentFor(K key) {
        int hash = CacheUtils.spread(key.hashCode());
        return segments[hash & segmentMask];
    }

    /**
     * 读操作：完全无锁（Node 内部使用 VarHandle）
     */
    @Override
    public V getIfPresent(K key) {
        LocalCacheSegment<K, V> segment = segmentFor(key);
        Node<K, V> node = segment.getNode(key);

        if (node == null) {
            missCount.increment();
            return null;
        }

        // 检查引用是否被清理（无锁读）
        if (node.isValueCollected()) {
            segment.removeNode(key);
            collectedCount.increment();
            missCount.increment();
            return null;
        }

        // 使用缓存时间而非实时时间（允许 100ms 误差）
        long now = cachedTime;

        if (isExpired(node, now)) {
            if (segment.removeNode(key, node)) {
                expireCount.increment();
                if (timerWheel != null) {
                    synchronized (node) {  // 时间轮操作仍需同步
                        timerWheel.cancel(node);
                    }
                }
            }
            missCount.increment();
            return null;
        }

        // 处理 expireAfterAccess：使用 VarHandle lazySet 更新时间戳
        if (builder.expiresAfterAccess()) {
            long newExpireAt = now + TimeUnit.NANOSECONDS.toMillis(builder.expireAfterAccessNanos);
            node.setAccessTime(now);        // setRelease
            node.setExpireAt(newExpireAt);  // setRelease
            reschedule(node, newExpireAt);
        }

        if (!isExpired(node, now) && !node.isValueCollected()) {
            hitCount.increment();
            // 可选：发布READ事件（注意性能影响）
            // publishEvent(CacheEventType.READ, key, node.getValue());
            return node.getValue();
        }

        missCount.increment();
        return null;
    }

    /**
     * 写操作：使用 Node.setValue（内部 setRelease）
     */
    @Override
    public void put(K key, V value) {
        LocalCacheSegment<K, V> segment = segmentFor(key);
        long now = System.currentTimeMillis();
        long expireAt = calculateExpireAt(now);
        ReferenceStrength strength = builder.valueStrength();

        // 创建节点（构造函数内部使用 setRelease）
        Node<K, V> newNode = new Node<>(key, value, now, expireAt, strength,
                usesReferences ? referenceQueue : null);

        Node<K, V> oldNode = segment.putNode(key, newNode);

        if (timerWheel != null) {
            if (oldNode != null) {
                synchronized (oldNode) {
                    timerWheel.cancel(oldNode);
                    ManualReference<V> oldRef = oldNode.getValueReference();
                    if (oldRef != null) oldRef.clear();
                }
            }
            if (expireAt > 0) {
                long delayMs = expireAt - now;
                synchronized (newNode) {
                    timerWheel.schedule(newNode, Math.max(1, delayMs));
                }
            }
        }

        // 发布写入事件
        publishEvent(CacheEventType.WRITE, key, value);
    }

    @Override
    public void invalidate(K key) {
        LocalCacheSegment<K, V> segment = segmentFor(key);
        Node<K, V> node = segment.removeNode(key);

        if (node != null) {
            V value = node.getValue(); // 获取值用于事件通知

            if (timerWheel != null) {
                synchronized (node) {
                    timerWheel.cancel(node);
                }
            }

            // 如果是引用类型，主动清理引用
            ManualReference<V> ref = node.getValueReference();
            if (ref != null) { ref.clear(); }

            // 发布移除事件（显式删除）
            publishEvent(CacheEventType.REMOVE, key, value);
        }
    }

    private void onTimerExpired(K key) {
        LocalCacheSegment<K, V> segment = segmentFor(key);
        Node<K, V> node = segment.getNode(key);
        if (node == null) return;

        long now = System.currentTimeMillis();
        if (isExpired(node, now)) {
            V value = node.getValue();
            if (segment.removeNode(key, node)) {
                expireCount.increment();

                // 清理引用
                ManualReference<V> ref = node.getValueReference();
                if (ref != null) ref.clear();

                // 发布过期事件
                publishEvent(CacheEventType.EXPIRE, key, value);
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
        // 收集所有条目用于事件通知（在清理前）
        if (enableEvents) {
            List<CacheEvent<K, V>> events = new ArrayList<>();
            long now = System.currentTimeMillis();

            stripedLock.lockAll();
            try {
                for (LocalCacheSegment<K, V> segment : segments) {
                    segment.getMap().forEach((k, node) -> {
                        if (!isExpired(node, now) && !node.isValueCollected()) {
                            V value = node.getValue();
                            if (value != null) {
                                events.add(CacheEvent.<K, V>builder()
                                        .type(CacheEventType.REMOVE)
                                        .key(k)
                                        .value(value)
                                        .build());
                            }
                        }
                    });
                }
            } finally {
                stripedLock.unlockAll();
            }

            // 发布事件（批量）
            events.forEach(e -> publishEvent(e.getType(), e.getKey(), e.getValue()));
        }

        // 执行实际清理
        for (LocalCacheSegment<K, V> segment : segments) {
            segment.processSafelyWrite(map -> {
                map.values().forEach(node -> {
                    if (timerWheel != null) {
                        synchronized (node) { timerWheel.cancel(node); }
                    }
                    ManualReference<V> ref = node.getValueReference();
                    if (ref != null) ref.clear();
                });
                map.clear();
            });
        }
    }

    // estimatedSize 不需要全局锁，各Segment size()近似即可
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

        // 关键：全局锁保护遍历
        stripedLock.lockAll();
        try {
            for (LocalCacheSegment<K, V> segment : segments) {
                segment.getMap().forEach((k, node) -> {
                    if (!isExpired(node, now) && !node.isValueCollected()) {
                        V value = node.getValue();
                        if (value != null) {
                            map.put(k, value);
                        }
                    }
                });
            }
        } finally {
            stripedLock.unlockAll();
        }
        return map;
    }

    @Override
    public CacheStats stats() {
        return new CacheStats(hitCount.sum(), missCount.sum(), expireCount.sum(), collectedCount.sum());
    }

    public void shutdown() {
        // 等待事件处理完成（简单实现：休眠一段时间）
        if (enableEvents) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 关键：等待异步RemovalProcessor完成
        if (asyncRemovalProcessor != null) {
            System.out.println("[Shutdown] 等待异步RemovalProcessor完成，已处理: " +
                    asyncRemovalProcessor.getProcessedCount());
            asyncRemovalProcessor.shutdown();
        }

        if (timerWheel != null) timerWheel.shutdown();
        if (memorySensitive) cleanupSoftReferences();
    }
}