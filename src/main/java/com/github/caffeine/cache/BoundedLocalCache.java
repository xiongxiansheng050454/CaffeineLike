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
    private final Thread referenceCleanupThread;
    private final boolean usesReferences;

    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder expireCount = new LongAdder();
    private final LongAdder collectedCount = new LongAdder();  // 新增：统计被 GC 回收的数量

    @SuppressWarnings("unchecked")
    public BoundedLocalCache(Caffeine<K, V> builder) {
        this.builder = builder;
        this.usesReferences = builder.valueStrength() != ReferenceStrength.STRONG;

        // 初始化引用队列（如果启用弱/软引用）
        if (usesReferences) {
            this.referenceQueue = new ManualReferenceQueue<>();
            this.referenceCleanupThread = new Thread(this::processReferenceQueue, "reference-cleanup");
            this.referenceCleanupThread.setDaemon(true);
            this.referenceCleanupThread.start();
        } else {
            this.referenceQueue = null;
            this.referenceCleanupThread = null;
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
    }

    /**
     * 引用队列处理线程：模拟 JVM 的 ReferenceHandler 线程
     * 对应 Caffeine 中检查 ReferenceQueue 并移除失效节点的逻辑
     */
    private void processReferenceQueue() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                ManualReference<V> ref = referenceQueue.poll();
                if (ref != null) {
                    // 引用被 GC 清理，需要从缓存中移除对应节点
                    // 由于 ManualReference 不直接持有 key，我们需要遍历查找（生产环境应优化）
                    cleanupCollectedReference(ref);
                } else {
                    // 无引用时休眠，避免 CPU 空转
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
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

        // 创建新节点（传入 referenceQueue 如果是弱/软引用）
        Node<K, V> newNode = new Node<>(key, value, now, expireAt, strength, referenceQueue);

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
        if (timerWheel != null) {
            timerWheel.shutdown();
        }
        if (referenceCleanupThread != null) {
            referenceCleanupThread.interrupt();
        }
    }
}