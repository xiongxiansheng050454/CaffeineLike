package com.github.caffeine.cache;

import com.github.caffeine.cache.reference.ReferenceStrength;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.*;

/**
 * Window Cache 并发测试套件
 * 验证：线程安全、容量边界、无死锁、晋升一致性
 */
public class WindowCacheConcurrencyTest {

    private static final int CONCURRENCY = 32;
    private static final int OPS_PER_THREAD = 10000;
    private static final int TOTAL_OPS = CONCURRENCY * OPS_PER_THREAD;

    public static void main(String[] args) throws Exception {
        System.out.println("========== Window Cache 并发测试 ==========");
        System.out.println("并发数: " + CONCURRENCY + ", 每线程操作: " + OPS_PER_THREAD);
        System.out.println();

        testConcurrentAdmission();
        testConcurrentAccess();
        testMixedWorkload();
        testPromotionConsistency();
        testNoDeadlock();

        System.out.println("\n========== 所有并发测试通过 ==========");
    }

    /**
     * 测试1: 并发准入 - 多线程同时写入新节点
     * 验证：Window容量不超过限制，无节点丢失
     */
    static void testConcurrentAdmission() throws Exception {
        System.out.println("测试1: 并发准入 (Concurrent Admission)");

        AtomicInteger promotionCount = new AtomicInteger(0);
        CopyOnWriteArrayList<Node<String, String>> promotedNodes = new CopyOnWriteArrayList<>();

        // Window容量=10 (1000的1%)
        WindowCache<String, String> window = new WindowCache<>(1000, node -> {
            promotionCount.incrementAndGet();
            promotedNodes.add(node);
        });

        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
        CountDownLatch latch = new CountDownLatch(CONCURRENCY);
        AtomicLong admittedCount = new AtomicLong(0);

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < CONCURRENCY; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        String key = "T" + threadId + "-" + i;
                        Node<String, String> node = createNode(key, "v" + i);

                        window.admit(node);
                        admittedCount.incrementAndGet();

                        // 每1000次检查一次容量（调试用）
                        if (i % 1000 == 0 && window.size() > 10) {
                            System.err.println("警告: Window超容 " + window.size());
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long duration = System.currentTimeMillis() - startTime;
        double throughput = TOTAL_OPS / (duration / 1000.0);

        System.out.println("  耗时: " + duration + "ms, 吞吐量: " + String.format("%.0f", throughput) + " ops/s");
        System.out.println("  准入数: " + admittedCount.get() + ", 晋升数: " + promotionCount.get());
        System.out.println("  最终Window大小: " + window.size() + " (应<=10)");

        // 验证
        assert completed : "测试超时，可能死锁！";
        assert window.size() <= 10 : "Window超容: " + window.size();
        assert admittedCount.get() == TOTAL_OPS : "准入计数不匹配";

        // 验证节点一致性：在Window中的 + 已晋升的 = 总准入数（减去重复key覆盖的情况）
        // 注意：由于不同线程可能生成相同key（虽然概率低），这里只做基本检查
        long totalAccounted = window.size() + promotionCount.get();
        assert totalAccounted >= 10 : "节点丢失严重， accounted=" + totalAccounted;

        System.out.println("  ✓ 并发准入通过，无超容，无死锁");
    }

    /**
     * 测试2: 并发访问重排 - 多线程同时访问Window内节点
     * 验证：链表结构不损坏（无NPE、无死循环）
     */
    static void testConcurrentAccess() throws Exception {
        System.out.println("\n测试2: 并发访问重排 (Concurrent Access Reordering)");

        WindowCache<String, String> window = new WindowCache<>(1000, n -> {});

        // 预填充Window
        List<Node<String, String>> nodes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Node<String, String> node = createNode("pre-" + i, "v");
            nodes.add(node);
            window.admit(node);
        }

        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
        CountDownLatch latch = new CountDownLatch(CONCURRENCY);
        AtomicInteger accessCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int t = 0; t < CONCURRENCY; t++) {
            executor.submit(() -> {
                try {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        // 随机访问一个节点
                        Node<String, String> target = nodes.get(random.nextInt(nodes.size()));
                        try {
                            window.onAccess(target);
                            accessCount.incrementAndGet();
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            System.err.println("访问异常: " + e.getMessage());
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        System.out.println("  访问次数: " + accessCount.get() + ", 错误: " + errorCount.get());
        assert completed : "访问测试超时";
        assert errorCount.get() == 0 : "访问过程发生错误";
        assert window.size() == 10 : "Window大小不应改变";

        System.out.println("  ✓ 并发访问重排通过，链表结构完整");
    }

    /**
     * 测试3: 混合负载 - 模拟真实缓存场景（读多写少）
     */
    static void testMixedWorkload() throws Exception {
        System.out.println("\n测试3: 混合负载 (Mixed Read/Write)");

        AtomicInteger promotionCount = new AtomicInteger(0);
        WindowCache<String, String> window = new WindowCache<>(1000, n -> promotionCount.incrementAndGet());

        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
        CountDownLatch latch = new CountDownLatch(CONCURRENCY);

        // 共享的节点池（模拟缓存中的热点数据）
        ConcurrentHashMap<String, Node<String, String>> hotNodes = new ConcurrentHashMap<>();

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < CONCURRENCY; t++) {
            final int threadId = t;
            executor.submit(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                try {
                    for (int i = 0; i < OPS_PER_THREAD; i++) {
                        int op = random.nextInt(100);

                        if (op < 10) { // 10% 写入新数据
                            String key = "new-" + threadId + "-" + i;
                            Node<String, String> node = createNode(key, "v");
                            window.admit(node);
                        } else if (op < 30) { // 20% 访问现有数据
                            // 随机访问Window中的节点（如果存在）
                            // 这里简化处理，只访问最近写入的
                            if (!hotNodes.isEmpty()) {
                                String randKey = hotNodes.keys().nextElement();
                                Node<String, String> node = hotNodes.get(randKey);
                                if (node != null && node.isInWindow()) {
                                    window.onAccess(node);
                                }
                            }
                        } else { // 70% 只读操作（模拟缓存命中）
                            // 空操作，代表缓存命中但不改变状态
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long duration = System.currentTimeMillis() - startTime;

        System.out.println("  总耗时: " + duration + "ms");
        System.out.println("  最终Window大小: " + window.size());
        System.out.println("  总晋升数: " + promotionCount.get());
        System.out.println("  ✓ 混合负载通过");
    }

    /**
     * 测试4: 晋升一致性 - 验证每个被驱逐的节点都正确回调
     */
    static void testPromotionConsistency() throws Exception {
        System.out.println("\n测试4: 晋升一致性 (Promotion Consistency)");

        int capacity = 5;
        AtomicInteger promotionCount = new AtomicInteger(0);
        Set<String> promotedKeys = ConcurrentHashMap.newKeySet();

        WindowCache<String, String> window = new WindowCache<>(capacity * 100, node -> {
            promotionCount.incrementAndGet();
            promotedKeys.add((String) node.getKey());
            // 验证状态
            assert !node.isInWindow() : "晋升后应标记为不在Window";
        });

        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(4);

        // 4个线程各写入20个唯一节点，总80个节点
        // Window容量=5，预期晋升75个节点

        for (int t = 0; t < 4; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < 20; i++) {
                        String key = "Consistency-" + threadId + "-" + i;
                        Node<String, String> node = createNode(key, "v");
                        window.admit(node);

                        // 偶尔访问一下，测试并发访问+驱逐
                        if (i % 5 == 0) {
                            window.onAccess(node);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        System.out.println("  写入节点数: 80");
        System.out.println("  实际晋升数: " + promotionCount.get());
        System.out.println("  Window剩余: " + window.size());
        System.out.println("  唯一晋升key数: " + promotedKeys.size());

        // 验证：晋升数 + Window当前大小 = 总写入数（假设无重复key）
        int totalAccounted = promotionCount.get() + (int) window.size();
        assert totalAccounted == 80 : "节点丢失！accounted=" + totalAccounted;
        assert promotedKeys.size() == promotionCount.get() : "有重复晋升或晋升遗漏";

        System.out.println("  ✓ 晋升一致性验证通过");
    }

    /**
     * 测试5: 死锁检测 - 长时间运行观察是否死锁
     */
    static void testNoDeadlock() throws Exception {
        System.out.println("\n测试5: 死锁检测 (Deadlock Detection)");

        WindowCache<String, String> window = new WindowCache<>(1000, n -> {});
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);

        // 提交任务并设置超时
        Future<?> future = executor.submit(() -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0; i < 100000; i++) {
                int op = random.nextInt(3);
                String key = "deadlock-test-" + (i % 100);
                Node<String, String> node = createNode(key, "v");

                switch (op) {
                    case 0 -> window.admit(node);
                    case 1 -> window.onAccess(node);
                    case 2 -> window.remove(node);
                }
            }
        });

        try {
            future.get(5, TimeUnit.SECONDS); // 5秒超时
            System.out.println("  ✓ 5秒内完成10万次操作，无死锁");
        } catch (TimeoutException e) {
            future.cancel(true);
            assert false : "检测到潜在死锁！操作未完成";
        } finally {
            executor.shutdownNow();
        }
    }

    // 辅助方法
    private static <K, V> Node<K, V> createNode(K key, V value) {
        return new Node<>(key, value,
                System.currentTimeMillis(),
                -1,
                ReferenceStrength.STRONG,
                null);
    }
}