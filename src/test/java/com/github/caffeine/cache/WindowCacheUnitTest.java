package com.github.caffeine.cache;

import com.github.caffeine.cache.reference.ReferenceStrength;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Window Cache 单元测试套件 (Java 8兼容版)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class WindowCacheUnitTest {

    private static final int CONCURRENCY = Runtime.getRuntime().availableProcessors() * 2;
    private WindowCache<String, String> windowCache;
    private AtomicInteger promotionCounter;
    private List<Node<String, String>> promotedNodes;
    private ExecutorService executor; // 用于清理

    @BeforeEach
    void setUp() {
        promotionCounter = new AtomicInteger(0);
        promotedNodes = Collections.synchronizedList(new ArrayList<>());
        windowCache = new WindowCache<>(1000, node -> {
            promotionCounter.incrementAndGet();
            promotedNodes.add(node);
        });
    }

    @AfterEach
    void tearDown() {
        if (executor != null && !executor.isShutdown()) {
            executor.shutdownNow();
        }
    }

    // ========== 基础功能测试 ==========

    @Test
    @Order(1)
    @DisplayName("Window容量应为总容量的1%，且至少为1")
    void testCapacityCalculation() {
        WindowCache<String, String> cache1 = new WindowCache<>(1000, n -> {});
        assertEquals(10, cache1.capacity(), "1000的1%应为10");

        WindowCache<String, String> cache2 = new WindowCache<>(50, n -> {});
        assertEquals(1, cache2.capacity(), "不足1时应至少为1");

        WindowCache<String, String> cache3 = new WindowCache<>(0, n -> {});
        assertEquals(1, cache3.capacity(), "0容量也应至少为1");
    }

    @Test
    @Order(2)
    @DisplayName("单线程准入与驱逐：应遵循LRU顺序")
    void testSingleThreadAdmissionAndEviction() {
        WindowCache<String, String> cache = new WindowCache<>(200, n -> {});

        Node<String, String> nodeA = createNode("A", "1");
        Node<String, String> nodeB = createNode("B", "2");
        Node<String, String> nodeC = createNode("C", "3");

        cache.admit(nodeA);
        cache.admit(nodeB);
        assertEquals(2, cache.size());
        assertTrue(nodeA.isInWindow() && nodeB.isInWindow());

        cache.admit(nodeC);
        assertEquals(2, cache.size());
        assertFalse(nodeA.isInWindow(), "A应被驱逐");
        assertTrue(nodeB.isInWindow() && nodeC.isInWindow());
    }

    @Test
    @Order(3)
    @DisplayName("访问重排：访问后节点应变为MRU")
    void testAccessReorder() {
        WindowCache<String, String> cache = new WindowCache<>(200, n -> {});

        Node<String, String> nodeA = createNode("A", "1");
        Node<String, String> nodeB = createNode("B", "2");

        cache.admit(nodeA);
        cache.admit(nodeB);
        cache.onAccess(nodeA); // A变为MRU

        Node<String, String> nodeC = createNode("C", "3");
        cache.admit(nodeC); // 应驱逐B

        assertFalse(nodeB.isInWindow(), "B应被驱逐（现在是LRU）");
        assertTrue(nodeA.isInWindow() && nodeC.isInWindow());
    }

    // ========== 并发测试（Java 8兼容） ==========

    @Test
    @Order(10)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("并发准入：32线程各写入1万节点，应无超容无死锁")
    void testConcurrentAdmission() throws InterruptedException {
        final int threads = CONCURRENCY;
        final int opsPerThread = 10000;

        CountDownLatch latch = new CountDownLatch(threads);
        AtomicLong admittedCount = new AtomicLong(0);

        executor = Executors.newFixedThreadPool(threads);

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "T" + threadId + "-" + i;
                        Node<String, String> node = createNode(key, "v");
                        windowCache.admit(node);
                        admittedCount.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "测试超时，可能死锁");
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "线程池未正常关闭");

        // 验证
        assertEquals(threads * opsPerThread, admittedCount.get(), "准入计数不匹配");
        assertTrue(windowCache.size() <= windowCache.capacity(),
                "Window超容: " + windowCache.size() + " > " + windowCache.capacity());

        long totalAccounted = windowCache.size() + promotionCounter.get();
        assertTrue(totalAccounted >= windowCache.capacity(),
                "节点丢失严重， accounted=" + totalAccounted);
    }

    @Test
    @Order(11)
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("并发访问重排：多线程同时访问Window内节点")
    void testConcurrentAccess() throws InterruptedException {
        List<Node<String, String>> nodes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Node<String, String> node = createNode("pre-" + i, "v");
            nodes.add(node);
            windowCache.admit(node);
        }

        CountDownLatch latch = new CountDownLatch(CONCURRENCY);
        AtomicInteger accessCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        executor = Executors.newFixedThreadPool(CONCURRENCY);

        for (int t = 0; t < CONCURRENCY; t++) {
            executor.submit(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                try {
                    for (int i = 0; i < 10000; i++) {
                        Node<String, String> target = nodes.get(random.nextInt(nodes.size()));
                        try {
                            windowCache.onAccess(target);
                            accessCount.incrementAndGet();
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertEquals(0, errorCount.get(), "访问过程发生异常");
        assertEquals(10, windowCache.size(), "Window大小不应改变");
    }

    @Test
    @Order(12)
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    @DisplayName("混合负载：读写比9:1")
    void testMixedWorkload() throws InterruptedException {
        final int durationMs = 2000;
        final AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong readCount = new AtomicLong(0);
        AtomicLong writeCount = new AtomicLong(0);

        List<Node<String, String>> hotNodes = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 5; i++) {
            Node<String, String> node = createNode("hot-" + i, "v");
            hotNodes.add(node);
            windowCache.admit(node);
        }

        CountDownLatch latch = new CountDownLatch(CONCURRENCY);
        executor = Executors.newFixedThreadPool(CONCURRENCY);

        for (int t = 0; t < CONCURRENCY; t++) {
            executor.submit(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                try {
                    while (running.get()) {
                        int op = random.nextInt(10);
                        if (op < 1) {
                            String key = "new-" + Thread.currentThread().getId() + "-" + random.nextInt(1000);
                            windowCache.admit(createNode(key, "v"));
                            writeCount.incrementAndGet();
                        } else {
                            if (!hotNodes.isEmpty()) {
                                Node<String, String> node = hotNodes.get(random.nextInt(hotNodes.size()));
                                if (node.isInWindow()) {
                                    windowCache.onAccess(node);
                                }
                            }
                            readCount.incrementAndGet();
                        }

                        if (random.nextInt(100) == 0) {
                            LockSupport.parkNanos(1);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        Thread.sleep(durationMs);
        running.set(false);
        assertTrue(latch.await(5, TimeUnit.SECONDS), "线程未正常结束");

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertTrue(windowCache.size() <= windowCache.capacity());
    }

    // ========== 边界测试 ==========

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5, 10})
    @DisplayName("边界测试：Window容量为{0}时的行为")
    void testBoundaryConditions(int capacity) {
        AtomicInteger promotions = new AtomicInteger(0);
        WindowCache<String, String> cache = new WindowCache<>(capacity * 100, n -> promotions.incrementAndGet());

        for (int i = 0; i < capacity * 2; i++) {
            cache.admit(createNode("key" + i, "v"));
        }

        assertEquals(capacity, cache.size(), "Window应始终保持容量限制");
        assertEquals(capacity, promotions.get(), "应有capacity个节点被晋升");
    }

    @Test
    @DisplayName("重复移除同一节点应安全无异常")
    void testDuplicateRemove() {
        Node<String, String> node = createNode("A", "1");
        windowCache.admit(node);

        windowCache.remove(node);
        assertFalse(node.isInWindow());

        assertDoesNotThrow(() -> windowCache.remove(node));

        assertDoesNotThrow(() -> windowCache.admit(node));
        assertTrue(node.isInWindow());
    }

    @Test
    @DisplayName("晋升回调：每个被驱逐的节点必须且只被回调一次")
    void testPromotionCallbackExactlyOnce() throws InterruptedException {
        int capacity = 5;
        Set<String> promotedKeys = ConcurrentHashMap.newKeySet();
        AtomicInteger callbackCount = new AtomicInteger(0);

        WindowCache<String, String> cache = new WindowCache<>(capacity * 100, node -> {
            callbackCount.incrementAndGet();
            String key = (String) node.getKey();
            assertTrue(promotedKeys.add(key), "节点 " + key + " 被重复晋升！");
            assertFalse(node.isInWindow(), "晋升后应立即标记为不在Window");
        });

        executor = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(4);

        for (int t = 0; t < 4; t++) {
            final int threadId = t;
            executor.submit(() -> {
                for (int i = 0; i < 20; i++) {
                    cache.admit(createNode("T" + threadId + "-" + i, "v"));
                }
                latch.countDown();
            });
        }

        latch.await();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        int expectedPromotions = 80 - capacity;
        assertEquals(expectedPromotions, callbackCount.get());
        assertEquals(expectedPromotions, promotedKeys.size());
    }

    // ========== 辅助方法 ==========

    private static <K, V> Node<K, V> createNode(K key, V value) {
        return new Node<>(key, value,
                System.currentTimeMillis(),
                -1,
                ReferenceStrength.STRONG,
                null);
    }
}