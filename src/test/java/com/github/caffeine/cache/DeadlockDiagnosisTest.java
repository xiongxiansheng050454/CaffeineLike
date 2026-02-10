package com.github.caffeine.cache;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 死锁诊断测试套件
 * 每个测试都设置了5秒超时，卡住即视为失败
 */
public class DeadlockDiagnosisTest {

    private BoundedLocalCache<String, String> cache;
    private Caffeine<String, String> builder;

    @BeforeEach
    void setUp() {
        builder = Caffeine.<String, String>newBuilder()
                .maximumSize(100)  // Window=1, Main=99
                .expireAfterWrite(100, TimeUnit.MILLISECONDS); // 快速过期
    }

    @AfterEach
    void tearDown() {
        if (cache != null) {
            // 在独立线程中关闭，防止卡住测试框架
            Thread shutdownThread = new Thread(() -> cache.shutdown());
            shutdownThread.start();
            try {
                shutdownThread.join(3000);  // 3秒超时
                if (shutdownThread.isAlive()) {
                    System.err.println("!!! shutdown() 死锁 detected !!!");
                    printThreadDump();  // 你的 dump 方法
                    shutdownThread.interrupt();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 测试1: 时间轮过期时的锁升级死锁
     * 预期：如果 onTimerExpired 存在读锁升级写锁，此测试会卡住
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @DisplayName("测试时间轮过期是否导致死锁")
    void testTimerExpirationDeadlock() throws InterruptedException {
        cache = (BoundedLocalCache<String, String>) builder.build();

        // 插入数据
        cache.put("key1", "value1");

        // 等待时间轮触发过期（100ms + 100ms 轮询间隔 + 缓冲）
        Thread.sleep(300);

        // 再次访问，可能触发过期清理
        String value = cache.getIfPresent("key1");
        assertNull(value, "数据应已过期");

        System.out.println("✓ 时间轮过期未死锁");
    }

    /**
     * 测试2: 并发 invalidate 和 getIfPresent 的竞争
     * 预期：检查 WindowCache 和 Segment 锁的交叉获取是否死锁
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @DisplayName("测试并发删除和访问")
    void testConcurrentInvalidateAndAccess() throws InterruptedException {
        cache = (BoundedLocalCache<String, String>) builder.build();
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger success = new AtomicInteger(0);

        // 先填充数据到 Window 和 Main
        for (int i = 0; i < 5; i++) {
            cache.put("key" + i, "value" + i);
        }

        // 线程1: 不断 invalidate
        Thread t1 = new Thread(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    cache.invalidate("key" + (i % 5));
                    LockSupport.parkNanos(1000); // 1微秒延迟
                }
                success.incrementAndGet();
            } finally {
                latch.countDown();
            }
        }, "invalidator");

        // 线程2: 不断读取
        Thread t2 = new Thread(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    cache.getIfPresent("key" + (i % 5));
                    LockSupport.parkNanos(1000);
                }
                success.incrementAndGet();
            } finally {
                latch.countDown();
            }
        }, "reader");

        t1.start();
        t2.start();

        assertTrue(latch.await(5, TimeUnit.SECONDS), "测试超时，可能死锁");
        assertEquals(2, success.get(), "所有线程应成功完成");
        System.out.println("✓ 并发删除和访问未死锁");
    }

    /**
     * 测试3: 模拟 onTimerExpired 和 invalidate 的竞争
     * 直接调用内部方法测试锁升级问题
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @DisplayName("测试过期回调和删除的竞争")
    void testExpirationCallbackVsInvalidate() throws InterruptedException {
        cache = (BoundedLocalCache<String, String>) builder.build();

        // 插入并立即过期（通过反射直接触发 onTimerExpired）
        cache.put("key1", "value1");

        // 使用反射模拟时间轮同时触发过期和用户删除
        // 如果锁升级有问题，这里会卡住

        Thread expireThread = new Thread(() -> {
            try {
                // 模拟时间轮线程调用 onTimerExpired
                java.lang.reflect.Method method = BoundedLocalCache.class
                        .getDeclaredMethod("onTimerExpired", Object.class);
                method.setAccessible(true);
                method.invoke(cache, "key1");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "expire-simulator");

        Thread invalidateThread = new Thread(() -> {
            try {
                Thread.sleep(10); // 稍微延迟，制造竞争
                cache.invalidate("key1");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "invalidate-competitor");

        expireThread.start();
        invalidateThread.start();

        expireThread.join(2000);
        invalidateThread.join(2000);

        assertFalse(expireThread.isAlive(), "过期线程应在2秒内完成");
        assertFalse(invalidateThread.isAlive(), "删除线程应在2秒内完成");
        System.out.println("✓ 过期回调和删除竞争未死锁");
    }

    /**
     * 测试4: 检查 WindowCache 和 MainCachePolicy 的锁顺序
     * W-TinyLFU 晋升路径: Window(写锁) -> Main(写锁)
     * 如果顺序不一致会导致死锁
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @DisplayName("测试双向晋升死锁")
    void testPromotionDeadlock() throws InterruptedException {
        // 使用极小的 Window 容量增加晋升频率
        Caffeine<String, String> smallWindowBuilder = Caffeine.<String, String>newBuilder()
                .maximumSize(10); // Window=1，Main=9

        cache = (BoundedLocalCache<String, String>) smallWindowBuilder.build();
        CountDownLatch latch = new CountDownLatch(2);

        // 线程1: 不断插入触发 Window->Main 晋升
        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 200; i++) {
                cache.put("t1-key" + i, "value");
                if (i % 50 == 0) Thread.yield();
            }
            latch.countDown();
        }, "writer-1");

        // 线程2: 反向操作，尝试访问 Probation 区并删除
        Thread t2 = new Thread(() -> {
            for (int i = 0; i < 200; i++) {
                cache.getIfPresent("t1-key" + (i % 100)); // 可能晋升到 Protected
                if (i % 30 == 0) {
                    cache.invalidate("t1-key" + (i % 100));
                }
            }
            latch.countDown();
        }, "promotion-tester");

        t1.start();
        t2.start();

        assertTrue(latch.await(5, TimeUnit.SECONDS), "晋升测试超时，可能死锁");
        System.out.println("✓ 双向晋升未死锁");
    }

    /**
     * 测试5: 诊断写缓冲和驱逐的交互
     * 如果 WriteBuffer flush 和驱逐线程竞争，可能死锁
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @DisplayName("测试写缓冲刷盘和驱逐死锁")
    void testWriteBufferEvictionDeadlock() throws InterruptedException {
        Caffeine<String, String> bufferBuilder = Caffeine.<String, String>newBuilder()
                .maximumSize(50)
                .enableWriteBuffer(); // 启用写缓冲

        cache = (BoundedLocalCache<String, String>) bufferBuilder.build();
        CountDownLatch latch = new CountDownLatch(3);

        // 高频写入
        Thread writer = new Thread(() -> {
            for (int i = 0; i < 500; i++) {
                cache.put("key" + i, "value" + i);
            }
            latch.countDown();
        }, "high-freq-writer");

        // 高频读取触发驱逐
        Thread reader = new Thread(() -> {
            for (int i = 0; i < 500; i++) {
                cache.getIfPresent("key" + (i % 100));
            }
            latch.countDown();
        }, "eviction-trigger");

        // 高频删除
        Thread remover = new Thread(() -> {
            for (int i = 0; i < 200; i++) {
                cache.invalidate("key" + (i % 50));
                LockSupport.parkNanos(10000); // 10微秒
            }
            latch.countDown();
        }, "remover");

        writer.start();
        reader.start();
        remover.start();

        assertTrue(latch.await(5, TimeUnit.SECONDS), "写缓冲测试超时");
        System.out.println("✓ 写缓冲和驱逐交互未死锁");
    }

    /**
     * 测试6: 诊断 invalidateAll 全局锁死锁
     * invalidateAll 使用 stripedLock.lockAll()，如果其他线程持有 segment 锁会死锁
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @DisplayName("测试 invalidateAll 全局锁死锁")
    void testInvalidateAllDeadlock() throws InterruptedException {
        cache = (BoundedLocalCache<String, String>) builder.build();

        // 填充数据
        for (int i = 0; i < 100; i++) {
            cache.put("key" + i, "value");
        }

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(2);

        // 线程1: 持有某个 segment 的写锁（通过 put 模拟）
        Thread holder = new Thread(() -> {
            try {
                // 通过高频 put 增加持有锁的概率
                for (int i = 0; i < 1000; i++) {
                    cache.put("holder-key" + i, "value");
                }
            } finally {
                endLatch.countDown();
            }
        }, "lock-holder");

        // 线程2: 调用 invalidateAll
        Thread clearer = new Thread(() -> {
            try {
                startLatch.await(); // 等待 holder 开始
                Thread.sleep(10);   // 确保 holder 持有锁
                cache.invalidateAll();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                endLatch.countDown();
            }
        }, "clearer");

        holder.start();
        startLatch.countDown(); // 通知 clearer 开始
        clearer.start();

        assertTrue(endLatch.await(5, TimeUnit.SECONDS), "invalidateAll 超时，可能死锁");
        System.out.println("✓ invalidateAll 全局锁未死锁");
    }

    /**
     * 工具方法：打印线程转储（死锁时调用）
     */
    private void printThreadDump() {
        System.out.println("========== 线程转储 ==========");
        Map<Thread, StackTraceElement[]> stacks = Thread.getAllStackTraces();
        for (Thread t : stacks.keySet()) {
            System.out.println("\n--- " + t.getName() + " ---");
            for (StackTraceElement e : stacks.get(t)) {
                System.out.println("\tat " + e);
            }
        }
        System.out.println("=============================");
    }
}