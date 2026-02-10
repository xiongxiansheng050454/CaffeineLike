package com.github.caffeine.cache;

import com.github.caffeine.cache.reference.ReferenceStrength;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Window Cache 功能测试
 * 验证：1%容量保护、LRU驱逐、MRU重排、晋升到Main区
 */
public class WindowCacheTest {

    public static void main(String[] args) {
        System.out.println("========== Window Cache 测试套件 ==========\n");

        testWindowCapacityCalculation();
        testBasicAdmission();
        testEvictionToMain();
        testAccessReorder();
        testIntegrationWithBoundedCache();
        testWindowProtectionEffect();

        System.out.println("\n========== 所有测试通过 ==========");
    }

    /**
     * 测试1: Window容量计算（总容量的1%）
     */
    static void testWindowCapacityCalculation() {
        System.out.println("测试1: Window容量计算");

        AtomicReference<Node<String, String>> promoted = new AtomicReference<>();
        WindowCache<String, String> window = new WindowCache<>(1000, promoted::set);

        assert window.capacity() == 10 : "Window容量应为1000的1%=10";
        assert window.capacity() >= 1 : "Window容量至少为1";

        // 测试小容量边界
        WindowCache<String, String> smallWindow = new WindowCache<>(50, promoted::set);
        assert smallWindow.capacity() == 1 : "50容量的1%不足1，应至少为1";

        System.out.println("  ✓ 容量计算正确: 1000->" + window.capacity() + ", 50->" + smallWindow.capacity());
    }

    /**
     * 测试2: 基本准入（新节点进入Window尾部/MRU）
     */
    static void testBasicAdmission() {
        System.out.println("\n测试2: 基本准入");

        AtomicReference<Node<String, String>> promoted = new AtomicReference<>();
        WindowCache<String, String> window = new WindowCache<>(100, promoted::set); // capacity=1

        Node<String, String> nodeA = createNode("A", "value1");
        Node<String, String> nodeB = createNode("B", "value2");

        // 准入第一个节点
        window.admit(nodeA);
        assert window.size() == 1 : "Window大小应为1";
        assert nodeA.isInWindow() : "NodeA应在Window中";

        // 准入第二个节点（触发驱逐，因为capacity=1）
        window.admit(nodeB);

        System.out.println("  ✓ 准入逻辑正确，size=" + window.size());
    }

    /**
     * 测试3: Window满时驱逐LRU到Main区
     */
    static void testEvictionToMain() {
        System.out.println("\n测试3: LRU驱逐到Main区");

        AtomicReference<Node<String, String>> promoted = new AtomicReference<>();
        WindowCache<String, String> window = new WindowCache<>(200, promoted::set); // capacity=2

        Node<String, String> nodeA = createNode("A", "value1");
        Node<String, String> nodeB = createNode("B", "value2");
        Node<String, String> nodeC = createNode("C", "value3");

        // 填满Window (A,B)
        window.admit(nodeA);
        window.admit(nodeB);
        assert window.size() == 2 : "Window应已满";

        // 准入C，应驱逐A（LRU）
        window.admit(nodeC);

        // 验证A被驱逐到Main（通过回调）
        assert promoted.get() == nodeA : "最老的A应被驱逐";
        assert !nodeA.isInWindow() : "A应标记为不在Window";
        assert nodeC.isInWindow() : "C应在Window中";
        assert window.size() == 2 : "Window仍应为满";

        System.out.println("  ✓ LRU驱逐正确: A被驱逐，C进入");
    }

    /**
     * 测试4: 访问重排（MRU更新）
     */
    static void testAccessReorder() {
        System.out.println("\n测试4: MRU访问重排");

        AtomicReference<Node<String, String>> promoted = new AtomicReference<>();
        WindowCache<String, String> window = new WindowCache<>(200, promoted::set); // capacity=2

        Node<String, String> nodeA = createNode("A", "value1");
        Node<String, String> nodeB = createNode("B", "value2");
        Node<String, String> nodeC = createNode("C", "value3");

        // A先进，B后进（顺序：A(LRU) <-> B(MRU)）
        window.admit(nodeA);
        window.admit(nodeB);

        // 访问A，使其变为MRU（顺序：B <-> A）
        window.onAccess(nodeA);

        // 准入C，应驱逐B（现在B是LRU）
        promoted.set(null); // 清空上次回调
        window.admit(nodeC);

        assert promoted.get() == nodeB : "被驱逐的应是B（而非A）";
        assert nodeA.isInWindow() : "A应仍在Window中";

        System.out.println("  ✓ 访问重排正确: A被访问后变为MRU，B被驱逐");
    }

    /**
     * 测试5: 与BoundedLocalCache集成
     */
    static void testIntegrationWithBoundedCache() {
        System.out.println("\n测试5: 与BoundedLocalCache集成");

        // 创建容量为100的缓存，Window=1
        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .maximumSize(100)
                .build();

        // 插入3个值（Window容量为1，第二个开始应触发驱逐）
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");

        // 验证所有值都可访问（被驱逐到Main，而非删除）
        assert cache.getIfPresent("key1") != null : "key1应存在（在Main区）";
        assert cache.getIfPresent("key2") != null : "key2应存在";
        assert cache.getIfPresent("key3") != null : "key3应存在（在Window区）";

        System.out.println("  ✓ 集成测试通过，所有键值可访问");
        System.out.println("    当前缓存大小: " + cache.estimatedSize());
    }

    /**
     * 测试6: Window保护效果验证（新数据不被立即淘汰）
     */
    static void testWindowProtectionEffect() {
        System.out.println("\n测试6: Window保护效果验证");

        // 创建小容量缓存（Window=1，Main=9），测试新数据保护
        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .maximumSize(10)
                .build();

        // 先填充缓存到满（10个条目）
        for (int i = 0; i < 10; i++) {
            cache.put("old-" + i, "value-" + i);
        }

        // 等待一下确保时间戳不同（如果用了expireAfterAccess）
        try { Thread.sleep(10); } catch (InterruptedException e) {}

        // 现在插入一个新热点数据
        cache.put("hot-new", "hot-value");

        // 立即访问这个新数据多次（模拟热点）
        for (int i = 0; i < 5; i++) {
            cache.getIfPresent("hot-new");
        }

        // 再插入更多数据，触发驱逐
        for (int i = 0; i < 5; i++) {
            cache.put("extra-" + i, "extra-value-" + i);
        }

        // 验证新热点数据仍在（受Window保护）
        String hotValue = cache.getIfPresent("hot-new");
        assert hotValue != null : "新热点数据应受Window保护，不被立即淘汰";
        assert hotValue.equals("hot-value") : "值应正确";

        System.out.println("  ✓ Window保护有效: 新热点数据在高压下存活");
        System.out.println("    最终缓存大小: " + cache.estimatedSize());
    }

    // 辅助方法：创建Node
    private static <K, V> Node<K, V> createNode(K key, V value) {
        return new Node<>(key, value,
                System.currentTimeMillis(),
                -1, // 无过期
                ReferenceStrength.STRONG,
                null);
    }
}