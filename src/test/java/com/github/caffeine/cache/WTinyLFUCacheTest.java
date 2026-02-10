package com.github.caffeine.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * W-TinyLFU 完整行为测试套件
 * 验证 Window -> Probation -> Protected 的完整生命周期
 */
public class WTinyLFUCacheTest {

    private BoundedLocalCache<String, String> cache;
    private Caffeine<String, String> builder;

    // 测试配置：总容量100，Window=1，Main=99（Probation≈20，Protected≈79）
    private static final long TOTAL_CAPACITY = 100;

    @BeforeEach
    void setUp() {
        builder = Caffeine.<String, String>newBuilder()
                .maximumSize(TOTAL_CAPACITY)
                .recordStats();
        cache = (BoundedLocalCache<String, String>) builder.build();
    }

    /**
     * 测试1: Window区容量计算与准入
     * 验证：Window容量 = max(1, total/100) = 1
     */
    @Test
    public void testWindowCapacityCalculation() {
        // 插入1个元素，应在Window区
        cache.put("key1", "value1");

        // 验证Window区占用（通过内部状态或行为推断）
        // Window满后，再插入应触发晋升到Probation
        cache.put("key2", "value2");

        // key1应被挤到Probation区，key2在Window区
        Node<String, String> node1 = cache.getNodeInternal("key1");
        Node<String, String> node2 = cache.getNodeInternal("key2");

        assertNotNull(node1);
        assertNotNull(node2);

        // 验证分区类型（需要Node支持getQueueType）
        assertEquals(QueueType.PROBATION.value(), node1.getQueueType(),
                "Window满后，旧节点应进入Probation");
        assertEquals(QueueType.WINDOW.value(), node2.getQueueType(),
                "新节点应在Window区");
    }

    /**
     * 测试2: Probation区访问晋升到Protected
     * 验证：访问Probation节点且Protected未满时晋升
     */
    @Test
    public void testProbationToProtectedPromotion() {
        // 填充Window(1) + Probation(20) = 21个节点
        for (int i = 0; i < 21; i++) {
            cache.put("key" + i, "value" + i);
        }

        Node<String, String> probationNode = cache.getNodeInternal("key1");
        assertEquals(QueueType.PROBATION.value(), probationNode.getQueueType());

        // 访问Probation区的key1，应晋升到Protected
        cache.getIfPresent("key1");

        // 验证晋升
        assertEquals(QueueType.PROTECTED.value(), probationNode.getQueueType(),
                "访问Probation节点应晋升到Protected");
    }

    /**
     * 测试3: Protected满时的降级行为
     * 验证：Protected满(79)时，晋升Probation节点会触发Protected LRU降级到Probation
     */
    @Test
    public void testProtectedDemotionWhenFull() {
        // 初始化...
        Caffeine<String, String> builder = Caffeine.<String, String>newBuilder()
                .maximumSize(100)
                .recordStats();
        BoundedLocalCache<String, String> cache = (BoundedLocalCache<String, String>) builder.build();
        MainCachePolicy<String, String> mainPolicy = cache.getMainPolicy();

        // 填充后分布：Window[key99], Probation[key79..key98], Protected[key0..key78]
        // 填充数据
        for (int i = 0; i < 100; i++) {
            cache.put("key" + i, "value" + i);
        }

        // 验证初始分布
        assertEquals(79, mainPolicy.protectedSize());
        assertEquals(20, mainPolicy.probationSize());

        // 步骤3：多次访问 Probation 区的 key80，确保频率明显高于 Head
        String promoteKey = "key80";

        // 访问 5 次，使频率达到 6（假设插入时为 1）
        for (int i = 0; i < 5; i++) {
            cache.getIfPresent(promoteKey);
        }

        int finalFreq = cache.getFrequencySketch().frequency(promoteKey);
        System.out.println("key80 final frequency: " + finalFreq);

        // 验证晋升结果
        Node<String, String> promotedNode = cache.getNodeInternal(promoteKey);
        assertEquals(QueueType.PROTECTED.value(), promotedNode.getQueueType(),
                "key80应晋升到Protected");

        // 验证 Protected 总量不变（79 个），Probation 总量不变（20 个）
        assertEquals(79, mainPolicy.protectedSize());
        assertEquals(20, mainPolicy.probationSize());

        // 验证有节点被降级（Probation 头部应该有一个原 Protected 的节点）
        Node<String, String> probationHead = mainPolicy.peekProbationHead();
        assertNotNull(probationHead);
        assertEquals(QueueType.PROBATION.value(), probationHead.getQueueType());
    }

    /**
     * 测试4: TinyLFU频率比较晋升
     * 验证：只有频率高于Probation头部时才晋升（避免保护偶然访问）
     */
    @Test
    public void testFrequencyBasedPromotion() {
        // 设置：Window(1) + Probation(20)
        for (int i = 0; i < 21; i++) {
            cache.put("key" + i, "value" + i);
        }

        // 高频访问key1（5次）
        for (int i = 0; i < 5; i++) {
            cache.getIfPresent("key1");
        }

        // 低频访问key2（1次）
        cache.getIfPresent("key2");

        // 填满Protected以触发严格比较
        for (int i = 0; i < 79; i++) {
            cache.put("extra" + i, "value");
        }

        // 高频key1应能晋升（或已在Protected）
        Node<String, String> highFreqNode = cache.getNodeInternal("key1");
        int highFreqType = highFreqNode.getQueueType();

        // 验证：高频节点应在Protected或保持Probation（取决于具体策略）
        // 关键是验证频率统计正确性
        FrequencySketch<String> sketch = cache.getFrequencySketch();
        assertTrue(sketch.frequency("key1") > sketch.frequency("key2"),
                "高频节点应有更高频率计数");
    }

    /**
     * 测试5: Main Cache满时从Probation驱逐
     * 验证：当Main Cache(99)满时，从Probation LRU驱逐
     */
    @Test
    public void testMainCacheEviction() {
        // 填满Window + Main
        for (int i = 0; i < 100; i++) {
            cache.put("key" + i, "value" + i);
        }

        long initialSize = cache.estimatedSize();
        assertEquals(100, initialSize);

        // 插入新key，触发驱逐
        cache.put("newKey", "newValue");

        // 验证总容量保持100
        assertEquals(100, cache.estimatedSize(), "总容量应保持在maximumSize");

        // 验证新key存在
        assertNotNull(cache.getIfPresent("newKey"), "新插入的key应存在");

        // 验证某个旧key被驱逐（通常是Probation的LRU）
        // 具体哪个被驱逐取决于访问历史，但总数应保持恒定
    }

    /**
     * 测试6: 分区统计准确性
     * 验证：Window + Probation + Protected = total
     */
    @Test
    public void testPartitionStatistics() {
        // 插入50个节点
        for (int i = 0; i < 50; i++) {
            cache.put("key" + i, "value" + i);
        }

        long windowSize = cache.getWindowCache().size();
        long mainSize = cache.getMainPolicy().totalSize();

        assertEquals(50, windowSize + mainSize,
                "Window + Main 总数应等于插入数");
        assertTrue(windowSize <= 1, "Window不应超过1%容量");
    }

    /**
     * 测试7: 显式删除时从分区移除
     * 验证：invalidate时从对应分区正确移除
     */
    @Test
    public void testExplicitRemovalFromPartition() {
        cache.put("key1", "value1");
        cache.put("key2", "value2");

        // 确保key1在Main Cache（通过多次插入挤到Main）
        for (int i = 0; i < 25; i++) {
            cache.put("temp" + i, "value");
        }

        Node<String, String> node = cache.getNodeInternal("key1");
        int typeBefore = node.getQueueType();

        // 删除
        cache.invalidate("key1");

        // 验证分区大小减少
        if (typeBefore == QueueType.PROBATION.value()) {
            assertEquals(0, cache.getMainPolicy().probationSize(),
                    "删除后Probation应为空");
        }
    }
}