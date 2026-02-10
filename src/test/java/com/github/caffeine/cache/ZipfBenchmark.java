package com.github.caffeine.cache;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

/**
 * Zipf分布缓存命中率测试
 *
 * 模拟真实场景：少数热点key被频繁访问（符合80/20法则）
 * 测试缓存的W-TinyLFU策略对这种偏斜访问模式的适应性
 */
public class ZipfBenchmark {

    // Zipf分布参数
    private final int itemCount;      // 总数据项数（如100万）
    private final double alpha;       // Zipf偏斜参数（0.5-1.0，越大越偏斜）
    private final int cacheSize;      // 缓存容量（如1万）
    private final long operations;    // 总操作数

    // 预计算的Zipf分布表
    private final double[] cumulativeProb;
    private final Random random;

    // 统计
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();

    public ZipfBenchmark(int itemCount, double alpha, int cacheSize, long operations) {
        this.itemCount = itemCount;
        this.alpha = alpha;
        this.cacheSize = cacheSize;
        this.operations = operations;
        this.random = ThreadLocalRandom.current();

        // 预计算Zipf累积分布（优化采样速度）
        this.cumulativeProb = new double[itemCount];
        computeCumulativeProb();
    }

    /**
     * 预计算累积分布函数（CDF），用于快速采样
     */
    private void computeCumulativeProb() {
        // 计算调和数H_n^s
        double[] probs = new double[itemCount];
        double sum = 0.0;

        for (int i = 0; i < itemCount; i++) {
            // Zipf概率：P(k) = 1/(k+1)^alpha
            probs[i] = 1.0 / Math.pow(i + 1, alpha);
            sum += probs[i];
        }

        // 归一化并计算累积概率
        cumulativeProb[0] = probs[0] / sum;
        for (int i = 1; i < itemCount; i++) {
            cumulativeProb[i] = cumulativeProb[i - 1] + (probs[i] / sum);
        }
    }

    /**
     * 使用二分查找根据随机数采样Zipf分布的key
     */
    private int sampleZipf() {
        double u = random.nextDouble();
        int idx = Arrays.binarySearch(cumulativeProb, u);
        if (idx < 0) {
            idx = -idx - 1;
        }
        return Math.min(idx, itemCount - 1);
    }

    /**
     * 执行测试
     */
    public Result runTest() {
        // 构建缓存
        Cache<Integer, String> cache = Caffeine.<Integer, String>newBuilder()
                .maximumSize(cacheSize)
                .recordStats()
                .build();

        System.out.println("========== Zipf分布命中率测试 ==========");
        System.out.printf("数据项总数: %,d%n", itemCount);
        System.out.printf("Zipf参数(α): %.2f%n", alpha);
        System.out.printf("缓存容量: %,d (%.2f%%)%n", cacheSize, 100.0 * cacheSize / itemCount);
        System.out.printf("总操作数: %,d%n", operations);
        System.out.println("======================================");

        // 阶段1：预热（填充缓存到稳态）
        long warmupOps = Math.min(operations / 10, 100_000);
        System.out.printf("%n[阶段1] 预热: %,d 次操作...%n", warmupOps);
        runOperations(cache, warmupOps, false);

        // 重置统计（可选：只看稳态表现）
        if (cache instanceof BoundedLocalCache) {
            // 无法真正重置BoundedLocalCache的统计，重新建一个
            cache = Caffeine.<Integer, String>newBuilder()
                    .maximumSize(cacheSize)
                    .recordStats()
                    .build();

            // 再次预热但不统计
            runOperations(cache, warmupOps, false);
        }

        // 阶段2：正式测试
        System.out.printf("%n[阶段2] 正式测试: %,d 次操作...%n", operations);
        long startTime = System.nanoTime();
        runOperations(cache, operations, true);
        long duration = System.nanoTime() - startTime;

        // 收集结果
        CacheStats stats = cache.stats();
        double hitRate = stats.hitRate();
        double theoreticalMax = computeTheoreticalOptimal();

        System.out.printf("%n========== 测试结果 ==========%n");
        System.out.printf("命中率: %.2f%%%n", hitRate * 100);
        System.out.printf("理论最优命中率: %.2f%%%n", theoreticalMax * 100);
        System.out.printf("相对效率: %.2f%%%n", (hitRate / theoreticalMax) * 100);
        System.out.printf("吞吐量: %,d ops/sec%n", (long)(operations * 1e9 / duration));
        System.out.printf("平均延迟: %.3f μs/op%n", (duration / 1e3) / operations);

        return new Result(hitRate, theoreticalMax, operations, duration);
    }

    /**
     * 执行操作序列
     */
    private void runOperations(Cache<Integer, String> cache, long ops, boolean recordStats) {
        for (long i = 0; i < ops; i++) {
            int key = sampleZipf();
            String value = cache.getIfPresent(key);

            if (value == null) {
                // 未命中，模拟加载（实际场景中可能是DB查询）
                value = "value-" + key;
                cache.put(key, value);
                if (recordStats) misses.increment();
            } else {
                if (recordStats) hits.increment();
            }

            // 每10万次打印进度
            if (recordStats && i > 0 && i % 100_000 == 0) {
                long currentHits = hits.sum();
                long currentMisses = misses.sum();
                double currentRate = (double) currentHits / (currentHits + currentMisses);
                System.out.printf("  进度: %,d/%,d, 当前命中率: %.2f%%%n",
                        i, ops, currentRate * 100);
            }
        }
    }

    /**
     * 计算理论最优命中率（基于Zipf分布的数学期望）
     *
     * 理论最优：缓存存放概率最高的M个key，命中率为sum(P(k)) for k=1 to M
     */
    private double computeTheoreticalOptimal() {
        double sum = 0.0;
        for (int i = 0; i < cacheSize && i < itemCount; i++) {
            sum += 1.0 / Math.pow(i + 1, alpha);
        }

        // 归一化分母（调和数）
        double harmonic = 0.0;
        for (int i = 0; i < itemCount; i++) {
            harmonic += 1.0 / Math.pow(i + 1, alpha);
        }

        return sum / harmonic;
    }

    /**
     * 测试结果记录
     */
    public record Result(
            double hitRate,
            double theoreticalOptimal,
            long totalOperations,
            long durationNanos
    ) {
        public double throughput() {
            return totalOperations * 1e9 / durationNanos;
        }
    }

    /**
     * 多场景对比测试
     */
    public static void main(String[] args) {
        System.out.println("开始Zipf分布缓存命中率测试...\n");

        // 场景1：轻度偏斜（模拟较均匀分布）
        runScenario("轻度偏斜(α=0.5)", 100_000, 0.5, 1000, 1_000_000);

        // 场景2：中度偏斜（典型Web访问模式）
        runScenario("中度偏斜(α=0.8)", 100_000, 0.8, 1000, 1_000_000);

        // 场景3：高度偏斜（极端热点，如热门商品）
        runScenario("高度偏斜(α=1.0)", 100_000, 1.0, 1000, 1_000_000);

        // 场景4：大容量测试（验证W-TinyLFU的扩展性）
        runScenario("大容量测试", 1_000_000, 0.9, 10_000, 5_000_000);
    }

    private static void runScenario(String name, int items, double alpha,
                                    int cacheSize, long ops) {
        System.out.printf("%n%s%n", "=".repeat(50));
        System.out.printf("场景: %s%n", name);
        System.out.printf("%s%n%n", "=".repeat(50));

        ZipfBenchmark benchmark = new ZipfBenchmark(items, alpha, cacheSize, ops);
        Result result = benchmark.runTest();

        System.out.printf("%n场景[%s]完成 - 命中率: %.2f%% (理论: %.2f%%)%n%n",
                name, result.hitRate() * 100, result.theoreticalOptimal() * 100);
    }
}