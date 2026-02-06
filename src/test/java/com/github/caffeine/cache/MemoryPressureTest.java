package com.github.caffeine.cache;

/**
 * 内存压力测试：验证OOM前自动清理Soft引用
 */
public class MemoryPressureTest {
    public static void main(String[] args) throws Exception {
        Cache<String, byte[]> cache = Caffeine.<String, byte[]>newBuilder()
                .softValues()
                .memorySensitive()
                .memoryThresholds(0.6, 0.7) // 降低阈值便于测试
                .recordStats()
                .build();

        System.out.println("=== 内存压力测试开始 ===");

        // 填充数据直到触发内存压力
        for (int i = 0; i < 200; i++) {
            byte[] data = new byte[2 * 1024 * 1024]; // 2MB
            cache.put("key-" + i, data);

            if (i % 50 == 0) {
                printStatus(cache, i);
                Thread.sleep(100);
            }
        }

        printStatus(cache, 200);

        System.out.println("等待内存压力检测与清理...");
        // 等待维护线程检测到内存压力并执行清理（最多等待5秒）
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 5000) {
            long size = cache.estimatedSize();
            if (size < 100) {
                break; // 清理已完成
            }
            System.out.println("  当前存活: " + size + "，等待清理...");
            Thread.sleep(500);
        }

        Thread.sleep(500); // 额外等待确保统计更新
        System.out.println("\n=== 最终结果 ===");
        printStatus(cache, 200);

        long finalSize = cache.estimatedSize();
        if (finalSize < 100) {
            System.out.println("✓ 测试通过：内存压力下Soft引用被自动清理（剩余 " + finalSize + " 条）");
        } else {
            System.out.println("✗ 测试失败：清理未生效（剩余 " + finalSize + " 条）");
            System.out.println("  提示：若使用强引用测试，请确保添加 .softValues() 配置");
        }

        ((BoundedLocalCache<?, ?>) cache).shutdown();
    }

    private static void printStatus(Cache<?, ?> cache, int inserted) {
        CacheStats stats = cache.stats();
        System.out.printf("插入: %d, 存活: %d, 命中率: %.2f, GC回收: %d%n",
                inserted,
                cache.estimatedSize(),
                stats.hitRate(),
                stats.collectedCount());
    }
}