package com.github.caffeine.cache.concurrent;

import com.github.caffeine.cache.Cache;
import com.github.caffeine.cache.Caffeine;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 精简版基准测试（内存友好）
 * 运行命令：
 * java -Xmx512m -jar target/benchmarks.jar -f 1 -wi 2 -i 3 -t 8
 *
 * JVM参数（必须）：
 * --add-exports java.base/jdk.internal.vm.annotation=ALL-UNNAMED
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class LiteCacheBenchmark {

    // 小容量测试，避免内存压力
    @State(Scope.Benchmark)
    public static class CacheState {
        // 只测试中等容量，避免GC压力
        static final int CAPACITY = 10000;

        Cache<Integer, String> cache;
        ConcurrentHashMap<Integer, String> hashMap;

        @Setup
        public void setup() {
            // 启用写缓冲的版本
            cache = Caffeine.<Integer, String>newBuilder()
                    .maximumSize(CAPACITY)
                    .enableWriteBuffer()
                    .build();

            hashMap = new ConcurrentHashMap<>(CAPACITY);

            // 只预热25%数据，减少内存占用
            for (int i = 0; i < CAPACITY / 4; i++) {
                String v = "v" + i;
                cache.put(i, v);
                hashMap.put(i, v);
            }
        }

        @TearDown
        public void tearDown() {
            if (cache instanceof com.github.caffeine.cache.BoundedLocalCache) {
                ((com.github.caffeine.cache.BoundedLocalCache<?, ?>) cache).shutdown();
            }
        }
    }

    // 线程本地写索引，避免竞争
    @State(Scope.Thread)
    public static class ThreadState {
        int writeIndex;
        @Setup
        public void setup() {
            writeIndex = ThreadLocalRandom.current().nextInt(1000000);
        }
    }

    // ==================== 核心测试1：纯读性能 ====================

    @Benchmark
    @Threads(1)
    public void read_1t(CacheState s, Blackhole bh) {
        String v = s.cache.getIfPresent(ThreadLocalRandom.current().nextInt(CacheState.CAPACITY / 4));
        bh.consume(v);
    }

    @Benchmark
    @Threads(4)
    public void read_4t(CacheState s, Blackhole bh) {
        String v = s.cache.getIfPresent(ThreadLocalRandom.current().nextInt(CacheState.CAPACITY / 4));
        bh.consume(v);
    }

    @Benchmark
    @Threads(8)
    public void read_8t(CacheState s, Blackhole bh) {
        String v = s.cache.getIfPresent(ThreadLocalRandom.current().nextInt(CacheState.CAPACITY / 4));
        bh.consume(v);
    }

    // ==================== 核心测试2：基线对比（ConcurrentHashMap） ====================

    @Benchmark
    @Threads(8)
    public void baseline_read_8t(CacheState s, Blackhole bh) {
        String v = s.hashMap.get(ThreadLocalRandom.current().nextInt(CacheState.CAPACITY / 4));
        bh.consume(v);
    }

    // ==================== 核心测试3：纯写性能 ====================

    @Benchmark
    @Threads(1)
    public void write_1t(CacheState s, ThreadState ts) {
        s.cache.put(ts.writeIndex++, "value");
    }

    @Benchmark
    @Threads(8)
    public void write_8t(CacheState s, ThreadState ts) {
        s.cache.put(ts.writeIndex++, "value");
    }

    // ==================== 核心测试4：读写混合（80%读20%写） ====================

    @Benchmark
    @Threads(8)
    public void mixed_80_20(CacheState s, ThreadState ts, Blackhole bh) {
        if (ThreadLocalRandom.current().nextInt(100) < 80) {
            // 读热点数据
            String v = s.cache.getIfPresent(ThreadLocalRandom.current().nextInt(CacheState.CAPACITY / 4));
            bh.consume(v);
        } else {
            // 写新数据
            s.cache.put(ts.writeIndex++, "value");
        }
    }

    // ==================== 核心测试5：W-TinyLFU扫描抵抗性（小数据量版） ====================

    @State(Scope.Benchmark)
    public static class ScanResistanceState {
        Cache<Integer, String> smallCache;
        final int SIZE = 1000;

        @Setup
        public void setup() {
            // 小容量缓存测试W-TinyLFU
            smallCache = Caffeine.<Integer, String>newBuilder()
                    .maximumSize(SIZE)
                    .build();

            // 填充热点数据
            for (int i = 0; i < SIZE; i++) {
                smallCache.put(i, "hot-" + i);
            }
        }

        @TearDown
        public void tearDown() {
            ((com.github.caffeine.cache.BoundedLocalCache<?, ?>) smallCache).shutdown();
        }
    }

    /**
     * 验证W-TinyLFU：扫描10倍数据后，热点数据保留率应>80%
     * 如果实现正确，这个小测试能快速验证算法有效性
     */
    @Benchmark
    @Threads(1)
    @BenchmarkMode(Mode.SingleShotTime)
    @Measurement(iterations = 1)
    @Warmup(iterations = 0)
    public void scanResistance(ScanResistanceState s, Blackhole bh) {
        // 扫描10倍容量的冷数据
        for (int i = s.SIZE; i < s.SIZE * 10; i++) {
            s.smallCache.put(i, "cold-" + i);
        }

        // 统计热点数据保留率
        int hits = 0;
        for (int i = 0; i < s.SIZE; i++) {
            if (s.smallCache.getIfPresent(i) != null) hits++;
        }

        // 保留率应大于80%，否则W-TinyLFU实现可能有bug
        if (hits < s.SIZE * 0.8) {
            throw new AssertionError("W-TinyLFU扫描抵抗性不足: " + (hits * 100 / s.SIZE) + "%");
        }
        bh.consume(hits);
    }

    // ==================== 主函数 ====================

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LiteCacheBenchmark.class.getSimpleName())
                .jvmArgs(
                        "--add-exports", "java.base/jdk.internal.vm.annotation=ALL-UNNAMED",
                        "-Xmx512m",          // 限制512MB内存
                        "-XX:+UseG1GC",      // G1GC适合小内存
                        "-XX:MaxGCPauseMillis=50"
                )
                .build();

        new Runner(opt).run();
    }
}