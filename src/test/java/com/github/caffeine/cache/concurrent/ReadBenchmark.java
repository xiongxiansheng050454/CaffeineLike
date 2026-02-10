package com.github.caffeine.cache.concurrent;

import com.github.caffeine.cache.BoundedLocalCache;
import com.github.caffeine.cache.Caffeine;
import com.github.caffeine.cache.Cache;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 读性能基准测试 - 对比 BoundedLocalCache vs ConcurrentHashMap
 * 测试场景：不同线程数、不同命中率
 */
@State(Scope.Benchmark)
public class ReadBenchmark {

    // 缓存配置
    private static final int CACHE_SIZE = 100_000;
    private static final int KEY_RANGE = 100_000; // 100%命中
    private static final int KEY_RANGE_MISS = 150_000; // 66%命中

    private Cache<String, String> caffeineLikeCache;
    private ConcurrentHashMap<String, String> hashMap;

    private AtomicInteger keyGenerator;

    @Setup(Level.Trial)
    public void setup() {
        // 初始化你的缓存
        caffeineLikeCache = Caffeine.<String, String>newBuilder()
                .maximumSize(CACHE_SIZE)
                .recordStats()
                .build();

        // 基线：ConcurrentHashMap
        hashMap = new ConcurrentHashMap<>(CACHE_SIZE);

        // 预热数据：填充100% key
        for (int i = 0; i < CACHE_SIZE; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            caffeineLikeCache.put(key, value);
            hashMap.put(key, value);
        }

        keyGenerator = new AtomicInteger(0);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (caffeineLikeCache instanceof BoundedLocalCache) {
            ((BoundedLocalCache<String, String>) caffeineLikeCache).shutdown();
        }
    }

    /**
     * 生成循环使用的key，确保CPU不空闲等待
     */
    private String nextKey(int range) {
        int idx = keyGenerator.getAndIncrement() % range;
        return "key-" + Math.abs(idx);
    }

    // ==================== 单线程读性能 ====================

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Threads(1)
    public void testCaffeineLike_SingleThread_Hit100(Blackhole bh) {
        String key = nextKey(KEY_RANGE);
        String value = caffeineLikeCache.getIfPresent(key);
        bh.consume(value);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Threads(1)
    public void testHashMap_SingleThread_Hit100(Blackhole bh) {
        String key = nextKey(KEY_RANGE);
        String value = hashMap.get(key);
        bh.consume(value);
    }

    // ==================== 多线程读性能（高并发） ====================

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Threads(4)
    public void testCaffeineLike_4Threads_Hit100(Blackhole bh) {
        String key = nextKey(KEY_RANGE);
        String value = caffeineLikeCache.getIfPresent(key);
        bh.consume(value);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Threads(16)
    public void testCaffeineLike_16Threads_Hit100(Blackhole bh) {
        String key = nextKey(KEY_RANGE);
        String value = caffeineLikeCache.getIfPresent(key);
        bh.consume(value);
    }

    // ==================== 部分未命中场景（测试miss路径） ====================

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Threads(4)
    public void testCaffeineLike_4Threads_Hit66(Blackhole bh) {
        // 33%的key不在缓存中，测试miss路径性能
        String key = nextKey(KEY_RANGE_MISS);
        String value = caffeineLikeCache.getIfPresent(key);
        bh.consume(value);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Threads(4)
    public void testHashMap_4Threads_Hit66(Blackhole bh) {
        String key = nextKey(KEY_RANGE_MISS);
        String value = hashMap.get(key);
        bh.consume(value);
    }

    // ==================== 纯读无竞争（遍历测试） ====================

    @State(Scope.Thread)
    public static class ThreadLocalState {
        private int counter = 0;

        String nextKeyLocal() {
            return "key-" + (counter++ % KEY_RANGE);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Threads(4)
    public void testCaffeineLike_NoContention(ThreadLocalState state, Blackhole bh) {
        // 每个线程使用自己的key序列，避免缓存行伪共享和锁竞争
        String key = state.nextKeyLocal();
        String value = caffeineLikeCache.getIfPresent(key);
        bh.consume(value);
    }

    // ==================== 主方法 ====================

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ReadBenchmark.class.getSimpleName())
                .forks(1)                           // 减少内存占用
                .warmupIterations(2)                // 减少预热次数
                .measurementIterations(3)           // 减少测试轮次
                .timeUnit(TimeUnit.MILLISECONDS)
                .build();

        new Runner(opt).run();
    }
}