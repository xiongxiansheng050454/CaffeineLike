package com.github.caffeine.cache;

import org.openjdk.jmh.annotations.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 2, jvmArgs = "-XX:-RestrictContended")  // 启用 @Contended
public class VarHandleBenchmark {
    private Cache<String, String> cache;

    @Setup
    public void setup() {
        cache = Caffeine.<String, String>newBuilder()
                .maximumSize(10000)
                .recordStats()
                .build();

        // 预热
        for (int i = 0; i < 1000; i++) {
            cache.put("key" + i, "value" + i);
        }
    }

    @Benchmark
    public String testRead() {
        return cache.getIfPresent("key500");  // 命中读取
    }

    @Benchmark
    public void testWrite() {
        cache.put("key" + System.nanoTime(), "value");
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}