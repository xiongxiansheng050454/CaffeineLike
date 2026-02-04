package com.github.benmanes.caffeine.cache;

import org.junit.Test;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;

public class CacheBuilderTest {

    @Test
    public void testChainConfiguration() {
        Cache<String, Integer> cache = Caffeine.<String, Integer>newBuilder()
                .initialCapacity(100)
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .recordStats()
                .build();

        assertNotNull(cache);
        assertEquals(0, cache.estimatedSize());
    }

    @Test
    public void testBasicCrud() {
        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .maximumSize(10)
                .build();

        cache.put("key1", "value1");
        assertEquals("value1", cache.getIfPresent("key1"));
        assertEquals(1, cache.estimatedSize());

        cache.invalidate("key1");
        assertNull(cache.getIfPresent("key1"));
    }

    @Test
    public void testExpiration() throws InterruptedException {
        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .expireAfterWrite(100, TimeUnit.MILLISECONDS)
                .build();

        cache.put("key", "value");
        assertEquals("value", cache.getIfPresent("key"));

        Thread.sleep(150);
        assertNull(cache.getIfPresent("key")); // 已过期
    }

    @Test
    public void testStats() {
        Cache<String, String> cache = Caffeine.<String, String>newBuilder()
                .recordStats()
                .build();

        cache.put("key", "value");
        cache.getIfPresent("key"); // hit
        cache.getIfPresent("missing"); // miss

        CacheStats stats = cache.stats();
        assertEquals(0.5, stats.hitRate(), 0.01);
    }
}