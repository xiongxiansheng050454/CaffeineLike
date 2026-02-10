package com.github.caffeine.cache;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class FrequencySketchTest {

    @Test
    void testBasicFrequency() {
        FrequencySketch<String> sketch = new FrequencySketch<>();
        sketch.ensureCapacity(1000);

        String key = "hot-key";

        // 初始频率为0
        assertEquals(0, sketch.frequency(key));

        // 访问5次
        for (int i = 0; i < 5; i++) {
            sketch.increment(key);
        }

        int freq = sketch.frequency(key);
        assertTrue(freq >= 5 && freq <= 15, "频率应在5左右，实际: " + freq);
    }

    @Test
    void testSaturation() {
        FrequencySketch<String> sketch = new FrequencySketch<>();
        sketch.ensureCapacity(100);

        String key = "very-hot-key";

        // 访问20次（超过15上限）
        for (int i = 0; i < 20; i++) {
            sketch.increment(key);
        }

        // 应被限制在15
        assertEquals(15, sketch.frequency(key));
    }

    @Test
    void testResetDecay() {
        FrequencySketch<String> sketch = new FrequencySketch<>();
        sketch.ensureCapacity(100);

        String key = "key";
        for (int i = 0; i < 10; i++) {
            sketch.increment(key);
        }

        int before = sketch.frequency(key);
        sketch.reset();
        int after = sketch.frequency(key);

        // 衰减后约为原来的一半
        assertEquals(before / 2, after);
    }
}