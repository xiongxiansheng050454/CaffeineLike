package com.github.caffeine.cache;

/**
 * W-TinyLFU 分区类型标识
 * 对应 Caffeine 的 Node 中的 queueType 字段
 */
public enum QueueType {
    WINDOW(0),      // 窗口区（1%容量）
    PROBATION(1),   // 候选区（Main的20%）
    PROTECTED(2);   // 保护区（Main的80%）

    private final int value;

    QueueType(int v) {
        this.value = v;
    }

    public int value() {
        return value;
    }

    public static QueueType fromValue(int v) {
        return switch (v) {
            case 0 -> WINDOW;
            case 1 -> PROBATION;
            case 2 -> PROTECTED;
            default -> throw new IllegalArgumentException("Invalid queue type: " + v);
        };
    }
}