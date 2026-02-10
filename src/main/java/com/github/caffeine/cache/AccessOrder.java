package com.github.caffeine.cache;

/**
 * 访问顺序接口 - 模仿Caffeine的AccessOrder
 * 用于解耦链表操作与具体Node类型
 */
public interface AccessOrder<E> {
    E getPreviousInAccessOrder();
    void setPreviousInAccessOrder(E prev);
    E getNextInAccessOrder();
    void setNextInAccessOrder(E next);
}