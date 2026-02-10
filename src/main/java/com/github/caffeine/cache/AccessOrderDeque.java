package com.github.caffeine.cache;

/**
 * 访问顺序双端队列 - 模仿Caffeine的LinkedDeque
 * 维护节点从LRU(头)到MRU(尾)的顺序
 */
public final class AccessOrderDeque<K, V> {
    private final Node<K, V> dummy; // 哨兵节点

    public AccessOrderDeque() {
        // 使用null创建哨兵节点（不存储实际数据）
        this.dummy = new Node<>(null, null, 0, 0, null, null);
        dummy.setPreviousInAccessOrder(dummy);
        dummy.setNextInAccessOrder(dummy);
    }

    /** 添加到尾部（MRU位置）- O(1) */
    public void add(Node<K, V> e) {
        Node<K, V> prev = dummy.getPreviousInAccessOrder();
        e.setPreviousInAccessOrder(prev);
        e.setNextInAccessOrder(dummy);
        prev.setNextInAccessOrder(e);
        dummy.setPreviousInAccessOrder(e);
    }

    /** 移除并返回头部（LRU位置）- O(1) */
    public Node<K, V> removeFirst() {
        Node<K, V> next = dummy.getNextInAccessOrder();
        if (next == dummy) return null;
        remove(next);
        return next;
    }

    /** 移除指定节点 - O(1) */
    public void remove(Node<K, V> e) {
        Node<K, V> prev = e.getPreviousInAccessOrder();
        Node<K, V> next = e.getNextInAccessOrder();

        if (prev != null) prev.setNextInAccessOrder(next);
        if (next != null) next.setPreviousInAccessOrder(prev);

        // 清理引用帮助GC
        e.setPreviousInAccessOrder(null);
        e.setNextInAccessOrder(null);
    }

    /** 移动到尾部（标记为最近使用）- O(1) */
    public void moveToTail(Node<K, V> e) {
        remove(e);
        add(e);
    }

    public boolean isEmpty() {
        return dummy.getNextInAccessOrder() == dummy;
    }

    public int size() {
        int count = 0;
        Node<K, V> current = dummy.getNextInAccessOrder();
        while (current != dummy) {
            count++;
            current = current.getNextInAccessOrder();
        }
        return count;
    }
}