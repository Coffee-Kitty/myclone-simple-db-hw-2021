package simpledb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 最近最久未使用
 *
 * 采用 hash表 + 双链表 实现
 */
public class LRUCache<K,V> {

    private class DLinkNode{
        private K key;
        private V value;
        private DLinkNode pre;
        private DLinkNode next;

        public DLinkNode() {
        }

        public DLinkNode(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DLinkNode dLinkNode = (DLinkNode) o;
            return Objects.equals(key, dLinkNode.key) && Objects.equals(value, dLinkNode.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    //hashMap k为key 值为linkNode
    //既然这里用了 key作为hash 则一定要注意key是否重写了hashcode equals
    private Map<K,DLinkNode> cache=new ConcurrentHashMap<K, DLinkNode>();
    private int size;
    private int capacity;
    private DLinkNode head;
    private DLinkNode tail;


    public LRUCache(int capacity) {
        this.capacity = capacity;
        size=0;
        head=new DLinkNode(null,null);
        tail=new DLinkNode(null,null);
        head.next=tail;
        tail.pre=head;
        tail.next=head;
        head.pre=tail;
    }

    public int getSize() {
        return size;
    }

    public DLinkNode getHead() {
        return head;
    }

    public DLinkNode getTail() {
        return tail;
    }

    public Map<K, DLinkNode> getCache() {
        return cache;
    }



    //get方法用于获取key对应的value
    ////必须要加锁，不然多线程链表指针会成环无法结束循环  synchronized是Java中的关键字，是一种同步锁
    public synchronized V get(K key){
        DLinkNode dLinkNode = cache.get(key);
        if(dLinkNode==null){
            return null;
        }
        //如果找到  先移动到队列头
        moveToHead(dLinkNode);
        return dLinkNode.value;
    }
    //removeTo方法用于将刚刚访问的节点移到队头
    private void moveToHead(DLinkNode node) {
        //先提出来
        node.next.pre = node.pre;
        node.pre.next = node.next;


        //再插入
        node.next = head.next;
        head.next.pre = node;
        node.pre = head;
        head.next = node;
    }

    //删除一个节点  内部使用
    private void remove(K key,DLinkNode node){
        node.next.pre = node.pre;
        node.pre.next = node.next;

        size--;
        cache.remove(key);
    }
    //提供一个删除K 的公有方法
    public void removeK(K key){
        DLinkNode dLinkNode = cache.get(key);
        remove(key,dLinkNode);//并未更改要删除的页的顺序！！！
    }

    public synchronized void put(K key,V value){
        //get命中 直接返回
        DLinkNode node = cache.get(key);
        if(node!=null){
            //修改值
            //移动到队列头
            node.value = value;
            moveToHead(node);
            return;
        }
        //否则添加
        DLinkNode dLinkNode = new DLinkNode(key, value);
        //每次入队都是队头
        addToHead(dLinkNode);
        size++;
        //注意添加到 cache
        cache.put(key,dLinkNode);

//        //存在问题  如果事务未提交  怎么可以丢弃呢 而且如果还是脏页  怎么没有刷盘呢？
        //交给外面来做吧
//        //需要查看是否超出容量
//        if(size>capacity){
//            //移除队尾  即最近最久未使用
//            removeTail();
//        }
    }
    private void addToHead(DLinkNode node) {
        node.pre = head;
        node.next = head.next;
        head.next.pre = node;
        head.next = node;
    }
    //移除队尾节点  注意不是tail 不是tail！！！！
    private void removeTail() {
        DLinkNode newTail = tail.pre;

        remove(newTail.key,newTail);

        size--;

    }

    //提供获取所有v的方法
    public List<V> getAllV(){
        List<V> vList=new ArrayList<>();
        //遍历双链表加入即可
        DLinkNode p=head;
        //vList.add(p.value);
        while (p.next!=tail){
            p=p.next;
            vList.add(p.value);
        }
        return vList;
    }
    //提供获取队尾的PageId的方法
    public V getTailV(){
        return tail.value;//并未调整顺序
    }

}
