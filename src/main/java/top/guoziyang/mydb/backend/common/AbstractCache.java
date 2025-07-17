package top.guoziyang.mydb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 */
public abstract class AbstractCache<T> {
    // 引用计数嘛，除了普通的缓存功能，还需要另外维护一个计数。
    // 除此以外，为了应对多线程场景，还需要记录哪些资源正在从数据源获取中
    //（从数据源获取资源是一个相对费时的操作）。于是有下面三个 Map：
    // 实际缓存的数据
    private HashMap<Long, T> cache;
    // 元素的引用个数
    private HashMap<Long, Integer> references;
    // 正在获取某资源的线程
    // me:记录请求的资源是否当前被某些线程获取
    private HashMap<Long, Boolean> getting;

    // 缓存的最大缓存资源数
    private int maxResource;

    // 缓存中元素的个数
    private int count = 0;
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }


//    于是，在通过 get() 方法获取资源时，首先进入一个死循环，来无限尝试从缓存里获取。
//    首先就需要检查这个时候是否有其他线程正在从数据源获取这个资源，如果有，就过会再来看看
    protected T get(long key) throws Exception {
        while(true) {
            lock.lock();
            // 请求的资源是否正在被其他线程获取
            if(getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                // me:跳过下面的步骤,继续回到循环开始请求获取资源
                continue;
            }

            // 当然如果资源在缓存中，就可以直接获取并返回了，记得要给资源的引用数 +1。
            if(cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(key);
                // 资源的引用数 +1
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }
            // 否则，如果缓存没满的话，就在 getting 中注册一下，该线程准备从数据源获取资源了。
            // 尝试获取该资源
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                // cache以满异常
                throw Error.CacheFullException;
            }
            // 资源的引用数 +1
            count ++;
            getting.put(key, true);
            lock.unlock();
            // 跳出循环获取
            break;
        }
        // 从数据源获取资源就比较简单了，直接调用那个抽象方法即可，获取完成记得从 getting 中删除 key。
        T obj = null;
        try {
            obj = getForCache(key);
        } catch(Exception e) {
            lock.lock();
            // me:说明没获取成功,资源的引用数 -1
            count --;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        // me:表明没人获取这个key?
        getting.remove(key);
        // 放入cache中
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();
        
        return obj;
    }

    // 释放一个缓存就简单多了，直接从references中减1，如果已经减到0了，就可以回源，并且删除缓存中所有相关的结构了：
    /**
     * 强行释放一个缓存
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key)-1;
            if(ref == 0) {
                // 已经减到0了，就可以回源，并且删除缓存中所有相关的结构了
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count --;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    // 缓存应当还有以一个安全关闭的功能，在关闭时，需要将缓存中所有的资源强行回源。
    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    // 这两个抽象类留给实例完成
    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
