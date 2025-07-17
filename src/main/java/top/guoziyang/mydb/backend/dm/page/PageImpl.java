package top.guoziyang.mydb.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
// 页面缓存
// 这里参考大部分数据库的设计，将默认数据页大小定为 8K。
// 如果想要提升向数据库写入大量数据情况下的性能的话，也可以适当增大这个值。

// 上一节我们已经实现了一个通用的缓存框架，那么这一节我们需要缓存页面，就可以直接借用那个缓存的框架了。
// 但是首先，需要定义出页面的结构。
// 注意这个页面是存储在内存中的，与已经持久化到磁盘的抽象页面有区别。

public class PageImpl implements Page {
    // pageNumber是这个页面的页号，该页号从1开始。
    private int pageNumber;
    // data 就是这个页实际包含的字节数据。
    private byte[] data;
    // dirty 标志着这个页面是否是脏页面，在缓存驱逐的时候，脏页面需要被写回磁盘。
    private boolean dirty;
    private Lock lock;

    // 这里保存了一个 PageCache（还未定义）的引用，
    // 用来方便在拿到 Page 的引用时可以快速对这个页面的缓存进行释放操作。
    private PageCache pc;

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void release() {
        pc.release(this);
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isDirty() {
        return dirty;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getData() {
        return data;
    }

}
