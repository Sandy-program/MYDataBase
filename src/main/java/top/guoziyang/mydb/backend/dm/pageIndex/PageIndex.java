package top.guoziyang.mydb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

// MYDB 用一个比较粗略的算法实现了页面索引，将一页的空间划分成了 40 个区间。
// 在启动时，就会遍历所有的页面信息，获取页面的空闲空间，安排到这 40 个区间中。
// insert 在请求一个页时，会首先将所需的空间向上取整，映射到某一个区间，随后取出这个区间的任何一页，都可以满足需求。

// PageIndex 的实现也很简单，一个 List 类型的数组。
public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    // me:每个区间的大小:8K/40
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    // me:一个长度40的数组,每个元素的类型为list
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    // 在上层模块使用完这个页面后，需要将其重新插入 PageIndex:
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    // 从 PageIndex 中获取页面也很简单，算出区间号，直接取即可：
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 算出区间号
            // me:按照剩余的空闲空间大小算区间号
            int number = spaceSize / THRESHOLD;
            if(number < INTERVALS_NO) number ++;
            while(number <= INTERVALS_NO) {
                if(lists[number].size() == 0) {
                    number ++;
                    continue;
                }
                // 返回的 PageInfo 中包含页号和空闲空间大小的信息。
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
