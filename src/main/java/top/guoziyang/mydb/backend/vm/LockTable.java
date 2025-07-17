package top.guoziyang.mydb.backend.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {
    // me:key是XID,value是这个XID是获得的UID列表
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    // me:key是UID,value是这个UID被某个XID所持有
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    // me:key是XID,value是这个XID正在等待的UID列表
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常
    // me:xid事务id,uid对象(数据项)id
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // me:这个对象(uid)是否已经被这个事务(uid)获得了,如果已经获得返回true
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            // me:当前uid是否被某个事务所拥有
            if(!u2x.containsKey(uid)) {
                // me:当前uid没有被某个事务所拥有,则让这个uid被当前事务xid所拥有
                u2x.put(uid, xid);
                // me:把这个uid假如到xid所拥有的资源的list中
                putIntoList(x2u, xid, uid);
                return null;
            }
            // me:表明事务xid正在等待数据项uid
            waitU.put(xid, uid);
            //putIntoList(wait, xid, uid);
            // me:将xid加入到uid的等待队列中
            putIntoList(wait, uid, xid);
            // me:核心代码,是否有死锁hasDeadLock()
            // me:这个死锁感觉判断得很low,直接判断是否有死锁,我的理解应该是只需要判断
            if(hasDeadLock()) {
                // me:放弃事务xid正在等待数据项uid
                waitU.remove(xid);
                // me:放弃将xid加入到uid的等待队列中
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            // 调用add,如果需要等待的话,会返回一个上了锁的Lock对象.调用方在获取到该对象时,需要尝试获取该对象的锁,由此实现阻塞线程的目的,例如:
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;

        } finally {
            lock.unlock();
        }
    }

    // 在一个事务 commit 或者 abort 时，就可以释放所有它持有的锁，并将自身从等待图中删除。
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if(l != null) {
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> l = wait.get(uid);
        if(l == null) return;
        assert l.size() > 0;

        while(l.size() > 0) {
            long xid = l.remove(0);
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if(l.size() == 0) wait.remove(uid);
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for(long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) {
                continue;
            }
            stamp ++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        // me:记录当前节点的访问时间戳为当前的 stamp
        xidStamp.put(xid, stamp);
        // me:查找当前节点 xid 正在等待的资源 uid（waitU 是一个 Map，key 是 xid，value 是 uid）
        Long uid = waitU.get(xid);
        // me:如果没有等待任何资源（uid 为 null），说明这条路径到头了，没环
        if(uid == null) return false;
        // me:查找持有这个资源 uid 的事务 xid（u2x 是一个 Map，key 是 uid，value 是 xid）
        Long x = u2x.get(uid);
        assert x != null;
        // me:继续对这个事务 xid 进行 DFS，形成一个调用链
        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        // me:这个事务没有拥有任何资源
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                // me:发现当前事务已经拥有了这个资源(uid0)
                return true;
            }
        }
        return false;
    }

}
