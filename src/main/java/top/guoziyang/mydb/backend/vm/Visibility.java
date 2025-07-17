package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManager;

public class Visibility {
    
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            return false;
        } else {
            // xmax > t.xid || t.isInSnapshot(xmax)
            // me:这个判断对于:
            // XID(Tj) > XID(Ti)
            // Tj in SP(Ti)
            // me:其实就是在描述事务Tj在哪些情况对事务Ti是不可见的
            // 1:Tj在Ti之后创建(XID(Tj) > XID(Ti))
            // 2:Ti创建时Tj还没提交(如果Tj提交了就没有冲突这种可能了),即Ti创建时Tj是活跃的(Tj in SP(Ti)),其中SP(Ti)记录Ti创建时所有处于active的事务

            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

//    (XMIN == Ti and                             // 由 Ti 创建且
//      XMAX == NULL                            // 还未被删除
//    )
//    or                                          // 或
//    (XMIN is commited and                       // 由一个已提交的事务创建且
//            (XMAX == NULL or                        // 尚未删除或
//            (XMAX != Ti and XMAX is not commited)   // 由一个未提交的事务删除
//    ))
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    // 于是，可重复读的隔离级别下，一个版本是否对事务可见的判断如下：
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;
        // me:t.isInSnapshot(xmin),事务xmin是否处于活跃
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
