package top.guoziyang.mydb.backend.dm.pageIndex;

public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        // 页号
        this.pgno = pgno;
        // 空闲空间大小
        this.freeSpace = freeSpace;
    }
}
