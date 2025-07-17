package top.guoziyang.mydb.backend.dm.page;

import java.util.Arrays;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.utils.Parser;

// MYDB 对于普通数据页的管理就比较简单了。
// 一个普通页面以一个 2 字节无符号数起始，表示这一页的空闲位置的偏移。
// 剩下的部分都是实际存储的数据。
// 所以对普通页的管理，基本都是围绕着对 FSO（Free Space Offset）进行的。
// me:FSO就空闲位置的起始地址

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    // me:实际上最大的可存放的空闲空间,PageCache.PAGE_SIZE - OF_DATA=8K-2
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    // me:将ofData作为长度2的数组写到raw中OF_FREE~OF_DATA的范围中
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    // 获取pg的FSO
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    // me:获取FSO,即获取空闲位置的起始地址(而这个数据记录在页面中的前2个字节)
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    // 向页面插入数据：
    // 将raw插入pg中，返回插入位置
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short) (offset + raw.length));
        return offset;
    }

    // 获取页面的空闲空间大小
    // me:页面大小8K-页面前2个字节中记录的空闲空间的开始地址
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int) getFSO(pg.getData());
    }

    // 剩余两个函数 recoverInsert() 和 recoverUpdate() 用于在数据库崩溃后重新打开时，恢复例程直接插入数据以及修改数据使用。
    // 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    // me:对于页面Page,要放入数据raw,并取输入max(pg前2个字节记录的offset,输入参数offset+输入参数raw.length)来作为实际的偏移量
    // me:如果输入参数offset+输入参数raw.length<pg前2个字节记录的offset,其实就说明了这块数据raw是往页面page的中间插入的
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        // me:设置为脏数据
        pg.setDirty(true);
        // me:根据偏移量offset把数据raw插入到页面数据pg.getData()
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        // me:更新偏移量offset
        short rawFSO = getFSO(pg.getData());
        if (rawFSO < offset + raw.length) {
            // me:如果输入参数offset+输入参数raw.length>pg前2个字节记录的offset,就更新pg前2个字节记录的offset
            setFSO(pg.getData(), (short) (offset + raw.length));
        }
    }

    // me:暂时不知道作用在哪,为什么不用更新偏移量offset,只是恢复更新,没有新的数据插入,所以头部前2个字节的偏移量offset不用更新
    // 将raw插入pg中的offset位置,不更新update
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
