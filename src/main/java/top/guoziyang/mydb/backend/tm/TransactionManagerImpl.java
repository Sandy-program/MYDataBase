package top.guoziyang.mydb.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

public class TransactionManagerImpl implements TransactionManager {

//    XID 文件给每个事务分配了一个字节的空间，用来保存其状态。
//    同时，在 XID 文件的头部，还保存了一个 8 字节的数字，记录了这个 XID 文件管理的事务的个数。
//    于是，事务 xid 在文件中的状态就存储在 (xid-1)+8 字节处，xid-1 是因为 xid 0（Super XID）的状态不需要记录。

//    每一个事务都有一个 XID，这个 ID 唯一标识了这个事务。事务的 XID 从 1 开始标号，并自增，不可重复。
//    并特殊规定 XID 0 是一个超级事务（Super Transaction）。
//    当一些操作想在没有申请事务的情况下进行，那么可以将操作的 XID 设置为 0。XID 为 0 的事务的状态永远是 committed。
    // XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    // me:用一个字节保存事务的状态
    private static final int XID_FIELD_SIZE = 1;

//    TransactionManager 维护了一个 XID 格式的文件，用来记录各个事务的状态。MYDB 中，每个事务都有下面的三种状态：
//    active,正在进行,尚未结束
//    committed,已提交
//    aborted,已撤销（回滚）

    // 事务的三种状态
    // 事务的状态是否是正在进行
    private static final byte FIELD_TRAN_ACTIVE   = 0;
    // 事务的状态是否是已提交
	private static final byte FIELD_TRAN_COMMITTED = 1;
	// 事务的状态是否是已取消
	private static final byte FIELD_TRAN_ABORTED  = 2;

    // 超级事务,永远为commited状态
    public static final long SUPER_XID = 0;

    static final String XID_SUFFIX = ".xid";

    // 以读写模式打开文件
    private RandomAccessFile file;
    // 文件的通道，用于后续高效读写操作
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

//    在构造函数创建了一个 TransactionManager 之后，
//    首先要对 XID 文件进行校验，以保证这是一个合法的 XID 文件。
//    校验的方式也很简单，通过文件头的 8 字节数字反推文件的理论长度，与文件的实际长度做对比。如果不同则认为 XID 文件不合法。
//    对于校验没有通过的，会直接通过 panic 方法，强制停机。在一些基础模块中出现错误都会如此处理，无法恢复的错误只能直接停机
    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        // 初始化文件长度为0
        long fileLen = 0;
        try {
            // 获取当前XID文件的实际长度（单位：字节）
            fileLen = file.length();
        } catch (IOException e1) {
            // 如果获取文件长度失败，说明文件可能损坏，触发 Panic 异常
            Panic.panic(Error.BadXIDFileException);
        }
        // 如果文件总长度小于XID文件头所需最小长度，说明文件结构不完整，非法
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        // 创建一个 ByteBuffer 来读取文件头数据，大小等于文件头长度
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            // 将文件指针定位到文件开头（偏移量为0），准备读取文件头
            fc.position(0);
            // 从文件通道中读取 LEN_XID_HEADER_LENGTH 字节的数据到缓冲区
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 从缓冲区中解析出 xidCounter（事务计数器），用于后续合法性判断
        this.xidCounter = Parser.parseLong(buf.array());
        // 根据当前 xidCounter + 1 计算理论上文件应该达到的长度
        // getXidPosition 是一个辅助方法，返回某个 XID 应该在文件中的偏移位置
        // 根据事务xid取得其在xid文件中对应的位置
        long end = getXidPosition(this.xidCounter + 1);
        // 如果理论长度和实际文件长度不一致，说明文件内容被篡改或损坏
        if(end != fileLen) {
            // 抛出错误，认为这是一个非法的 XID 文件
            Panic.panic(Error.BadXIDFileException);
        }
    }

    // 根据事务xid取得其在事务xid文件中对应的位置
    // me:比如第1个事务开始在LEN_XID_HEADER_LENGTH
    // me:比如第2个事务开始在LEN_XID_HEADER_LENGTH+(2-1)*XID_FIELD_SIZE
    // me:比如第3个事务开始在LEN_XID_HEADER_LENGTH+(3-1)*XID_FIELD_SIZE
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid-1)*XID_FIELD_SIZE;
    }

    // commit() 和 abort() 方法就可以直接借助 updateXID() 方法实现。
    /**
     * 更新 xid 事务的状态为 status
     * @param xid    事务唯一标识符
     * @param status 要设置的新状态（例如：active, committed, aborted）
     */
    // 更新xid事务的状态为status
    private void updateXID(long xid, byte status) {
        // 根据事务 ID 计算它在 XID 文件中的偏移位置
        long offset = getXidPosition(xid);
        // 创建一个大小为 XID_FIELD_SIZE 的字节数组，用于存储事务状态
        // 状态只占第一个字节，其余可能是填充或预留字段
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        // 将字节数组封装成 ByteBuffer，便于通过 FileChannel 写入文件
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            // me:如果是添加事务的话,其实就是在XID文件最后面添加一个事务状态
            // 定位文件通道到指定偏移位置，准备写入事务状态
            fc.position(offset);
            // 将状态信息写入文件
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 强制将缓冲区内容刷新到磁盘，保证数据持久化
            // 参数 false 表示不强制元数据同步（仅同步文件内容）
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 将XID加一，并更新XID Header
    private void incrXIDCounter() {
        xidCounter ++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 注意，这里的所有文件操作，在执行后都需要立刻刷入文件中，防止在崩溃后文件丢失数据，
            // fileChannel 的 force() 方法，强制同步缓存内容到文件中，类似于 BIO 中的 flush() 方法。
            // force 方法的参数是一个布尔，表示是否同步文件的元数据（例如最后修改时间等）。
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * begin() 方法会开始一个事务。
     * 更具体地说：
     * 1. 设置 xidCounter + 1 的事务状态为 active（活跃）
     * 2. 将 xidCounter 自增
     * 3. 更新文件头信息
     */
//    begin() 方法会开始一个事务，更具体的，首先设置 xidCounter+1 事务的状态为 active，随后 xidCounter 自增，并更新文件头。
    // 开始一个事务，并返回XID
    public long begin() {
        counterLock.lock();
        try {
            // 新事务的 XID 是当前计数器值加一
            long xid = xidCounter + 1;
            // 将这个新事务的状态设置为“活跃”（FIELD_TRAN_ACTIVE），写入 XID 文件中
            updateXID(xid, FIELD_TRAN_ACTIVE);
            // 增加事务计数器，表示已经分配了一个新的事务 ID
            incrXIDCounter();
            // 返回新生成的事务 ID（XID）
            return xid;
        } finally {
            // 无论 try 块是否正常完成，都释放锁，防止死锁
            counterLock.unlock();
        }
    }

    // 提交XID事务
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 回滚XID事务
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    // isActive()、isCommitted() 和 isAborted() 都是检查一个 xid 的状态，可以用一个通用的方法解决：
    // 检测XID事务是否处于status状态
    private boolean checkXID(long xid, byte status) {
        // 根据事务xid取得其在事务xid文件中对应的位置
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            // 偏移到对应位置
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
