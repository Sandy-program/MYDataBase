package top.guoziyang.mydb.backend.tm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

public interface TransactionManager {
    // 开启一个新事务
    long begin();
    // 提交一个事务
    void commit(long xid);
    // 取消一个事务
    void abort(long xid);
    // 查询一个事务的状态是否是正在进行的状态
    boolean isActive(long xid);
    // 查询一个事务的状态是否是已提交
    boolean isCommitted(long xid);
    // 查询一个事务的状态是否是已取消
    boolean isAborted(long xid);
    // 关闭 TM
    void close();

//    两个静态方法：create() 和 open()，分别表示创建一个 xid 文件并创建 TM 和从一个已有的 xid 文件来创建 TM。
//    从零创建 XID 文件时需要写一个空的 XID 文件头，即设置 xidCounter 为 0，否则后续在校验时会不合法：
    public static TransactionManagerImpl create(String path) {
        // 构建事务日志文件（XID 文件）的完整路径，通常为 path + ".xid"
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        try {
            // 尝试创建新文件，如果返回 false 表示文件已存在
            if(!f.createNewFile()) {
                // 如果文件已存在，则触发 Panic 异常终止程序
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 创建文件过程中发生异常（如权限不足、路径无效等），也触发 Panic
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        // 文件的通道，用于后续高效读写操作
        FileChannel fc = null;
        // 保存以读写模式打开文件
        RandomAccessFile raf = null;
        try {
            // 以读写模式打开文件
            raf = new RandomAccessFile(f, "rw");
            // 获取该文件的通道，用于后续高效读写操作
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            // 理论上不会出现，因为前面已经创建了文件，但如果出现则触发 Panic
           Panic.panic(e);
        }

        // 准备一个缓冲区用于写入文件头信息，大小为 LEN_XID_HEADER_LENGTH（8 字节）
        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            // 定位到文件起始位置（偏移量为0）
            fc.position(0);
            // 写入初始化的文件头内容（当前是空数据）
            // 那数据为空,为什么还要写
            // 现在写入“空数据”的作用，是为这个文件头分配固定大小的空间，让整个文件结构清晰、可扩展。
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 创建并返回 TransactionManagerImpl 实例，传入已打开的 RandomAccessFile 和 FileChannel
        return new TransactionManagerImpl(raf, fc);
    }

    public static TransactionManagerImpl open(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }
}
