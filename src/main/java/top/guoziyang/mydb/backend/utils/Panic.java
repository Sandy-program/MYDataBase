package top.guoziyang.mydb.backend.utils;

// me:感觉这个类都可以放入异常类
public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        // 立即结束程序的执行，并返回一个状态码给操作系统
        System.exit(1);
    }
}
