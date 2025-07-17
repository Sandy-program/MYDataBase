package top.guoziyang.mydb.backend.common;


// Java 中，将数组看作一个对象，在内存中，也是以对象的形式存储的。
// 而 c、cpp 和 go 之类的语言，数组是用指针来实现的。这就是为什么有一种说法：
//
// 只有 Java 有真正的数组
// 于是，我写了一个 SubArray 类，来（松散地）规定这个数组的可使用范围:

public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
