package top.guoziyang.mydb.backend.parser.statement;

// <insert statement>
//    insert into <table name> values <value list>
//        insert into student values 5 "Zhang Yuanjia" 22
// me:这个insert也相对简陋,应该是不能只插某几个字段值,必须按属性顺序把所有字段插入
// 从表格的insert方法中调用string2Entry()可以看出来
public class Insert {
    public String tableName;
    public String[] values;
}
