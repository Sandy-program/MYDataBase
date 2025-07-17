package top.guoziyang.mydb.backend.parser.statement;

// <delete statement>
//    delete from <table name> <where statement>
//        delete from student where name = "Zhang Yuanjia"
public class Delete {
    public String tableName;
    public Where where;
}
