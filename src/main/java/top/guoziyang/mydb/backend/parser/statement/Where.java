package top.guoziyang.mydb.backend.parser.statement;

public class Where {
    // me:这么设计怎么感觉where只能有两个判断条件,不能三个判断以上的条件,比如说field1=a and field2=b and field3=c
    // me:为了解决这个问题,我们可以Where中嵌套Where,如下面的结构
    // public class Where{
    //    public SingleExpression singleExp1;
    //    public String logicOp;
    //    public Where where;
    // }
    // me:其中 singleExp1与where的逻辑关系为logicOp

    // "目前MYDB的Where只支持两个条件的与和或。例如有条件的 Delete,计算 Where,最终就需要获取到条件范围内所有的UID."
    // me:上面这句话是作者原话,说明Where 确实最多支持两个条件的与和或
    public SingleExpression singleExp1;
    public String logicOp;
    public SingleExpression singleExp2;
}
