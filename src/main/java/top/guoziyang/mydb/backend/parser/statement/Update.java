package top.guoziyang.mydb.backend.parser.statement;

//<update statement>
//    update <table name> set <field name>=<value> [<where statement>]
//        update student set name = "ZYJ" where id = 5
public class Update {
    public String tableName;
    public String fieldName;
    public String value;
    public Where where;
}
