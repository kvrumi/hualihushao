package com.atguigu.flink.sql.query;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.*;


public class SystemFunction {

    public static void main(String[] args) {

        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        String readSql =
                " create table t2 ( " +
                        " id String , " +
                        " vc Int , " +
                        " ts Bigint , " +
                        " ) with ( " +
                        " 'connector' = 'jdbc', " +
                        " 'url' = 'jdbc:mysql://hadoop102:3306/test', " +
                        " 'table-name' = 's2', " +
                        " 'username' = 'root', " +
                        " 'password' = '000000' " +
                        ")" ;

        tableEnvironment.executeSql(readSql);

        Table table = tableEnvironment.from("t2");

        table.select($("id").upperCase(),$("ts")).execute().print();
        //table.select(call("upper",$("id")),$("ts")).execute().print();

    }

}
















