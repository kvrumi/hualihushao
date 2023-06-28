package com.atguigu.flink.sql.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class JdbcConnector {

    public static void main(String[] args) {

        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        String readSql =
                " create table t1 (" +
                        " id STRING , " +
                        " vc INT , " +
                        " ts BIGINT " +
                        ") WITH (" +
                        " 'connector' = 'jdbc', " +
                        " 'url' = 'jdbc:mysql://hadoop102:3306/test', " +
                        " 'table-name' = 's1', " +
                        " 'username' = 'root', " +
                        " 'password' = '000000' " +
                        ")";

        tableEnvironment.executeSql(readSql);

        Table table = tableEnvironment.sqlQuery("select * form t1");
        tableEnvironment.createTemporaryView("t2", table);

        String writeSql =
                " create table t2 (" +
                        " id STRING , " +
                        " vc INT , " +
                        " ts BIGINT ," +
                        " PRIMARY KEY (id) NOT ENFORCED " +
                        ") WITH (" +
                        " 'connector' = 'jdbc', " +
                        " 'url' = 'jdbc:mysql://hadoop102:3306/test', " +
                        " 'table-name' = 's2', " +
                        " 'username' = 'root', " +
                        " 'password' = '000000' " +
                        ")";

        tableEnvironment.executeSql(writeSql);
        tableEnvironment.executeSql("insert into t2 select * from t1");

    }

}
