package com.atguigu.flink.sql.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FileConnector {

    public static void main(String[] args) {

        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        String readSql =
                " create table t1 ( " +
                        " id String , " +
                        " vc int , " +
                        " ts bigint , " +
                        " `file.size` BIGINT NOT NULL METADATA " +
                        " ) with ( " +
                        " 'connector' = 'filesystem' , " +
                        " 'path' = ''" +
                        " 'format' = 'csv'" +
                        " ) ";


        tableEnvironment.executeSql(readSql);

        Table table = tableEnvironment.sqlQuery("select id ,vc ,ts  , `file.size` as fs from t1  where vc > 200");
        tableEnvironment.createTemporaryView("t2",table);

        //File Connector write
        String writeSql =
                " create table t3 (" +
                        " id STRING , " +
                        " vc INT , " +
                        " ts BIGINT, " +
                        " fs BIGINT " +
                        ") WITH ( " +
                        " 'connector' = 'filesystem' , " +
                        " 'path' = 'output' ," +
                        " 'format' = 'json' " +
                        ")" ;

        tableEnvironment.executeSql(writeSql) ;

        //从t2查询数据，写入到t3表
        tableEnvironment.executeSql("insert into t3 select * from t2 ") ;

    }

}
