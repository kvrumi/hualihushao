package com.atguigu.flink.sql.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class UpsertKafkaConnector {

    public static void main(String[] args) {

        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        String readSql =
                " create table t1 ( " +
                        " id String , " +
                        " vc Int , " +
                        " ts BIGINT " +
                        " ) WITH ( " +
                        " 'connector' = 'kafka', " +
                        " 'topic' = 'topicA', " +
                        " 'properties.bootstrap.servers' = 'hadoop102:9092', " +
                        " 'properties.group.id' = 'flinkSql', " +
                        " 'scan.startup.mode' = 'latest-offset', " +
                        " 'value.format' = 'csv' " +
                        ")";

        tableEnvironment.executeSql(readSql);

        Table table = tableEnvironment.sqlQuery("select id , sum(vc) svc from t1 group by id ");
        tableEnvironment.createTemporaryView("t2",table);


        String writeSql =
                " create table t3 (" +
                        " id STRING , " +
                        " svc INT , " +
                        " PRIMARY KEY (id) NOT ENFORCED " +
                        ") WITH (" +
                        " 'connector' = 'upsert-kafka', " +
                        " 'topic' = 'topicB', " +
                        " 'properties.bootstrap.servers' = 'hadoop102:9092', " +
                        //" 'sink.transactional-id-prefix' = 'flink" + RandomUtils.nextInt(0,100) + "' ," +
                        " 'key.format' = 'csv'," +
                        " 'value.format' = 'csv' " +
                        ")";
        tableEnvironment.executeSql(writeSql);

        tableEnvironment.executeSql("insert into t3 select * from t2");

    }

}
