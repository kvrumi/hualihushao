package com.atguigu.flink.sql.connector;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DataGenConnector {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        String dataGenSql =
                " create table t1 (" +
                        " id String , " +
                        " vc int , " +
                        " ts bigint , " +
                        " ) with ( " +
                        " 'connector' = 'datagen' , " +
                        " 'fields.id.kind' = 'random' ," +
                        " 'fields.id.length' = '5' ," +
                        " 'fields.vc.kind' = 'random' ," +
                        " 'fields.ts.kind' = 'sequence' ," +
                        " 'fields.ts.start' = '1000' , " +
                        " 'fields.ts.end' = '10000'  " +
                        " ) ";

        tableEnvironment.executeSql(dataGenSql);

        String printSql =
                " create table t2 (" +
                        " id String , " +
                        " vc int , " +
                        " ts bigint , " +
                        " ) with ( " +
                        " 'connector' = 'print' " +
                        " ) ";

        tableEnvironment.executeSql(printSql);

        tableEnvironment.executeSql("insert into t2 select * from t1");

    }

}
