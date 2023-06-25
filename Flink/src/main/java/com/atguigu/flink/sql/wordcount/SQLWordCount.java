package com.atguigu.flink.sql.wordcount;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SQLWordCount {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> ds = env.addSource(new ClickSource());
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        Table table = tableEnvironment.fromDataStream(ds);

        tableEnvironment.createTemporaryView("t1", ds);

        Table result = tableEnvironment.sqlQuery(
                "select *" +
                        "from t1" +
                        "where user <> 'Tom'"
        );

        result.execute().print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
