package com.atguigu.flink.sql.wordcount;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

public class TableAPIWordCount {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        Table table = tableEnvironment.fromDataStream(ds);

        table.printSchema();
        //import org.apache.flink.table.api.Expressions
        //Table result = table.where(Expressions.$("user").isEqual("Tom"));
        Table result = table.where($("user").isEqual("Tom"))
                .select($("user"), $("url"), $("ts"));

        result.execute().print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

}
