package com.atguigu.flink.sql.wordcount;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class BaseTableAPI {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        TableEnvironment tableEnvironment1 = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());


        Table table = tableEnvironment.fromDataStream(ds);

        DataStream<Row> rowDataStream = tableEnvironment.toDataStream(table);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

}
