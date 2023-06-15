package com.atguigu.flink.datastream.sink;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class IJdbcSink {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        SinkFunction<Event> jdbcSink = JdbcSink.<Event>sink(
                "replace into clicks1(user, url, ts) value(?,?,?)",
                // Sql赋值
                new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                        preparedStatement.setString(1, event.getUser());
                        preparedStatement.setString(2, event.getUrl());
                        preparedStatement.setLong(3, event.getTimestamp());
                    }
                },
                // 执行参数
                JdbcExecutionOptions.builder()
                        .withBatchSize(5) // 批次大小
                        .withBatchIntervalMs(10000) // 批次间隔时间， 超过指定时间也要写入
                        .withMaxRetries(3)
                        .build(),
                // 连接配置
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withUsername("root")
                        .withPassword("000000")
                        .build()
        );

        ds.addSink(jdbcSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
