package com.atguigu.flink.datastream.sink;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JdbcXASink {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        SinkFunction<Event> jdbcSink = JdbcSink.<Event>exactlyOnceSink(
                "replace into clicks1(user, url, ts) values(?,?,?)",
                new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                        preparedStatement.setString(1, event.getUser());
                        preparedStatement.setString(2, event.getUrl());
                        preparedStatement.setLong(3, event.getTimestamp());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(10000)
                        .withMaxRetries(0)
                        .build(),
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true)
                        .build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        MysqlXADataSource mysqlXADataSource = new MysqlXADataSource();
                        mysqlXADataSource.setURL("jdbc:mysql://hadoop102:3306/test");
                        mysqlXADataSource.setUser("root");
                        mysqlXADataSource.setPassword("000000");
                        return mysqlXADataSource;
                    }
                }
        );

        ds.addSink(jdbcSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
