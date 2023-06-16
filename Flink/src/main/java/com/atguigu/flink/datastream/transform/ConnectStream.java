package com.atguigu.flink.datastream.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class ConnectStream {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> ds3 = env.fromElements("a", "b", "c", "d", "e");

        ConnectedStreams<Integer, String> connect = ds1.connect(ds3);

        SingleOutputStreamOperator<String> processDs = connect.process(
                new CoProcessFunction<Integer, String, String>() {
                    // 处理第一条数据
                    @Override
                    public void processElement1(Integer value, CoProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(value.toString());
                    }

                    // 处理第二条数据
                    @Override
                    public void processElement2(String value, CoProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(value.toUpperCase());
                    }
                }
        );

        processDs.print("connect");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
