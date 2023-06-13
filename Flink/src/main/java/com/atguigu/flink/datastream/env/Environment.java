package com.atguigu.flink.datastream.env;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
    流式执行环境的创建
    1.LocalStreamEnvironment  本地环境

    2. RemoteStreamEnvironment 远程环境
 */
public class Environment {

    public static void main(String[] args) {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取外部传入的参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");

        DataStreamSource<String> ds = env.socketTextStream(hostname, port);
        // 转换处理
        ds.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    for (String word : line.split(" ")) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1)
                .print();
        // 启动
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
