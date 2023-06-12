package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 有界流式wordCount
public class BoundedStreamWordCount {
    public static void main(String[] args) {

//        1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        2. 读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/words.txt");
//        3. 转换处理
        streamSource.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1)
//        4. 输出结果
                .print();
//        5. 启动执行
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
