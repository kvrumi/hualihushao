package com.atguigu.flink.datastream.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class BaseSource {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合读取数据
        List<Integer> list = Arrays.asList(1, 2, 3, 6, 4, 5, 45, 90, 654, 7);
        DataStreamSource<Integer> ds1 = env.fromCollection(list);

        DataStreamSource<Integer> ds2 = env.fromElements(1, 2, 3, 4);

        // 从socket读取数据
        DataStreamSource<String> ds3 = env.socketTextStream("hadoop102", 8888);

        // 从数据生成器读取数据
        DataGeneratorSource<String> generatorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long aLong) throws Exception {
                        return UUID.randomUUID().toString() + " -> " + aLong;
                    }
                }, 100, RateLimiterStrategy.perSecond(1), Types.STRING
        );

        DataStreamSource<String> ds4 = env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "generatorSource");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
