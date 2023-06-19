package com.atguigu.flink.timeAndwindow;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

public class PvUv {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.getTimestamp();
                                            }
                                        }
                                )
                );

        //需求: 求pv / uv
        // ACC: Tuple2<Long ,HashSet<String> > :
        //      Long : pv
        //      HashSet: uv
        ds.keyBy(event -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double>() {
                            // 创建累加器
                            @Override
                            public Tuple2<Long, HashSet<String>> createAccumulator() {
                                return Tuple2.of(0L, new HashSet<String>());
                            }

                            // 每来一条数据处理一次
                            @Override
                            public Tuple2<Long, HashSet<String>> add(Event value, Tuple2<Long, HashSet<String>> accumulator) {
                                accumulator.f0++;
                                accumulator.f1.add(value.getUser());
                                return accumulator;
                            }

                            // 返回结果
                            @Override
                            public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
                                return (double) accumulator.f0 / accumulator.f1.size();
                            }

                            @Override
                            public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> a, Tuple2<Long, HashSet<String>> b) {
                                return null;
                            }
                        }
                )
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
