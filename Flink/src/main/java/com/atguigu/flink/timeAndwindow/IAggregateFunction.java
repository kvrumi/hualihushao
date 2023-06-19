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

/**
 * 增量聚合函数: 数据到达窗口就进行聚合。 (来一个聚合一次)
 *    1.ReduceFunction
 *        要求输入类型和输出类型必须一致。
 *    2.AggregateFunction
 *        输入类型和输出类型可以不一样，且可以按照自己的需求灵活的定义ACC类型，实现自定义的聚合。
 */
public class IAggregateFunction {

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

        ds.print("input");

        //需求: 每10秒统计UV(独立访客数)
        ds.keyBy(t -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<Event, HashSet<String>, Integer>() {
                            /**
                             * 创建累加器对象
                             */
                            @Override
                            public HashSet<String> createAccumulator() {
                                return new HashSet<>();
                            }

                            /**
                             * 每来一条数据累加一次
                             */
                            @Override
                            public HashSet<String> add(Event value, HashSet<String> accumulator) {
                                accumulator.add(value.getUser());
                                return accumulator;
                            }

                            /**
                             * 返回结果
                             */
                            @Override
                            public Integer getResult(HashSet<String> accumulator) {
                                return accumulator.size();
                            }

                            /**
                             * 一般不用实现。
                             */
                            @Override
                            public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
                                return null;
                            }
                        }
                )
                .print("aggregate");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
