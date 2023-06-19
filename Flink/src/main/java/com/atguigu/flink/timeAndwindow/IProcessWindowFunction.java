package com.atguigu.flink.timeAndwindow;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * 全窗口函数: 收集齐所有的数据后，统一计算一次输出结果。 可以获取到一些窗口相关的信息(窗口的起始和结束时间)
 * 1. WindowFunction(过时)
 * 2. ProcessWindowFunction
 */
public class IProcessWindowFunction {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        (element, recordTimestamp) -> element.getTimestamp()
                                )
                );

        ds.print("ds");

        //需求: 每10秒统计每个user的点击次数 需要包含窗口信息
        //需求: 统计10秒内每个user的点击次数， 5秒输出一次结果。
        ds.map(event -> Tuple2.of(event.getUser(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .window(
//                        TumblingEventTimeWindows.of(Time.seconds(10))
                        SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))
                )
                .process(
                        new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                                // 汇总每个user的点击次数
                                long count = 0L;
                                for (Tuple2<String, Integer> element : elements) {
                                    count++;
                                }
                                // 获取窗口的信息
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                // 输出
                                System.out.println("窗口[" + start + " - " + end + ")，用户" + s + "的点击次数为" + count);
                            }
                        }
                )
                .print("process");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
