package com.atguigu.flink.timeAndwindow;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * 统计url的点击次数
 *    通过增量聚合和全窗口函数的结合来实现。
 */
public class IUrlViewCount {

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

        ds.map(event -> Tuple2.of(event.getUser(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Long>, UrlViewCount>() {
                            @Override
                            public Tuple2<String, Long> createAccumulator() {
                                return Tuple2.of("", 0L);
                            }

                            @Override
                            public Tuple2<String, Long> add(Tuple2<String, Integer> value, Tuple2<String, Long> accumulator) {
                                return Tuple2.of(value.f0, accumulator.f1 + 1);
                            }

                            @Override
                            public UrlViewCount getResult(Tuple2<String, Long> accumulator) {
                                return new UrlViewCount(null, null, accumulator.f0, accumulator.f1);
                            }

                            @Override
                            public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                                return null;
                            }
                        }
                        ,
                        new ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>.Context context, Iterable<UrlViewCount> elements, Collector<UrlViewCount> out) throws Exception {
                                // 获取窗口信息
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                // 获取窗口中的数据
                                // 窗口中的数据已经被增量聚合函数聚合成一个结果， 所以, 全窗口函数中的elements中只会有一条数据。
                                UrlViewCount urlViewCount = elements.iterator().next();

                                urlViewCount.setStart(start);
                                urlViewCount.setEnd(end);
                                // 写出数据
                                out.collect(urlViewCount);
                            }
                        }
                )
                .print("UrlViewCount");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
