package com.atguigu.flink.timeAndwindow;


import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Flink对迟到数据的处理:
 *  1. 推迟水位线的推进(设定乱序时间,能兼顾大多数的迟到数据)
 *        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
 *  2. 延迟窗口的关闭（兼顾迟到很久的数据）
 *       .allowedLateness(Time.minutes(1))
 *       窗口会在水位线推进到窗口的结束时间开始计算，但是窗口不会关闭， 会再等待1分钟， 接下来来的数据，还可以正常
 *       进入该窗口，只要有新的数据到来，窗口会再次计算。
 *  3. 输出到侧输出流（漏网之鱼）
 *      .sideOutputLateData(lateOutputTag)
 */
public class LateData {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                                .withTimestampAssigner(
                                        (element, recordTimestamp) -> element.getTimestamp()
                                )
                );

        // 每10秒统计每个用户的点击次数
        OutputTag<Tuple2<String, Integer>> late = new OutputTag<>("late");

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = ds.map(event -> Tuple2.of(event.getUser(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .sum(1);

        sumDS.print("sum");

        sumDS.getSideOutput(late)
                .print("late");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
