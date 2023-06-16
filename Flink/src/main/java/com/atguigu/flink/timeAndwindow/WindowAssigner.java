package com.atguigu.flink.timeAndwindow;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowAssigner {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> eds = ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
        );

        // keyBy
        //计数
        ds.keyBy(Event::getUser)
//                .countWindow(5)
                .countWindow(5, 3)
                .sum("timestamp");
        // 时间
        ds.map(event -> Tuple2.of(event.getUser(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .window(
                        // 滚动
//                        TumblingProcessingTimeWindows.of(Time.seconds(5)) // 处理时间
                        TumblingEventTimeWindows.of(Time.seconds(5)) // 事件时间
                        // 滑动
//                        SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)) // 处理时间
//                        SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5) // 事件时间
                        // 会话
//                        ProcessingTimeSessionWindows.withGap(Time.seconds(5)) // 处理时间
//                        EventTimeSessionWindows.withGap(Time.seconds(5)) // 事件时间
                        // 全局
//                        GlobalWindows.create()
                )
                .sum(
                        1
                ).print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
