package com.atguigu.flink.timeAndwindow;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class FlinkEmbeddedWaterMark {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        // 有序流水位线
        ds.assignTimestampsAndWatermarks(
            WatermarkStrategy.<Event>forMonotonousTimestamps()
                    .withTimestampAssigner(
                            new SerializableTimestampAssigner<Event>() {
                                @Override
                                public long extractTimestamp(Event element, long recordTimestamp) {
                                    return element.getTimestamp();
                                }
                            }
                    )
        );
        // 乱序流生成
        ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                (element, recordTimestamp) -> element.getTimestamp()
                        )
        );

    }

}
