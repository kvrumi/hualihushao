package com.atguigu.flink.state;

import com.atguigu.flink.pojo.UserBehavior;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class TopN {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(StandardCharsets.UTF_8.name()),
                new Path("input/UserBehavior.csv")
        ).build();

        WatermarkStrategy<UserBehavior> watermarkStrategy = WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(
                        (element, recordTimestamp) -> element.getTimestamp() * 1000
                );

        SingleOutputStreamOperator<UserBehavior> ds = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource")
                .map(
                        line -> {
                            String[] words = line.split(",");
                            return new UserBehavior(
                                    Long.valueOf(words[0]),
                                    Long.valueOf(words[1]),
                                    Integer.valueOf(words[2]),
                                    words[3],
                                    Long.valueOf(words[4])
                            );
                        }
                )
                .filter(bean -> "pv".equals(bean.getBehavior()))
                .assignTimestampsAndWatermarks(watermarkStrategy);


        ds.keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
                .aggregate(
                        new AggregateFunction<UserBehavior, Long, HotItem>() {
                            @Override
                            public Long createAccumulator() {
                                return null;
                            }

                            @Override
                            public Long add(UserBehavior value, Long accumulator) {
                                return null;
                            }

                            @Override
                            public HotItem getResult(Long accumulator) {
                                return null;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        }
                );

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class HotItem{

        //定义窗口范围 时间窗口
        private Long start;
        private Long end;
        //定义统计的指标
        private Long itemId;
        private Long count;

    }

}
