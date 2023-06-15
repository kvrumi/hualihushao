package com.atguigu.flink.datastream.sink;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class IFileSink {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启检查点
        env.enableCheckpointing(5000L);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        // 将流中的数据写入文件
        FileSink<Event> fileSink = FileSink.<Event>forRowFormat(new Path("output"), new SimpleStringEncoder<>())
                // 滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 文件的最大的大小
                                .withMaxPartSize(MemorySize.parse("10m"))
                                // 间隔多久滚动一次
                                .withRolloverInterval(Duration.ofSeconds(10))
                                // 文件多久没有数据进入就滚动
                                .withInactivityInterval(Duration.ofSeconds(5))
                                .build()
                )
                .withBucketCheckInterval(1000L) // 检查间隔
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("atguigu-") // 文件名前缀
                                .withPartSuffix(".log") // 文件名后缀
                                .build()
                )
                // 目录滚动
                .withBucketAssigner(
                        new DateTimeBucketAssigner<>("yyyy-MM-dd HH-mm")
                )
                .build();

        ds.sinkTo(fileSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
