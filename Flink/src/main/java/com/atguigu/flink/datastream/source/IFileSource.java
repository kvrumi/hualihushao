package com.atguigu.flink.datastream.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

public class IFileSource {
    // 从文件中读取数据
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(StandardCharsets.UTF_8.name()),
                new Path("input/words.txt")
//                new Path("hdfs://hadoop102:8020/README.txt")
        ).build();

        DataStreamSource<String> ds = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource");

        ds.print("fileSource");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
