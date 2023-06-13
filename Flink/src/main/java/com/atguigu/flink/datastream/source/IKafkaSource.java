package com.atguigu.flink.datastream.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class IKafkaSource {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092")
                .setGroupId("test")
                .setTopics("topic_test")
                // 只针对没有key的消息
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 默认使用记录的offset消费，如果涉及到offset重置，选择重置到头
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000")
                .build();

        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        ds.print("test");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
