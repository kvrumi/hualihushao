package com.atguigu.flink.state;

import com.atguigu.flink.pojo.Event;
import com.sun.scenario.effect.Offset;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaToFlinkToKafkaEOS {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092")
                .setGroupId("flink")
                .setTopics("TopicA")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();

        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103;9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("TopicA")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("flink" + RandomUtils.nextInt(1, 100))
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000")
                .build();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
