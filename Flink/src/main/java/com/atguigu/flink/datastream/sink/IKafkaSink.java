package com.atguigu.flink.datastream.sink;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Random;

public class IKafkaSink {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092, hadoop103:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("topicA")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // EOS(Exactly Once semantic): 精确一次,
                // AT_LEAST_ONCE : 至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 使用精准一次时，kafkaSink要求设置事务id
                .setTransactionalIdPrefix("flink" + RandomUtils.nextInt(1, 100))
                // kafkaSink的默认的生产者事务的超时时间为：1 hour
                // KafkaBroker默认允许的事务最大的超时时间为：15 minutes
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "36000")
                .build();

        ds.map(Event::toString).sinkTo(kafkaSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
