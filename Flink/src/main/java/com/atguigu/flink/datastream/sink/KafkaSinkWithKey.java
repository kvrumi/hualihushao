package com.atguigu.flink.datastream.sink;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class KafkaSinkWithKey {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        KafkaSink<Event> kafkaSink = KafkaSink.<Event>builder()
                .setBootstrapServers("hadoop102:9092, hadoop103:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<Event>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Event element, KafkaSinkContext context, Long timestamp) {
                                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topicA", element.getUser().getBytes(), element.getUrl().getBytes());
                                return producerRecord;
                            }
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("flink" + RandomUtils.nextInt(1, 100))
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "36000")
                .build();

        ds.sinkTo(kafkaSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
