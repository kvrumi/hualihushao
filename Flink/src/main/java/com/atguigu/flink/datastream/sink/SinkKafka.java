package com.atguigu.flink.datastream.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class SinkKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("clicks")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                //EOS下必须设置
                .setTransactionalIdPrefix("atguigu-")
                //事务的超时时间不能大于KafkaBroker设置的事务最大超时时间
                //KafkaSink默认1小时， KafkaBroker默认15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG , "300000")
                .build();

        env.socketTextStream("hadoop103",8888)
                .sinkTo(kafkaSink);

        env.execute();
    }
}

