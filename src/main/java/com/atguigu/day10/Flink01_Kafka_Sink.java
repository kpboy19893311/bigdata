package com.atguigu.day10;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @author Felix
 * @date 2024/9/24
 * 该案例演示了将流中数据写到kafka主题的一致性保证
 * 在Sink端，要想保证写入到kafka的一致性，需要做如下操作
 *      开启检查点
 *      设置一致性级别为精准一次
 *          .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
 *      设置事务id的前缀
 *          .setTransactionalIdPrefix("flink01_kafka_sink_")
 *      设置事务的超时时间
 *          .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 + "")
 *          检查点超时时间 < 事务的超时时间 <= 事务最大超时时间(默认15min)
 *      在消费端，设置消费数据的隔离级别为读已提交
 *          .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
 */
public class Flink01_Kafka_Sink {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //TODO 2.从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 3.将读取的数据写到kafka主题
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("first")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("flink01_kafka_sink_")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 + "")
                .build();
        socketDS.sinkTo(kafkaSink);
        //TODO 4.提交作业
        env.execute();
    }
}
