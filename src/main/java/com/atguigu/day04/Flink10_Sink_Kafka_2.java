package com.atguigu.day04;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author Felix
 * @date 2024/9/14
 * 该案例演示了将流中数据写到Kafka不同主题
 */
public class Flink10_Sink_Kafka_2 {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 3.对流中数据进行类型转换    String->WaterSensor对象
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        //TODO 4.将流中数据写到Kafka不同主题    传感器id是ws1-ws1   传感器id是ws2-ws2
        //4.1 创建kafkaSink对象
        KafkaSink<WaterSensor> kafkaSink = KafkaSink.<WaterSensor>builder()
                .setBootstrapServers("hadoop102:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<WaterSensor>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(WaterSensor ws, KafkaSinkContext context, Long timestamp) {
                                return new ProducerRecord<>(ws.getId(), ws.toString().getBytes());
                            }
                        }
                )
                .build();
        wsDS.sinkTo(kafkaSink);

        //TODO 4.提交作业
        env.execute();
    }
}
