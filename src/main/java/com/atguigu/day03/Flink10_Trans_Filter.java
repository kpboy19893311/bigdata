package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.MyMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/13
 * 该案例演示了转换算子-filter
 * 需求：过滤出传感器id问sensor_1的
 */
public class Flink10_Trans_Filter {
    public static void main(String[] args) throws Exception {
        //TODO 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 准备数据
        DataStreamSource<WaterSensor> ds = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)

        );
        //TODO 过滤数据
        SingleOutputStreamOperator<WaterSensor> filterDS = ds.filter(
                new FilterFunction<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor ws) throws Exception {
                        return "sensor_1".equals(ws.id);
                    }
                }
        );

        //TODO 打印
        filterDS.print();
        //TODO 提交作业
        env.execute();
    }
}