package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/18
 * 该案例演示了开窗前是否keyBy的效果
 */
public class Flink03_Window_KeyBy {
    public static void main(String[] args) {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.从指定的网络端口中读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 3.对流中数据进行类型转换    String->WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        //TODO 4.开窗
        //no-keyBy：针对于整条流进行开窗，相当于并行度设置为1
        //wsDS.windowAll(窗口分配器);
        //wsDS.countWindowAll()

        //keyBy：针对于keyBy后每一个组独立开窗，窗口间互不响应
        //wsDS
                //.keyBy(WaterSensor::getId)
                //.window()
                //.countWindow()
    }
}
