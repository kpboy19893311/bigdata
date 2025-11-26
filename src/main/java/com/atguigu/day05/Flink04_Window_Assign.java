package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author Felix
 * @date 2024/9/18
 * 该案例演示了窗口分配器
 */
public class Flink04_Window_Assign {
    public static void main(String[] args) {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.从指定的网络端口中读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 3.对流中数据进行类型转换    String->WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        //TODO 4.按照传感器的id进行分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);

        //TODO 5.开窗（针对keyBy后的每一组进行独立开窗）---时间窗口
        //滚动处理时间窗口
        WindowedStream<WaterSensor, String, TimeWindow> windowDS
                = keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));


    }
}
