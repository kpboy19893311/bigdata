package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @author Felix
 * @date 2024/9/19
 * 该案例演示了计数窗口
 */
public class Test {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 3.对流中数据进行类型转换
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        //TODO 4.按照传感器的id进行分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);
        //TODO 5.开窗
        //WindowedStream<WaterSensor, String, GlobalWindow> windowDS = keyedDS.countWindow(5);
        WindowedStream<WaterSensor, String, GlobalWindow> windowDS = keyedDS.countWindow(10,2);

        //TODO 6.对窗口数据进行处理
        SingleOutputStreamOperator<WaterSensor> processDS = windowDS.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("value1:" + value1);
                        System.out.println("value2:" + value2);
                        value1.vc += value2.vc;
                        return value1;
                    }
                }/*,
                new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long count = elements.spliterator().estimateSize();

                        out.collect("key=" + s + "的窗口包含" + count + "条数据===>" + elements.toString());
                    }
                }*/
        );
        /*SingleOutputStreamOperator<String> processDS = windowDS.process(
                new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        Integer vc = 0;
                        for (WaterSensor ws : elements) {
                            vc += ws.getVc();
                        }
                        out.collect(vc + "");
                    }
                }
        );*/
        //TODO 7.打印输出
        processDS.print();
        //TODO 8.提交作业
        env.execute();
    }
}
