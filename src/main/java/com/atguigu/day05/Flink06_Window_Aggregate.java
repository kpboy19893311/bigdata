package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author Felix
 * @date 2024/9/18
 * 该案例演示窗口处理函数Aggregate
 * 需求：每隔10s，统计不同的传感器采集的水位和
 */
public class Flink06_Window_Aggregate {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 3.对流中数据进行类型转换    String->WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        //TODO 4.按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);
        //TODO 5.开窗---滚动处理时间窗口
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        //TODO 6.对窗口数据进行处理---增量处理函数---reduce
       /* SingleOutputStreamOperator<WaterSensor> reduceDS = windowDS.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("中间累加的结果:" + value1);
                        System.out.println("新来的数据:" + value2);
                        value1.setVc(value1.getVc() + value2.getVc());
                        return value1;
                    }
                }
        );*/

        SingleOutputStreamOperator<String> aggregateDS = windowDS.aggregate(
                new AggregateFunction<WaterSensor, Tuple2<Integer, Integer>, String>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        System.out.println("~~~createAccumulator~~~");
                        return Tuple2.of(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(WaterSensor ws, Tuple2<Integer, Integer> accumulator) {
                        System.out.println("~~~add~~~");
                        accumulator.f0 = accumulator.f0 + ws.getVc();
                        accumulator.f1 = accumulator.f1 + 1;
                        return accumulator;
                    }

                    @Override
                    public String getResult(Tuple2<Integer, Integer> accumulator) {
                        System.out.println("~~~getResult~~~");
                        Integer vc = accumulator.f0;
                        Integer count = accumulator.f1;
                        return "平均水位:" + vc * 1d / count;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        //注意：只有会话窗口才需要重写merge方法
                        System.out.println("~~~merge~~~");
                        return null;
                    }
                }
        );

        //TODO 7.打印输出
        aggregateDS.print("~~~");
        //TODO 8.提交作业
        env.execute();

    }
}
