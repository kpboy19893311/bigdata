package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/9/18
 * 该案例演示窗口处理函数 Aggregate + process
 * 需求：每隔10s，统计不同的传感器采集的水位信息
 */
public class Flink09_Window_Aggregate_Process {
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
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedDS
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        
        //TODO 6.增量  +  全量对窗口数据进行处理
        SingleOutputStreamOperator<String> aggregateDS = windowDS.aggregate(
                new AggregateFunction<WaterSensor, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        System.out.println("~~~createAccumulator~~~");
                        return Tuple2.of(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(WaterSensor ws, Tuple2<Integer, Integer> accumulator) {
                        System.out.println("~~~add~~~");
                        accumulator.f0 += ws.getVc();
                        accumulator.f1 += 1;
                        return accumulator;
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                        System.out.println("~~~getResult~~~");
                        return accumulator.f0 * 1d / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return null;
                    }
                },
                new ProcessWindowFunction<Double, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Double, String, String, TimeWindow>.Context context, Iterable<Double> elements, Collector<String> out) throws Exception {
                        System.out.println("~~~process~~~");
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                        out.collect("key是" + s + "窗口[" + windowStart + "~" + windowEnd + "]平均水位是" + elements.iterator().next());
                    }
                }
        );


        //TODO 7.打印输出
        aggregateDS.print("~~~");

        //TODO 8.提交作业
        env.execute();

    }
}
