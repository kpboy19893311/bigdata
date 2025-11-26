package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/9/20
 * 该案例演示了事件时间定时器
 */
public class Flink04_Timer_Event {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.设置并行度
        env.setParallelism(1);
        //TODO 3.从指定的网络端口中读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 4.对读取的数据进行类型转换
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<WaterSensor> withWatermarkDS = wsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor ws, long recordTimestamp) {
                                        return ws.getTs();
                                    }
                                }
                        )
        );

        //TODO 5.按照传感器的id进行分组
        KeyedStream<WaterSensor, String> keyedDS = withWatermarkDS.keyBy(WaterSensor::getId);

        //TODO 6.注册定时器
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //获取当前分组的key
                        String currentKey = ctx.getCurrentKey();
                        System.out.println("currentKey:" + currentKey);

                        //获取当前元素的事件时间
                        Long timestamp = ctx.timestamp();
                        System.out.println("timestamp:" + timestamp);

                        //将数据"放到"侧输出流中
                        //注意：如果要使用侧输出流，必须用process算子对流中数据进行处理，
                        //ctx.output();


                        //获取定时服务
                        TimerService timerService = ctx.timerService();

                        long currentProcessingTime = timerService.currentProcessingTime();
                        System.out.println("currentProcessingTime:" + currentProcessingTime);

                        long currentWatermark = timerService.currentWatermark();
                        System.out.println("currentWatermark:" + currentWatermark);

                        //注册处理时间定时器   定时器的触发是由系统时间触发的
                        //timerService.registerProcessingTimeTimer(currentProcessingTime + 10000);

                        //注意：同一时间的定时器只会有一个

                        //注册事件时间定时器   定时器的触发是由水位线(逻辑时钟)触发的
                        timerService.registerEventTimeTimer(10);

                        //删除处理时间定时器
                        //timerService.deleteProcessingTimeTimer(10);

                        //删除事件时间定时器
                        //timerService.deleteEventTimeTimer(10);

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        //定时器被触发的时候执行的方法
                        out.collect("key是" + ctx.getCurrentKey() + "定时器在" + timestamp + "触发了");
                    }
                }
        );
        //TODO 7.打印输出
        processDS.print();
        //TODO 8.提交作业
        env.execute();
    }
}
