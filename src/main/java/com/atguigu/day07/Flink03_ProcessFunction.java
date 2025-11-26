package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/20
 * 该案例演示了处理函数的分类
 */
public class Flink03_ProcessFunction {
    public static void main(String[] args) {
        //TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.设置并行度
        env.setParallelism(1);
        //TODO 3.从指定的网络端口中读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 4.对读取的数据进行类型转换
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        //TODO 5.处理函数的使用
       /* wsDS.map(
                new MapFunction<WaterSensor, String>() {
                    @Override
                    public String map(WaterSensor value) throws Exception {
                        return null;
                    }
                }
        )*/
        /*wsDS.process(
                new ProcessFunction<WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, ProcessFunction<WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                    }
                }
        )
        wsDS
                .keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                            }
                        }
                )

        wsDS
                .windowAll()
                .process(
                        new ProcessAllWindowFunction<WaterSensor, Object, Window>() {
                            @Override
                            public void process(ProcessAllWindowFunction<WaterSensor, Object, Window>.Context context, Iterable<WaterSensor> elements, Collector<Object> out) throws Exception {

                            }
                        }
                )

        wsDS
                .keyBy()
                .window()
                .process(
                        new ProcessWindowFunction<WaterSensor, Object, Tuple, Window>() {
                            @Override
                            public void process(Tuple tuple, ProcessWindowFunction<WaterSensor, Object, Tuple, Window>.Context context, Iterable<WaterSensor> elements, Collector<Object> out) throws Exception {

                            }
                        }
                )

        wsDS
                .connect(wsDS)
                .process(
                        new CoProcessFunction<WaterSensor, WaterSensor, Object>() {
                            @Override
                            public void processElement1(WaterSensor value, CoProcessFunction<WaterSensor, WaterSensor, Object>.Context ctx, Collector<Object> out) throws Exception {

                            }

                            @Override
                            public void processElement2(WaterSensor value, CoProcessFunction<WaterSensor, WaterSensor, Object>.Context ctx, Collector<Object> out) throws Exception {

                            }
                        }
                )

        wsDS
                .keyBy()
                .intervalJoin()
                .between()
                .process(
                        new ProcessJoinFunction<WaterSensor, Object, Object>() {
                            @Override
                            public void processElement(WaterSensor left, Object right, ProcessJoinFunction<WaterSensor, Object, Object>.Context ctx, Collector<Object> out) throws Exception {

                            }
                        }
                )

        wsDS.connect(wsDS.broadcast(new MapStateDescriptor<Object, Object>()))
                .process(
                        new BroadcastProcessFunction<WaterSensor, WaterSensor, Object>() {
                            @Override
                            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, WaterSensor, Object>.ReadOnlyContext ctx, Collector<Object> out) throws Exception {

                            }

                            @Override
                            public void processBroadcastElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, WaterSensor, Object>.Context ctx, Collector<Object> out) throws Exception {

                            }
                        }
                )

         */
    }
}
