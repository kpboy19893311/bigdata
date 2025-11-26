package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/9/21
 * 该案例演示了键控状态-值状态
 * 需求：检测每种传感器的水位值，如果连续的两个水位差值超过10，就输出报警
 */
public class Flink02_KeyedState_ValueState_2 {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.设置并行度
        env.setParallelism(1);
        //TODO 3.从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 4.对流中数据进行类型转换    String->WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        //TODO 5.按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);
        //TODO 6.对分组后的数据进行处理
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    //声明值状态
                    //注意：虽然键控状态声明在成员变量的位置，但是作用范围不是每一个子任务，而是keyBy之后的每一个组
                    //不能在声明的时候直接对状态进行初始化，因为这个时候算子的生命周期还没有开始，获取不到运行时上下文对象的
                    ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //对于键控状态，是在open方法中进行初始化
                        ValueStateDescriptor<Integer> valueStateDescriptor
                                = new ValueStateDescriptor<Integer>("lastVcState", Integer.class);
                        lastVcState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //获取当前水位值
                        Integer curVc = ws.getVc();
                        String id = ctx.getCurrentKey();
                        //从状态中获取上次水位值
                        Integer lastVc = lastVcState.value() == null ? 0 : lastVcState.value();
                        if (Math.abs(curVc - lastVc) > 10) {
                            out.collect("传感器id" + id + "当前水位值" + curVc + "和上一次水位值" + lastVc + "之差大于10");
                        }
                        //将当前水位值更新到状态中
                        lastVcState.update(curVc);
                    }
                }
        );
        //TODO 7.打印
        processDS.printToErr();
        //TODO 8.提交作业
        env.execute();

    }
}
