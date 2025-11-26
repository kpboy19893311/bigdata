package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Felix
 * @date 2024/9/21
 * 该案例演示了键控状态-值状态
 * 需求：检测每种传感器的水位值，如果连续的两个水位差值超过10，就输出报警
 */
public class Flink01_KeyedState_ValueState_1 {
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
                    //如果用普通的变量记录上次水位值，其作用访问值算子子任务，在一个子任务上的多个组共享当前变量
                    //Integer lastVc = 0;
                    Map<String,Integer> lastVcMap = new HashMap<>();
                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer curVc = ws.getVc();
                        String id = ctx.getCurrentKey();
                        Integer lastVc = lastVcMap.get(id);
                        lastVc = lastVc == null ? 0 : lastVc;
                        if(Math.abs(curVc - lastVc) > 10){
                            out.collect("传感器id"+id+"当前水位值"+curVc+"和上一次水位值"+lastVc+"之差大于10");
                        }
                        lastVcMap.put(id,curVc);
                    }
                }
        );
        //TODO 7.打印
        processDS.printToErr();
        //TODO 8.提交作业
        env.execute();

    }
}
