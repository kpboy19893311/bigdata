package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/13
 * 该案例演示了聚合算子-规约聚合reduce
 * 需求：统计不同传感器采集的水位和
 * reduce规约聚合
 *      如果当前流中只有一条数据的话，reduce方法不会被执行的
 *      reduce(value1,value2)
 *          value1:规约的结果
 *          value2:新来的数据
 */
public class Flink13_Agg_Reduce {
    public static void main(String[] args) throws Exception {
        //TODO 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 从指定的网络端口读取数据   ws1,10,10
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 对流中数据进行转换    String->WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(
                new WaterSensorMapFunction()
        );
        //TODO 按照传感器id进行分组
        //KeyedStream<WaterSensor, Tuple> keyedDS = wsDS.keyBy("id");
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(
                new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor ws) throws Exception {
                        return ws.getId();
                    }
                }
        );
        //wsDS.keyBy(ws->ws.getId())
        //wsDS.keyBy(WaterSensor::getId)
        //wsDS.keyBy(ws->1);

        //TODO 求和
        SingleOutputStreamOperator<WaterSensor> reduceDS = keyedDS.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("value1:" + value1);
                        System.out.println("value2:" + value2);
                        value1.setVc(value1.getVc() + value2.getVc());
                        return value1;
                    }
                }
        );
        //TODO 打印输出
        reduceDS.print("~~~~~~");
        //TODO 提交作业
        env.execute();
    }
}
