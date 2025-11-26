package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.MyMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/9/13
 * 该案例演示了转换算子-flatMap
 * 需求：如果输入的数据是sensor_1，只打印vc；如果输入的数据是sensor_2，既打印ts又打印vc。
 */
public class Flink11_Trans_FlatMap {
    public static void main(String[] args) throws Exception {
        //TODO 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 准备数据
        DataStreamSource<WaterSensor> ds = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)

        );

        //TODO 处理数据
        //如果使用map对流中数据进行处理，通过map方法的返回值向下游传递数据，返回值只有一个，所以只能向下游传递一次数据
        /*ds.map(
                new MapFunction<WaterSensor, String>() {
                    @Override
                    public String map(WaterSensor value) throws Exception {
                        return null;
                    }
                }
        )*/
        //如果使用flatMap对流中数据进行处理，不是通过返回值传递数据，是通过Collector向下游传递数据，可以传递多次
        ds.flatMap(
                new FlatMapFunction<WaterSensor, String>() {
                    @Override
                    public void flatMap(WaterSensor ws, Collector<String> out) throws Exception {
                        String id = ws.getId();
                        if("sensor_1".equals(id)){
                            out.collect(ws.getVc()+"");
                        } else if ("sensor_2".equals(id)) {
                            out.collect(ws.getTs() +"");
                            out.collect(ws.getVc()+"");
                        }
                    }
                }
        );

        //TODO 打印

        //TODO 提交作业
        env.execute();
    }
}