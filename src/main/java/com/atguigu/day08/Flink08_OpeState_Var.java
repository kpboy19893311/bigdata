package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/21
 * 该案例演示了算子状态
 * 需求：在map算子中的每个并行度上计算数据的个数
 */
public class Flink08_OpeState_Var {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.设置并行度
        env.setParallelism(2);
        //TODO 3.从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 4.对流中数据进行类型转换    String->WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        //TODO 5.对分组后的数据进行处理
        SingleOutputStreamOperator<String> mapDS = wsDS.map(
                new RichMapFunction<WaterSensor, String>() {
                    Integer count = 0;

                    @Override
                    public String map(WaterSensor ws) throws Exception {
                        return "并行子任务:" + getRuntimeContext().getIndexOfThisSubtask() + "处理了" + ++count + "条数据";
                    }
                }
        );
        //TODO 7.打印
        mapDS.printToErr();
        //TODO 8.提交作业
        env.execute();

    }
}
