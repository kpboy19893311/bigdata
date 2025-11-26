package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author Felix
 * @date 2024/9/13
 * 该案例演示了源算子-从集合中读取数据
 */
public class Flink04_Source_Col {
    public static void main(String[] args) throws Exception {
        //TODO 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 从集合中读取数据
        //DataStreamSource<Integer> ds = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));
        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4, 5);
        //TODO 打印输出
        ds.print();
        //TODO 提交作业
        env.execute();
    }
}
