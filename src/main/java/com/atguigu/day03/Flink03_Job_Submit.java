package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/13
 * 该案例演示了作业提交
 */
public class Flink03_Job_Submit {
    public static void main(String[] args) throws Exception {
        //TODO 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 打印输出
        socketDS.print();
        //TODO 提交作业
        //同步提交作业
        //env.execute();
        //异步提交作业
        env.executeAsync();
        //~~~~~~~~~~~~~~~~~~~~~~~~~~~
        //TODO 准备环境
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 从指定的网络端口读取数据
        DataStreamSource<String> socketDS1 = env1.socketTextStream("hadoop102", 8889);
        //TODO 打印输出
        socketDS1.print();
        //TODO 提交作业
        //同步提交作业
        //env1.execute();
        //异步提交作业
        env1.executeAsync();
    }
}
