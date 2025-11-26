package com.atguigu.day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2024/9/25
 * 该案例演示了FlinkSQL的开发环境准备
 */
public class Flink01_Env {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //EnvironmentSettings settings = EnvironmentSettings.newInstance()
        //        .inStreamingMode()
        //        .build();
        //StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        //TODO 提交作业
        //注意：如果在程序中，最后操作的是流的话，需要通过env.execute()方法提交作业
        //如果在程序中，最后操作的是动态表，不需要显示的提交作业

    }
}
