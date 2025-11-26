package com.atguigu.day03;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/13
 * 该案例演示了执行模式
 *      两种方式:
 *          代码
 *              env.setRuntimeMode(RuntimeExecutionMode.BATCH);
 *          在提交作业的时候，通过参数指定
 *              -Dexecution.runtime-mode=BATCH
 */
public class Flink02_Exe_Mo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置执行模式
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env
                .readTextFile("/opt/module/flink-1.17.0/words.txt")
                .print();
        env.execute();
    }
}
