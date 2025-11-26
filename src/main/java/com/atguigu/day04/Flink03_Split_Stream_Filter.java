package com.atguigu.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/14
 * 该案例演示了通过filter算子实现分流
 * 需求：读取一个整数数字流，将数据流划分为奇数流和偶数流
 */
public class Flink03_Split_Stream_Filter {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        //TODO 2.从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 3.对流中数据进行类型转换    String->Integer
        SingleOutputStreamOperator<Integer> mapDS = socketDS.map(Integer::valueOf);
        //TODO 4.过滤出奇数流
        SingleOutputStreamOperator<Integer> ds1 = mapDS.filter(num -> num % 2 != 0).setParallelism(2);
        //TODO 5.过滤出偶数流
        SingleOutputStreamOperator<Integer> ds2 = mapDS.filter(num -> num % 2 == 0).setParallelism(3);
        //TODO 6.打印输出
        ds1.print("奇数:");
        ds2.print("偶数:");
        //TODO 7.提交作业
        env.execute();

    }
}
