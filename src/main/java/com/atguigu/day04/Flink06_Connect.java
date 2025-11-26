package com.atguigu.day04;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/9/14
 * 该案例演示了合流算子-connect
 */
public class Flink06_Connect {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        //TODO 2.从指定的网络端口读取字符串数据
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop102", 8888);
        //TODO 3.从指定的网络端口读取字符串数据并转换为Integer
        SingleOutputStreamOperator<Integer> ds2 = env
                .socketTextStream("hadoop102", 8889)
                .map(Integer::valueOf);


        //TODO 4.使用connect合流
        ConnectedStreams<String, Integer> connectDS = ds1.connect(ds2);

        //TODO 5.对连接后的数据进行处理
        SingleOutputStreamOperator<String> processDS = connectDS.process(
                new CoProcessFunction<String, Integer, String>() {
                    //处理第一条数据
                    @Override
                    public void processElement1(String value, CoProcessFunction<String, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("字符串:" + value);
                    }

                    //处理第二条数据
                    @Override
                    public void processElement2(Integer value, CoProcessFunction<String, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("数字:" + value);
                    }
                }

        );
        //TODO 6.打印输出
        processDS.print();
        //TODO 5.提交作业
        env.execute();

    }
}
