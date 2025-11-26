package com.atguigu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/9/12
 * 该案例演示了算子链
 */
public class Flink02_OpeChain {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        //全局禁用算子链
        env.disableOperatorChaining();
        //TODO 2.从指定的网络端口中读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 3.对读取的数据进行扁平化处理 ---- 向下游传递的是一个个单词不是二元组
        SingleOutputStreamOperator<String> flatMapDS = socketDS.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String lineStr, Collector<String> out) throws Exception {
                        String[] wordArr = lineStr.split(" ");
                        for (String word : wordArr) {
                            out.collect(word);
                        }
                    }
                }
        );
        //).disableChaining();
        //).startNewChain();
        //TODO 4.对流中数据进行类型转换    String->Tuple2<String,Long>
        SingleOutputStreamOperator<Tuple2<String, Long>> mapDS = flatMapDS.map(
                new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String word) throws Exception {
                        return Tuple2.of(word, 1L);
                    }
                }
        );

        //TODO 5.按照单词进行分组
        KeyedStream<Tuple2<String, Long>, Tuple> keyedDS = mapDS.keyBy(0);
        //TODO 6.求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sumDS = keyedDS.sum(1);
        //TODO 7.打印结果
        sumDS.print();
        //TODO 8.提交作业
        env.execute();
    }
}
