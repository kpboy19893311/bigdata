package com.atguigu.day01;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/9/11
 * 该案例演示了以流的形式处理有界数据
 */
public class Flink02_Bound_Stream {
    public static void main(String[] args) {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //TODO 2.从指定的文件中读取数据
        DataStreamSource<String> ds = env.readTextFile("D:\\dev\\workspace\\bigdata-0422\\input\\words.txt");
        //TODO 3.对读取的数据进行扁平化处理      封装为二元组对象 Tuple2<单词,1L>
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDS = ds.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String lineStr, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] wordArr = lineStr.split(" ");
                        for (String word : wordArr) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                }
        );
        //TODO 4.按照单词进行分组
        KeyedStream<Tuple2<String, Long>, Tuple> keyedDS = flatMapDS.keyBy(0);
        //TODO 5.聚合计算
        SingleOutputStreamOperator<Tuple2<String, Long>> sumDS = keyedDS.sum(1);
        //TODO 6.将结果打印输出
        sumDS.print();
        //TODO 7.注意：如果使用的是DataStreamAPI,需要通过env.execute()显示提交作业
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
