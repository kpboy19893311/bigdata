package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author Felix
 * @date 2024/9/11
 * 以流的形式对无界数据进行处理
 */
public class Flink04_UnBound_Stream_Generic {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.从指定的网络端口读取数据
        //TODO 3.对读取的数据进行扁平化处理  封装为二元组对象向下游传递 Tuple2<String,Long>
        //TODO 4.按照单词进行分组
        //TODO 5.求和计算
        //TODO 6.将结果打印输出
        env
                .socketTextStream("hadoop102",8888)
               /* .flatMap(
                        new FlatMapFunction<String, Tuple2<String,Long>>() {
                            @Override
                            public void flatMap(String lineStr, Collector<Tuple2<String, Long>> out) throws Exception {
                                String[] wordArr = lineStr.split(" ");
                                for (String word : wordArr) {
                                    out.collect(Tuple2.of(word,1L));
                                }
                            }
                        }
                )*/
                .flatMap(
                        (String lineStr, Collector<Tuple2<String, Long>> out) -> {
                            String[] wordArr = lineStr.split(" ");
                            for (String word : wordArr) {
                                out.collect(Tuple2.of(word,1L));
                            }
                        }
                )
                //注意：在使用lambda表达式的时候，因为有泛型擦除的存在，需要显式的指定返回的类型
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(0)
                .sum(1)
                .print();
        //TODO 7.提交作业
        env.execute();
    }
}
