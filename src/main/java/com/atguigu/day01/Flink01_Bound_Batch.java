package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/9/11
 * 该案例演示了以批的形式处理有界数据
 */
public class Flink01_Bound_Batch {
    public static void main(String[] args) {
        //TODO 1.环境准备
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.从指定的文件中读取数据
        DataSource<String> ds = env.readTextFile("D:\\dev\\workspace\\bigdata-0422\\input\\words.txt");
        //TODO 3.对读取的数据进行扁平化处理      封装为二元组对象 Tuple2<单词,1L>
        FlatMapOperator<String, Tuple2<String, Long>> flatMapDS = ds.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String lineStr, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] wordArr = lineStr.split(" ");
                        for (String word : wordArr) {
                            //将封装好的二元组对象 发送到下游
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                }
        );
        //TODO 4.按照单词进行分组
        UnsortedGrouping<Tuple2<String, Long>> groupByDS = flatMapDS.groupBy(0);
        //TODO 5.聚合计算
        AggregateOperator<Tuple2<String, Long>> sumDS = groupByDS.sum(1);
        //TODO 6.将结果打印输出
        try {
            //(flink,1)
            //(world,1)
            //(hello,3)
            //(java,1)
            sumDS.print();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
