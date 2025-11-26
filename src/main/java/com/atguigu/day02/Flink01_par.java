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
 * 该案例演示了并行度
 *      当要处理的数据量非常大时，我们可以把一个算子操作，“复制”多份到多个节点，数据来了之后就可以到其中任意一个执行。
 *      这样一来，一个算子任务就被拆分成了多个并行的“子任务”（subtasks）,可以提升程序的处理能力
 *      一个特定算子的子任务（subtask）的个数被称之为其并行度
 *      一个Flink应用的并行度==并行度最大的算子的并行度的个数
 *      在IDEA中去开发Flink程序的时候，如果没有显式指定的并行度，默认并行度的个数是当前机器CPU的线程数
 * 如何设置并行度
 *     在代码中全局设置并行度
 *           env.setParallelism(3)
 *     在代码中单独设置算子并行度
 *          算子.setParallelism(4)
 *     在flink-conf.yaml配置文件中设置并行度
 *          parallelism.default: 1
 *     在命令行提交作业的时候设置并行度
 *          -p
 * 并行度设置优先级
 *     在代码中单独设置算子并行度 >  在代码中全局设置并行度 > 在命令行提交作业的时候设置并行度 > 在flink-conf.yaml配置文件中设置并行度
 */
public class Flink01_par {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Configuration conf = new Configuration();
        //conf.set(RestOptions.PORT,8081);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(3);
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
        //TODO 4.对流中数据进行类型转换    String->Tuple2<String,Long>
        SingleOutputStreamOperator<Tuple2<String, Long>> mapDS = flatMapDS.map(
                new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String word) throws Exception {
                        return Tuple2.of(word, 1L);
                    }
                }
        ).setParallelism(4);

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
