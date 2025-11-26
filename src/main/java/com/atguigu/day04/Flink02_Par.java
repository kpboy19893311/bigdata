package com.atguigu.day04;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/14
 * 该案例演示了分区算子
 */
public class Flink02_Par {
    public static void main(String[] args) throws Exception {
        //TODO 环境准备
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(10);
        //env.disableOperatorChaining();
        //TODO 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO shuffle
        //socketDS.shuffle().print();
        //TODO rebalance
        //socketDS.map(a->a).setParallelism(2).rebalance().print();
        //TODO rescale
        socketDS.map(a->a).setParallelism(2).rescale().print();
        //TODO broadcast
        //socketDS.broadcast().print();
        //TODO global
        //socketDS.global().print();
        //TODO keyby
        //socketDS.keyBy(a->a).print();
        //TODO forward
        //socketDS.print();
        //TODO 自定义分区器
        //socketDS.partitionCustom(
        //        new MyPartitioner(),
        //        a->a
        //).print();

        env.execute();

    }
}


class MyPartitioner implements Partitioner<String>{

    @Override
    public int partition(String key, int numPartitions) {
        return key.hashCode()%numPartitions;
    }
}