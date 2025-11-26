package com.atguigu.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/14
 * 该案例演示了合流算子-union
 *      union可以合并两条或者多条流
 *      参与合并的流中数据类型必须要一致
 */
public class Flink05_Union {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        //TODO 2.从指定的网络端口读取数据
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> ds2 = env.socketTextStream("hadoop102", 8889);
        DataStreamSource<Integer> ds3 = env.fromElements(1, 1, 2, 4);
        //TODO 3.使用union合流
        DataStream<String> unionDS = ds1.union(ds2);
        //TODO 4.打印输出
        unionDS.print();
        //TODO 5.提交作业
        env.execute();

    }
}
