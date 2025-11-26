package com.atguigu.day03;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Types;

/**
 * @author Felix
 * @date 2024/9/13
 * 该案例演示了源算子-自定义数据源
 */
public class Flink08_Source_Custom {
    public static void main(String[] args) throws Exception {
        //TODO 环境准备
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //TODO 从数据源读取数据
        DataStreamSource<String> ds = env.addSource(new MySource());

        //TODO 打印输出
        ds.print();
        //TODO 提交作业
        env.execute();
    }
}

class MySource implements SourceFunction<String>{

    //从数据源读取数据(生成数据)
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        for (int i = 0; i < 10000000; i++) {
            //将数据向下游传递
            ctx.collect("数据->" + i);
            Thread.sleep(1000);
        }
    }

    //job取消的时候调用的方法
    @Override
    public void cancel() {
        System.out.println("~~~cancel~~~");
    }
}


