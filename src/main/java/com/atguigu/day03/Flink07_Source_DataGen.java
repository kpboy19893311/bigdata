package com.atguigu.day03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/13
 * 该案例演示了源算子-数据生成器
 */
public class Flink07_Source_DataGen {
    public static void main(String[] args) throws Exception {
        //TODO 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //TODO 通过数据生成器生成数据
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "数据->" + value;
                    }
                },
                100,
                RateLimiterStrategy.perSecond(10),
                TypeInformation.of(String.class)
        );
        //TODO 封装为流
        DataStreamSource<String> ds = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data_Gen_Source");
        //TODO 打印输出
        ds.print();
        //TODO 提交作业
        env.execute();
    }
}
