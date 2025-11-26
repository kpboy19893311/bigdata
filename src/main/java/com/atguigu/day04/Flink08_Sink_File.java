package com.atguigu.day04;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 * @author Felix
 * @date 2024/9/14
 * 该案例演示了Sink算子-File
 */
public class Flink08_Sink_File {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(5000L);
        //TODO 2.从数据生成器中读取数据
        //2.1 创建数据生成器对象
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "数据:" + value;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                Types.STRING
        );
        //2.2 封装为流
        DataStreamSource<String> ds = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data_gen_Sour");
        //TODO 3.将流中数据写到文件中
        FileSink<String> sink = FileSink
                .forRowFormat(new Path("E:\\output\\"), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(new OutputFileConfig("atguigu-",".log"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                //每15分钟滚动一次
                                .withRolloverInterval(Duration.ofMinutes(15))
                                //距离上一条写入时间间隔大于5分钟 滚动一次
                                .withInactivityInterval(Duration.ofMinutes(5))
                                //文件到了1k 滚动一次
                                //.withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                .withMaxPartSize(new MemorySize(1024))
                                .build())
                .build();
        ds.sinkTo(sink);

        //TODO 4.提交作业
        env.execute();
    }
}
