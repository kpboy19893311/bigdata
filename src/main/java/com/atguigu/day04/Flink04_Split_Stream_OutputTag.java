package com.atguigu.day04;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Felix
 * @date 2024/9/14
 * 该案例演示了通过侧输出流实现分流
 * 需求：将WaterSensor按照Id类型进行分流
 * 注意：如果使用侧输出流，必须使用process算子对流中数据进行处理，因为只有process底层的processElement方法中提供了上下文对象 ctx.output();
 */
public class Flink04_Split_Stream_OutputTag {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        //TODO 2.从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 3.对流中数据进行类型转换    String->WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        //TODO 4.分流
        //4.1 创建侧输出流标签
        OutputTag<WaterSensor> ws1Tag = new OutputTag<WaterSensor>("ws1"){};
        //OutputTag<WaterSensor> ws2Tag = new OutputTag<WaterSensor>("ws2", TypeInformation.of(WaterSensor.class));
        OutputTag<WaterSensor> ws2Tag = new OutputTag<WaterSensor>("ws2", Types.POJO(WaterSensor.class));
        //4.2 分流逻辑
        SingleOutputStreamOperator<WaterSensor> mainDS = wsDS.process(
                new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor ws, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        String id = ws.getId();
                        if ("ws1".equals(id)) {
                            //将数据"放到"侧输出流中(给这条数据打标签)
                            ctx.output(ws1Tag, ws);
                        } else if ("ws2".equals(id)) {
                            ctx.output(ws2Tag, ws);
                        } else {
                            //将数据放到主流
                            out.collect(ws);
                        }
                    }
                }
        );

        //TODO 5.打印输出
        mainDS.print("主流:");
        mainDS.getSideOutput(ws1Tag).print("ws1:");
        mainDS.getSideOutput(ws2Tag).print("ws2:");

        //TODO 6.提交作业
        env.execute();
    }
}
