package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/9/21
 * 该案例演示了算子状态-广播状态
 * 需求：水位超过指定的阈值发送告警，阈值可以动态修改
 */
public class Flink10_OpeState_BroadcastState {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.设置并行度
        env.setParallelism(2);

        //env.setStateBackend(new HashMapStateBackend());
        //env.setStateBackend(new EmbeddedRocksDBStateBackend());

        //TODO 3.从指定的网络端口读取水位信息并进行类型转换  String->WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = env
                .socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        //TODO 4.从指定的网络端口读取阈值信息并进行类型转换  String->Integer
        SingleOutputStreamOperator<Integer> thresholdDS = env
                .socketTextStream("hadoop102", 8889)
                .map(Integer::valueOf);

        //TODO 5.对阈值流数据进行广播  并声明广播状态描述器
        MapStateDescriptor<String, Integer> mapStateDescriptor
                = new MapStateDescriptor<String, Integer>("mapStateDescriptor", String.class, Integer.class);
        BroadcastStream<Integer> broadcastDS = thresholdDS.broadcast(mapStateDescriptor);

        //TODO 6.关联非广播流(水位信息)以及广播流(阈值)---connect
        BroadcastConnectedStream<WaterSensor, Integer> connectDS = wsDS.connect(broadcastDS);

        //TODO 7.对关联后的数据进行处理---process
        SingleOutputStreamOperator<String> processDS = connectDS.process(
                new BroadcastProcessFunction<WaterSensor, Integer, String>() {
                    //processElement:处理非广播流数据            从广播状态中获取阈值信息判断是否超过警戒线
                    @Override
                    public void processElement(WaterSensor ws, BroadcastProcessFunction<WaterSensor, Integer, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        //获取广播状态
                        ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        //从广播状态中获取阈值信息
                        Integer threshold = broadcastState.get("threshold")==null ? 0 : broadcastState.get("threshold");
                        //获取当前采集的水位值
                        Integer vc = ws.getVc();
                        if (vc > threshold) {
                            out.collect("当前水位" + vc + "超过阈值" + threshold);
                        }
                    }

                    //processBroadcastElement:处理广播流数据     将广播流中的阈值信息放到广播状态中
                    @Override
                    public void processBroadcastElement(Integer threshold, BroadcastProcessFunction<WaterSensor, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                        //获取广播状态
                        BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        //将广播流中的阈值信息放到广播状态中
                        broadcastState.put("threshold", threshold);
                    }
                }
        );

        //TODO 8.打印
        processDS.print();
        //TODO 9.提交作业
        env.execute();
    }
}
