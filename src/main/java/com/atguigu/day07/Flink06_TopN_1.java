package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Felix
 * @date 2024/9/20
 * 需求：实时统计一段时间内的出现次数最多的水位。例如，统计最近10秒钟内出现次数最多的两个水位，并且每5秒钟更新一次
 */
public class Flink06_TopN_1 {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.设置并行度
        env.setParallelism(1);
        //TODO 3.从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 4.对读取的数据进行类型转换   String->WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        //TODO 5.指定Watermark的生成策略 并提取事件时间字段
        SingleOutputStreamOperator<WaterSensor> withWatermarkDS = wsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor ws, long recordTimestamp) {
                                        return ws.getTs() * 1000;
                                    }
                                }
                        )
        );
        //TODO 6.开窗---滑动事件时间窗口
        AllWindowedStream<WaterSensor, TimeWindow> windowDS
                = withWatermarkDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        //TODO 7.对窗口数据进行处理---topn
        SingleOutputStreamOperator<String> processDS = windowDS.process(
                new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {

                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        Map<Integer, Integer> vcCountMap = new HashMap<>();
                        //对窗口中的所有元素进行遍历
                        for (WaterSensor ws : elements) {
                            Integer vc = ws.getVc();
                            if (vcCountMap.containsKey(vc)) {
                                //如果map中包含当前水位值，直接在原来出现次数基础上 + 1
                                vcCountMap.put(vc, vcCountMap.get(vc) + 1);
                            } else {
                                //如果map中不包含当前水位值，直接将当前水位出现次数计数为1
                                vcCountMap.put(vc, 1);
                            }
                        }

                        //为了对水位值出现的次数进行排序，定义一个List集合
                        List<Tuple2<Integer, Integer>> vcCountList = new ArrayList();
                        for (Map.Entry<Integer, Integer> entry : vcCountMap.entrySet()) {
                            vcCountList.add(Tuple2.of(entry.getKey(), entry.getValue()));
                        }

                        //排序
                        vcCountList.sort((o1, o2) -> o2.f1 - o1.f1);

                        //取top2 向下游传递
                        StringBuilder outStr = new StringBuilder();

                        outStr.append("================================\n");
                        // 遍历 排序后的 List，取出前2个， 考虑可能List不够2个的情况  ==》 List中元素的个数 和 2 取最小值
                        for (int i = 0; i < Math.min(2, vcCountList.size()); i++) {
                            Tuple2<Integer, Integer> vcCount = vcCountList.get(i);
                            outStr.append("Top" + (i + 1) + "\n");
                            outStr.append("vc=" + vcCount.f0 + "\n");
                            outStr.append("count=" + vcCount.f1 + "\n");
                            outStr.append("窗口结束时间=" + DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS") + "\n");
                            outStr.append("================================\n");
                        }

                        out.collect(outStr.toString());
                    }
                }
        );
        //TODO 8.打印输出
        processDS.print();
        //TODO 9.提交作业
        env.execute();
    }
}
