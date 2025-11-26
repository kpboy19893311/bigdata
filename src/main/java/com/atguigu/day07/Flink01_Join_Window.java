package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author Felix
 * @date 2024/9/20
 * 该案例演示了基于窗口实现的双流join
 * 注意：如果处理的是有界数据，在程序结束的时候，会自动生成WM,值是Long.MAX_VALUE，用于进行窗口的提交操作
 */
public class Flink01_Join_Window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 3)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
                );


        SingleOutputStreamOperator<Tuple3<String, Integer,Integer>> ds2 = env
                .fromElements(
                        Tuple3.of("a", 1,1),
                        Tuple3.of("b", 2,1)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Integer,Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
                );
        //TODO 基于窗口对两条流进行join
        ds1
                .join(ds2)
                .where(tup2->tup2.f0)
                .equalTo(tup3-> tup3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(
                        new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                            @Override
                            public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                                return first + "----" + second;
                            }
                        }
                ).print();

        env.execute();

    }
}

//Tuple2.of("a", 1),
//Tuple2.of("a", 2),
//Tuple2.of("b", 3),

//Tuple3.of("a", 1,1),
//Tuple3.of("b", 2,1),



//-----
//        (a,1)----(a,1,1)
//        (a,2)----(a,1,1)
//        (b,3)----(b,2,1)