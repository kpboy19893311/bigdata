package com.atguigu.day09;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/9/11
 * 该案例演示了检查点常用的配置
 */
public class Flink01_CheckPoint {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8081);

        //禁用最终检查点
        //conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);

        //env.disableOperatorChaining();

        //启用检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //检查点存储
        //默认如果使用HashMap状态后端，检查点存储在JobManager的堆内存中
        //checkpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage());
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck");

        //检查点模式（CheckpointingMode）
        //checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //超时时间（checkpointTimeout）
        checkpointConfig.setCheckpointTimeout(60000L);

        //最小间隔时间（minPauseBetweenCheckpoints）
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);

        //最大并发检查点数量（maxConcurrentCheckpoints）
        //checkpointConfig.setMaxConcurrentCheckpoints(1);

        //开启外部持久化存储（enableExternalizedCheckpoints） 当作业取消后，检查点是否保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //检查点连续失败次数（tolerableCheckpointFailureNumber）
        //checkpointConfig.setTolerableCheckpointFailureNumber(0);

        //设置重启策略   flink提供的另外一种容错手段----重启
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

        //非对齐检查点（enableUnalignedCheckpoints）
        //checkpointConfig.enableUnalignedCheckpoints();

        //对齐检查点超时时间（alignedCheckpointTimeout）
        //checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(10));

        //开启增量检查点
        //env.enableChangelogStateBackend(true);
        //设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");


        env
                .socketTextStream("hadoop102",8888).uid("socket_uid")
                .flatMap(
                        (String lineStr, Collector<Tuple2<String, Long>> out) -> {
                            String[] wordArr = lineStr.split(" ");
                            for (String word : wordArr) {
                                out.collect(Tuple2.of(word,1L));
                            }
                        }
                ).uid("flat_map_uid")
                //注意：在使用lambda表达式的时候，因为有泛型擦除的存在，需要显式的指定返回的类型
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(0)
                .sum(1).uid("sum_uid")
                .print().uid("print_uid");

        env.execute();
    }
}
