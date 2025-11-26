package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/21
 * 该案例演示了算子状态-ListState
 * 需求：在map算子中的每个并行度上计算数据的个数
 */
public class Flink09_OpeState_ListState {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.设置并行度
        env.setParallelism(2);
        env.enableCheckpointing(10000);
        //TODO 3.从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 4.对流中数据进行类型转换    String->WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        //TODO 5.对分组后的数据进行处理
        SingleOutputStreamOperator<String> mapDS = wsDS.map(
                new MyMap()
        );
        //TODO 7.打印
        mapDS.printToErr();
        //TODO 8.提交作业
        env.execute();

    }
}

class MyMap extends RichMapFunction<WaterSensor, String> implements CheckpointedFunction {
    Integer count = 0;
    ListState<Integer> countState;

    @Override
    public String map(WaterSensor ws) throws Exception {
        return "并行子任务:" + getRuntimeContext().getIndexOfThisSubtask() + "处理了" + ++count + "条数据";
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("~~~snapshotState~~~");
        countState.clear();
        countState.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("~~~initializeState~~~");
        ListStateDescriptor<Integer> listStateDescriptor
                = new ListStateDescriptor<Integer>("countState", Integer.class);
        countState = context.getOperatorStateStore().getListState(listStateDescriptor);

        if(context.isRestored()){
            Integer restorCount = countState.get().iterator().next();
            count = restorCount;
        }
    }
}
