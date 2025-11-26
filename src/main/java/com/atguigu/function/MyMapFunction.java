package com.atguigu.function;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author Felix
 * @date 2024/9/13
 */
public class MyMapFunction implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor ws) throws Exception {
        return ws.getId();
    }
}
