package com.atguigu.function;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author Felix
 * @date 2024/9/13
 * 将字符串封装为WaterSensor对象
 */
public class WaterSensorMapFunction  implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String lineStr) throws Exception {
        String[] fieldArr = lineStr.split(",");
        return new WaterSensor(fieldArr[0], Long.valueOf(fieldArr[1]), Integer.valueOf(fieldArr[2]));
    }
}
