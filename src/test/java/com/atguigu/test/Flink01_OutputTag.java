package com.atguigu.test;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.OutputTag;

/**
 * @author Felix
 * @date 2024/9/14
 */
public class Flink01_OutputTag {
    public static void main(String[] args) {
        //OutputTag<String> aaaa = new OutputTag<String>("aaaa"){};
        OutputTag<String> aaaa = new OutputTag<String>("aaaa", Types.STRING);
    }
}
