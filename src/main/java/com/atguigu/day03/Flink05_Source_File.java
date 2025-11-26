package com.atguigu.day03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/9/13
 * 该案例演示了源算子-从文件中读取数据
 */
public class Flink05_Source_File {
    public static void main(String[] args) throws Exception {
        //TODO 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 从文件中读取数据
        //DataStreamSource<String> ds = env.readTextFile("D:\\dev\\workspace\\bigdata-0422\\input\\words.txt");

        FileSource<String> source  = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("D:\\dev\\workspace\\bigdata-0422\\input\\words.txt")).build();

        //TODO 封装为流
        //flink1.12前
        //env.addSource()
        //从flink1.12
        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file_source");
        //TODO 打印输出
        ds.print();
        //TODO 提交作业
        env.execute();
    }
}
