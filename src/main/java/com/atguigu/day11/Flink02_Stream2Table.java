package com.atguigu.day11;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Felix
 * @date 2024/9/25
 * 该案例演示了流到动态表的转换
 */
public class Flink02_Stream2Table {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.将流转换为动态表
        DataStreamSource<Integer> numDS = env.fromElements(1, 2, 3, 4);
        //2.1 api的方式-1    可以直降将表注册到表执行环境中
        //tableEnv.createTemporaryView("t_num",numDS);
        //tableEnv.executeSql("select * from t_num").print();
        //2.1 api的方式-2   得到的是一个java对象(已过时，推荐用上面的方式替代)
        Table numTable = tableEnv.fromDataStream(numDS,$("num"));
        //需要将表对象注册到表执行环境中
        //tableEnv.registerTable("num_table",numTable);
        //tableEnv.createTemporaryView("num_table",numTable);
        //tableEnv.executeSql("select * from num_table").print();

        //tableEnv.executeSql("select * from " + numTable).print();


        //2.2 执行sql语句
        tableEnv.executeSql("CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT \n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ")");
        tableEnv.executeSql("select * from source").print();

    }
}
