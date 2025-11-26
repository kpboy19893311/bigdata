package com.atguigu.day11;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Felix
 * @date 2024/9/25
 * 该案例演示了将动态表的数据进行sink输出以及将动态表转换为流
 */
public class Flink04_Sink {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.从指定的数据源(流)读取数据创建动态表
        tableEnv.executeSql("CREATE TABLE source ( \n" +
                "    id string, \n" +
                "    ts BIGINT, \n" +
                "    vc INT \n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.length'='1', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ")");

        //TODO 3.对表进行查询
        //tableEnv.executeSql()
        //Table filterTable = tableEnv.sqlQuery("select * from source");

        //TODO 4.将查询结果(动态表)进行Sink输出
        //tableEnv.executeSql("\n" +
        //        "CREATE TABLE sink (\n" +
        //        "    id INT, \n" +
        //        "    ts BIGINT, \n" +
        //        "    vc INT\n" +
        //        ") WITH (\n" +
        //        "'connector' = 'print'\n" +
        //        ")");
        //tableEnv.createTemporaryView("filter_table",filterTable);
        //tableEnv.executeSql("insert into sink select * from filter_table");
        //filterTable.executeInsert("sink");
        //TODO 5.将查询结果(动态表)转换为流
        //DataStream<Row> ds = tableEnv.toDataStream(filterTable);

        Table filterTable = tableEnv.sqlQuery("select id,sum(vc) from source group by id");
        //DataStream<WaterSensor> ds = tableEnv.toDataStream(filterTable, WaterSensor.class);
        //DataStream<Row> ds = tableEnv.toDataStream(filterTable);
        DataStream<Row> ds = tableEnv.toChangelogStream(filterTable);
        ds.print();

        env.execute();



    }
}
