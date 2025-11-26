package com.atguigu.day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author Felix
 * @date 2024/9/25
 * 该案例演示了持续查询
 */
public class Flink03_Query {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.将流转换为动态表
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
                ");");
        //TODO 3.从动态表中查询数据(持续查询)
        //3.1 SQL
        //executeSql返回值是TableResult---直接对结果进行输出
        //tableEnv.executeSql("select * from source where id=1").print();
        //sqlQuery返回值是Table对象    ---需要对对象再进行处理才能输出
        //Table table = tableEnv.sqlQuery("select * from source where id=1");
        //execute 直接得到表数据
        //table.execute().print();
        //createTemporaryView 将动态表对象注册到表执行环境中
        //tableEnv.createTemporaryView("xxx",table);
        //executeInsert将动态表对象中的结果插入到其他表中
        //table.executeInsert("xxxx");

        //3.2 API
        Table sourceTable = tableEnv.sqlQuery("select * from source");
        sourceTable
                .select($("id"),$("ts"),$("vc"))
                .where($("id").isEqual(1))
                .execute()
                .print();


    }
}
