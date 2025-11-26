package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.WaterSensorMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author Felix
 * @date 2024/9/18
 *
 * 该案例演示了自定义Sink-将流中数据写到MySQL数据库
 */
public class Flink02_Sink_Custom {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.从指定的网络端口中读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);
        //TODO 3.对流中数据进行类型转换    String->WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        //TODO 4.将数据写到MySQL数据库
        wsDS.addSink(new MySinkFunction());
        //TODO 5.提交作业
        env.execute();
    }
}

class MySinkFunction extends RichSinkFunction<WaterSensor> {

    Connection conn ;
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("~~~open~~~");
        //注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        conn
                = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8", "root", "123456");
    }

    @Override
    public void close() throws Exception {
        System.out.println("~~~close~~~");
        conn.close();
    }

    @Override
    public void invoke(WaterSensor ws, Context context) throws Exception {
        //获取数据库操作对象
        String sql = "insert into ws values(?,?,?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, ws.getId());
        ps.setLong(2,ws.getTs());
        ps.setString(3,ws.getVc() + "");
        //执行SQL语句
        ps.execute();
        //释放资源
        ps.close();

    }
}
