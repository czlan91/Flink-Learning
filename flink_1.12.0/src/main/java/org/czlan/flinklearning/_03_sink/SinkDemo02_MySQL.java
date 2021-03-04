package org.czlan.flinklearning._03_sink;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description DataStream-Sink-自定义sink
 *
 * JDBC 官方提供的有jar包
 * @date 2021/2/19 22:04
 */
public class SinkDemo02_MySQL {


    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<Student> studentDS = env.fromElements(new Student(null,"tom",18));
        // todo 2.transformation
        // todo 3.sink
        studentDS.addSink(new MySQLSink());

        // todo 4.execute
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Student{
        private Integer id;
        private String name;
        private Integer age;
    }

    public static class MySQLSink extends RichSinkFunction<Student> {


        private Connection conn = null;
        private PreparedStatement ps = null;
        private ResultSet rs = null;

        @Override
        public void invoke(Student value, Context context) throws Exception {
            // 设置？占位符参数值
            ps.setString(1,value.getName());
            ps.setInt(1,value.getAge());
            // 执行sql
            ps.executeUpdate();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "123456");
            String sql = "INSERT INTO t_student (id, name, age) VALUES (null , ?, ?);";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void close() throws Exception {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }

        }
    }
}
