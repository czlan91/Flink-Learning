package org.czlan.flinklearning._04_connectors;

import lombok.*;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description flink 官方提供的jdbcSink
 * @date 2021/2/19 22:40
 */
public class JDBCDemo {

    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<Student> studentDS = env.fromElements(new Student(null, "tom", 18));
        // todo 2.transformation
        // todo 3.sink
        studentDS.addSink(JdbcSink.sink(
                "INSERT INTO t_student (id, name, age) VALUES (null , ?, ?);",
                (ps, value) -> {
                    ps.setString(1, value.getName());
                    ps.setInt(2, value.getAge());
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/bigdata")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));

        // todo 4.execute
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }


}
