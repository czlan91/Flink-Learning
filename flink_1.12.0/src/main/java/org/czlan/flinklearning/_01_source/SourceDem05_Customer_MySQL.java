package org.czlan.flinklearning._01_source;


import lombok.*;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Random;

/**
 * @author chenzhuanglan
 * @program Flink_Learning
 * @description DataStram-Source-自定义数据源-MySQL
 *
 * @date 2021/2/19 13:22
 */
public class SourceDem05_Customer_MySQL {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<Student> studentrDS = env.addSource(new MySQLSource()).setParallelism(2);


        // todo 2.transformation


        // todo 3.sink
        studentrDS.print();


        // todo 4.execute
        env.execute();
    }


    /**
     * create table t_student(
     * id int(11) not null auto_increment,
     * name varchar(255) default null,
     * age int(11) default null,
     * primary key (id)
     * ) engine=InnoDB auto_increment=7 default charset=utf8;
     * <p>
     * Insert into t_student values('1','jack','18');
     * Insert into t_student values('2','tom','19');
     * Insert into t_student values('3','rose','20');
     * Insert into t_student values('4','tom','19');
     * Insert into t_student values('5','jack','18');
     * Insert into t_student values('6','rose','21');
     */


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

    public static class MySQLSource extends RichParallelSourceFunction<Student> {

        private Boolean flag = true;

        private Connection conn = null;
        private PreparedStatement ps = null;
        private ResultSet rs = null;

        // open 方法 只执行一次，适合开启资源，
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "123456");
            String sql = "select id,name,age from t_student";
            ps = conn.prepareStatement(sql);

        }

        // close方法，关闭资源
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

        // 执行并生成数据
        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                rs = ps.executeQuery();
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    int age = rs.getInt("age");
                    ctx.collect(new Student(id, name, age));
                }


                Thread.sleep(5000);
            }
        }

        // 执行 cancel命令时执行
        @Override
        public void cancel() {
            flag = false;
        }


    }
}
