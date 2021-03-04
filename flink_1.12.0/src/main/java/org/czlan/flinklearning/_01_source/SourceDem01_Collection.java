package org.czlan.flinklearning._01_source;



import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author chenzhuanglan
 * @program Flink_Learning
 * @description DataStram-Source-基于集合
 * env.fromElements(可变参数);
 * env.fromCollection(各种集合);
 * env.generateSequence(开始，结束);
 * env.fromSequence(开始，结束);
 * @date 2021/2/19 13:22
 */
public class SourceDem01_Collection {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<String> ds1 = env.fromElements("hadoop spark flink", "hadoop spark flink");
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("hadoop spark flink", "hadoop spark flink"));
        DataStream<Long> ds3 = env.generateSequence(1, 100);
        DataStream<Long> ds4 = env.fromSequence(1, 100);

        // todo 2.transformation

        // todo 3.sink
        ds1.print();
        ds2.print();
        ds3.print();
        ds4.print();

        // todo 4.execute
        env.execute();
    }
}
