package org.czlan.flinklearning._01_source;



import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author chenzhuanglan
 * @program Flink_Learning
 * @description DataStram-Source-基于文件
 * 本地/hdfs 文件/文件夹，压缩文件也可以，但是只能是gz文件
 * @date 2021/2/19 13:22
 */
public class SourceDem02_File {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<String> ds1 = env.readTextFile("/Users/chenzhuanglan/WorkSpace/Flink_Learning/Flink_Learning/flink_1.12.0/src/main/resources/log4j.properties");
        DataStream<String> ds2 = env.readTextFile("/Users/chenzhuanglan/WorkSpace/Flink_Learning/Flink_Learning/flink_1.12.0/src/main/resources");
        DataStream<String> ds3 = env.readTextFile("/Users/chenzhuanglan/WorkSpace/Flink_Learning/Flink_Learning/flink_1.12.0/src/main/resources/log4j.properties.gz");

        // todo 2.transformation

        // todo 3.sink
        ds1.print();
        ds2.print();
        ds3.print("压缩文件：");


        // todo 4.execute
        env.execute();
    }
}
