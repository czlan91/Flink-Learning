package org.czlan.flinklearning._03_sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description DataStream-Sink-给予控制台和文件
 * @date 2021/2/19 22:04
 */
public class SinkDemo01 {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<String> ds = env.readTextFile("/Users/chenzhuanglan/WorkSpace/Flink_Learning/Flink_Learning/flink_1.12.0/src/main/resources/log4j.properties");

        // todo 2.transformation
        // todo 3.sink
        ds.print();
        ds.print("输出标识");
        ds.printToErr();// 会在控制台 以红色输出
        ds.printToErr("输出标识");// 会在控制台 以红色输出

        // 当前并行度为1，则为文件，大于1，则为目录
        ds.writeAsText("data/output/result1").setParallelism(1);
        ds.writeAsText("data/output/result2").setParallelism(2);




        // todo 4.execute
        env.execute();
    }
}
