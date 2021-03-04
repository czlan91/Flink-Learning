package org.czlan.flinklearning._02_transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description DataStream-Transformation-其他分区
 *
 * dataStream.global()          全部发往第一个task
 * dataStream.broadcase()       广播
 * dataStream.forward()         上下游并发度一样时一对一发送
 * dataStream.shuffle()         随机均匀分配
 * dataStream.rebalance()       Round_Robin(轮流分配）
 * dataStream.recale()          Local Round-Robin (本地轮流分配）
 * dataStream.partitionCuston() 自定义单播
 * @date 2021/2/19 17:38
 */
public class TransformationDemo05 {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<String> linesDS = env.readTextFile("/Users/chenzhuanglan/WorkSpace/Flink_Learning/Flink_Learning/flink_1.12.0/src/main/resources/log4j.properties");
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS = linesDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] arr = value.split(" ");
                for (String word :
                        arr) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });



        // todo 2.transformation
        DataStream<Tuple2<String, Integer>> result1 = tupleDS.global();
        DataStream<Tuple2<String, Integer>> result2 = tupleDS.broadcast();
        DataStream<Tuple2<String, Integer>> result3 = tupleDS.forward();
        DataStream<Tuple2<String, Integer>> result4 = tupleDS.shuffle();
        DataStream<Tuple2<String, Integer>> result5 = tupleDS.rebalance();
        DataStream<Tuple2<String, Integer>> result6 = tupleDS.rescale();
        DataStream<Tuple2<String, Integer>> result7 = tupleDS.partitionCustom(new MyPartitioner(),t-> t.f0);


        // todo 3.sink
        result1.print("result1");
        result2.print("result2");
        result3.print("result3");
        result4.print("result4");
        result5.print("result5");
        result6.print("result6");
        result7.print("result7");


        // todo 4.execute
        env.execute();
    }

    // 自定义分区器
    public static class MyPartitioner implements Partitioner<String> {
        @Override
        public int partition(String key, int numPartitions) {
            // 自己的分区逻辑
            // xxxxxx
            return 0;
        }
    }


}
