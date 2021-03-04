package org.czlan.flinklearning._02_transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description DataStream-Transformation-合并和连接
 * 合并只能合并2个流，连接可以连接多个流。 connect后还需要做其他处理
 * @date 2021/2/19 17:38
 */
public class TransformationDemo02 {
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
        // union 能合并同类型
        DataStream<String> result1 = ds1.union(ds2);
        // union 不能合并不同类型
        // ds1.union(ds3)

        // connect 能合并同类型
        ConnectedStreams<String, String> result2 = ds1.connect(ds2);
        // connect 能合并不同类型
        ConnectedStreams<String, Long> result3 = ds1.connect(ds3);

        SingleOutputStreamOperator<String> result = result3.map(new CoMapFunction<String, Long, String>() {
            @Override
            public String map1(String value) throws Exception {
                return "String" + value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Long" + value;
            }
        });

        // todo 3.sink
        result1.print();

        // connect之后需要做其他处理，不能直接输出

        result.print();


        // todo 4.execute
        env.execute();
    }
}
