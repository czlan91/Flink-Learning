package org.czlan.flinklearning._02_transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description DataStream-Transformation-重平衡分区
 * 解决数据倾斜
 * @date 2021/2/19 17:38
 */
public class TransformationDemo04 {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<Long> longDS = env.fromSequence(0, 100);

        DataStream<Long> filterDS = longDS.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 10;
            }
        });

        // todo 2.transformation
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result1 = filterDS
                .map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Long value) throws Exception {
                        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();// 子任务id或分区编号

                        return Tuple2.of(subTaskId, 1);
                    }

                    // 按照 子任务id或分区编号 分组，统计每个子任务中有几个元素
                }).keyBy(t -> t.f0).sum(1);

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result2 = filterDS.rebalance()
                .map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Long value) throws Exception {
                        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();// 子任务id或分区编号

                        return Tuple2.of(subTaskId, 1);
                    }

                    // 按照 子任务id或分区编号 分组，统计每个子任务中有几个元素
                }).keyBy(t -> t.f0).sum(1);


        // todo 3.sink
        result1.print("result1");
        result2.print("result2");


        // todo 4.execute
        env.execute();
    }
}
