package org.czlan.flinklearning._07_state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description 使用keyedState中的 valueSate获取流量中的最大值，实际开发中可以直接使用maxby。
 * @date 2021/2/22 18:10
 */
public class StateDemo01_KeyedState {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<Tuple2<String,Long>> tupleDS = env.fromElements(
                Tuple2.of("北京",1L),
                Tuple2.of("上海",2L),
                Tuple2.of("北京",6L),
                Tuple2.of("上海",8L),
                Tuple2.of("北京",3L),
                Tuple2.of("上海",4L)
        );

        // todo 2.transformation
        DataStream<Tuple2<String, Long>> result1 = tupleDS.keyBy(t -> t.f0).maxBy(1);

        DataStream<Tuple3<String, Long, Long>> result2 = tupleDS.keyBy(t -> t.f0).map(new RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>>() {
            // 1. 定义一个状态，用来存放最大值
            private ValueState<Long> maxValueState;

            // 2. 状态初始化
            @Override
            public void open(Configuration parameters) throws Exception {

                // 2.1 创建状态描述器
                ValueStateDescriptor stateDescriptor = new ValueStateDescriptor("maxValueSatate", Long.class);
                // 2.2 根据状态描述器获取/初始化状态
                maxValueState = getRuntimeContext().getState(stateDescriptor);
            }

            // 3. 使用状态
            @Override
            public Tuple3<String, Long, Long> map(Tuple2<String, Long> value) throws Exception {
                Long currentValue = value.f1;
                // 获取状态
                Long histroyValue = maxValueState.value();
                // 判断状态
                if (histroyValue == null || currentValue > histroyValue) {
                    histroyValue = currentValue;
                    maxValueState.update(histroyValue);
                    return Tuple3.of(value.f0, value.f1, histroyValue);
                } else {
                    maxValueState.update(histroyValue);
                    return Tuple3.of(value.f0, value.f1, histroyValue);
                }

            }
        });

        // todo 3.sink
        result1.print();
        result2.print();

        // todo 4.execute
        env.execute();
    }
}
