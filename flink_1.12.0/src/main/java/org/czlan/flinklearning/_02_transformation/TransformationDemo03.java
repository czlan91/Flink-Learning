package org.czlan.flinklearning._02_transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description DataStream-Transformation-拆分和选择
 * split 和 select 在flink 1.12中已经过期并移出,现在使用 outPutTag 和 process来实现
 * @date 2021/2/19 17:38
 */
public class TransformationDemo03 {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<Integer> ds = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // todo 2.transformation
        OutputTag<Integer> oddTag = new OutputTag<>("奇数",TypeInformation.of(Integer.class));
        OutputTag<Integer> evenTag = new OutputTag<>("偶数",TypeInformation.of(Integer.class));

        SingleOutputStreamOperator<Integer> result = ds.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                // out 收集完了还是放在一起的， ctx可以将数据放到不同的OutputTag
                if (value % 2 == 0) {
                    ctx.output(evenTag, value);
                } else {
                    ctx.output(oddTag, value);
                }
            }
        });

        DataStream<Integer> oddResult = result.getSideOutput(oddTag);
        DataStream<Integer> evenResult = result.getSideOutput(evenTag);


        // todo 3.sink
        oddResult.print("奇数");
        evenResult.print("偶数");




        // todo 4.execute
        env.execute();
    }
}
