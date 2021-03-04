package org.czlan.flinklearning._01_source;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.UUID;

/**
 * @author chenzhuanglan
 * @program Flink_Learning
 * @description DataStram-Source-自定义数据源
 * 每隔一秒自动产生订单信息
 * @date 2021/2/19 13:22
 */
public class SourceDem04_Customer {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<Order> orderDS = env.addSource(new MyOrderSource()).setParallelism(2);


        // todo 2.transformation


        // todo 3.sink
        orderDS.print();


        // todo 4.execute
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Order {
        private String id;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }

    /**
     * FLink 自定义数据源分为：
     * SourceFunction：非并行数据源，并行度只能是1
     * RichSourceFunction：多功能非并行数据源，并行度只能是1
     * ParallelSourceFunction：并行数据源，并行度>=1
     * RichParallelSourceFunction：多功能并行数据源，并行度>=1
     *
     *
     * 多功能体现在 方法多，我们可以复写这些方法
     * 如果是 SourceFunction 和 RichSourceFunction ，则不能设置并行度 > 1，否则会报错。
     */
    public static class MyOrderSource extends RichParallelSourceFunction<Order> {

        private Boolean flag = true;

        // 执行并生成数据
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                String oid = UUID.randomUUID().toString();
                int userId = random.nextInt(3);
                int money = random.nextInt(101);
                long createTime = System.currentTimeMillis();
                ctx.collect(new Order(oid,userId,money,createTime));

                Thread.sleep(1000);
            }
        }

        // 执行 cancel命令时执行
        @Override
        public void cancel() {
            flag = false;
        }
    }
}
