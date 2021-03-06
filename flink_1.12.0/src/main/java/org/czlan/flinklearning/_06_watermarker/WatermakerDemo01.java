package org.czlan.flinklearning._06_watermarker;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description 基于事件时间的窗口计算+watermarker解决一定程度额数据乱序/延迟问题
 * @date 2021/2/22 15:32
 */
public class WatermakerDemo01 {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<Order> orderDS = env.addSource(new SourceFunction<Order>() {
            private boolean flag = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(2);
                    int money = random.nextInt(101);
                    // 模拟随机延迟
                    long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;

                    ctx.collect(new Order(orderId, userId, money, eventTime));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                this.flag = false;
            }
        });


        // todo 2.transformation
        // 每隔5s，计算最近5s的数据求每个用户的订单总金额，基于事件时间进行窗口计算+ watermarker
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 在Flink 1.12.0新版本中默认就是eventTime

        // watermarker = 当前最大事件时间 - 最大允许的延迟时间或乱序时间
        SingleOutputStreamOperator<Order> orderDsWithWatermark = orderDS.assignTimestampsAndWatermarks(WatermarkStrategy
                // 指定 最大无序度， 最大允许的延迟时间
                .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 指定事件时间列
                .withTimestampAssigner((order, timestamp) -> order.getEventTime()));


        SingleOutputStreamOperator<Order> result = orderDsWithWatermark.keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("money");


        // todo 3.sink
        result.print();


        // todo 4.execute
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Order{
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long eventTime;
    }
}
