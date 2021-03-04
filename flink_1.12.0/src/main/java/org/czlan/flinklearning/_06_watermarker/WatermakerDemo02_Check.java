package org.czlan.flinklearning._06_watermarker;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description 基于事件时间的窗口计算+watermarker解决一定程度额数据乱序/延迟问题
 * @date 2021/2/22 15:32
 */
public class WatermakerDemo02_Check {
    public static void main(String[] args) throws Exception {

        FastDateFormat df = FastDateFormat.getInstance("hh:mm:ss");

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
        /*SingleOutputStreamOperator<Order> orderDsWithWatermark = orderDS.assignTimestampsAndWatermarks(WatermarkStrategy
                // 指定 最大无序度， 最大允许的延迟时间
                .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 指定事件时间列
                .withTimestampAssigner((order, timestamp) -> order.getEventTime()));*/

        SingleOutputStreamOperator<Order> orderDsWithWatermark = orderDS.assignTimestampsAndWatermarks(
                new WatermarkStrategy<Order>() {
                    @Override
                    public WatermarkGenerator<Order> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Order>() {
                            private int userId = 0;
                            private long eventTime = 0L;
                            private final long outOfOrdernessMillis = 3000;
                            private long maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;

                            @Override
                            public void onEvent(Order event, long eventTimestamp, WatermarkOutput output) {
                                userId = event.userId;
                                eventTime = event.eventTime;
                                maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                // Watermarker = 当前最大事件时间 - 最大允许的延迟时间或乱序时间
                                Watermark watermark = new Watermark(maxTimestamp - outOfOrdernessMillis - 1);
                                System.out.println("key:" + userId + ",系统时间：" + df.format(System.currentTimeMillis()) + ",事件时间：" + df.format(eventTime) + ",水印时间：" + df.format(watermark.getTimestamp()));
                                output.emitWatermark(watermark);
                            }
                        };
                    }
                }.withTimestampAssigner((order, timestamp) -> order.getEventTime())
        );


        /*SingleOutputStreamOperator<Order> result = orderDsWithWatermark.keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("money");*/

        SingleOutputStreamOperator<String> result = orderDsWithWatermark.keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Order, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer key, TimeWindow window, Iterable<Order> input, Collector<String> out) throws Exception {
                        // 准备一个集合用来存放属于该窗口的数据的事件时间
                        List<String> eventTimeList = Lists.newArrayList();
                        for (Order order :
                                input) {
                            Long eventTime = order.eventTime;
                            eventTimeList.add(df.format(eventTime));

                        }
                        String outStr = String.format("key:%s,窗口开始结束：【%s～%s)，属于该窗口的事件时间：%s",
                                key.toString(), df.format(window.getStart()), df.format(window.getEnd()), eventTimeList);

                        out.collect(outStr);
                    }
                });


        // todo 3.sink
        result.print();


        // todo 4.execute
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long eventTime;
    }
}
